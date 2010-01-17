"""A multi-producer, multi-consumer queue."""

from __future__ import with_statement
from time import time as _time
from collections import deque
import heapq
import logging
import threading

__all__ = ['Empty', 'Full', 'Closed', 'Barrier', 'QueueIterator', 'Queue', 'PriorityQueue', 'LifoQueue']

class Empty(Exception):
    "Exception raised by Queue.get(block=0)/get_nowait()."
    pass

class Full(Exception):
    "Exception raised by Queue.put(block=0)/put_nowait()."
    pass
    
class Closed(Exception):
    "Exception raised by Queue.put()/put_nowait()/get()/get_nowait()."
    pass

class Barrier:
    def __init__(self, threshold):
        self.lock=threading.Lock()
        self.cond=threading.Condition(self.lock)
        self.total=threshold
        self.current=threshold
        self.generation=0

    def wait(self):
        with self.lock:
            gen=self.generation
            self.current-=1
            if self.current==0:
                self.generation+=1
                self.current=total
                self.cond.notify_all()
            while gen==self.generation:
                self.cond.wait()

class QueueIterator:
    def __init__(self, q, block=True, timeout=None, timeout_target=None, target_arg=None):
        self.queue=q
        self.block=block
        self.timeout=timeout
        self.timeout_target=timeout_target
        self.target_arg=target_arg

    def __iter__(self):
        return self

    def next(self):
        ret=None
        while ret is None:
            try:
                ret=self.queue.get(self.block, self.timeout)
            except Empty:
                if self.timeout_target:
                    if self.timeout_target(self.target_arg)==False:
                        raise StopIteration
            except Closed:
                raise StopIteration
        return ret

class Queue:
    """Create a queue object with a given maximum size.

    If maxsize is <= 0, the queue size is infinite.
    """
    def __init__(self, maxsize=0, auto_open=True):
        try:
            import threading
        except ImportError:
            import dummy_threading as threading
        self.maxsize = maxsize
        self._init(maxsize)
        # mutex must be held whenever the queue is mutating.  All methods
        # that acquire mutex must release it before returning.  mutex
        # is shared between the three conditions, so acquiring and
        # releasing the conditions also acquires and releases mutex.
        self.mutex = threading.Lock()
        # Notify not_empty whenever an item is added to the queue; a
        # thread waiting to get is notified then.
        self.not_empty = threading.Condition(self.mutex)
        # Notify not_full whenever an item is removed from the queue;
        # a thread waiting to put is notified then.
        self.not_full = threading.Condition(self.mutex)
        # Notify all_tasks_done whenever the number of unfinished tasks
        # drops to zero; thread waiting to join() is notified to resume
        self.all_tasks_done = threading.Condition(self.mutex)
        self.unfinished_tasks = 0

        self.isopen = False
    
        if auto_open:
            self.open()

    def open(self):
        with self:
            if self.isopen:
                return
            self.isopen = True;

    def close(self, and_join=False):
        with self:
            if not self.isopen:
                return
            self.isopen = False
            self.not_full.notify()
            self.not_empty.notify()
            self.all_tasks_done.notify()
        if and_join:
            self.join()

    def abort(self, and_join=False):
        """Close queue and abandon all data
        """
        with self:
            if not self.isopen:
                return
            self.isopen = False
            self._clear()
            self.not_full.notify()
            self.not_empty.notify()
            self.all_tasks_done.notify()
        if and_join:
            self.join()

    def task_done(self):
        """Indicate that a formerly enqueued task is complete.

        Used by Queue consumer threads.  For each get() used to fetch a task,
        a subsequent call to task_done() tells the queue that the processing
        on the task is complete.

        If a join() is currently blocking, it will resume when all items
        have been processed (meaning that a task_done() call was received
        for every item that had been put() into the queue).

        Raises a ValueError if called more times than there were items
        placed in the queue.
        """
        with self.all_tasks_done:
            unfinished = self.unfinished_tasks - 1
            if unfinished <= 0:
                if unfinished < 0:
                    raise ValueError('task_done() called too many times')
                self.all_tasks_done.notify_all()
            self.unfinished_tasks = unfinished

    def join(self):
        """Blocks until all items in the Queue have been gotten and processed.

        The count of unfinished tasks goes up whenever an item is added to the
        queue. The count goes down whenever a consumer thread calls task_done()
        to indicate the item was retrieved and all work on it is complete.

        When the count of unfinished tasks drops to zero, join() unblocks.
        """
        with self.all_tasks_done:
            while self.unfinished_tasks:
                self.all_tasks_done.wait()

    def qsize(self):
        """Return the approximate size of the queue (not reliable!)."""
        with self.mutex:
            n = self._qsize()
            return n

    def empty(self):
        """Return True if the queue is empty, False otherwise (not reliable!)."""
        with self.mutex:
            n = not self._qsize()
            return n

    def full(self):
        """Return True if the queue is full, False otherwise (not reliable!)."""
        with self.mutex:
            n = 0 < self.maxsize == self._qsize()
            return n

    def put(self, item, block=True, timeout=None):
        """Put an item into the queue.

        If optional args 'block' is true and 'timeout' is None (the default),
        block if necessary until a free slot is available. If 'timeout' is
        a positive number, it blocks at most 'timeout' seconds and raises
        the Full exception if no free slot was available within that time.
        Otherwise ('block' is false), put an item on the queue if a free slot
        is immediately available, else raise the Full exception ('timeout'
        is ignored in that case).
        """
        if timeout and timeout < 0:
            raise ValueError("'timeout' must be a positive number")

        with self:
            if not self.isopen:
                raise Closed

        with self.not_full:
            if self.maxsize > 0:
                if not block:
                    if self._qsize() == self.maxsize:
                        raise Full
                elif timeout is None:
                    while self._qsize() == self.maxsize and self.isopen:
                        self.not_full.wait()
                    if not self.isopen:
                        raise Closed
                else:
                    endtime = _time() + timeout
                    while self._qsize() == self.maxsize and self.isopen:
                        remaining = endtime - _time()
                        if remaining <= 0.0:
                            raise Full
                        self.not_full.wait(remaining)
                    if not self.isopen:
                        raise Closed
            self._put(item)
            self.unfinished_tasks += 1
            self.not_empty.notify()

    def put_nowait(self, item):
        """Put an item into the queue without blocking.

        Only enqueue the item if a free slot is immediately available.
        Otherwise raise the Full exception.
        """
        return self.put(item, False)

    def get(self, block=True, timeout=None):
        """Remove and return an item from the queue.

        If optional args 'block' is true and 'timeout' is None (the default),
        block if necessary until an item is available. If 'timeout' is
        a positive number, it blocks at most 'timeout' seconds and raises
        the Empty exception if no item was available within that time.
        Otherwise ('block' is false), return an item if one is immediately
        available, else raise the Empty exception ('timeout' is ignored
        in that case).
        """
        with self.not_empty:
            if not block:
                if not self._qsize():
                    raise Empty
            elif timeout is None:
                while not self._qsize() and self.isopen:
                    self.not_empty.wait()
                    if not self._qsize() and not self.isopen:
                        raise Closed
                if not self._qsize() and not self.isopen:
                    raise Closed
            elif timeout < 0:
                raise ValueError("'timeout' must be a positive number")
            else:
                endtime = _time() + timeout
                while not self._qsize() and self.isopen:
                    remaining = endtime - _time()
                    if remaining <= 0.0:
                        raise Empty
                    self.not_empty.wait(remaining)
                    if not self._qsize() and not self.isopen:
                        raise Closed
                if not self._qsize() and not self.isopen:
                    raise Closed
            item = self._get()
            self.not_full.notify()
            return item

    def get_nowait(self):
        """Remove and return an item from the queue without blocking.

        Only get an item if one is immediately available. Otherwise
        raise the Empty exception.
        """
        return self.get(False)

    def __enter__(self):
        return self.mutex.__enter__()

    def __exit__(self, exc_type, exc_value, traceback):
        self.mutex.__exit__(exc_type, exc_value, traceback)

    def __iter__(self):
        return QueueIterator(self)

    def __del__(self):
        self.abort(True)

    def make_iter(self, block=True, timeout=None, timeout_target=None):
        return QueueIterator(self, block, timeout, timeout_target)

    # Override these methods to implement other queue organizations
    # (e.g. stack or priority queue).
    # These will only be called with appropriate locks held

    # Initialize the queue representation
    def _init(self, maxsize):
        self.queue = deque()

    def _qsize(self, len=len):
        return len(self.queue)

    # Put a new item in the queue
    def _put(self, item):
        self.queue.append(item)

    # Get an item from the queue
    def _get(self):
        return self.queue.popleft()
    
    # Get an item from the queue
    def _clear(self):
        pass

class PriorityQueue(Queue):
    '''Variant of Queue that retrieves open entries in priority order (lowest first).

    Entries are typically tuples of the form:  (priority number, data).
    '''

    def _init(self, maxsize):
        self.queue = []

    def _qsize(self, len=len):
        return len(self.queue)

    def _put(self, item, heappush=heapq.heappush):
        heappush(self.queue, item)

    def _get(self, heappop=heapq.heappop):
        return heappop(self.queue)

    def _clear(self):
        pass


class LifoQueue(Queue):
    '''Variant of Queue that retrieves most recently added entries first.'''

    def _init(self, maxsize):
        self.queue = []

    def _qsize(self, len=len):
        return len(self.queue)

    def _put(self, item):
        self.queue.append(item)

    def _get(self):
        return self.queue.pop()

    def _clear(self):
        pass
