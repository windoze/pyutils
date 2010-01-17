#!/usr/bin/env python
# encoding: utf-8
"""
worker.py

Created by Chen Xu on 2009-11-25.
Copyright (c) 2009 eBay Inc. All rights reserved.
"""

from __future__ import with_statement
import threading
import time
#from collections import Callable, defaultdict
from collections import defaultdict
from queue import *
import logging

def is_callable(v):
    return '__call__' in v.__dict__

__all__ = [
    'Closed',
    'Discard',
    'Worker',
    'WorkerPool',
    'Workflow',
    'WorkflowIterator',
    'is_worker_thread',
    'current_worker',
    'current_worker_pool',
    'current_workflow',
    'output',
    'rendezvous',
    'statistics',
]

def vdebug(msg, *args, **kwargs):
    """Verbose debug log"""
    return logging.log(5, msg, *args, **kwargs)

def vvdebug(msg, *args, **kwargs):
    """Very verbose debug log"""
    return logging.log(3, msg, *args, **kwargs)

worker_thread_data = threading.local()

def is_worker_thread():
    """Check if the current thread is a worker thread"""
    try:
        return worker_thread_data.is_worker_thread
    except AttributeError:
        return False

def current_worker():
    """Return the current worker"""
    try:
        return worker_thread_data.worker
    except AttributeError:
        return None

def current_worker_pool():
    """Return the current worker pool"""
    try:
        return worker_thread_data.pool
    except AttributeError:
        return None

def current_workflow():
    """Return the current workflow"""
    try:
        return current_worker_pool.workflow
    except AttributeError:
        return None

def output(d):
    """Output data to next worker pool"""
    try:
        current_worker().output(d)
    except AttributeError:
        pass    

def rendezvous(leader_action=None):
    """Rendezvous with other workers belong to same worker pool.
    Will be blocked until all workers run into the same point of code
    If there is a leader action, it'll called and rendezvous again
    """
    worker=current_worker()
    pool=current_worker_pool()
    if (not is_worker_thread()) or (not worker) or (not pool) or (not worker.isopen) or (not pool.isopen) or (worker.index<0):
        logging.warning("rendezvous can only be called from activated worker thread")
        return
    pool.barrier.wait()
    # Remember, all workers in same worker pool are exceuting same code, that means
    # there is no any difference to let anyone to do the action, we just need to guarantee
    # the action is executed only once.
    if leader_action and worker.index==0:
        leader_action()
        pool.barrier.wait()

def statistics(target):
    """Access worker statistics data
    taget is a function to read and/or write a dict
    This function locks data before calling target
    Statistics data can be read/written without locking
    """
    worker=current_worker()
    if (not is_worker_thread()) or (not worker) or (not worker.isopen) or (worker.index<0):
        logging.warning("statistics can only be called from activated worker thread")
        return
    with worker.statistics_lock:
        return target(worker.statistics_data)

class Discard(Exception):
    """Exception raised when woker discards the incoming data"""
    # TODO: do we need a trash bin to store all discarded data?

class Cache(object):
    def __init__(self, d):
        self.d=d

class Worker(object):
    """Worker executes within a thread that belongs to a worker pool
    You shouldn't to create instance directly, use worker pool instead
    Worker pool will create worker instance for each thread and set
    some useful properties
    """
    def __init__(self, pool, name):
        self.name=name
        self.pool=pool
        self.index=-1
        self.target=None
        self.generator=None
        self.isopen=False
        self.data=None
        self.batch_size=0
        self.batch_repo=[]
        self.batch_timeout=0
        self.batch_target=None
        self.statistics_lock=threading.Lock()
        self.statistics_data=defaultdict(lambda:0)

    def __enter__(self):
        """Context management protocol.  Returns self."""
        self.open()
        return self
    
    def __exit__(self, exc_type, exc_value, traceback):
        """Context management protocol.  Calls close()"""
        self.close()

    def __call__(self, d):
        """Calling worker returns 3 possible values
        1. None, means the data is cached for later process
        2. Some other d, means data is processed, and should be output
        3. raise a Discard exception, means input data is discarded
        For cases 1 and 3, worker pool will call queue.task_done()
        """
        try:
            with self.statistics_lock:
                self.statistics_data['input']+=1
            if self.batch_size<=1:
                try:
                    self.output(self._work(d))
                except Discard:
                   with self.statistics_lock:
                       self.statistics_data['discarded']+=1
                except Exception, e:
                   logging.exception("Caught exception, Data discarded")
                   with self.statistics_lock:
                       self.statistics_data['discarded']+=1
                finally:
                   self.task_done()
            else:
                self.batch_repo.append(d)
                if len(self.batch_repo)>=self.batch_size:
                    self._batch_work()
        except:
            pass

    def open(self):
        if self.isopen:
            vdebug("Worker already opened")
            return
        vdebug("Worker started")
        if self.generator:
            try:
                self.isopen=True
                vdebug("Generator start generating")
                for d in self.generator:
                    vdebug("Generated %s", d)
                    self.output(d)
            except Exception, e:
                logging.exception(e)
            finally:
                vdebug("Generator stop generating")
                self.isopen=False
                raise Closed
        self.isopen=True

    def close(self):
        self.isopen=False
        vdebug("Worker stopped")

    def exception(self, e):
        for n in range(self.pop_count):
            self.task_done()
        logging.exception("Got exception")

    def output(self, d):
        if isinstance(d, Discard):
            raise d
        self.pool.output(d)
        with self.statistics_lock:
            self.statistics_data['output']+=1

    def task_done(self):
        try:
            self.pool.task_done()
            with self.statistics_lock:
                self.statistics_data['processed']+=1
        except:
            pass

    def _work(self, d):
        vdebug("Working on input data :%s" % d)
        if self.target:
            vdebug("Calling target on input data %s" % d)
            return self.target(d)
        elif hasattr(self, "work"):
            vdebug("Calling work() on input data %s" % d)
            return self.work(d)
        #elif isinstance(d, Callable):
        elif callable(d):
            vdebug("Calling callable input data %s" % d)
            return d()
        else:
            vdebug("Discarding input data %s" % d)
            raise Discard(d)
    
    def _batch_work(self):
        if len(self.batch_repo)<=0:
            return
        ret=[None] * len(self.batch_repo)
        try:
            vdebug('Doing batch work')
            if self.batch_target:
                vdebug("Calling batch_target on batch")
                ret=self.batch_target(self.batch_repo)
                for i in range(len(self.batch_repo)):
                    try:
                        self.output(ret[i])
                    finally:
                        self.task_done()
            else:
                vdebug("Working on batch data individually")
                for d in self.batch_repo:
                    try:
                        self.output(self._work(d))
                    except Exception, e:
                        logging.exception("Data discarded")
                        with self.statistics_lock:
                            self.statistics_data['discarded']+=1
                    finally:
                        self.task_done()
        finally:
            vdebug("Batch processed")
            self.batch_repo=[]
    
    def _state_changed(self, state):
        if hasattr(self.target, state):
            vdebug("Calling target's state handler %s" % state)
            getattr(self.target, state)(self.target)
        if hasattr(self.batch_target, state):
            vdebug("Calling batch_target's state handler %s" % state)
            getattr(self.batch_target, state)(self.batch_target)
        elif hasattr(self, state):
            vdebug("Calling worker's state handler %s" % state)
            getattr(self, state)(self)
    
class _WorkerTimer(object):
    def __init__(self, worker):
        self.worker=worker
        self.start_time=0

    def __enter__(self):
        self.start_time=time.time()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        try:
            t=time.time()
            with self.worker.statistics_lock:
                # Make sure time is float
                n=1.0*self.worker.statistics_data['time']
                n+=(t-self.start_time)
                self.worker.statistics_data['time'] =n
            self.start_time=0
        except Exception, e:
            logging.exception("Error in WorkerTimer", e)

class _WorkerThreadDataGuard(object):
    def __init__(self, pool, worker):
        self.pool=pool
        self.worker=worker

    def __enter__(self):
        worker_thread_data.is_worker_thread=True
        worker_thread_data.pool=self.pool
        worker_thread_data.worker=self.worker
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        del worker_thread_data.is_worker_thread
        del worker_thread_data.pool
        del worker_thread_data.worker

class WorkerPool(object):
    """Worker pool manages a set of worker threads, each thread creates a worker instance
    and calles it's method to do real job.
    There are 3 types of worker pool, generator, filter, and batch filter.
    Generator doesn't need input, it generates data and put them into next
    Filter reads input data, processes and then writes result to the next
    Batch filter behaves like a filter, but it does job in batches, that means
    it only do real job when there is a number of input data available or the batch is timed out
    These 3 types are exclusive, generator > batch filer > filter
    NOTE: the generator/target/batch_target are used as a factory, they take worker index
    as the parameter, so basically you need to supply a class, if a function is needed,
    you can use something like (lambda x: your_func) as a factory
    """
    def __init__(self, WorkerFactory=Worker, threads=1, max_items=0, name="<AnonymouseWorkerPool>", target=None, generator=None, batch_size=0, batch_timeout=1, batch_target=None, **kwargs):
        self.name=name
        self.queue=Queue(max_items)
        self.threads=[]
        self.workers=[]
        self.worker_factory=WorkerFactory
        self.batch_size=int(batch_size)
        self.batch_timeout=float(batch_timeout)
        self.thread_num=int(threads)
        if self.thread_num<1:
            self.thread_num=1
        self.isopen=False
        self.next=None
        self.prev=None
        self.target=target
        self.generator=generator
        self.batch_target=batch_target
        self.data=None
        self.open_time=0
        self.statistics_data=defaultdict(lambda:0)
        self.barrier=Barrier(self.thread_num)
        self.kwargs=kwargs
    
    def open(self):
        if self.isopen:
            vdebug("Worker pool %s already started" % self.name)
            return True
        logging.debug("Worker pool %s started" % self.name)
        self.queue.open()
        self.workers=[None] * self.thread_num
        for i in range(self.thread_num):
            vdebug("Creating worker[%d]" % i)
            self.workers[i]=self.worker_factory(self, "%s(%d)" % (self.name, i))
            self._setup_worker(self.workers[i], i)
        """
        self.threads=[None] * self.thread_num
        for i in range(self.thread_num):
            vdebug("Creating worker thread[%d]" % i)
            self.threads[i]=threading.Thread(name=self.workers[i].name, target=lambda : self._worker_thread_func(self.workers[i]))
            vdebug("Starting worker thread[%d]" % i)
        """
        class _Starter:
            def __init__(self, pool, worker):
                self.pool=pool
                self.worker=worker
            def __call__(self):
                self.pool._worker_thread_func(self.worker)
        self.threads=[]
        for w in self.workers:
            vdebug("Creating worker thread for worker %s" % w.name)
            t=threading.Thread(name=w.name, target=_Starter(self, w))
            vdebug("Starting worker thread for worker %s" % w.name)
            t.start()
            self.threads.append(t)
        self.isopen=True
        self.open_time=time.time()
    
    def close(self, and_join=False, and_join_threads=False):
        if not self.isopen:
            return
        logging.debug("Closing queue")
        self.queue.close(and_join)
        if and_join_threads:
            logging.debug("Joining worker threads")
            self.join_threads()
        logging.debug("Worker pool %s closed" % self.name)
        self.isopen=False

    def abort(self, and_join=False, and_join_threads=False):
        if not self.isopen:
            return
        vdebug("Aborting queue")
        self.queue.abort(and_join)
        if and_join_threads:
            vdebug("Joining worker threads")
            self.join_threads()
        vdebug("Worker pool %s aborted" % self.name)
        self.isopen=False

    def put(self, d, timeout=None):
        self.queue.put(d, timeout!=0, timeout)

    def flush(self, and_join=True):
        self.set_state("flush")
        if and_join:
            self.join()
    
    def set_state(self, state):
        if not self.isopen:
            vdebug("Setting state %s for closed worker pool %s" % (state, self.name))
            return
        if not is_worker_thread():
            vdebug("Setting worker pool %s state to %s" % (self.name, state))
            for w in self.workers:
                with w._state_callback_lock:
                    vdebug("Setting worker %s state to %s" % (w.name, state))
                    w._state_callback=state
        else:
            logging.warning("Calling set_state from worker threads, ignoring")

    def join(self):
        self.queue.join()
        
    def join_threads(self):
        for t in self.threads:
            vdebug("Joining thread %s" % t)
            t.join()
            vdebug("Thread %s joined" % t)

    def output(self, d):
        if self.next:
            try:
                self.next.put(d)
            except Closed:
                logging.exception("Put %s into a closed queue" % d)
                # We needn't to raise exception here
                raise Discard(d)
            except Exception, e:
                logging.exception(e)
                raise Discard(d)
        else:
            vdebug("No next worker, discarding data %s" % d)
            # We needn't to raise exception here
            #raise Discard(d)
    
    def task_done(self):
        try:
            self.queue.task_done()
            vdebug("Input task done")
        except:
            pass

    def _setup_worker(self, worker, index):
        worker.index=index
        worker.pop_count=0
        worker.batch_size=self.batch_size
        worker.batch_timeout=self.batch_timeout
        worker.target=self.target
        worker.batch_target=self.batch_target
        if self.target:
            worker.target=self.target(index=index, **self.kwargs)
        else:
            worker.target=None
        if self.batch_target:
            worker.batch_target=self.batch_target(index=index, **self.kwargs)
        else:
            worker.batch_target=None
        if self.generator:
            worker.generator=self.generator(index=index, **self.kwargs)
        else:
            worker.generator=None
        # HACK: We add a set of attributes into worker
        worker._state_callback=False
        worker._state_callback_lock=threading.Lock()

    def _worker_thread_func(self, worker):
        try:
            vdebug("Worker thread started")
            with _WorkerThreadDataGuard(self, worker):
                try:
                    worker.open()
                    try:
                        self._work(worker)
                    except Exception, e:
                        for n in range(worker.pop_count):
                            self.queue.task_done()
                        worker.exception(e)
                        self.abort()
                except Closed:
                    vdebug("Worker closed in open()")
                    pass
                except Exception, e:
                    logging.exception("Exception occured in worker exception handler", e)
                    self.abort()
                finally:
                    vdebug("Closing worker")
                    worker.close()
        except Exception, e:
            logging.warning("Ignoring unhandled exception", e)
            pass

    def _work(self, worker):
        def _pop_timeout(worker):
            if worker.batch_size>1 and worker.batch_timeout>0:
                # This worker is in batch mode and has a batch timeout
                if time.time()-worker.batch_idle_start >= worker.batch_timeout:
                    # Batch timed out, doing batch work
                    worker._batch_work()
                    # Reset batch timer
                    worker.batch_idle_start=time.time()
            with worker._state_callback_lock:
                if worker._state_callback:
                    vdebug("State changed to %s" % worker._state_callback)
                    worker._state_changed(worker._state_callback)
                    worker._state_callback=None;
                    vdebug("State changed handled and cleared")
        worker.batch_idle_start=time.time()
        # Check flushing flag every 10ms
        for d in QueueIterator(self.queue, True, 0.01, _pop_timeout, worker):
            worker.pop_count+=1
            with _WorkerTimer(worker):
                worker(d)
                worker.pop_count-=1
            worker.batch_idle_start=time.time()
        # Input queue closed, handle remain data
        if worker.batch_size>1:
            worker._batch_work()
        vdebug("Input queue closed")

    def statistics(self):
        for w in self.workers:
            with w.statistics_lock:
                for k,v in w.statistics_data.items():
                    self.statistics_data[k]+=v
                self.statistics_data[w.name]=w.statistics_data.copy()
                w.statistics_data.clear()
        self.statistics_data['threads']=self.thread_num
        return self.statistics_data

class WorkflowIterator(object):
    def __init__(self, workflow, isreversed=False):
        self.isreversed=isreversed
        if isreversed:
            self.current=workflow.tail
        else:
            self.current=workflow.head
    
    def __iter__(self):
        return self
    
    def next(self):
        ret=self.current
        if self.current:
            if self.isreversed:
                self.current=self.current.prev
            else:
                self.current=self.current.next
        if ret:
            return ret
        else:
            raise StopIteration

class Workflow(object):
    def __init__(self, name="<AnonymousWorkflow>", max_items=0):
        self.name=name
        self.max_items=max_items
        self.head=None
        self.tail=None
        self.length=0
        self.isopen=False
        self.workers=[]
        self.worker_map={}

    def append(self, w):
        # Append workflow name as prefix
        w.name="%s:%s" % (self.name, w.name)
        w.workflow=self
        if self.tail is None:
            vdebug("Adding the first worker pool %s" % w.name)
            self.head=w
            self.tail=w
            w.next=None
            w.prev=None
        else:
            vdebug("Appending worker pool %s at the next of %s" % (w.name, self.tail.name))
            self.tail.next=w
            w.prev=self.tail
            w.next=None
            self.tail=w
        self.workers.append(w)
        self.worker_map[w.name]=w
        self.length+=1

    def open(self):
        if self.isopen:
            logging.debug("Workflow %s already opened" % self.name)
            return
        # Open worker pools in reversed order
        logging.debug("Opening workflow %s" % self.name)
        for w in reversed(self):
            vdebug("Opening worker pool %s" % w.name)
            w.open()
            vdebug("Worker pool %s opened" % w.name)
        self.isopen=True
        logging.debug("Workflow %s opened" % self.name)

    def close(self):
        if not self.isopen:
            logging.debug("Workflow %s already closed" % self.name)
            return
        logging.debug("Closing workflow %s" % self.name)
        for w in self:
            vdebug("Closing worker pool %s" % w.name)
            w.close(False, True)
            vdebug("Worker pool %s closed" % w.name)
        self.isopen=False
        logging.debug("Workflow %s closed" % self.name)

    def abort(self):
        if not self.isopen:
            logging.debug("Workflow %s already closed" % self.name)
            return
        logging.debug("Aborting workflow %s" % self.name)
        for w in self:
            vdebug("Aborting worker pool %s" % w.name)
            w.abort(False, True)
            vdebug("Worker pool %s aborted" % w.name)
        self.isopen=false
        logging.debug("Workflow %s aborted" % self.name)
    
    def set_state(self, state):
        logging.debug("Setting workflow %s state to %s" % (self.name, state))
        for w in self:
            vdebug("Setting worker pool %s state to %s" % (w.name, state))
            w.set_state(state)
            vdebug("State of worker pool %s is set to %s" % (w.name, state))
        
    def put(self, d):
        self.head.put(d)

    def flush(self, and_join=True):
        logging.debug("Flushing workflow %s" % self.name)
        for w in self:
            vdebug("Flushing worker pool %s" % w.name)
            w.flush(and_join)
            vdebug("Worker pool %s flushed" % w.name)

    def join(self):
        logging.debug("Joining workflow %s" % self.name)
        for w in self:
            vdebug("Joining worker pool %s" % w.name)
            w.join()
            vdebug("Worker pool %s joined" % w.name)

    def statistics(self):
        ret={}
        for w in self:
            ret[w.name]=w.statistics().copy()
        return ret

    def __iter__(self):
        return WorkflowIterator(self)

    def __reversed__(self):
        return WorkflowIterator(self, True)
    
    def __len__(self):
        return self.length

    def __del__(self):
        self.abort()

    def __getitem__(self, k):
        if isinstance(k, basestring):
            return self.worker_map[k]
        return self.workers[k]
