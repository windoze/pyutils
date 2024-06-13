# A simple matrix
# This matrix is a list of lists
# Column and row numbers start with 1

import numbers
import sys

class SudokuException(Exception):
    pass

class Matrix(object):
    def __init__(self, cols, rows):
        self.cols = cols
        self.rows = rows
        # initialize matrix and fill with zeroes
        self.matrix = []
        for i in range(rows):
            ea_row = []
            for j in range(cols):
                ea_row.append(0)
            self.matrix.append(ea_row)
    def setitem(self, col, row, v):
        self.matrix[col-1][row-1] = v
    def getitem(self, col, row):
        return self.matrix[col-1][row-1]
    def __repr__(self):
        outStr = ""
        for i in range(self.rows):
            outStr += 'Row %s = %s\n' % (i+1, self.matrix[i])
        return outStr

class Cell(object):
    def __init__(self, v=None):
        self.candidates=set([1,2,3,4,5,6,7,8,9])
        self.value=v
        self.row=-1
        self.col=-1
    def setvalue(self, n):
        if n==None:
            return
        self.value=int(n)
        self.candidates=set()
    def has_value(self):
        return self.value!=None
    def has_candidate(self, n):
        return n in self.candidates
    def removecandidate(self, n):
        if n==None:
            return
        elif isinstance(n, set):
            self.candidates-=n
        elif isinstance(n, Cell):
            self.candidates-=set([n.value])
        else:
            self.candidates-=set([n])
        if len(self.candidates)==0 and self.value==None:
            raise SudokuException("Invalid cell state, row=%d, col=%d" % (self.row, self.col))
    def clone(self):
        v=Cell()
        v.candidates=set().union(self.candidates)
        v.value=self.value
        return v
    def is_single(self):
        return len(self.candidates)==1
    def __repr__(self):
        if self.value==None:
            return "Cell()"
        return "Cell(%d)" % self.value
    def __str__(self):
        if self.value==None:
            return '_'
        return str(self.value)

def check_single(l):
    for n in range(1,10):
        last_found=None
        count=0
        for i in range(len(l)):
            if l[i].has_candidate(n):
                count+=1
                if count>1:
                    # Candidate appears 2 or more times
                    last_found=None
                    break
                last_found=i
        if count==1:
            return (last_found, n)
    return (None, 0)

def check_double(l):
    # if one row has 2 cells, which have same candidates (x, y)
    # All other cells in the line will *not* be either x or y
    all_cand=[c.candidates for c in l if len(c.candidates)==2]
    all_cand.sort()
    for i in range(len(all_cand)):
        if i>0 and all_cand[i]==all_cand[i-1]:
            for cell in l:
                if (cell.candidates!=all_cand[i]) and (cell.candidates&all_cand[i]):
                    cell.removecandidate(all_cand[i])
                    return True
    return False
    
def verify_unit(l):
    # TODO:
    values=set()
    for c in l:
        if not c.has_value:
            return False
        if c.value in values:
            return False
        values.union(set([c.value]))
    return True

class Sudoku(object):
    def __init__(self):
        self.board=Matrix(9,9)
        for i in range(9):
            for j in range(9):
                self.board.setitem(i, j, Cell())
                self.getitem(i,j).row=i
                self.getitem(i,j).col=j
    def clone(self):
        v=Sudoku()
        for i in range(9):
            for j in range(9):
                if self.getitem(i,j).has_value():
                    v.setitem(i, j, self.getitem(i,j).value)
        return v
    def read_board(self, s):
        if isinstance(s, str):
            for row in range(9):
                for col in range(9):
                    c=s[row][col]
                    if c in "123456789":
                        self.setitem(row, col, int(c))
        else:
            row=0
            for line in s:
                if len(line)<9:
                    continue
                line=line[0:9]
                for col in range(9):
                    c=line[col]
                    if c in "123456789":
                        self.setitem(row, col, int(c))
                row+=1
                if row>=9:
                    break
    def solved(self):
        for i in range(9):
            for j in range(9):
                if not self.getitem(i, j).has_value():
                    return False
        return self.verify()
    def verify(self):
        for i in range(9):
            l=self.get_row(i)
            if not verify_unit(l):
                return False
        for j in range(9):
            l=self.get_col(j)
            if not verify_unit(l):
                return False
        for i in range(0, 9, 3):
            for j in range(0, 9, 3):
                l=self.get_block(i, j)
                if not verify_unit(l):
                    return False
        return True
    def remove_candidate_in_col(self, col, n):
        for i in range(9):
            self.getitem(i, col).removecandidate(n)
    def remove_candidate_in_row(self, row, n):
        for j in range(9):
            self.getitem(row, j).removecandidate(n)
    def remove_candidate_in_block(self, row, col, n):
        for c in self.get_block(row, col):
            c.removecandidate(n)
    def getitem(self, row, col):
        return self.board.getitem(row, col)
    def setitem(self, row, col, n):
        i=self.getitem(row, col)
        self.getitem(row, col).setvalue(n)
        self.remove_candidate_in_row(row,n)
        self.remove_candidate_in_col(col,n)
        self.remove_candidate_in_block(row,col,n)
    def fill_definitized(self):
        ret=0
        for i in range(9):
            for j in range(9):
                if len(self.getitem(i, j).candidates)==1:
                    self.setitem(i, j, list(self.getitem(i, j).candidates)[0])
                    ret+=1
        return ret
    def get_row(self, row):
        v=[]
        for j in range(9):
            v.append(self.getitem(row, j))
        return v
    def get_col(self, col):
        v=[]
        for i in range(9):
            v.append(self.getitem(i, col))
        return v
    def get_block(self, row, col):
        v=[]
        block_row=row//3*3
        block_col=col//3*3
        for i in range(block_row, block_row+3):
            for j in range(block_col, block_col+3):
                v.append(self.getitem(i, j))
        return v
    def find_single(self):
        ret=0
        # Check rows
        for i in range(9):
            c=check_single(self.get_row(i))
            if c[0]:
                self.setitem(i, c[0], c[1])
                ret+=1
        # Check columns
        for j in range(9):
            c=check_single(self.get_col(j))
            if c[0]:
                self.setitem(c[0], j, c[1])
                ret+=1
        # Check blocks
        for i in range(0, 9, 3):
            for j in range(0, 9, 3):
                c=check_single(self.get_block(i, j))
                if c[0]:
                    row=i+c[0]//3
                    col=j+c[0]%3
                    self.setitem(row, col, c[1])
                    ret+=1
        return ret
    def fill_possible(self):
        ret=0
        c=0
        while True:
            c=self.find_single()
            if c==0:
                break
            ret+=c
            while True:
                c=self.fill_definitized()
                if c==0:
                    break
                ret+=c
        return ret>0
    def find_double(self):
        ret=False
        for i in range(9):
            ret=ret or check_double(self.get_row(i))
        for j in range(9):
            ret=ret or check_double(self.get_col(i))
        for i in range(0, 9, 3):
            for j in range(0, 9, 3):
                ret=ret or check_double(self.get_block(i, j))
        return ret
    def __str__(self):
        s=""
        for i in range(9):
            for j in range(9):
                s+=str(self.getitem(i,j))
            s+="\n"
        return s
    def formatted(self):
        s="*|123456789\n-+---------\n"
        for i in range(9):
            s+="%d|" % (i+1)
            for j in range(9):
                s+=str(self.getitem(i,j))
            s+="\n"
        return s

def solve_simple(sudoku):
    while True:
        ret=sudoku.fill_possible()
        ret=ret or sudoku.find_double()
        if not ret:
            break;
def solve(sudoku):
    #print "Filling cells can be determined..."
    solve_simple(sudoku)
    if sudoku.solved():
        return
    # Nothing can be done easily, we're going to guess
    #print "Guessing..."
    guess(sudoku)
    if not sudoku.solved():
        print("Sudoku is incorrect")
def guess(sudoku):
    #TODO: Find a unfilled cell and begin guess
    for row in range(9):
        for col in range(9):
            c=sudoku.getitem(row, col)
            if not c.has_value():
                # find first empty cell, try every candidates
                for cand in c.candidates:
                    # Make a clone of current board
                    s1=sudoku.clone()
                    #print "Cloned sudoku", sudoku, s1
                    try:
                        #print "Try %d at (%d,%d)" % (cand, row, col)
                        s1.setitem(row, col, cand)
                        solve(s1)
                        if(s1.solved):
                            # Good guess
                            sudoku.setitem(row, col, cand)
                            solve(sudoku)
                            return
                    except SudokuException:
                        #print "Bad luck, (%d,%d) is not %d" % (row, col, cand)
                        continue
                #print "None candidate is correct"
                raise SudokuException("Wrong guess in previous cell")


def test_sudoku(s, result):
    a=Sudoku()
    a.read_board(s)
    solve(a)
    r=a.formatted()
    assert r==result
def test():
    # Easy
    src="""_389_5__1
6_743____
_________
_61___8_9
_8_3_4_1_
5_9___23_
_________
____536_2
2__1_748_
"""
    result="""*|123456789
-+---------
1|438925761
2|627431958
3|915786324
4|361572849
5|782394516
6|549618237
7|856249173
8|174853692
9|293167485
"""
    test_sudoku(src.split(), result)

    src="""___4_592_
_6__7____
39______1
2_____1_7
1_97_36_2
6_3_____8
9______65
____9__7_
_243_7___
"""
    result="""*|123456789
-+---------
1|781435926
2|462179583
3|395628741
4|258946137
5|149783652
6|673251498
7|937814265
8|816592374
9|524367819
"""
    test_sudoku(src.split(), result)

    # Expert
    src="""4___35___
__872___4
_63_____7
1__57___3
_5_____9_
6___98__5
9_____23_
3___847__
___36___9
"""
    result="""*|123456789
-+---------
1|479635821
2|518729364
3|263841957
4|192576483
5|857413692
6|634298175
7|946157238
8|325984716
9|781362549
"""
    test_sudoku(src.split(), result)

    src="""15__286__
8_9_1___2
4_2__7___
__6__2___
__3_4_2__
___5__7__
___2__3_7
2___3_1_6
__517__28
"""
    result="""*|123456789
-+---------
1|157328694
2|839614572
3|462957813
4|716892435
5|583741269
6|924563781
7|691285347
8|278439156
9|345176928
"""
    test_sudoku(src.split(), result)

    # Test
    src="""1__2__3__
_2__3__4_
__3__4__5
6__4__5__
_7__5__6_
__8__6__7
8__9__7__
_9__1__8_
__1__2__9
"""
    result="""*|123456789
-+---------
1|145298376
2|726531948
3|983764125
4|619487532
5|274359861
6|358126497
7|862945713
8|597613284
9|431872659
"""
    test_sudoku(src.split(), result)
    print("all tests passed")

if __name__=="__main__":
    if len(sys.argv)>1:
        s=open(sys.argv[1])
        print("Reading...")
        a=Sudoku()
        a.read_board(s)
        print(a.formatted())
        solve(a)
        print(a.formatted())
        print(a.solved())
    else:
        test()