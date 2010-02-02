#!/usr/vbin/env python

import sys
import subprocess
from optparse import OptionParser

def start_cmd(cmd, out, err):
    return subprocess.Popen(cmd, stdin=subprocess.PIPE, stdout=out, stderr=err, shell=True)

def start_cmds(cmds):
    ret=[]
    n=1
    for cmd in cmds:
        out=file('output-%d.log' % n, 'a')
        err=file('error-%d.log' % n, 'a')
        ret.append(start_cmd(cmd, out, err))
        n+=1
    return ret

def close_cmds(cmds):
    for cmd in cmds:
        cmd.stdin.close()

def join_cmds(cmds):
    for cmd in cmds:
        cmd.wait()

def multiput(src, cmds):
    n=0
    for l in src:
        cmds[n].stdin.write(l)
        n+=1
        if n==len(cmds):
            n=0

def multee(src, cmds):
    p=start_cmds(cmds)
    multiput(src, p)
    close_cmds(p)
    join_cmds(p)
    
def main():
    parser = OptionParser()
    parser.add_option("-f", "--file", dest="input",
                      help="read input from FILE", metavar="FILE")
    parser.add_option("-c", "--command", dest="command",
                    help="command", action="append", metavar="COMMAND")
    parser.add_option("-n", "--multiplier", dest="times",
                    help="start n instances of COMMAND", metavar="MULTIPLIER", type='int', default='1')
    (options, args) = parser.parse_args()
    if options.input is None or options.input=='-':
        options.input=sys.stdin
    else:
        options.input==open(options.input, 'r')
    multee(options.input, options.command * options.times)

if __name__=="__main__":
    main()
