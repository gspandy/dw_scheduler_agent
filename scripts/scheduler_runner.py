#!/usr/bin/env python2.7
# -*- coding: utf-8 -*-
# 调度使用 python 这个调度脚本

import os
import subprocess
import threading
import sys
command= sys.argv[1:]
command=" ".join(command)

def readValue():
    while True:
        msg=raw_input()
        print 'msg received: %s' % msg
        if 'kill'==msg:
            os.killpg(os.getpgid(p.pid),9)


newThread = threading.Thread(target = readValue, name = "readingThread" )
newThread.setDaemon(1)

#os.setsid 完美解决
p=subprocess.Popen(command,shell=True,preexec_fn=os.setsid,stdin=sys.stdin, stdout=None, stderr=None)

newThread.start()

p.wait()

exitCode=p.poll()

# test run result
# print exitCode

sys.exit(exitCode)

