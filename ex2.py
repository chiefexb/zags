#!/usr/bin/python
#coding: utf8
from os import *
import datetime
import timeit
import time
from odsmod import *
from random import random
from multiprocessing import Process, Lock,Queue
class Profiler(object):
    def __enter__(self):
        self._startTime = time.time()

    def __exit__(self, type, value, traceback):
        #print "Elapsed time:",time.time() - self._startTime # {:.3f} sec".format(time.time() - self._startT$
        st=u"Время выполнения:"+str(time.time() - self._startTime) # {:.3f} sec".format(time.time() - self._$
        print st
        #logging.info(st
def worker(queue):
 while not queue.empty():
  LOCK.acquire()
  sq=queue.get()
  LOCK.release()
  #print type(queue)

def main():
 q=Queue()
 
 print "время ложим в очередь"
 with Profiler() as p:
  for i in range(100000):
   p=int(random()*100)
   q.put(p)
 print q.qsize()
 print "время берем из  очереди"
 q2=q
 with Profiler() as p:
  for i in range(100000):
   p=q.get()
   #print p
 print "время ложим в список"
 qq=[]
 with Profiler() as p:
  for i in range(100000):
   p=int(random()*100)
   qq.append(p)
 print q.qsize()
 print "время берем из списка"
 with Profiler() as p:
  for r in qq:
   p=r
   #print p
 print "тестируем разбор очереди 5 поток"
 threadcount=10
 for i in range(1,threadcount):
  Process(target=worker, args=(q2,)).start()
 with Profiler() as p:
  while not q2.empty():#threading.active_count() >1:
   b=b
  print 'FIN'
if __name__ == "__main__":
    main()
