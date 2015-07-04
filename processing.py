#!/usr/bin/python
#coding: utf8
from lxml import etree
import sys
from os import *
import fdb
import logging
import datetime
import timeit
import time
from odsmod import *
def getgenerator(cur,gen):
 sq="SELECT GEN_ID("+gen+", 1) FROM RDB$DATABASE"
 try:
  cur.execute(sq)
 except:
  print "err"
  g=-1
 else:
  r=cur.fetchall()
  g=r[0][0]
 return g
def inform(st):
 logging.info(st)
 print st
 return
class Profiler(object):
    def __enter__(self):
        self._startTime = time.time()

    def __exit__(self, type, value, traceback):
        #print "Elapsed time:",time.time() - self._startTime # {:.3f} sec".format(time.time() - self._startT$
        st=u"Время выполнения:"+str(time.time() - self._startTime) # {:.3f} sec".format(time.time() - self._$
        print st
        logging.info(st)
def quoted(a):
 st=u"'"+a+u"'"
 return st
def main():
 print  len(sys.argv)
 if len(sys.argv) <2:
  print "Для запуска набери: ./processing.py loadrbd|process|get"
  print '	loadrbd - Загрузка новых данных из РБД и очистка таблицы от предыдущей версии'
  print '	process - Поиск соответвий реестров из ГИБДД с данными из РБД'
  print '	get     - Выгрузка реестров для загрузки в подразделениях'
  sys.exit(2)
 logging.basicConfig(format = u'%(levelname)-8s [%(asctime)s] %(message)s',level = logging.DEBUG, filename = './processing.log')
 fileconfig=file('./config.xml')
 xml=etree.parse(fileconfig)
 xmlroot=xml.getroot()

 main_database=xmlroot.find('main_database')
 main_dbname=main_database.find('dbname').text
 main_user=main_database.find('user').text
 main_password=main_database.find('password').text
 main_host=main_database.find('hostname').text
 rbd_database=xmlroot.find('rbd')
 rbd_dbname=rbd_database.find('dbname').text
 rbd_user=rbd_database.find('user').text
 rbd_password=rbd_database.find('password').text
 rbd_host=rbd_database.find('hostname').text

 nd=xmlroot.find('output_path')
 output_path=nd.text
 nd=xmlroot.find('output_path2')
 output_path2=nd.text
 sq1="SELECT  doc_ip_doc.id , document.doc_number, doc_ip_doc.id_dbtr_name,entity.entt_firstname,entity.entt_patronymic, entity.entt_surname,doc_ip_doc.id_dbtr_born, doc_ip.id_debtsum, document.docstatusid, doc_ip.ip_exec_prist_name FROM DOC_IP_DOC DOC_IP_DOC JOIN DOC_IP ON DOC_IP_DOC.ID=DOC_IP.ID JOIN DOCUMENT ON DOC_IP.ID=DOCUMENT.ID join entity on doc_ip.id_dbtr=entity.entt_id   where document.docstatusid=9      and DOC_IP_DOC.ID_DBTR_ENTID IN (2,71,95,96,97,666)"
 if sys.argv[1]=='loadrbd':
  try:
   con = fdb.connect (host=main_host, database=main_dbname, user=main_user, password=main_password,charset='WIN1251')
  except  Exception, e:
   print("Ошибка при открытии базы данных:\n"+str(e))
   sys.exit(2)
  try:
   con2 = fdb.connect (host=rbd_host, database=rbd_dbname, user=rbd_user,  password=rbd_password,charset='WIN1251')
  except  Exception, e:
   print("Ошибка при открытии базы данных:\n"+str(e))
   sys.exit(2)
  cur = con.cursor()
  cur2 = con2.cursor()
  sq= sq1
  sq2="INSERT INTO DOCIPDOC (ID, DOC_NUMBER, ID_DBTR_FIRSTNAME, ID_DBTR_SECONDNAME, ID_DBTR_LASTNAME, ID_DBTR_FULLNAME,ID_DBTR_BORN, ID_DEBTSUM, DOCSTATUSID, IP_EXEC_PRIST_NAME ) VALUES (?,?,?,?,?,?,?,?,?,?)"
  print sq
  st=u"Генерация скрипта вставки данных из РБД во временную таблицу"
  logging.info(st)
  print st
  with Profiler() as p:
   cur2.execute(sq)
   r=cur2.fetchall()
  cur.execute (sq2,r[0]) 
  st=u"Выбрано " +str(len(r))+ u"записей"
  logging.info(st)
  st=u"Вставка данных во временную таблицу"
  logging.info(st)
  with Profiler() as p:
   for rr in r:
    cur.execute (sq2,rr)
   con.commit()
if __name__ == "__main__":
    main()
