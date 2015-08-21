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
import xlwt
flds={"ID":0,
"DOC_NUMBER":1,
"ID_DBTR_FULLNAME":2,
"ID_DBTR_FIRSTNAME":3,
"ID_DBTR_SECONDNAME":4,
"ID_DBTR_LASTNAME":5,
"ID_DBTR_BORN":6,
"ID_DEBTSUM":7,
"DOCSTATUSID":8,
"IP_EXEC_PRIST_NAME":9,
"STATUS":10}
#from Queue import Queue
#import threading
#LOCK = threading.RLock()
#queue = Queue()
#exitflag=False
#th=0
#curs={}
#test
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
  print '	loadrbd - Загрузка новых данных из РБД'
  print '	delete    Очистка	'
  print '	process - Поиск соответвий реестров из ЗАГС с данными из РБД'
  print '	get <КОД ОТДЕЛА> <КОЛ-ВО>     - Выгрузка Файлов для ЗАГС'
  print sys.argv
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
 output_scheme=xmlroot.find('output_scheme');
 nd=xmlroot.find('output_path')
 output_path=nd.text
 nd=xmlroot.find('output_path2')
 output_type=xmlroot.find('output_type').text
 output_path2=nd.text
 sq1="SELECT  doc_ip_doc.id , document.doc_number, trim(doc_ip_doc.id_dbtr_name),entity.entt_firstname,entity.entt_patronymic, entity.entt_surname,doc_ip_doc.id_dbtr_born, doc_ip.id_debtsum, document.docstatusid, doc_ip.ip_exec_prist_name FROM DOC_IP_DOC DOC_IP_DOC JOIN DOC_IP ON DOC_IP_DOC.ID=DOC_IP.ID JOIN DOCUMENT ON DOC_IP.ID=DOCUMENT.ID join entity on doc_ip.id_dbtr=entity.entt_id   where document.docstatusid=9      and DOC_IP_DOC.ID_DBTR_ENTID IN (2,71,95,96,97,666) and doc_ip_doc.id_dbtr_born is not null and  doc_ip_doc.id_dbtr_born >='01.01.1900'"
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
  sq2="INSERT INTO DOCIPDOC (ID, DOC_NUMBER,ID_DBTR_FULLNAME, ID_DBTR_FIRSTNAME, ID_DBTR_SECONDNAME, ID_DBTR_LASTNAME,ID_DBTR_BORN, ID_DEBTSUM, DOCSTATUSID, IP_EXEC_PRIST_NAME,STATUS ) VALUES (?,?,?,?,?,?,?,?,?,?,0)"
  print sq
  st=u"Генерация скрипта вставки данных из РБД во временную таблицу"
  logging.info(st)
  print st
  with Profiler() as p:
   cur2.execute(sq)
   r=cur2.fetchall()
   #r=cur2.fetchmany(1000)
  #cur.execute (sq2,r[]) 
  st=u"Выбрано " +str(len(r))+ u"записей"
  logging.info(st)
  st=u"Вставка данных во временную таблицу"
  logging.info(st)
  with Profiler() as p:
   for rr in r:
    cur.execute (sq2,rr)
   con.commit()
 if sys.argv[1]=='group':
  try:
   con = fdb.connect (host=main_host, database=main_dbname, user=main_user, password=main_password,charset='WIN1251')
  except  Exception, e:
   print("Ошибка при открытии базы данных:\n"+str(e))
   sys.exit(2)
  cur = con.cursor()
  #print curs
  #try:
  # con2 = fdb.connect (host=main_host, database=main_dbname, user=main_user, password=main_password,charset='WIN1251')
  #except  Exception, e:
  # print("Ошибка при открытии базы данных:\n"+str(e))
  # sys.exit(2)
  #cur2 = con2.cursor()
  #cur2 = con2.cursor()  
  st=u"Начинаем группировку должников"
  inform(st)
  sq="select count(id) from DOCIPDOC"
  cur.execute(sq)
  r=cur.fetchall()
  lb=int(r[0][0])
  #print lb
  sq2="select upper(d.id_dbtr_fullname),  (select first 1 id_dbtr_firstname from docipdoc), (select first 1 id_dbtr_secondname from docipdoc), (select first 1 id_dbtr_lastname from docipdoc), d.id_dbtr_born from docipdoc d group by upper( d.id_dbtr_fullname),d.id_dbtr_born"
  #"select upper(docipdoc.id_dbtr_fullname), docipdoc.id_dbtr_born from docipdoc
  # group by upper( docipdoc.id_dbtr_fullname),docipdoc.id_dbtr_born"
  st=u"Меряем время скрипта группировки"
  inform(st)
  with Profiler() as p:
   cur.execute(sq2)
   r=cur.fetchall()
  la=len(r)
  print la,lb
  #prc=int( (float(la)/float(lb)) * 100)
  #print prc
  #st=unicode(lb)+ u" должников сгруппированы. Найдено "+unicode(la) +u" соответствий. Коэффициент сжатия "+unicode(prc)+u" процентов."
  #inform(st)
  st= u"Вставляем результат в отдельную таблицу."
  inform(st)
  with Profiler() as p:
   stt=[]
   for rr in r:
    id=getgenerator(cur,"GEN_R")
    r2=[id]
    r2.extend(rr)
    #sq3="select * from docipdoc where upper(docipdoc.id_dbtr_fullname)="+quoted(rr[0]) +" and docipdoc.id_dbtr_born="+quoted(str(rr[1]))
    sq3="INSERT INTO REESTR (ID, ID_DBTR_FULLNAME, ID_DBTR_FIRSTNAME, ID_DBTR_SECONDNAME, ID_DBTR_LASTNAME, ID_DBTR_BORN) VALUES (?, ?, ?, ?, ?, ?)" 
    #print rr[4], type (rr[4])
    sq4="UPDATE DOCIPDOC SET STATUS=1, FK="+str(id)+" where UPPER(DOCIPDOC.ID_DBTR_FULLNAME)="+quoted(rr[0])+" and  docipdoc.id_dbtr_born="+quoted( str (rr[4].strftime("%d.%m.%Y") ) )
    #print sq4
    cur.execute(sq3,r2)
    #cur2.execute(sq4)
    #stt.append(sq4)
    #print queue.qsize()
  #with Profiler() as p:
  #Иницилизация
  st= u"Меряем коммит"
  inform(st)
  with Profiler() as p:
   con.commit()
  #st= u"Найдено "+unicode(len(stt))
  #inform(st)
  
  #st= u"Проставляем связи в таблицах исходной и группированной"
  #inform(st)
  #print stt[0]
  #with Profiler() as p:
  # for rr in stt:
  #   cur2.execute(rr)
   #con2.commit()
   #con2.commit()
  #  r2=cur.fetchmany()
  #  s= (id, rr[0])
  #  # r2[0][flds["ID_DBTR_FIRSTNAME"])#, r2[0][flds["ID_DBTR_SECONDNAME"]], r2[0][flds["ID_DBTR_LASTNAME"]], rr[1]) )
  #  t.append(s)
  #print len(t) 
 if sys.argv[1]=='get':
  #print sys.argv[2],sys.argv[3]
  try:
   osp=sys.argv[2]
  except :
   st=u"Не указан отдел"
   inform(st)
   sys.exit(2)

  #print osp+'0000000000'
  #print str(int(osp)+1)+'0000000000'
  try:
   con = fdb.connect (host=main_host, database=main_dbname, user=main_user, password=main_password,charset='WIN1251')
  except  Exception, e:
   print("Ошибка при открытии базы данных:\n"+str(e))
   sys.exit(2)
  cur = con.cursor()
  cur2=con.cursor()
  sq1='select * from DOCIPDOC where id>='+osp+'0000000000 and id<'+str(int(osp)+1)+'0000000000 and status=0'
  cur.execute(sq1)
  r=cur.fetchall()
  try:
   cnt=int(sys.argv[3])
  except:
   st=u"Не указано количество сделаю полную выборку."
   inform(st)
   cnt=len(r)
  #print sys.argv[3]
  if cnt>len(r):
   cnt=len(r)
  st=u"Для отдела "+ osp + u" найдено "+unicode(str(len(r)))+u" записей для выгрузки."
  inform(st)
  st=u"Будет выгружено " +unicode(str(cnt)) +u" записей."
  inform(st)
  #print output_type
  if output_type=="xls":
   matr=[]
   for ch in  output_scheme.getchildren():
    matr.append(ch.text)
   print matr
   wb = xlwt.Workbook()
   ws = wb.add_sheet('A Test Sheet')
   cl=0
   rw=0
   style1 = xlwt.XFStyle()
   style1.num_format_str = 'DD.MM.YY'
   i=0
   for i in range(0,cnt):
    cl=0
    for mm in matr:
     if str(type(r[i][flds[mm]]))=="<type 'datetime.date'>":
      ws.write(rw, cl,r[i][flds[mm]],style1)
     else:
      ws.write(rw, cl,r[i][flds[mm]])
     cl+=1 
    rw+=1
    i+=1
   wb.save(output_path+osp+".xls")
 if sys.argv[1]=='delete':
  try:
   con = fdb.connect (host=main_host, database=main_dbname, user=main_user,  password=main_password,charset='WIN1251')
  except  Exception, e:
   print("Ошибка при открытии базы данных:\n"+str(e))
   sys.exit(2)
  cur = con.cursor()
  st=u"Очистка базы результат предыдущей обработки будет потерян"
  inform(st)
  with Profiler() as p:  
   cur.execute ("delete from docipdoc" )
   con.commit()  
if __name__ == "__main__":
    main()
