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
from dbfpy import dbf
flds={"ID":0,
"ID":0,
"ID_DBTR_FULLNAME":1,
"ID_DBTR_FIRSTNAME":2,
"ID_DBTR_SECONDNAME":3,
"ID_DBTR_LASTNAME":4,
"ID_DBTR_BORN":5}
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

 nd=xmlroot.find('input_path')
 input_path=nd.text

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
  #cur2 = con.cursor()
  #cur2 = con2.cursor()  
  st=u"Начинаем группировку должников"
  inform(st)
  sq="select count(id) from DOCIPDOC"
  cur.execute(sq)
  r=cur.fetchall()
  lb=int(r[0][0])
  #print lb
  sq2="select upper(d.id_dbtr_fullname), d.id_dbtr_born from docipdoc d group by upper( d.id_dbtr_fullname),d.id_dbtr_born"
#"select upper(d.id_dbtr_fullname),  (select first 1 id_dbtr_firstname from docipdoc where  upper(d.id_dbtr_fullname)=upper(docipdoc.id_dbtr_fullname)) ,(select first 1 id_dbtr_secondname from docipdoc where  upper(d.id_dbtr_fullname)=upper(docipdoc.id_dbtr_fullname) ),(select first 1 id_dbtr_lastname from docipdoc where  upper(d.id_dbtr_fullname)=upper(docipdoc.id_dbtr_fullname)) ,d.id_dbtr_born from docipdoc d group by upper( d.id_dbtr_fullname),d.id_dbtr_born"
#"select upper(d.id_dbtr_fullname),  (select first 1 id_dbtr_firstname from docipdoc), (select first 1 id_dbtr_secondname from docipdoc), (select first 1 id_dbtr_lastname from docipdoc), d.id_dbtr_born from docipdoc d group by upper( d.id_dbtr_fullname),d.id_dbtr_born"  #"select upper(docipdoc.id_dbtr_fullname), docipdoc.id_dbtr_born from docipdoc  # group by upper( docipdoc.id_dbtr_fullname),docipdoc.id_dbtr_born"
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
    rs=(rr[0]).split(' ') 
    id=getgenerator(cur,"GEN_R")
    r2=[id]
    #print rs
    r2.extend( [rr[0]] )
    r2.extend( [rs[0] ])
    r2.extend( [rs[1] ])
    try:
     r2.extend( [rs[2] ] )
    except:
     r2.extend( [None] )
    #print r2,rr[1]
    r2.extend( [rr[1]] ) 
    #sq3="select * from docipdoc where upper(docipdoc.id_dbtr_fullname)="+quoted(rr[0]) +" and docipdoc.id_dbtr_born="+quoted(str(rr[1]))
    sq3="INSERT INTO REESTR (ID, ID_DBTR_FULLNAME, ID_DBTR_FIRSTNAME, ID_DBTR_SECONDNAME, ID_DBTR_LASTNAME, ID_DBTR_BORN) VALUES (?, ?, ?, ?, ?, ?)" 
    #print rr[4], type (rr[4])
    #sq4="UPDATE DOCIPDOC SET STATUS=1, FK="+str(id)+" where UPPER(DOCIPDOC.ID_DBTR_FULLNAME)="+quoted(rr[0])+" and  docipdoc.id_dbtr_born="+quoted( str (rr[4].strftime("%d.%m.%Y") ) )
    #print sq4
    cur.execute(sq3,r2)
    #cur.execute(sq4)
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
  
 # st= u"Проставляем связи в таблицах исходной и группированной.Загоняем результат в буфер."
 # inform(st)
  #print stt[0]
 # stt=[]
 # with Profiler() as p:
 #  sq4="select reestr.id ,docipdoc.id from  reestr join docipdoc on  UPPER(DOCIPDOC.ID_DBTR_FULLNAME)=reestr.id_dbtr_fullname and  docipdoc.id_dbtr_born=reestr.id_dbtr_born"
 #  cur.execute(sq4)
 #  for raw in cur:
 #   sq5="update DOCIPDOC SET STATUS=1, FK="+str(raw[0])+" where id="+str(raw[1])
 #   stt.append(sq5)
    #cur2.execute(sq5)
 # st= u"Загрузка запросов:"+unicode(str(len(stt)))
 # inform(st)
 # with Profiler() as p:
 #  for rr in stt:
 #   try:
 #    cur.execute(rr)
 #    print rr
 #    sys.exit(2)
 #    print stt
 #   except Exception, e:
 #    print("Ошибка при открытии базы данных:\n"+str(e))
 #    sys.exit(2)
 #    
 #    print stt
 # st= u"Меряем коммит"
 # inform(st)
  #print stt[0]
 # with Profiler() as p:
 #  con.commit()
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
  #try:
  # osp=sys.argv[2]
  #except :
  # st=u"Не указан отдел"
  # inform(st)
  # sys.exit(2)

  #print osp+'0000000000'
  #print str(int(osp)+1)+'0000000000'
  try:
   con = fdb.connect (host=main_host, database=main_dbname, user=main_user, password=main_password,charset='WIN1251')
  except  Exception, e:
   print("Ошибка при открытии базы данных:\n"+str(e))
   sys.exit(2)
  cur = con.cursor()
  cur2= con.cursor()
  #sq1='select * from DOCIPDOC where id>='+osp+'0000000000 and id<'+str(int(osp)+1)+'0000000000 and status=0'
  sq1='select * from reestr where (status is null or status=0)'
  cur.execute(sq1)
  r=cur.fetchall()
  try:
   cnt=int(sys.argv[2])
  except:
   st=u"Не указано количество сделаю полную выборку."
   inform(st)
   cnt=len(r)
  #print sys.argv[3]
  if cnt>len(r):
   cnt=len(r)
  st=u"Найдено "+unicode(str(len(r)))+u" записей для выгрузки."
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
   dd=datetime.now()
   zap=getgenerator(cur,"GEN_ZAP")
   fn= str(zap)+'_'+dd.strftime("%d_%m_%Y") +'.xls'
   for i in range(0,cnt):
    cl=0
    rr=r[i]
    #dd=datetime.now()
    #zap=getgenerator(cur,"GEN_ZAP")
    print i
    
    sq4="UPDATE DOCIPDOC SET STATUS=1, FK="+str(rr[0])+" where UPPER(DOCIPDOC.ID_DBTR_FULLNAME)="+quoted(rr[1])+" and  docipdoc.id_dbtr_born="+quoted( str (rr[5].strftime("%d.%m.%Y") ) )
   
    sq5="UPDATE REESTR SET STATUS=1, ZAPROS_ID="+str(zap)+", FILENAME="+quoted(fn)+' where id='+str(rr[0])
    #print sq4
    #print sq5
    cur2.execute(sq4)
    cur2.execute(sq5)
    for mm in matr:
     if str(type(r[i][flds[mm]]))=="<type 'datetime.date'>":
      ws.write(rw, cl,r[i][flds[mm]],style1)
     else:
      ws.write(rw, cl,r[i][flds[mm]])
     cl+=1 
    rw+=1
    i+=1
   wb.save(output_path+fn)
  st=u"Меряем коммит"
  inform(st)
  with Profiler() as p:
   con.commit()

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
 if sys.argv[1]=='upload':
  try:
   con = fdb.connect (host=main_host, database=main_dbname, user=main_user,  password=main_password,charset='WIN1251')   
  except  Exception, e:
   print("Ошибка при открытии базы данных:\n"+str(e))
   sys.exit(2)
  cur = con.cursor()
  st=u"Загрузка файлов ответов."
  inform(st)
  #answ_id
  #id
  sq="INSERT INTO ANSWERS (ID, FSUBJFAM, FSUBJNAME, FSUBJOTCH, FSUBJDATER, FSUBJADDIT, NAMETYPEAZ, NAMEZAGS, NUMAZ, DATEAZ, NUMSV, NUMSV2, DATESV, DATESV2, FAMSUB1, NAMESUB1, OTCHSUB1, FAMSUB1P, NAMESUB1P, OTCHSUB1P, POLSUB1, DATERSUB1, MESTORSUB1, DOCSUB1, NATIONSUB1, GRAJDSUB1, MESTOLSUB1, FAMSUB2, NAMESUB2, OTCHSUB2, FAMSUB2P, NAMESUB2P, POLSUB2, DATERSUB2, MESTORSUB2, DOCSUB2, NATIONSUB2, GRAJDSUB2, MESTOLSUB2, FAMSUB3, NAMESUB3, OTCHSUB3, POLSUB3, DATERSUB3, MESTORSUB3, DOCSUB3, NATIONSUB3, GRAJDSUB3, MESTOLSUB3, FAMSUB4, NAMESUB4, OTCHSUB4, POLSUB4, DATERSUB4, MESTORSUB4, DOCSUB4, NATIONSUB4, GRAJDSUB4, MESTOLSUB4, FAMSUB5, NAMESUB5, OTCHSUB5, POLSUB5, DATERSUB5, MESTORSUB5, DOCSUB5, NATIONSUB5, GRAJDSUB5, MESTOLSUB5, DATESM, MESTOSM, PRICHSM, ANSWER_ID) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
  print input_path
  st=u'Начало процесса загрузки, файлов для обработки:'+str( len(listdir(input_path) ))
  logging.info( st )
  
  for ff in listdir(input_path):
   try:
    db=dbf.Dbf(input_path+ff)
   except Exception, e:
    print str(e),"ERR FILE"
    #sys.exit(2)
   #Конвертация данных
   r=db[0]
   id=1;
   answerid=2
   answ=[]
   print len(db.fieldNames)
   print (db.fieldNames)
   answ.append(id)
   for i in range (0,len(db.fieldNames)):
    print i+2, db.fieldNames[i]
    answ.append( str(r[i]).decode('CP866'))
   answ.append(answerid)
   print answ
   cur.execute(sq,answ)
   #print str(db[0]).decode('CP866')
  #with Profiler() as p:
if __name__ == "__main__":
    main()
