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
  print '	loadrbd	- Загрузка новых данных из РБД'
  print '	group	- Группировка'
  print '	get <КОД ОТДЕЛА> <КОЛ-ВО>	- Выгрузка Файлов для ЗАГС'
  print '	delete	Очистка	'
  print '	upload	- Загрузка ответов'
  print '	process	- Поиск соответвий реестров из ЗАГС с данными из РБД'
  print '	download	Выгрузка ответов в ОСП'
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
 nd=xmlroot.find('output_path3')
 output_path3=nd.text
 nd=xmlroot.find('input_path')
 input_path=nd.text
 nd=xmlroot.find('input_arc_path')
 input_arc_path =nd.text
 sq1="SELECT  doc_ip_doc.id , document.doc_number, trim(doc_ip_doc.id_dbtr_name),entity.entt_firstname,entity.entt_patronymic, entity.entt_surname,doc_ip_doc.id_dbtr_born, doc_ip.id_debtsum, document.docstatusid, doc_ip.ip_exec_prist_name FROM DOC_IP_DOC DOC_IP_DOC JOIN DOC_IP ON DOC_IP_DOC.ID=DOC_IP.ID JOIN DOCUMENT ON DOC_IP.ID=DOCUMENT.ID join entity on doc_ip.id_dbtr=entity.entt_id   where document.docstatusid=9      and DOC_IP_DOC.ID_DBTR_ENTID IN (2,71,95,96,97,666) and doc_ip_doc.id_dbtr_born is not null and  doc_ip_doc.id_dbtr_born >='01.01.1900' and doc_ip_doc.id_dbtr_name is not null"   # and doc_ip.ip_risedate<'14.10.2011' "# and doc_ip.id_debtsum>=3000"
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
    sq3="INSERT INTO REESTR (ID, ID_DBTR_FULLNAME, ID_DBTR_FIRSTNAME, ID_DBTR_SECONDNAME, ID_DBTR_LASTNAME, ID_DBTR_BORN) VALUES (?, ?, ?, ?, ?, ?)"  ##TEST
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
  print len(r)
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
   print "Z", fn
   for i in range(0,cnt):
    cl=0
    rr=r[i]
    #dd=datetime.now()
    #zap=getgenerator(cur,"GEN_ZAP")
    #print i
    
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
   wb.save(output_path3+fn)
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
   cur.execute ("delete from answers" )
   cur.execute ("delete from answers_osp" )
   cur.execute ("delete from reestr") 

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
  sq="INSERT INTO ANSWERS (ID, FSUBJFAM, FSUBJNAME, FSUBJOTCH, FSUBJDATER, FSUBJADDIT, NAMETYPEAZ, NAMEZAGS, NUMAZ, DATEAZ, NUMSV, NUMSV2, DATESV, DATESV2, FAMSUB1, NAMESUB1, OTCHSUB1, FAMSUBP1, NAMESUBP1, OTCHSUBP1, VOZRSUB1, DOCRSUB1, OBRAZSUB1, SEMPSUB1, DATERSP1, MESTORSP1, POLSUB1, DATERSUB1, MESTORSUB1, DOCSUB1, NATIONSUB1, GRAJDSUB1, MESTOLSUB1, FAMSUB2, NAMESUB2, OTCHSUB2, VOZRSUB2, FAMSUBP2, NAMESUBP2, OTCHSUBP2, DOCRSUB2, OBRAZSUB2, SEMPSUB2, POLSUB2, DATERSUB2, MESTORSUB2, DOCSUB2, NATIONSUB2, GRAJDSUB2, MESTOLSUB2, FAMSUB3, NAMESUB3, OTCHSUB3, VOZRSUB3, POLSUB3, DATERSUB3, MESTORSUB3, DOCSUB3, NATIONSUB3, GRAJDSUB3, MESTOLSUB3, FAMSUB4, NAMESUB4, OTCHSUB4, ZAJAVSUB4, POLSUB4, DATERSUB4, MESTORSUB4, DOCSUB4, NATIONSUB4, GRAJDSUB4, MESTOLSUB4, FAMSUB5, NAMESUB5, OTCHSUB5, ZAJAVSUB5, POLSUB5, DATERSUB5, MESTORSUB5, DOCSUB5, NATIONSUB5, GRAJDSUB5, MESTOLSUB5, KOLICH_CH, JIVOROJD, OSNOV_V, OSNOVANIE, ZAJAVSUB1, DATEPRBRAK, AZ_ZB, ZAJAVSUB2, AZROGDSUB1, USINRODIT, DATESM, TIMESM, MESTOSM, PRICHSM, ANSWER_ID, STATUS) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
  #"INSERT INTO ANSWERS (ID, FSUBJFAM, FSUBJNAME, FSUBJOTCH, FSUBJDATER, FSUBJADDIT, NAMETYPEAZ, NAMEZAGS, NUMAZ, DATEAZ, NUMSV, NUMSV2, DATESV, DATESV2, FAMSUB1, NAMESUB1, OTCHSUB1, FAMSUB1P, NAMESUB1P, OTCHSUB1P, DATERSUB1P, MESTORS1P, POLSUB1, DATERSUB1, MESTORSUB1, DOCSUB1, NATIONSUB1, GRAJDSUB1, MESTOLSUB1, FAMSUB2, NAMESUB2, OTCHSUB2, FAMSUB2P, NAMESUB2P, OTCHSUB2P, POLSUB2, DATERSUB2, MESTORSUB2, DOCSUB2, NATIONSUB2, GRAJDSUB2, MESTOLSUB2, FAMSUB3, NAMESUB3, OTCHSUB3, POLSUB3, DATERSUB3, MESTORSUB3, DOCSUB3, NATIONSUB3, GRAJDSUB3, MESTOLSUB3, FAMSUB4, NAMESUB4, OTCHSUB4, POLSUB4, DATERSUB4, MESTORSUB4, DOCSUB4, NATIONSUB4, GRAJDSUB4, MESTOLSUB4, FAMSUB5, NAMESUB5, OTCHSUB5, POLSUB5, DATERSUB5, MESTORSUB5, DOCSUB5, NATIONSUB5, GRAJDSUB5, MESTOLSUB5, DATESM, MESTOSM, PRICHSM, ANSWER_ID, STATUS) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
  #"INSERT INTO ANSWERS (ID, FSUBJFAM, FSUBJNAME, FSUBJOTCH, FSUBJDATER, FSUBJADDIT, NAMETYPEAZ, NAMEZAGS, NUMAZ, DATEAZ, NUMSV, NUMSV2, DATESV, DATESV2, FAMSUB1, NAMESUB1, OTCHSUB1, FAMSUB1P, NAMESUB1P, OTCHSUB1P, POLSUB1, DATERSUB1, MESTORSUB1, DOCSUB1, NATIONSUB1, GRAJDSUB1, MESTOLSUB1, FAMSUB2, NAMESUB2, OTCHSUB2, FAMSUB2P, NAMESUB2P, OTCHSUB2P, POLSUB2, DATERSUB2, MESTORSUB2, DOCSUB2, NATIONSUB2, GRAJDSUB2, MESTOLSUB2, FAMSUB3, NAMESUB3, OTCHSUB3, POLSUB3, DATERSUB3, MESTORSUB3, DOCSUB3, NATIONSUB3, GRAJDSUB3, MESTOLSUB3, FAMSUB4, NAMESUB4, OTCHSUB4, POLSUB4, DATERSUB4, MESTORSUB4, DOCSUB4, NATIONSUB4, GRAJDSUB4, MESTOLSUB4, FAMSUB5, NAMESUB5, OTCHSUB5, POLSUB5, DATERSUB5, MESTORSUB5, DOCSUB5, NATIONSUB5, GRAJDSUB5, MESTOLSUB5, DATESM, MESTOSM, PRICHSM, ANSWER_ID, STATUS) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
  #"INSERT INTO ANSWERS (ID, FSUBJFAM, FSUBJNAME, FSUBJOTCH, FSUBJDATER, FSUBJADDIT, NAMETYPEAZ, NAMEZAGS, NUMAZ, DATEAZ, NUMSV, NUMSV2, DATESV, DATESV2, FAMSUB1, NAMESUB1, OTCHSUB1, POLSUB1, DATERSUB1, MESTORSUB1, DOCSUB1, NATIONSUB1, GRAJDSUB1, MESTOLSUB1, FAMSUB2, NAMESUB2, OTCHSUB2, POLSUB2, DATERSUB2, MESTORSUB2, DOCSUB2, NATIONSUB2, GRAJDSUB2, MESTOLSUB2, FAMSUB3, NAMESUB3, OTCHSUB3, POLSUB3, DATERSUB3, MESTORSUB3, DOCSUB3, NATIONSUB3, GRAJDSUB3, MESTOLSUB3, FAMSUB4, NAMESUB4, OTCHSUB4, POLSUB4, DATERSUB4, MESTORSUB4, DOCSUB4, NATIONSUB4, GRAJDSUB4, MESTOLSUB4, FAMSUB5, NAMESUB5, OTCHSUB5, POLSUB5, DATERSUB5, MESTORSUB5, DOCSUB5, NATIONSUB5, GRAJDSUB5, MESTOLSUB5, DATESM, MESTOSM, PRICHSM, ANSWER_ID, STATUS) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
  #"INSERT INTO ANSWERS (ID, FSUBJFAM, FSUBJNAME, FSUBJOTCH, FSUBJDATER, FSUBJADDIT, NAMETYPEAZ, NAMEZAGS, NUMAZ, DATEAZ, NUMSV, NUMSV2, DATESV, DATESV2, FAMSUB1, NAMESUB1, OTCHSUB1, FAMSUB1P, NAMESUB1P, OTCHSUB1P, POLSUB1, DATERSUB1, MESTORSUB1, DOCSUB1, NATIONSUB1, GRAJDSUB1, MESTOLSUB1, FAMSUB2, NAMESUB2, OTCHSUB2, FAMSUB2P, NAMESUB2P, OTCHSUB2P, POLSUB2, DATERSUB2, MESTORSUB2, DOCSUB2, NATIONSUB2, GRAJDSUB2, MESTOLSUB2, FAMSUB3, NAMESUB3, OTCHSUB3, POLSUB3, DATERSUB3, MESTORSUB3, DOCSUB3, NATIONSUB3, GRAJDSUB3, MESTOLSUB3, FAMSUB4, NAMESUB4, OTCHSUB4, POLSUB4, DATERSUB4, MESTORSUB4, DOCSUB4, NATIONSUB4, GRAJDSUB4, MESTOLSUB4, FAMSUB5, NAMESUB5, OTCHSUB5, POLSUB5, DATERSUB5, MESTORSUB5, DOCSUB5, NATIONSUB5, GRAJDSUB5, MESTOLSUB5, DATESM, MESTOSM, PRICHSM, ANSWER_ID) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
  print input_path                                                                                                                          
  st=u'Начало процесса загрузки, файлов для обработки:'+unicode( len(listdir(input_path) )) +u' файлов.'
  inform( st )
  
  for ff in listdir(input_path):
   with Profiler() as p:
    try:
     db=dbf.Dbf(input_path+ff)
    except Exception, e:
     print str(e),"ERR FILE"
     #sys.exit(2)
    #Конвертация данных 
    print db
    #r=db[0]
    st=u'Загружаем файл '+unicode(ff) +u' содержащий '+ unicode(db.recordCount) +u' записей.'
    inform(st) 
    answerid=getgenerator(cur,"GEN_ANSWER_ID")
    for r in db:
     id=getgenerator(cur,"GEN_ANSW")
     answ=[]
     print len(db.fieldNames)
     #print (db.fieldNames)
     answ.append(id)
     for i in range (0,len(db.fieldNames)):
      #print i+2, db.fieldNames[i]
      if 'DATE' in db.fieldNames[i]:
       try:
        dd=datetime.strptime(r[i],"%d.%m.%Y")
       except:
        dd=None
       answ.append( dd)
      else:
       answ.append( str(r[i]).decode('CP866'))
       #print i+2,'|',db.fieldNames[i], "|", str(len(str(r[i]).decode('CP866') )) ,"|",str(r[i]).decode('CP866')
     answ.append(answerid)
     answ.append(None)
     #for ss in answ:
     # print ss
     #print answ
     print 'ANW  ' , len(answ)
     cur.execute(sq,answ)
    #print str(db[0]).decode('CP866')
    con.commit()
    db.close()
    rename(input_path+ff, input_arc_path+ff)
  con.close()
  #with Profiler() as p:
#select * from reestr reestr join answers on  (reestr.id=cast(answers.fsubjaddit as bigint))   
 if sys.argv[1]=='process':
  try:
   con = fdb.connect (host=main_host, database=main_dbname, user=main_user, password=main_password,charset='WIN1251')
  except  Exception, e:
   print("Ошибка при открытии базы данных:\n"+str(e))
   sys.exit(2)
  cur = con.cursor()
  with Profiler() as p:
   sq='select FSUBJFAM, FSUBJNAME, FSUBJOTCH,FSUBJDATER, FSUBJADDIT, NAMETYPEAZ, NAMEZAGS, NUMAZ, DATEAZ, NUMSV, MESTOLSUB1, DATESM, MESTOSM, PRICHSM,docipdoc.id,docipdoc.doc_number,docipdoc.id_dbtr_fullname,answers.id,reestr.zapros_id,docipdoc.IP_EXEC_PRIST_NAME  from answers  join reestr on reestr.id=answers.fsubjaddit join docipdoc on docipdoc.fk=reestr.id where answers.datesm is not null and answers.status is null'
      #select FSUBJFAM, FSUBJNAME, FSUBJOTCH,FSUBJDATER, FSUBJADDIT, NAMETYPEAZ, NAMEZAGS, NUMAZ, DATEAZ, NUMSV, MESTOLSUB1, DATESM, MESTOSM, PRICHSM,docipdoc.id,docipdoc.doc_number,docipdoc.id_dbtr_fullname,answers.id,reestr.zapros_id,docipdoc.IP_EXEC_PRIST_NAME  from docipdoc join reestr on reestr.id=docipdoc.fk   join answers  on (reestr.id=answers.fsubjaddit) where docipdoc.fk in (select reestr.id  from answers  join reestr on reestr.id=answers.fsubjaddit where answers.datesm is not null and answers.status is null)'
   sq2='select count (id) from answers where answers.status is null'
   sq3='INSERT INTO ANSWERS_OSP (ID, FSUBJFAM, FSUBJNAME, FSUBJOTCH, FSUBJDATER, FSUBJADDIT, NAMETYPEAZ, NAMEZAGS, NUMAZ, DATEAZ, NUMSV, MESTOLSUB1, DATESM, MESTOSM, PRICHSM, IP_ID, DOC_NUMBER, ID_DBTR_FULLNAME, ANSWER_ID, ZAPROS_ID, IP_EXEC_PRIST_NAME,STATUS) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)'
   sq4='update answers  set status=2 where answers.id in (select  answers.id from answers  join reestr on reestr.id=answers.fsubjaddit where answers.datesm is not  null  and answers.status is null)'
   sq5='update answers  set status=1 where answers.id in (select  answers.id from answers  join reestr on reestr.id=answers.fsubjaddit where answers.datesm is   null  and answers.status is null)' 
   st=u'Начало процесса сверки. Объединяем положительные ответы '
   logging.info( st )
   cur.execute(sq2)
   l=cur.fetchone()[0]
   cur.execute(sq)
   r=cur.fetchall()
   st=u'Найдено положительных ответов '+unicode (len(r))+u' из '+unicode(l) +'.'
   inform( st )
   for rr in r:
    id=getgenerator(cur,'GEN_OSP')
    r2=[]
    r2.append(id)
    r2.extend(rr)
    r2.append(0)
    #for ii in range(0,len(r2)):
    # print ii+1 ,r2[ii]
    cur.execute(sq3,r2)
   con.commit()
   st=u'Отмечаем положительные и отрицательные ответы как обработанные'
   inform( st )
   cur.execute (sq4)
   cur.execute(sq5)
   con.commit()
   con.close()
    
    #for ii in range(0,len(r2)):
    # print ii+2 ,r2[ii]
 if sys.argv[1]=='download':
  try:
   con = fdb.connect (host=main_host, database=main_dbname, user=main_user, password=main_password,charset='WIN1251')
  except  Exception, e:
   print("Ошибка при открытии базы данных:\n"+str(e))
   sys.exit(2)
  cur = con.cursor()
  sq='select answers_osp.osp from answers_osp where answers_osp.status =0 group by answers_osp.osp'
  sq2='select answers_osp.id, answers_osp.ip_id,answers_osp.packet_id,answers_osp.doc_number,answers_osp.id_dbtr_fullname,answers_osp.nametypeaz,answers_osp.namezags,answers_osp.numaz,dateaz,answers_osp.numsv,answers_osp.mestolsub1,answers_osp.datesm,answers_osp.mestosm,answers_osp.prichsm,ip_exec_prist_name from answers_osp where osp='
  #sq3='update answers_osp set status=1, packet'
  cur.execute(sq)
  p=cur.fetchall()
  ff=['id', 'ip_id','packet_id','doc_number','id_dbtr_fullname','nametypeaz','namezags','numaz','dateaz','numsv','mestolsub1','datesm','mestosm','prichsm','ip_exec_prist_name']
  fd=['ID', 'IP_ID','packet_id','Номер ИП','Должник','Наименование док','Наименование ЗАГС','Номер ЗАГСа','Дата свид-ва','номер св-ва','Место жительства','Дата смерти','Место смерти','Причина смерти','Пристав']
  if len(p)>0:
   datedir=datetime.now().strftime('%d_%m_%Y')
   try:
    mkdir(output_path2+datedir)
   except:
    print output_path2+datedir
   for pp in p: 
    packet_id=getgenerator(cur,'GEN_PACK')
    #packet_id=1
    
    d=datetime.now().strftime('%d.%m.%y')
    df=datetime.now().strftime('%Y_%m_%d')
    #print pp,packet_id
    fn=pp[0]+'_'+df+'_'+str(packet_id)+'_zags.xml'
    fn2=pp[0]+'_'+df+'_'+str(packet_id)+'.ods'
    textdoc=initdoc()
    table,tablecontents,textdoc=inittable(textdoc)
    row=(fd[3], fd[4], fd[5], fd[6], fd[7], fd[8], fd[9], fd[10],  fd[11], fd[12], fd[13],fd[14]  )
    #print row
    table=addrow(row,table,tablecontents)

    cur.execute(sq2+pp[0]+' order by answers_osp.ip_exec_prist_name')
    r=cur.fetchall()
    root=etree.Element('answers')
    for j in range(0,len(r)):
     sq3='update answers_osp set status=1, packet_id='+str(packet_id)+' , filename='+quoted(fn)+' where id='+str(r[j][0]) 
     cur.execute(sq3)
     #print sq3
     root2=etree.SubElement(root,'answer') 
     row=(r[j][3], r[j][4], r[j][5], r[j][6], r[j][7], r[j][8].strftime('%d.%m.%y'), r[j][9], r[j][10], r[j][11].strftime('%d.%m.%y'), r[j][12], r[j][13],r[j][14]  )
     #print row
     table=addrow(row,table,tablecontents)
     #el=etree.SubElement(root2,'packet_id')
     #el.text=unicode( unicode(packet_id) )
     #ff['packet_id']=unicode(packet_id)
     #print 'LEN', len(r[j])
     for i in range(0, len(r[j])):
      if ff[i]=='packet_id':
       el=etree.SubElement(root2,'packet_id')
       el.text=unicode( unicode(packet_id) )
      else:
       if (str(type( r[j][i] ))=="<type 'datetime.datetime'>") or (str(type( r[j][i] ))=="<type 'datetime.date'>"):
        el=etree.SubElement(root2,ff[i])
        el.text=( r[j][i].strftime('%d.%m.%Y'))
       else:
        el=etree.SubElement(root2,ff[i])
        el.text=unicode(r[j][i])

    con.commit() 
    savetable(table,textdoc,output_path2+'/'+datedir+'/'+fn2)
    xml= etree.tostring(root, pretty_print=True, encoding='UTF-8', xml_declaration=True)
    #print output_path+fn,xml
    f=file (output_path+fn , 'w')
    f.write(xml)
    f.close()
  else:
   inform(u"Нет файлов для выгрузки")
  con.close()
if __name__ == "__main__":
    main()
