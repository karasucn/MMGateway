#add xml analyze func base on demo.py
#coding=UTF-8
import re, pickle, os
from twisted.application import internet, service
from twisted.web import server, resource, http
#from xml.etree import ElementTree
from twisted.web import client
from twisted.enterprise import adbapi
from twisted.internet import protocol,defer,reactor
from twisted.python.logfile import DailyLogFile
from twisted.python import logfile #
from twisted.python.log import ILogObserver, FileLogObserver #
from twisted.internet import task
import time
import datetime
import random
try:
    import xml.etree.cElementTree as ET
except ImportError:
    import xml.etree.ElementTree as ET
#import markCity2

DB_DRIVER = "MySQLdb"
DB_ARGS = {
    'db':'chinagame',
    'user':'sa',
    'passwd':'Wr1066@#**sa',
    'host':'localhost',
    'charset':'utf8'
}

class DBServer(object):

    def __init__(self,  dbQ):
        self.dbQ = dbQ
        self.dblog = open('./log/db.log', 'a') 
	self.synlog = open('./log/syn2cp.log', 'a')
        self.getQ()
	self._checkdbQ()
	self._checkversion()

    def _checkversion(self):
	dif = dbpool.runQuery("call getGameversion(1000)")
        dif.addCallback(self.getVersionResult)
        dif.addErrback(catchError)
        self.call = reactor.callLater(3600, self._checkversion)

    def getVersionResult(self,l):
        result = l[0][0]
        if result == 0:
            print "WR Game Version : %s" % l[0][1]
        else :
            print "Get Version Error"

    def _checkdbQ(self) :
        print "#####_checkdbQ!!!!!!"
        num = self.dbQ.getpendinglength()
        if num > 10 :
            print "Too many data in dbQ self.getQ() again!!  dbQ = ", num
            errlog = open('./log/errlog.log', 'a')
            time0=(time.strftime("%Y-%m-%d %H:%M:%S "))
            msg = "Error : Too many data in dbQ self.getQ() again!!" 
            data = time0 + msg + '\n'
            errlog.write(data)
            errlog.close()
            self.getQ()
        self.call = reactor.callLater(120, self._checkdbQ)

    def getQ(self):
        d = self.dbQ.get()
        d.addErrback(self.getQError)
        def dbData(data):
	    print "Get dbQ:"
            print data
            tmpdata = data.split(',')
            str_FeeMSISDN = tmpdata[0]
            str_AppID= tmpdata[1]
            str_PayCode = tmpdata[2]
            str_OrderID= tmpdata[3]
            str_ActionID = tmpdata[4]
            TotalPrice = int(tmpdata[5])
            str_Version = tmpdata[6]
            str_TransactionID = tmpdata[7]
	    str_TransactionID = str_TransactionID + str(random.randint(100,999))
	    str_ExData = tmpdata[8]
	    str_hRet = "0"
	    str_status = "1101"
	    if str_OrderID == "00000000000000000000" :
                str_hRet = "1"
                str_status = "-1"
	    dif = dbpool.runQuery("call Proc_Provision_MM('%s','%s','%s','%s','%s','%s','%s','%s', %d)" % \
                (str_FeeMSISDN,str_AppID,str_PayCode,str_ExData,str_hRet,str_status,str_Version,\
                str_TransactionID,TotalPrice)) 
            dif.addCallback(self.Syn2cp, str_FeeMSISDN, str_ExData, str_hRet, str_status, str_TransactionID, str_PayCode)
            dif.addErrback(self.dbError, data)
            logdata = str_FeeMSISDN+','+str_AppID+','+str_PayCode+','+str_OrderID+','+str_TransactionID+','+str_ExData 
            self.writedblog(logdata)
        d.addCallback(dbData)

    def dbSuccess(self, l, data) :
	print "Write DB Success !!!"
	self.getQ()

    def getQError(self, err) :
        print err
        errlog = open('getQError.log', 'a')
        time0=(time.strftime("%Y-%m-%d %H:%M:%S "))
        data = time0 + err + '\n'
        errlog.write(data)
        errlog.close()
        self.getQ()

    def dbError(self, l, data):
	print l
        self.writedblog(data)
        print "DB Write Error !!!!! Write DBLog !!!"
	self.getQ()

    def Syn2cp(self, l, userid, cpParam, hRet, status, transIDO, consumeCode) :
        print l
        if l[0][0] == '0' :  #do not syntize to the cp
            self.getQ()
            return
        cpId = str(l[0][1])
        cpUrl = l[0][2]
	print cpUrl
        if cpUrl == '' :
            self.getQ()
            return
        cpchannel = SynServer(cpUrl)
        dif = cpchannel.send2cp(userid, cpParam, cpId, hRet, status, transIDO, consumeCode)
        dif.addCallback(self.syn2cpRespond, userid, cpParam, cpId, hRet, status, transIDO, consumeCode)
        dif.addErrback(self.syn2cpErrorRespond, userid, cpParam, cpId, hRet, status, transIDO, consumeCode)
  	self.getQ()
 
    def syn2cpErrorRespond(self, result, userid, cpParam, cpId, hRet, status, packageId, consumeCode):
        msg = result.getErrorMessage()
        logdata = userid+','+cpParam+','+cpId+','+hRet+','+status+','+packageId+','+consumeCode+','+msg
        self.writesynlog(logdata)

    def syn2cpRespond(self, result, userid, cpParam, cpId, hRet, status, packageId, consumeCode):
        logdata = userid+','+cpParam+','+cpId+','+hRet+','+status+','+packageId+','+consumeCode+','+result
        self.writesynlog(logdata)

    def writedblog(self,xml):
        result = self.checkLogDate('./log/db.log')
        if result == 1 :
            self.changeDBlog()
        time0=(time.strftime("%Y-%m-%d %H:%M:%S "))
        data = time0 + xml + '\n'
        self.dblog.write(data)
        self.dblog.flush()

    def writesynlog(self,logdata):
        result = self.checkLogDate('./log/syn2cp.log')
        if result == 1 :
            self.changeSynlog()
        time0=(time.strftime("%Y-%m-%d %H:%M:%S,"))
        data = time0 + logdata + '\n'
        self.synlog.write(data)
        self.synlog.flush()
    
    def checkLogDate(self, filename):
        today = datetime.date.today()
        info = os.stat(filename)
        last_modify_date = datetime.date.fromtimestamp(info.st_mtime)
        if today == last_modify_date:
            return 0
        else :
            return 1

    def changeSynlog(self):
        filename = './log/syn2cp.log'
        self.synlog.close()
        if os.path.isfile(filename):
            today = datetime.date.today()
            yesterday = today - datetime.timedelta(days=1)
            oldfilename = filename+'.'+str(yesterday)
            os.rename(filename,oldfilename)
            self.synlog = open('./log/syn2cp.log', 'w')

    def changeDBlog(self):
        filename = './log/db.log'
        self.dblog.close()
        if os.path.isfile(filename):
            today = datetime.date.today()
            yesterday = today - datetime.timedelta(days=1)
            oldfilename = filename+'.'+str(yesterday)
            os.rename(filename,oldfilename)
            self.dblog = open('./log/db.log', 'w')

class ServerData(object):

    def __init__(self, dbQ):
	self.dbQ = dbQ
        self.xml=None
        self.TransactionID=0
	self.xmllog = open('./log/mm.log', 'a') 

    def changeXmllog(self):
	filename = './log/mm.log'
	self.xmllog.close()
    	if os.path.isfile(filename):
            today = datetime.date.today()
            yesterday = today - datetime.timedelta(days=1)
            oldfilename = filename+'.'+str(yesterday)
            os.rename(filename,oldfilename)
            self.xmllog = open('./log/mm.log', 'a')

    def setPage(self,content):
        print time.strftime("%Y-%m-%d %H:%M:%S :")
        self.writexmllog(content)
        self.xml=content
        self.read_xml(self.xml)

    def read_xml(self,xmldata1):
	xmldata = re.sub(r' xmlns=".*"','',xmldata1)

	root = ET.fromstring(xmldata)
        Version = root.find('Version')
	OrderID = root.find('OrderID')
	ActionTime = root.find('ActionTime')
	ActionID = root.find('ActionID')
	AppID = root.find('AppID')
	PayCode = root.find('PayCode')
	OrderType = root.find('OrderType')
	MD5Sign = root.find('MD5Sign')
	TransactionID = root.find('TransactionID')
	FeeMSISDN = root.find('FeeMSISDN')
	TotalPrice = root.find('TotalPrice')
	ExData = root.find('ExData')
	
	str_Version = Version.text
	str_OrderID = OrderID.text
	str_ActionTime = ActionTime.text
	str_ActionID = ActionID.text
	str_AppID = AppID.text
	str_PayCode = PayCode.text
	str_OrderType = OrderType.text
	str_MD5Sign = MD5Sign.text
	str_TransactionID = TransactionID.text
	str_FeeMSISDN = FeeMSISDN.text
	str_TotalPrice = TotalPrice.text
	str_ExData = ExData.text
	if str_ExData == None :
	    str_ExData = '0'
	self.TransactionID = str_TransactionID

        dbdata = str_FeeMSISDN+','+str_AppID+','+str_PayCode+','+str_OrderID+','+str_ActionID+','+str_TotalPrice+','+ \
	    str_Version+','+str_TransactionID + ',' + str_ExData	
	dbQ.put(dbdata)
	print "Put dbQ :"	
	print dbdata
	if str_AppID == "300008535837" or str_AppID == "300007697557" :
	    cpUrl = "http://121.14.38.20:25468/iap_tw/SyncAppOrderReq"
	    cpchannel = SynServer(cpUrl)
            dif = cpchannel.send2CoCP(xmldata1)
            dif.addCallback(self.send2cpsuccess, cpUrl)
            dif.addErrback(catchError)
        elif str_AppID == "300008524622" or str_AppID == "300008516661" or str_AppID == "300008351595" :
	    cpUrl = "http://121.14.38.20:25498/iap/SyncAppOrderReq"
            cpchannel = SynServer(cpUrl)
            dif = cpchannel.send2CoCP(xmldata1)
            dif.addCallback(self.send2cpsuccess, cpUrl)
            dif.addErrback(catchError)
	elif str_AppID == "300008421729" or str_AppID == "300008656634":
	    cpUrl = "http://111.67.194.135:851/MMSyncAppOrder.aspx"
	    cpchannel = SynServer(cpUrl)
	    dif = cpchannel.send2CoCP(xmldata1)
	    dif.addCallback(self.send2cpsuccess, cpUrl)
	    dif.addErrback(catchError)
 
    def send2cpsuccess(self, l, cpUrl) :
        print "Send MM XML data to CoCP : %s" % cpUrl
 
    def checkLogDate(self, filename):
    	today = datetime.date.today()
    	info = os.stat(filename)
    	last_modify_date = datetime.date.fromtimestamp(info.st_mtime)
    	if today == last_modify_date:
	    return 0
	else :
	    return 1

    def writexmllog(self,xml):
	result = self.checkLogDate('./log/mm.log')
	if result == 1 :
	    self.changeXmllog()
        time0=(time.strftime("%Y-%m-%d %H:%M:%S "))
	data = time0 + xml + '\n'
        self.xmllog.write(data)
	self.xmllog.flush()

class RestResource(object):
    def __init__(self, uri):
        self.uri = uri

    def get(self):
        return self._sendRequest('GET')

    def post(self, **kwargs):
        postData = urllib.urlencode(kwargs)
        mimeType = 'application/x-www-form-urlencoded'
        return self._sendRequest('POST', postData, mimeType)

    def put(self, data, mimeType):
        return self._sendRequest('POST', data, mimeType)

    def delete(self):
        return self._sendRequest('DELETE')

    def _sendRequest(self, method, data="", mimeType=None):
        headers = {}
        if mimeType:
            headers['Content-Type'] = mimeType
        if data:
            headers['Content-Length'] = str(len(data))
        return client.getPage(
            self.uri, method=method, postdata=data, headers=headers)

class SynServer(object):
    def __init__(self, baseUri):
        self.baseUri = baseUri

    def send2cp(self, userid, para, cpid, msg, status, linkid, consumecode) :
	url = ""
	if '?' in self.baseUri :
	    url = "&userid=%s&para=%s&cpid=%s&msg=%s&status=%s&linkid=%s&consumecode=%s" % \
			(userid, para, cpid, msg, status, linkid, consumecode)
	else :
	    url = "?userid=%s&para=%s&cpid=%s&msg=%s&status=%s&linkid=%s&consumecode=%s" % \
                        (userid, para, cpid, msg, status, linkid, consumecode)
	cpUrl = self.baseUri + url 
	print cpUrl 
        return RestResource(cpUrl).get()  

    def send2CoCP(self, xmldata) :
        return RestResource(self.baseUri).put(xmldata,"text/xml")
	
class RootResource(resource.Resource):
    def __init__(self, serverData):
	self.dbpool = dbpool
        self.serverData = serverData 
        resource.Resource.__init__(self)
        self.putChild('MM',billPage(self.serverData))

class billPage(resource.Resource):
    def __init__(self, serverData):
        self.serverData = serverData
        resource.Resource.__init__(self)

    def render_POST(self, request):
        print "SDK GameBill Render_POST in billPage"
        request.setResponseCode(http.OK, "Modified existing page")
        request.content.seek(0)
        self.serverData.setPage(request.content.read( ))
	return """<?xml version="1.0" encoding="UTF-8"?>
<SyncAppOrderResp>  xmlns="http://www.monternet.com/dsmp/schemas/">
	<TransactionID>%s</TransactionID>
	<MsgType>SyncAppOrderResp</MsgType>
	<Version>1.0.0</Version>
        <hRet>0</hRet>
<SyncAppOrderResp>""" % (self.serverData.TransactionID)
                
#===================================================

def catchError(err):
    print err

class DeferredQueue(object):

    def __init__(self):
        # It would be better if these both used collections.deque (see comments section below).
        self.waiting = [] # Deferreds that are expecting an object
        self.pending = [] # Objects queued to go out via get.
    
    def put(self, obj):
        if self.waiting:
            self.waiting.pop(0).callback(obj)
        else:
            self.pending.append(obj)
        
    def get(self):
        if self.pending: 
            return defer.succeed(self.pending.pop(0))
        else:
            d = defer.Deferred()
            self.waiting.append(d)
            return d

    def getwaitinglength(self) :
	return len(self.waiting)

    def getpendinglength(self):
	return len(self.pending)

import sys
dbpool = adbapi.ConnectionPool(DB_DRIVER, **DB_ARGS)
dbQ = DeferredQueue()
serverData = ServerData(dbQ)
dbServer = DBServer(dbQ)

_logfilename = './log/twistd.log'
_logfile = logfile.DailyLogFile.fromFullPath(_logfilename)

application = service.Application('MMGateway', uid=0, gid=0)
serviceCollection = service.IServiceCollection(application)

application.setComponent(ILogObserver, FileLogObserver(_logfile).emit) 

webservice = internet.TCPServer(40000, server.Site(RootResource(serverData)))
webservice.setServiceParent(serviceCollection)

