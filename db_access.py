#!/usr/bin/python
# -*- coding: utf8-*-

from pymongo import MongoClient
import datetime
class DBAcess:
    def __init__(self,host,port,user,passwd,domain_name):
        self._host = host
        self._port = port
        self._user = user
        self._passwd = passwd
        self._conn = MongoClient(host,port)  
        self._domain = domain_name
    
    def getLogicServer(self):
        """根据所给的服务器名称返回所有的逻辑服务器信息"""
        information = self._conn[self._domain]["server_name"]
        for i in information.find():
            print i["domain"],
            print i["game_id"],
            print i["path"],
            print i["pid"],
            print i["sid"],
            print i["runstatus"]
    def get_opts(self,gt_create_tm):
        """根据主机名及大于操作插入时间返回一批操作记录"""
        get_obj = self._conn[self._domain]["server_opts"]
        for infor in get_obj.find({"create_tm":{"$gt":gt_create_tm}}):
            print infor
    
    def update_opts(self,objectId,finish_tm,result_code,result_txt):
        """根据所给的对象ID更新这三个字段"""
        up_set = {"finish_tm":finish_tm,"result_code":result_code,"result_txt":result_txt}
        self._conn[self._domain]["server_opts"].update({"_id":objectId},{"$set":up_set})

    def log_centos_info(self,txt):
        """将日志记录到所给的domain日志数组下"""
        logs = self._conn[self._domain]["centos_info"].find_one({"domain":self._domain})["logs[]"]   #获得当前服务器名的日志数组
        logs.append(unicode(txt))                                                                    #将新值添加到数组中
        self._conn[self._domain]["centos_info"].update({"domain":self._domain},{"$set":{"logs[]":logs}})   #更新centos_info表

    def log(self,typ,txt):
        try:
            self._conn[self._domain]["logs"].update({"typ":typ,"log":txt})
        except:
            print "更新失败"
if __name__ == "__main__":
    print 'mongodb test ok!'
    d = DBAcess("localhost", 27017,"aaa", 1234, "db_test")

    #测试getLogicServer
    testdoc_servername = d._conn[d._domain]["server_name"]
    testdoc_servername.insert({"domain":"yao","game_id":1001,"path":"C:\\","pid":1,"sid":100,"runstatus":0})
    testdoc_servername.insert({"domain":"yao","game_id":1001,"path":"C:\\","pid":1,"sid":100,"runstatus":0})
    

    #测试get_opts 和update_opts
    testdoc_server_opts = d._conn[d._domain]["server_opts"]
    obj_id = testdoc_server_opts.insert({"domain":"yao","create_tm":datetime.datetime.now(),"finish_tm":datetime.datetime.now(),"result_code":1,"result_txt":"AAA"})
    testdoc_server_opts.insert({"domain":"li","create_tm":datetime.datetime.now(),"finish_tm":datetime.datetime.now(),"result_code":2,"result_txt":"BBB"})

    #测试log_centos_info
    testdoc_centos = d._conn[d._domain]["centos_info"]
    testdoc_centos.insert({"domain":"db_test","logs[]":["new logs"]})

    

