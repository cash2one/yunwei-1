#!/usr/bin/env python
#-*- coding:utf-8 -*-

'''
date: 2016/07/11
role: mysql的增删改查类
usage: m = mysqlBase(host='xxx',db='xxx',user='xxx',pwd='xxx')    实例化
       m.insert('core',{'host_name':'ccc','process_name':'ddd','ip_addr':'192.168.136.41','status':'4'})
       m.update('table',{'field1':'value1','field2':'value2'},'id=1')  更新表名  字段名：值  条件   
       m.delete('core','status=5 and id=12')
       m.change("update core set a='aaa' where id=1")   可以多条插入
       m.query("select * from core")
'''

from __future__ import absolute_import

from yunwei.operate.prefix import log
logIns = log('112')

import sys,warnings
try:
    import MySQLdb
except ImportError, e:
    logIns.writeLog('error','MySQLdb import error')
    raise ImportError('112,No module named MySQLdb')

###mysql操作类
class mysqlBase:
    ###连接数据库
    def __init__(self,log_path,**args):
        ###log_path为日志写入文件
        logIns = log('112',log_path)

        ###获取参数
        self.host = args.get('host','localhost')
        self.user = args.get('user')
        self.pswd = args.get('passwd')
        self.db   = args.get('db','mysql')
        self.port = args.get('port','3306')
        self.charset = args.get('charset','utf8')
        self.read_default_file = args.get('read_default_file','/etc/my.cnf')
 
        try:
            self.conn = MySQLdb.connect(host=self.host,user=self.user,passwd=self.pswd,db=self.db,port=int(self.port),charset=self.charset)
        except:
            try:
                from yunwei.getInfo.parser import parseIni
                all_optu = parseIni(log_path,self.read_default_file,'client')
                socket   =  all_optu.getOption('socket')
                self.conn = MySQLdb.connect(host=self.host,user=self.user,passwd=self.pswd,db=self.db,port=int(self.port),charset=self.charset,unix_socket=socket)
            except:
                logIns.writeLog('error','%s mysql connect error'% self.host)
                raise ValueError('112,mysql connect error %s'% self.host)

        self.curs = self.conn.cursor()
        self.curs.execute('SET NAMES utf8')

    ###释放资源
    def __del__(self):
        self.curs.close()
        self.conn.close() 

    ###插入
    def insert(self,table,data):
        _field = ','.join(['`%s`'%(k_insert) for k_insert in data.keys()])
        ###_value = ','.join(["'%s'"%(str(v_insert).replace("'", "\'")) for v_insert in data.values()])
        _value = ','.join(["'%s'"%(self.conn.escape_string(str(v_insert))) for v_insert in data.values()])
        ###拼接成sql语句
        _sql   = 'INSERT INTO `%s`(%s) VALUES(%s)'%(table,_field,_value)

        ###执行
        self.curs.lastrowid = 0
        try:
            self.curs.execute(_sql)
            ###提交
            self.conn.commit()
        except:
            self.conn.rollback()
            logIns.writeLog('error','%s insert error'% _sql)
            raise ValueError('112,insert error %s'% _sql)

        return self.curs.lastrowid

    ###更新
    def update(self,table,data,condition):
        _field = ','.join(["`%s`='%s'"%(k_update,self.conn.escape_string(str(data[k_update]))) for k_update in data]) 
        ###_field = ','.join(["`%s`='%s'"%(k_update,str(data[k_update]).replace("'", "\'")) for k_update in data]) 

        _sql = 'UPDATE `%s` SET %s WHERE %s'%(table,_field,condition)
        
        ###执行
        resNum = 0
        try:
            resNum = self.curs.execute(_sql)
            ###提交
            self.conn.commit()
        except:
            self.conn.rollback()
            logIns.writeLog('error','%s update error'% _sql)
            raise ValueError('112,update error %s'% _sql)

        return resNum

    ###删除
    def delete(self,table,condition):
        _sql = 'DELETE FROM `%s` WHERE %s'% (table,condition)
       
        ###执行
        resNum = 0
        try:
            resNum = self.curs.execute(_sql)
            ###提交
            self.conn.commit()
        except:
            self.conn.rollback()
            logIns.writeLog('error','%s delete error'% _sql)
            raise ValueError('112,delete error %s'% _sql)

        return resNum

    ###直接给修改语句执行
    def change(self,sql,many=False):
        ###过滤unknow table的warning
        warnings.filterwarnings('ignore')
        resNum = 0
        if many:
            try:
                ###多条同时插入
                resNum = self.curs.executemany(sql,many) 
                self.conn.commit()
            except:
                self.conn.rollback()
                logIns.writeLog('error','%s exec error'% sql)
                raise ValueError('112,exec error %s'% sql)
        else:
            try:
                resNum = self.curs.execute(sql)
                ###提交
                self.conn.commit()
            except:
                self.conn.rollback()
                logIns.writeLog('error','%s exec error'% sql)
                raise ValueError('112,exec error %s'% sql)

        return resNum

    ###查询
    def query(self,sql):
        res = ''
        try:
            self.curs.execute(sql)
            res = self.curs.fetchall()
        except:
            logIns.writeLog('error','%s query error'% sql)
        #    raise ValueError('112,query error %s'% sql)

        return res
