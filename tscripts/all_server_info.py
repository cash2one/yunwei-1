#!/usr/bin/env python
#-*- coding:utf-8 -*- 

'''
date: 2016/07/30
role: 从hms数据库的ucloud_host_instance表中获取ip的相关信息写入yunwei库中的all_server
usage: all_server_info.py
'''
from yunwei.operate.prefix import log,execShell,exclusiveLock
from yunwei.operate.mysql import mysqlBase
from yunwei.getInfo.parser import parseIni
from yunwei.getInfo.connDb import mysqlConn

import os,sys,re,time,datetime,shutil
import socket,fcntl,struct,base64
reload(sys)
sys.setdefaultencoding("utf-8")

###从hms数据库中获取ip相关数据函数
def get_hms_instance():
    ###错误码
    ucloud_flag = 0

    ###读取ucloud_host_instance mysql连接数据
    prefix_hms = "hms"
    option_hms = "hms_db"
    mch = mysqlConn(log_path) 
    hms_flag,tb_hms,info_hms = mch.getConn(conf_main,conf_sub,prefix_hms,option_hms)
  
    ###sql
    server_sql = "SELECT private_ip,eip,name FROM %s" %(tb_hms,)

    ###连接hms数据库
    mbh = mysqlBase(log_path,**info_hms)
    server_query = mbh.query(server_sql)

    if not server_query:
        logIns.writeLog('error','get data error from %s' %tb_hms)
        ucloud_flag = 1

    ###返回结果
    return (ucloud_flag,server_query)

###把数据写入all_server函数
def write_server_yw(server_data):
    ###错误码
    server_flag = 0

    ###获取all_server mysql连接数据
    prefix_sv = "sv"
    option_sv = "sv_db"
    mcs = mysqlConn(log_path)
    sv_flag,tb_sv,info_sv = mcs.getConn(conf_main,conf_sub,prefix_sv,option_sv)

    ###连接yunwei数据库
    mbi = mysqlBase(log_path,**info_sv)

    ###处理数据后写入yunwei.all_server表中
    for record in server_data:
        try:
            ###插入选项
            in_condition = {}
            in_condition['server_ip']  = record[0]
            in_condition['mapping_ip'] = record[1]
            in_condition['host_name']  = record[2]
            ###调用mysql类完成插入
            mbi.insert(tb_sv,in_condition)
        except:
            logIns.writeLog('error','insert server data %s error' %(tb_sv,))            
            server_flag = 1

    ###返回结果
    return server_flag
  
if __name__ == "__main__":
    ###脚本名
    script_name = os.path.basename(__file__)
    sub_name    = script_name.split('.')[0]

    ###日志路径
    log_path = '/log/yunwei/%s.log' %script_name

    ###定义日志标识
    logIns  = log('1016',log_path)
    logMain = log('1016','/log/yunwei/yunwei.log')

    script_info = ' '.join(sys.argv)

    ###脚本排它锁
    exclusiveLock(script_name)

    logMain.writeLog('info','%s start'% script_info)

    ###配置文件路径
    conf_pwd  = os.path.join(os.path.dirname(os.path.realpath(__file__)),'conf')
    conf_main = os.path.join(conf_pwd,'common.conf')
    conf_sub  = os.path.join(conf_pwd,'%s.conf' %sub_name)

    ###获取hms数据库中server相关信息
    hms_code,server_data = get_hms_instance()

    ###把server相关信息写入yunwei.all_server
    server_code = hms_code
    if hms_code == 0:
        server_code = write_server_yw(server_data)
    
    ###确认脚本是否成功
    if server_code != 0:
        logMain.writeLog('info','%s error end'% script_info)
    else:
        logMain.writeLog('info','%s success end'% script_info)
        print "success"
