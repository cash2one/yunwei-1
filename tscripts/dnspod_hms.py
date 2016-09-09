#!/usr/bin/env python
#-*- coding:utf-8 -*- 

'''
date: 2016/07/21
role: 从hms数据库的dnspod_domain，dnspod_record表中获取domain的相关信息写入yunwei库中的dns_domain_server
usage: dnspod_hms.py
'''
from yunwei.operate.prefix import log,execShell,exclusiveLock
from yunwei.operate.mysql import mysqlBase
from yunwei.getInfo.parser import parseIni
from yunwei.getInfo.connDb import mysqlConn

import os,sys,re,time,datetime,shutil
import socket,fcntl,struct,base64
reload(sys)
sys.setdefaultencoding("utf-8")

###从hms数据库中获取dnspod相关数据函数
def get_hms_dnspod():
    ###错误码
    pod_flag = 0

    ###读取dnspod_domain mysql连接数据
    prefix_pd = "pd"
    option_pd = "pd_db"
    mcd = mysqlConn(log_path) 
    pd_flag,tb_pd,info_pd = mcd.getConn(conf_main,conf_sub,prefix_pd,option_pd)
  
    ###读取dnspod_record mysql连接数据
    prefix_pr = "pr"
    option_pr = "pr_db"
    mcr = mysqlConn(log_path) 
    pr_flag,tb_pr,info_pr = mcr.getConn(conf_main,conf_sub,prefix_pr,option_pr)

    ###sql
    pod_sql = "SELECT %s.punycode,%s.name,types,value FROM %s INNER JOIN %s WHERE %s.domain_id=%s.domain_id AND %s.status='enabled' AND enabled=1" %(tb_pd,tb_pr,tb_pd,tb_pr,tb_pd,tb_pr,tb_pr)

    ###连接hms数据库
    mbp = mysqlBase(log_path,**info_pd)
    pod_query = mbp.query(pod_sql)

    if not pod_query:
        logIns.writeLog('error','get data error from %s and %s' %(tb_pd,tb_pr))
        pod_flag = 1

    ###返回结果
    return (pod_flag,pod_query)

###把数据写入dns_domain_server函数
def write_dns_yw(pod_data):
    ###错误码
    dns_flag = 0

    ###获取dns_domain_server mysql连接数据
    prefix_dns = "dns"
    option_dns = "dns_db"
    mc = mysqlConn(log_path)
    dns_flag,tb_dns,info_dns = mc.getConn(conf_main,conf_sub,prefix_dns,option_dns)

    ###连接yunwei数据库
    mbi = mysqlBase(log_path,**info_dns)

    ###清空表
    trun_sql = 'truncate table %s' %tb_dns
    mbi.change(trun_sql)

    ###处理数据后写入yunwei.dns_domain_server表中
    for record in pod_data:
        try:
            ###插入选项
            in_condition = {}
            in_condition['punycode'] = record[0]
            in_condition['name']     = record[1]
            in_condition['type']     = record[2]
            in_condition['value']    = record[3]
            ###调用mysql类完成插入
            mbi.insert(tb_dns,in_condition)
        except:
            logIns.writeLog('error','insert dns data %s error' %(tb_dns,))            
            dns_flag = 1
            break

    ###返回结果
    return dns_flag
  
if __name__ == "__main__":
    ###脚本名
    script_name = os.path.basename(__file__)
    sub_name    = script_name.split('.')[0]

    ###日志路径
    log_path = '/log/yunwei/%s.log' %script_name

    ###定义日志标识
    logIns  = log('1011',log_path)
    logMain = log('1011','/log/yunwei/yunwei.log')

    script_info = ' '.join(sys.argv)

    ###脚本排它锁
    exclusiveLock(script_name)

    logMain.writeLog('info','%s start'% script_info)

    ###配置文件路径
    conf_pwd  = os.path.join(os.path.dirname(os.path.realpath(__file__)),'conf')
    conf_main = os.path.join(conf_pwd,'common.conf')
    conf_sub  = os.path.join(conf_pwd,'%s.conf' %sub_name)

    ###获取hms数据库中dnspod的域名相关信息
    hms_code,pod_data = get_hms_dnspod()

    ###把dnspod的域名相关信息写入yunwei.dns_domain_server
    dns_code = write_dns_yw(pod_data)
    
    ###确认脚本是否成功
    if hms_code != 0 or dns_code != 0:
        logMain.writeLog('info','%s error end'% script_info)
    else:
        logMain.writeLog('info','%s success end'% script_info)
        print "success"
