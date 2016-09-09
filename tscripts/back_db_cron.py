#!/usr/bin/env python
#-*- coding:utf-8 -*- 

'''
date: 2016/09/08
role: 从sql_info表中获取需要备份的数据库ip,通过ansible检查crontab，并记录到data_get_info表中
usage: back_db_cron.py
'''
from yunwei.operate.prefix import log,execShell,exclusiveLock
from yunwei.install.cryptology import cryptoBase
from yunwei.operate.mail import mailBase
from yunwei.operate.mysql import mysqlBase
from yunwei.getInfo.parser import parseIni
from yunwei.getInfo.connDb import mysqlConn

import os,sys,re,time,datetime,shutil
import socket,fcntl,struct,base64
reload(sys)
sys.setdefaultencoding("utf-8")

###获取需要备份的mysql服务器函数
def get_sql_back():
    ###错误码
    gsb_flag = 0

    ###读取sql_info mysql连接数据
    prefix_si = "si"
    option_si = "si_db"
    mcs = mysqlConn(log_path)
    si_flag,tb_si,info_si = mcs.getConn(conf_main,conf_sub,prefix_si,option_si

    ###普通库sql
    single_sql = "SELECT db_host FROM %s WHERE is_valid=1 AND sync_status=0" %tb_si

    ###主库的全部从库,包括同步异常的从库
    slave_sql  = "SELECT db_host FROM %s WHERE master_ip IN (SELECT db_host FROM %s WHERE is_valid=1 AND sync_status=1) AND is_valid=1 AND sync_status=2" %(tb_si,tb_si)

    ###异常从库,用来查找没有其它从库的主库
    smas_sql   = "SELECT db_host,master_ip FROM %s WHERE master_ip IN (SELECT db_host FROM %s WHERE is_valid=1 AND sync_status=1) AND is_valid=1 AND sync_status=2 AND is_slave=0" %(tb_si,tb_si)

    ###连接数据库
    mbs = mysqlBase(log_path,**info_si)
    single_query = mbp.query(single_sql)
    slave_query  = mbp.query(slave_sql)
    smas_query   = mbp.query(smas_sql)

    ###把所有ip获取出来
    back_ips = []
    fro sing in single_query:
         


    ###创建临时文件目录
    write_dir = '/tmp/yunweitmp'
    if not os.path.isdir(write_dir):
        ###创建目录
        try:
            os.makedirs(write_dir,mode=0777)
        except:
            logIns.writeLog('error','%s mkdir error'% write_dir)
            spv_flag = 2

    ###临时文件名
    write_file = '%s_%s.tmp'%(pub_prog,format_date)

    ###临时文件路径
    write_path = os.path.join(write_dir,write_file)

    ###ini文件的项
    ansible_host = ["[%s]" %host_option]
    for ip in pp_query:
        if re.search(r'(\d{1,3}\.){3}\d{1,3}',ip[0]):
            ansible_host.append("\n%s" %ip[0])

    ###写入临时文件
    with open(write_path,'w') as fw:
        fw.writelines(ansible_host)

    ###返回错误码
    return (spv_flag,write_path)


if __name__ == "__main__":
    ###脚本名
    script_name = os.path.basename(__file__)
    sub_name    = script_name.split('.')[0]

    ###日志路径
    log_path = '/log/yunwei/%s.log' %script_name

    ###定义日志标识
    logIns  = log('1039',log_path)
    logMain = log('1039','/log/yunwei/yunwei.log')

    script_info = ' '.join(sys.argv)

    ###脚本排它锁
    exclusiveLock(script_name)

    logMain.writeLog('info','%s start'% script_info)

    ###配置文件路径
    conf_pwd  = os.path.join(os.path.dirname(os.path.realpath(__file__)),'conf')
    conf_main = os.path.join(conf_pwd,'common.conf')
    conf_sub  = os.path.join(conf_pwd,'%s.conf' %sub_name)

    ###获取sql_info表中数据
    gsb_code,sql_ips,sql_path = get_sql_back()

    ###通过ansible加备份crontab

    ###把db的ip写入表data_get_info
    
    ###确认脚本是否成功
    if fin_code != 0:
        logMain.writeLog('info','%s error end'% script_info)
    else:
        logMain.writeLog('info','%s success end'% script_info)
        print "success"

