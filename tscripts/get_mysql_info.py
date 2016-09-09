#!/usr/bin/env python
#-*- coding:utf-8 -*- 

'''
date: 2016/08/08
role: 从port_process_server数据库中获取mysql服务器,通过逐个连入mysql获取db_name,master,slave等信息
usage: get_mysql_info.py
'''
from yunwei.operate.prefix import log,execShell,exclusiveLock
from yunwei.operate.mail import mailBase
from yunwei.operate.mysql import mysqlBase
from yunwei.getInfo.parser import parseIni,parseNgx
from yunwei.getInfo.connDb import mysqlConn

import os,sys,re,time,datetime,shutil
import socket,fcntl,struct,itertools
reload(sys)
sys.setdefaultencoding("utf-8")

###获取port_process_server表中mysql服务器函数
def all_mysql_ip():
    ###错误码
    mysql_flag = 0

    ###读取ap mysql连接数据
    prefix_ap = "ap"
    option_ap = "ap_db"
    mca = mysqlConn(log_path) 
    ap_flag,tb_ap,info_ap = mca.getConn(conf_main,conf_sub,prefix_ap,option_ap)
  
    ###错误码跟随
    mysql_flag = ap_flag

    ###sql
    ap_sql = "SELECT local_ip FROM %s WHERE is_valid=1 AND process='mysqld' GROUP BY local_ip" %(tb_ap,)
    ###连接数据库
    mba = mysqlBase(log_path,**info_ap)
    ap_query = mba.query(ap_sql)

    ###获取所有mysql的ip
    all_ips = []
    for ip in ap_query:
        if re.search(r'(\d{1,3}\.){3}\d{1,3}',ip[0]):
            all_ips.append(ip[0])

    ###列表去重
    mysql_ips = []
    all_ips = sorted(all_ips)
    it_gr   = itertools.groupby(all_ips)
    for k,g in it_gr:
        mysql_ips.append(k)
             
    ###如果数据为空代表错误
    if not mysql_ips:
        logIns.writeLog('error','%s  get mysql ips error' %(tb_ap,))
        mysql_flag = 1
    
    ###返回
    return (mysql_flag,mysql_ips)

###写入数据库函数        
def write_mysql_info(mysql_ips):
    ###错误码
    wr_flag = 0

    ###获取mysql通用连接信息
    prefix_mi = "mi"
    option_mi = "mi_db"
    mcm = mysqlConn(log_path)
    mi_flag,tb_mi,info_mi = mcm.getConn(conf_main,conf_sub,prefix_mi,option_mi)

    ###获取mysql详细表信息
    prefix_si = "si"
    option_si = "si_db"
    mcs = mysqlConn(log_path)
    si_flag,tb_si,info_si = mcs.getConn(conf_main,conf_sub,prefix_si,option_si)

    ###连接数据库
    mbi = mysqlBase(log_path,**info_si)

    ###逐条插入
    for mysql_ip in mysql_ips:
        ###更新mysql的host
        info_mi['host'] = mysql_ip
        ###连接mysql
        try:
            mbm = mysqlBase(log_path,**info_mi)
        except:
            continue

        ###获取主从服务器信息及库名
        master_sql = 'show slave hosts'
        slave_sql  = 'show slave status'
        alldb_sql  = 'show databases'

        ###逐个查询
        master_data = mbm.query(master_sql)
        slave_data  = mbm.query(slave_sql)
        alldb_data  = mbm.query(alldb_sql)

        ###整理master
        try:
            master_str = master_data[0][0]
        except:
            master_str = ''
        ###整理slave
        try:
            slave_str = slave_data[0][1]
            IO_Run    = slave_data[0][10]
            SQL_Run   = slave_data[0][11]
        except:
            slave_str = ''
            IO_Run    = 'No'
            SQL_Run   = 'No'
        ###整理db
        all_dbs = []
        for db in alldb_data:
            if db[0] not in exclude_db:
                all_dbs.append(db[0])
        dbs_str = ','.join(all_dbs)
 
        ###判断是普通库,主库,从库,主从库
        if master_str:
            if slave_str:
                sync_status = 3   
            else:
                sync_status = 1
        elif slave_str:
            if master_str:
                sync_status = 3
            else:
                sync_status = 2
        else:
            sync_status = 0

        ###确认从库是否正常运行
        is_slave = 1
        if slave_str and (IO_Run == 'No' or SQL_Run == 'No'):
            is_slave = 0

        ###获取ip和port
        db_host = info_mi.get('host','')
        db_port = info_mi.get('port','')

        ###之前的记录更新为无效
        up_condition = {}
        up_condition['is_valid'] = '0'
        mbi.update(tb_si,up_condition,"db_host='%s' AND db_port='%s'"%(db_host,db_port))

        ###插入选项
        in_condition = {}
        in_condition['db_host']     = db_host
        in_condition['db_port']     = db_port
        in_condition['db_name']     = dbs_str
        in_condition['sync_status'] = sync_status
        in_condition['master_ip']   = slave_str
        in_condition['is_slave']    = is_slave

        ###调用mysql类完成插入
        try:
            mbi.insert(tb_si,in_condition)
        except:
            logIns.writeLog('error','%s insert mysql error' %tb_si)
            wr_flag = 1

    ###返回错误码
    return wr_flag

if __name__ == "__main__":
    ###脚本名
    script_name = os.path.basename(__file__)
    sub_name    = script_name.split('.')[0]

    ###日志路径
    log_path = '/log/yunwei/%s.log' %script_name

    ###定义日志标识
    logIns  = log('1023',log_path)
    logMain = log('1023','/log/yunwei/yunwei.log')

    script_info = ' '.join(sys.argv)

    ###脚本排它锁
    exclusiveLock(script_name)

    logMain.writeLog('info','%s start'% script_info)

    ###配置文件路径
    conf_pwd  = os.path.join(os.path.dirname(os.path.realpath(__file__)),'conf')
    conf_main = os.path.join(conf_pwd,'common.conf')
    conf_sub  = os.path.join(conf_pwd,'%s.conf' %sub_name)

    ###mysql排除库
    exclude_db = ['information_schema','mysql','performance_schema','test']

    ###获取数据库数据整理好后返回列表
    mysql_code,mysql_ips = all_mysql_ip()

    ###写入数据库
    wr_code = mysql_code
    if mysql_code == 0:
        wr_code = write_mysql_info(mysql_ips)
    
    ###确认脚本是否成功
    if wr_code != 0:
        logMain.writeLog('info','%s error end'% script_info)
    else:
        logMain.writeLog('info','%s success end'% script_info)
        print "success"

