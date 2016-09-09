#!/usr/bin/env python
#-*- coding:utf-8 -*- 

'''
date: 2016/08/04
role: 从port_process_server和publish_proc_srv数据库中获取服务器应用信息,拼接后写入application_info表中
usage: splice_port_publish.py  
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

###获取port_process_server和publish_proc_srv的数据拼接成列表函数
def splice_app_data():
    ###错误码
    splice_flag = 0

    ###读取ap mysql连接数据
    prefix_ap = "ap"
    option_ap = "ap_db"
    mca = mysqlConn(log_path) 
    ap_flag,tb_ap,info_ap = mca.getConn(conf_main,conf_sub,prefix_ap,option_ap)
  
    ###错误码跟随
    splice_flag = ap_flag

    ###sql
    ap_sql = "SELECT local_ip,port,username,cwd,cmdline FROM %s WHERE is_valid=1" %(tb_ap,)
    ###连接数据库
    mba = mysqlBase(log_path,**info_ap)
    ap_query = mba.query(ap_sql)

    ###读取pb mysql连接数据
    prefix_pb = "pb"
    option_pb = "pb_db"
    mcp = mysqlConn(log_path)
    pb_flag,tb_pb,info_pb = mcp.getConn(conf_main,conf_sub,prefix_pb,option_pb)

    ###错误码跟随
    splice_flag = pb_flag

    ###查询sql
    pb_sql = "SELECT prod_line,sys_name,prog_name,prog_type,sit_srv,uat_srv,pro_srv FROM %s" %(tb_pb,)
    ###连接数据库
    mbp = mysqlBase(log_path,**info_pb)
    pb_query = mbp.query(pb_sql)

    ###遍历两个结果,必须先遍历ap_query
    app_list = []

    for ap in ap_query:
        ###ap变量赋值
        local_ip,port,username,cwd,cmdline = ap
        ###如果是java,则切成tomcat和app
        if re.search(r'java',cmdline):
            if re.search(r'tomcat',cmdline):
                line_tidy = "%s tomcat" %cmdline.split()[0]
            else:
                line_tidy = "%s app" %cmdline.split()[0]
        else:
            line_tidy = cmdline
        
        ###添加标识
        add_sign = 0
        for pb in pb_query:
            ###变量赋值
            prod_line,sys_name,prog_name,prog_type,sit_srv,uat_srv,pro_srv = pb

            ###sit_srv判断添加
            if re.search(r'(\d{1,3}\.)\d{1,3}',sit_srv):
                ###多个ip拆分
                for ip in str(sit_srv).split(','):
                    if ip == local_ip:
                        app_list.append([ip,'sit_srv',prod_line,sys_name,prog_name,prog_type,port,username,cwd,line_tidy]) 
                        add_sign = 1
             
            ###uat_srv判断添加
            if re.search(r'(\d{1,3}\.)\d{1,3}',uat_srv):
                ###多个ip拆分
                for ip in str(uat_srv).split(','):
                    if ip == local_ip:
                        app_list.append([ip,'uat_srv',prod_line,sys_name,prog_name,prog_type,port,username,cwd,line_tidy]) 
                        add_sign = 1
             
            ###pro_srv判断添加
            if re.search(r'(\d{1,3}\.)\d{1,3}',pro_srv):
                ###多个ip拆分
                for ip in str(pro_srv).split(','):
                    if ip == local_ip:
                        app_list.append([ip,'pro_srv',prod_line,sys_name,prog_name,prog_type,port,username,cwd,line_tidy]) 
                        add_sign = 1
        ###如果发布系统没匹配的ip,则加已有信息
        if add_sign == 0:
            app_list.append([local_ip,'test_srv','','','','',port,username,cwd,line_tidy])

    ###如果数据为空代表错误
    if not app_list:
        logIns.writeLog('error','%s and %s get data error' %(tb_ap,tb_pb))
        splice_flag = 1

    ###列表去重
    app_info = []
    app_list = sorted(app_list)
    it_gr    = itertools.groupby(app_list)
    for k,g in it_gr:
        app_info.append(k)
             
    ###返回
    return (splice_flag,app_list)

###写入数据库函数        
def write_event_info(splice_data):
    ###错误码
    wr_flag = 0

    ###获取数据库连接信息
    prefix_ai = "ai"
    option_ai = "ai_db"
    mc = mysqlConn(log_path)
    wr_flag,tb_ai,info_ai = mc.getConn(conf_main,conf_sub,prefix_ai,option_ai)

    ###连接数据库
    mbi = mysqlBase(log_path,**info_ai)

    ###清空表
    trun_sql = 'truncate table %s' %tb_ai
    mbi.change(trun_sql)

    ###逐条插入
    for sd in splice_data:
        server_ip,env_run,prod_line,sys_name,prog_name,prog_type,port,username,cwd,cmdline = sd
        
        ###插入选项
        in_condition = {}
        in_condition['server_ip'] = server_ip
        in_condition['env_run']   = env_run
        in_condition['prod_line'] = prod_line[:45]
        in_condition['sys_name']  = sys_name
        in_condition['prog_name'] = prog_name
        in_condition['prog_type'] = prog_type
        in_condition['port']      = port
        in_condition['username']  = username
        in_condition['cwd']       = cwd
        in_condition['cmdline']   = cmdline[:1500]

        ###调用mysql类完成插入
        try:
            mbi.insert(tb_ai,in_condition)
        except:
            logIns.writeLog('error','%s insert mysql error' %tb_ai)
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
    logIns  = log('1022',log_path)
    logMain = log('1022','/log/yunwei/yunwei.log')

    script_info = ' '.join(sys.argv)

    ###脚本排它锁
    exclusiveLock(script_name)

    logMain.writeLog('info','%s start'% script_info)

    ###配置文件路径
    conf_pwd  = os.path.join(os.path.dirname(os.path.realpath(__file__)),'conf')
    conf_main = os.path.join(conf_pwd,'common.conf')
    conf_sub  = os.path.join(conf_pwd,'%s.conf' %sub_name)

    ###获取数据库数据整理好后返回列表
    splice_code,splice_data = splice_app_data()

    ###写入数据库
    wr_code = splice_code
    if splice_code == 0:
        wr_code = write_event_info(splice_data)
    
    ###确认脚本是否成功
    if wr_code != 0:
        logMain.writeLog('info','%s error end'% script_info)
    else:
        logMain.writeLog('info','%s success end'% script_info)
        print "success"

