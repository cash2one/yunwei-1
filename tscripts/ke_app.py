#!/usr/bin/env python
#-*- coding:utf-8 -*- 

'''
date: 2016/08/04
role: 从yunwei库中的all_server获取所有服务器执行get_app_info.py
usage: ke_app.py 
'''
from yunwei.operate.prefix import log,execShell,exclusiveLock
from yunwei.operate.mysql import mysqlBase
from yunwei.getInfo.parser import parseIni
from yunwei.getInfo.connDb import mysqlConn
from yunwei.operate.centralization import cenManage

import os,sys,re,time,datetime,shutil
import socket,fcntl,struct
reload(sys)
sys.setdefaultencoding("utf-8")

###获取服务器ip函数
def get_host_path(host_option):
    ###错误码
    ip_flag = 0

    ###获取all_server mysql连接数据
    prefix_sv = "sv"
    option_sv = "sv_db"
    mcs = mysqlConn(log_path)
    sv_flag,tb_sv,info_sv = mcs.getConn(conf_main,conf_sub,prefix_sv,option_sv)

    ###sql
    ip_sql = "SELECT server_ip FROM %s WHERE server_ip NOT IN (SELECT server_ip FROM %s WHERE is_del!=0) AND enter_time REGEXP curdate()" %(tb_sv,tb_sv)

    ###连接yunwei数据库
    mbs = mysqlBase(log_path,**info_sv)
    ip_query = mbs.query(ip_sql)

    ###确认有无取到数据
    if not ip_query:
        logIns.writeLog('error','get ip data error from %s' %tb_sv)
        ip_flag = 1
 
    ###创建临时文件目录
    write_dir = '/tmp/yunweitmp'
    if not os.path.isdir(write_dir):
        ###创建目录
        try:
            os.makedirs(write_dir,mode=0777)
        except:
            logIns.writeLog('error','%s mkdir error'% write_dir)
            ip_flag = 2

    ###临时文件名
    write_file = 'app_%s.tmp'%time_string
    
    ###临时文件路径
    write_path = os.path.join(write_dir,write_file)

    ###ini文件的项
    ansible_host = ["[%s]" %host_option]
    for ip in ip_query:
        if re.search(r'(\d{1,3}\.){3}\d{1,3}',ip[0]):
            ansible_host.append("\n%s" %ip[0])

    ###写入临时文件
    with open(write_path,'w') as fw:
        fw.writelines(ansible_host)

    ###返回结果
    return (ip_flag,write_path)

###调用ansible执行获取服务器应用脚本函数
def tool_app_info(host_ansible,host_option):
    ###实例化ansible
    center_manage = cenManage(log_path)

    ###先拷贝
#    center_manage.execAnsible('copy','src=/data/tscripts/ dest=/data/tscripts/ owner=root group=root mode=0777',host_ansible,host_option,10)

    ###调用ansible完成执行
    r_flag,r_out = center_manage.execAnsible('raw','python /data/tscripts/get_app_info.py',host_ansible,host_option,30)
    if isinstance(r_out, dict):
        for k,v in r_out.items():
            if not re.search(r'success',v):
                logIns.writeLog('error','have server exec get app error %s'%k)
                r_flag = 1

    ###返回结果码
    return r_flag

if __name__ == "__main__":
    ###脚本名
    script_name = os.path.basename(__file__)
    sub_name    = script_name.split('.')[0]

    ###日志路径
    log_path = '/log/yunwei/%s.log' %script_name

    ###定义日志标识
    logIns  = log('1021',log_path)
    logMain = log('1021','/log/yunwei/yunwei.log')

    script_info = ' '.join(sys.argv)

    ###脚本排它锁
    exclusiveLock(script_name)

    logMain.writeLog('info','%s start'% script_info)

    ###配置文件路径
    conf_pwd  = os.path.join(os.path.dirname(os.path.realpath(__file__)),'conf')
    conf_main = os.path.join(conf_pwd,'common.conf')
    conf_sub  = os.path.join(conf_pwd,'%s.conf' %sub_name)

    ###时间格式化
    update_time = datetime.datetime.now()
    time_string = update_time.strftime('%y%m%d%H%M%S')
    
    ###ini文件上的选项
    host_option = 'get_app'

    ###获取all_server中服务器ip,并写入临时的ansible host文件中
    ip_code,ip_path = get_host_path(host_option)

    ###调用ansible执行所有服务器上获取应用脚本
    ansi_code = ip_code
    if ip_code == 0:
        ansi_code = tool_app_info(ip_path,host_option)
    
    ###确认脚本是否成功
    if ansi_code != 0:
        logMain.writeLog('info','%s error end'% script_info)
    else:
        logMain.writeLog('info','%s success end'% script_info)
        print "success"
