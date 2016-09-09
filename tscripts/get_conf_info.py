#!/usr/bin/env python
#-*- coding:utf-8 -*- 

'''
date: 2016/08/03
role: 解析发布系统中的所有项目配置文件，把相关的项目应用信息写入数据库publish_proc_srv
usage: get_conf_info.py
'''
from yunwei.operate.prefix import log,execShell,exclusiveLock
from yunwei.operate.mysql import mysqlBase
from yunwei.getInfo.parser import parseIni,parseNgx
from yunwei.getInfo.connDb import mysqlConn

import os,sys,re,time,datetime,shutil
import socket,fcntl,struct

###获取配置文件中指定项的值函数
def get_conf_kv(prog_path,prog_file):
    ###错误码
    conf_flag = 0

    ###初始化应用字典
    app_dict = {}

    ###抽取的项
    extract_list = ['prog_svn_path','prog_type','sit_srv','uat_srv','pro_srv']

    ###读取文件
    all_lines = []
    with open(prog_path,'rb') as pf:
        all_lines = pf.readlines()

    ###遍历文件取出值组成字典
    for line in all_lines:
        kv_match = re.search(r'(\w+?)=(\S+)',line)
        if kv_match:
            k_p,v_p = kv_match.group(1),kv_match.group(2)
            ###匹配的添加到字典
            if k_p in extract_list:
                app_dict[k_p] = v_p

    ###确认有无取出信息
    if not app_dict:
        conf_flag = 1
        logIns.writeLog('error','%s not have point value' %prog_path)

    ###切割文件名解出项目目录
    try: 
        deal_name = prog_file[:-5].split('-')
        prod_line = deal_name[0]
        sys_name  = deal_name[1]
        prog_name = '-'.join(deal_name[2:])
        ###加入到字典
        app_dict['prod_line'] = prod_line
        app_dict['sys_name'] = sys_name
        app_dict['prog_name'] = prog_name
    except:
        conf_flag = 2
        logIns.writeLog('error','%s not conform to the naming rules' %prog_file)
    
    ###返回指定值
    return (conf_flag,app_dict)

###解析发布系统中的应用配置文件函数
def get_pub_info():
    ###错误码
    prog_flag = 0

    ###publish项
    option_par  = "pub_conf"

    ###读取get_conf_info配置文件获取pub_conf相关变量
    prog_dir = None
    math_pro = None
    try:
        all_optu = parseIni(log_path,conf_sub,option_par)
        try:
            prog_dir = all_optu.getOption('conf_path')
            math_pro = all_optu.getOption('conf_math')
        except:
            prog_flag = 1
            logIns.writeLog('debug','%s pub_conf option not exists' %conf_sub)
    except:
        prog_flag = 2
        logIns.writeLog('debug','%s pub conf file not exists' %conf_sub)

    ###判断publish目录
    if not os.path.isdir(prog_dir):
        prog_flag = 3

    ###所有配置文件指定信息组成字典的列表
    prog_list = []

    ###遍历所有配置文件解析指定字段
    for cf in os.listdir(prog_dir):
        prog_path = os.path.join(prog_dir,cf)
        ###判断是否是需要解析的配置文件
        if os.path.isfile(prog_path) and re.search(r'%s' %math_pro,cf):
            ###调用读文件函数获取指定信息
            conf_code,app_dict = get_conf_kv(prog_path,cf)
            if conf_code == 0:
                prog_list.append(app_dict)    

    ###返回错误码和项目信息列表
    return (prog_flag,prog_list)
             
###把解析的信息入库函数
def write_prog_data(prog_list):
    ###错误码
    write_flag = 0

    ###读取mysql连接数据
    prefix_pb = "pb"
    option_pb = "pb_db"
    mc = mysqlConn(log_path) 
    pb_flag,tb_pb,info_pb = mc.getConn(conf_main,conf_sub,prefix_pb,option_pb)
  
    ###错误码跟随
    write_flag = pb_flag

    ###连接数据库
    mbi = mysqlBase(log_path,**info_pb)

    ###清空表
    trun_sql = 'TRUNCATE TABLE %s' %tb_pb
    mbi.change(trun_sql)

    extract_list = ['prog_svn_path','prog_type','sit_srv','uat_srv','pro_srv']
    ###遍历插入
    for prog_one in prog_list:
        ###插入选项
        in_condition = {}
        in_condition['prod_line']  = prog_one.get('prod_line','')
        in_condition['sys_name']  = prog_one.get('sys_name','')
        in_condition['prog_name']  = prog_one.get('prog_name','')
        in_condition['svn_path']   = prog_one.get('prog_svn_path','')
        in_condition['prog_type']  = prog_one.get('prog_type','')
        in_condition['sit_srv']    = prog_one.get('sit_srv','')
        in_condition['uat_srv']    = prog_one.get('uat_srv','')
        in_condition['pro_srv']    = prog_one.get('pro_srv','')

        ###调用mysql类完成插入
        try:
            mbi.insert(tb_pb,in_condition)
        except:
            write_flag = 1
            logIns.writeLog('error','%s insert into %s error' %(tb_pb,prog_one))
        
    ###返回错误码
    return write_flag                       

if __name__ == "__main__":
    ###脚本名
    script_name = os.path.basename(__file__)
    sub_name    = script_name.split('.')[0]

    ###日志路径
    log_path = '/log/yunwei/%s.log' %script_name

    ###定义日志标识
    logIns  = log('1019',log_path)
    logMain = log('1019','/log/yunwei/yunwei.log')

    script_info = ' '.join(sys.argv)

    ###脚本排它锁
    exclusiveLock(script_name)

    logMain.writeLog('info','%s start'% script_info)

    ###配置文件路径
    conf_pwd  = os.path.join(os.path.dirname(os.path.realpath(__file__)),'conf')
    conf_main = os.path.join(conf_pwd,'common.conf')
    conf_sub  = os.path.join(conf_pwd,'%s.conf' %sub_name)

    ###获取所有配置文件信息的列表
    prog_code,prog_list = get_pub_info()

    ###获取nginx配置信息并写入数据库
    write_code = prog_code
    if prog_code == 0 and prog_list:
        write_code = write_prog_data(prog_list)

    ###确认脚本是否成功
    if write_code != 0:
        logMain.writeLog('info','%s error end'% script_info)
    else:
        logMain.writeLog('info','%s success end'% script_info)
        print "success"
