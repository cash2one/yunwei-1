#!/usr/bin/env python
#-*- coding:utf-8 -*- 

'''
date: 2016/07/20
role: 确认可用内存大于swap，swap使用比例较大的情况，清理swap
usage: swap_clear.py
'''
from yunwei.operate.prefix import log,execShell,exclusiveLock
from yunwei.operate.mysql import mysqlBase
from yunwei.getInfo.parser import parseIni
from yunwei.getInfo.connDb import mysqlConn

import os,sys,re,time,datetime,shutil,psutil
import socket,fcntl,struct

###获取相应网卡的ip函数
def get_ip_addr(ifname):
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        ipaddr = socket.inet_ntoa(fcntl.ioctl(
            s.fileno(),
            0x8915,  # SIOCGIFADDR
            struct.pack('256s', ifname[:15])
        )[20:24])
    except IOError:
        logIns.writeLog('error',"get %s ip error!" % ifname)
        ipaddr = ''

    return ipaddr

###获取剩余内存和swap的使用率函数
def get_mem_swap():
    ###通知级别
    notice_level = 0

    ###获取剩余内存
    space_mem = psutil.virtual_memory().available

    ###获取swap使用大小
    use_swap  = psutil.swap_memory().used    

    ###获取swap使用率
    per_swap  = psutil.swap_memory().percent

    ###判断重启swap条件
    if per_swap < 5:
        ms_flag = ''
    else:
        if space_mem > use_swap + 200000000:
            ###调用shell命令
            swap_cmd = 'swapoff -a && swapon -a'
            swap_status,swap_result = execShell(swap_cmd)
            if swap_status != 0:
                logIns.writeLog('error','%s exec swap clear error'% local_ip)
                ms_flag = 'exec swap clear error'
            else:
                logIns.writeLog('error','%s exec swap clear finished'% local_ip)
                ms_flag = 'exec swap clear finished'
        else:
            logIns.writeLog('error','%s mem is less than use swap'% local_ip)
            ms_flag = 'mem is less than use swap'
            notice_level = 1

    ###返回
    return (ms_flag,notice_level)
  
###写入数据库函数        
def write_event_info(event_info,notice_level):   
    ###错误码
    wr_flag = 0

    ###如果事件为空,直接返回
    if not event_info:
        return wr_flag

    ###获取数据库连接信息
    prefix_er = "er"
    option_er = "er_db"
    mc = mysqlConn(log_path)
    event_flag,tb_er,info_er = mc.getConn(conf_main,conf_sub,prefix_er,option_er)

    ###连接数据库
    mbi = mysqlBase(log_path,**info_er)

    ###插入选项
    in_condition = {}
    in_condition['local_ip']     = local_ip
    in_condition['event_info']   = event_info
    in_condition['event_flag']   = sub_name
    in_condition['notice_level'] = notice_level

    ###调用mysql类完成插入
    try:
        mbi.insert(tb_er,in_condition)    
    except:
        logIns.writeLog('error','%s insert mysql error' %tb_er)
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
    logIns  = log('1010',log_path,display=True)
    logMain = log('1010','/log/yunwei/yunwei.log',display=True)

    script_info = ' '.join(sys.argv)

    ###脚本排它锁
    exclusiveLock(script_name)

    logMain.writeLog('info','%s start'% script_info)

    ###配置文件路径
    conf_pwd  = os.path.join(os.path.dirname(os.path.realpath(__file__)),'conf')
    conf_main = os.path.join(conf_pwd,'common.conf')
    conf_sub  = os.path.join(conf_pwd,'%s.conf' %sub_name)

    ###获取本机ip
    local_ip = get_ip_addr('eth0')
    if not local_ip:
        local_ip = get_ip_addr('em3')

    ###获取本机的内存及swap信息
    event_info,notice_level = get_mem_swap()

    ###写入数据库
    wr_code = write_event_info(event_info,notice_level)

    ###确认脚本是否成功
    if wr_code != 0:
        logMain.writeLog('info','%s error end'% script_info)
    else:
        logMain.writeLog('info','%s success end'% script_info)
        print "success"
