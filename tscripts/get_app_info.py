#!/usr/bin/env python
#-*- coding:utf-8 -*- 

'''
date: 2016/08/02
role: 获取端口、进程名、进程的启动路径写入数据库port_process_server
usage: get_app_info.py
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

###获取端口、进程名、进程路径函数
def port_proc_pid(port_range,proc_range):
    ###错误码
    get_flag = 0

    ###整理端口成列表
    fit_ports = []
    ports = port_range.split(',')
    for port in ports:
        ###正则取出返回的边界
        port_match = re.match(r'(\d+)-(\d+)',port)
        if port_match:
            port_min = int(port_match.group(1))
            port_max = int(port_match.group(2))

            ###添加到需要扫描端口的列表中
            fit_ports.extend(range(port_min,port_max))

        ###数字直接加入
        elif re.match(r'\d+',port):
            fit_ports.append(int(port))

    ###整理进程成列表
    fit_proc = proc_range.split(',')
    
    ###调用shell命令
    site_cmd = "ss -npl|awk '{for(i=1;i<=NF;i+=1){if($i~/Address:Port/ && $(i-1)~/Local/){print i-1}}}'"
    site_status,site_result = execShell(site_cmd)
    if site_status != 0:
        logIns.writeLog('error','%s exec ss site error'% local_ip)
        get_flag = 1

    ###确定ss端口的列位
    port_site = int(site_result)
    info_site = port_site + 2

    port_cmd = "ss -npl|awk '{print $%s}'" %(port_site,)
    port_status,port_result = execShell(port_cmd)
    if port_status != 0:
        logIns.writeLog('error','%s exec ss port error'% local_ip)
        get_flag = 2


    ###获取进程信息
    proc_cmd = """ss -npl|awk '{for(i=%s;i<=NF;i++)printf  $i" ";printf "\\n"}'""" %(info_site,)
    proc_status,proc_result = execShell(proc_cmd)
    if proc_status != 0:
        logIns.writeLog('error','%s exec ss proc error'% local_ip)
        get_flag = 3

    ###定义端口,进程名,进程路径列表
    app_dict = {}

    ###pid列表
    exclude_pids = []
    
    ###处理ss运行结果
    port_list = port_result.split('\n')
    proc_list = proc_result.split('\n')
    for ss_list in zip(port_list,proc_list):
        ss_str = ' '.join(ss_list)
        ###匹配端口,进程名,pid
        ss_match = re.search(r'(\d+)\s+users:\(\(\"(.+?)\"\,(\d+)\,',ss_str)
        if ss_match:
            in_port,in_proc,in_pid = ss_match.group(1),ss_match.group(2),ss_match.group(3)
            ###把端口转成int型
            in_port = int(in_port)
            ###端口匹配的加入列表
            if in_port in fit_ports:
                pp  = psutil.Process(int(in_pid))

                username = pp.username
                if not isinstance(username, str):
                    username = pp.username()

                try:
                    cwd_str  = pp.getcwd()
                except:
                    try:
                        cwd_str  = pp.cwd()
                    except:
                        pass
                cmd_list = pp.cmdline
                if not isinstance(cmd_list, list):
                    cmd_list = pp.cmdline()

                cmd_str  = ' '.join(cmd_list)

                ###添加
                app_dict[in_port] = [in_proc,username,cwd_str,cmd_str]
                exclude_pids.append(int(in_pid))

    ###获取所有进程pid
    try:
        pid_list = psutil.get_pid_list()  
    except:
        pid_list = psutil.pids()

    ###循环所有pid抓到进程名符合的进程
    for in_pid in [pids for pids in pid_list if pids not in exclude_pids]:
        ###虚拟端口前加00
        in_port = '00%s' %in_pid 

        ###获取进程名
        eachProcess = psutil.Process(in_pid)
        in_proc     = eachProcess.name
        if not isinstance(in_proc, str):
             in_proc = eachProcess.name()
        
        ###文件名匹配的加入列表
        for str_proc in fit_proc:
            if re.search(r'%s' %str_proc,in_proc,re.I):
                ###获取psutil中的cwd和cmdline
                pp  = psutil.Process(int(in_pid))

                username = pp.username
                if not isinstance(username, str):
                    username = pp.username()

                try:
                    cwd_str  = pp.getcwd()
                except:
                    try:
                        cwd_str  = pp.cwd()
                    except:
                        pass
                cmd_list = pp.cmdline
                if not isinstance(cmd_list, list):
                    cmd_list = pp.cmdline()

                cmd_str  = ' '.join(cmd_list)

                ###排除自身、ansible、puuppet、salt这样的关键字
                add_no = True
                exclude_keys = ['ansible','puuppet','salt',script_name]
                for exclude_key in exclude_keys:
                    if re.search(r'%s' %exclude_key,cmd_str):
                       add_no = False
                       break

                ###确认是否添加
                if add_no:
                    app_dict[in_port] = [in_proc,username,cwd_str,cmd_str]

    ###返回
    return (get_flag,app_dict)
  
###把信息写入数据库函数
def write_app_info(app_info):
    ###错误码
    wr_flag = 0

    ###获取数据库连接信息
    prefix_ap = "ap"
    option_ap = "ap_db"
    mc = mysqlConn(log_path)
    ap_flag,tb_ap,info_ap = mc.getConn(conf_main,conf_sub,prefix_ap,option_ap)

    ###连接数据库
    mbi = mysqlBase(log_path,**info_ap)

    ###如果没抓到应用则只插入ip
    if not app_info:
        in_condition = {}
        in_condition['local_ip'] = local_ip
        in_condition['is_valid'] = '0'
        ###调用mysql类完成插入
        try:
            mbi.insert(tb_ap,in_condition)
        except:
            logIns.writeLog('error','%s insert mysql error' %tb_ap)
            wr_flag = 1
    
    ###清掉本ip所有之前记录
    mbi.delete(tb_ap,"local_ip='%s'" %local_ip)
    
    ###逐条插入数据库
    for k,v in app_info.items():
        ###更新选项
        up_condition = {}
        up_condition['is_valid']  = '0'

        ###调用mysql类完成更新
        mbi.update(tb_ap,up_condition,"local_ip='%s' and (port='%s' or port like '00%%')" %(local_ip,k))
        
        ###插入选项
        in_condition = {}
        in_condition['local_ip'] = local_ip
        in_condition['port']     = k
        in_condition['process']  = v[0][:45]
        in_condition['username'] = v[1]
        in_condition['cwd']      = v[2]
        in_condition['cmdline']  = v[3][:2000]

        ###调用mysql类完成插入
        try:
           mbi.insert(tb_ap,in_condition)
        except:
            logIns.writeLog('error','%s insert mysql error' %tb_ap)
            wr_flag = 2

    ###返回错误码
    return wr_flag
    
if __name__ == "__main__":
    ###脚本名
    script_name = os.path.basename(__file__)
    sub_name    = script_name.split('.')[0]

    ###日志路径
    log_path = '/log/yunwei/%s.log' %script_name

    ###定义日志标识
    logIns  = log('1018',log_path)
    logMain = log('1018','/log/yunwei/yunwei.log')

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
        local_ip = get_ip_addr('bond0')

    ###获取匹配的端口及进程名
    option_app = "app_info"
    range_port = "fit_port"
    range_proc = "fit_proc"
    all_optu   = parseIni(log_path,conf_sub,option_app)
    port_range = all_optu.getOption(range_port)
    proc_range = all_optu.getOption(range_proc)
    
    ###获取本机的端口、进程名、pid信息
    get_code,app_info = port_proc_pid(port_range,proc_range)

    ###写入数据库
    wr_code = get_code
    if get_code == 0:
        wr_code = write_app_info(app_info)

    ###确认脚本是否成功
    if wr_code != 0:
        logMain.writeLog('info','%s error end'% script_info)
    else:
        logMain.writeLog('info','%s success end'% script_info)
        print "success"
