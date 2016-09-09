#!/usr/bin/env python
#-*- coding:utf-8 -*- 

'''
date: 2016/07/12
role: 从数据库中获取要扫描的ip，扫描出开放的端口写回数据库,所有端口扫描完成后把汇总信息写入每天端口汇报信息表
usage: scan_port.py
'''
from yunwei.operate.prefix import log,execShell,exclusiveLock
from yunwei.install.cryptology import cryptoBase
from yunwei.operate.mysql import mysqlBase
from yunwei.getInfo.parser import parseIni

from multiprocessing import cpu_count 
import os,sys,re,time,datetime,multiprocessing,threading
import nmap,json,shutil,base64

###获取数据库连接信息
def get_db_info(conf_file,option_par,prefix):
    ###退出码
    db_flag = 0

    ###判断配置文件是否存在
    if not os.path.isfile(conf_file):
        db_flag = 1
        return (db_flag,'',{})

    ###解析数据库信息
    all_optu   = parseIni(log_path,conf_file,option_par)
    try:
        info_mysql = all_optu.getOption('%s_host' %prefix,'%s_user' %prefix,'%s_passwd' %prefix,'%s_db' %prefix,'%s_tb' %prefix,'%s_port' %prefix,'%s_charset' %prefix)
    except:
        info_mysql = {}

    ###判断配置文件
    db_record = ['%s_host' %prefix,'%s_user' %prefix,'%s_passwd' %prefix,'%s_db' %prefix,'%s_port' %prefix,'%s_charset' %prefix]
    tb_name   = info_mysql.get('%s_tb' %prefix,'')

    ###把符合的信息重新组成字典
    info_dic = {}
    for k,v in info_mysql.items():
        if k in db_record:
            info_dic[k] = v

    ###如果取出的数据为空
    if not info_dic:
        logIns.writeLog('error','get mysql config error for %s'% conf_file)
        db_flag = 2

    return (db_flag,tb_name,info_dic)

###获取数据库数据信息
def get_conn_info(prefix_flag,option_par):
    ###获取数据库连接
    db_flag_m,tb_name_m,info_dic_m = get_db_info(conf_main,option_par,prefix_flag)
    db_flag_s,tb_name_s,info_dic_s = get_db_info(conf_sub,option_par,prefix_flag)

    ###导入解密模块
    cb = cryptoBase(log_path)

    ###把错误码合并
    db_flag_h = 0
    if db_flag_m != 0 and db_flag_s != 0:
        db_flag_h = 1
 
    ###把表信息合并
    if tb_name_s:
        tb_name_m = tb_name_s

    ###把数据库连接信息合并
    info_dic_m.update(info_dic_s)

    ###把字段转换成mysql统一字段
    info_dic_h = {}
    for k,v in info_dic_m.items():
        k_n = k.split('_')[-1]
        ###如果是passwd的需要解密
        if k_n == 'passwd':
            try:
                v_n = cb.decrypt_with_certificate(v)
            except:
                v_n = base64.b64decode(v)
        else:
            v_n = v

        ###组成可以连接数据库的字典
        info_dic_h[k_n] = v_n 

    ###返回错误码，表名，连接数据库字典
    return (db_flag_h,tb_name_m,info_dic_h)

###获取扫描ip函数
def get_ip_scan():
    ###获取连接数据库参数
    prefix_flag = "hms"
    option_par  = "hms_db"
    db_flag,tb_name,info_dic = get_conn_info(prefix_flag,option_par)

    ###连接数据库
    query_sql = "SELECT eip FROM %s WHERE eip REGEXP '[1-9]'" %tb_name
    mb = mysqlBase(log_path,**info_dic)
    ip_info = mb.query(query_sql)

    ###获取的数据库信息转成ip列表
    ip_list = []
    for ip in ip_info:
        if re.search(r'(\d{1,3}.){3}\d{1,3}',ip[0]):
            ip_list.append(ip[0])

    return (db_flag,ip_list)

###扫描端口函数
def port_nmap(point_ip,port_range):
    ###调用nmap模块
    nm    = nmap.PortScanner()
    ports = port_range.split(',')

    ###把范围的端口整理出来
    black_ports = []
    for port in ports:
        ###正则取出返回的边界
        port_match = re.match(r'(\d+)-(\d+)',port)   
        if port_match:
            port_min = int(port_match.group(1))
            port_max = int(port_match.group(2))
            
            ###添加到需要扫描端口的列表中
            black_ports.extend(range(port_min,port_max))
        
        ###数字直接加入
        elif re.match(r'\d+',port):
            black_ports.append(int(port))
            
    ###逐个扫描端口 
    open_list = []
    for port in black_ports:
        s_p = nm.scan(str(point_ip),str(port))
        try:
            port_state = s_p['scan'][str(point_ip)]['tcp'][int(port)]['state']
        except:
            port_state = "close"
  
        ###判断端口状态
        if port_state == 'open':
            open_list.append(port)

    ###排序
    for i in range(0,len(open_list)):
        open_list[i] = str(open_list[i])

    ###串接
    open_str = ','.join(open_list)

    ###前缀flag
    prefix_flag = "yw"
    option_par  = "yw_db"
    db_flag,tb_name,info_dic = get_conn_info(prefix_flag,option_par)

    ###更新选项
    up_condition = {}
    up_condition['server_ip']  = "%s" %(point_ip,)
    up_condition['open_port']  = "%s" %(open_str,)
    up_condition['enter_time'] = "%s" %(update_time,)

    ###写入数据库
    mb = mysqlBase(log_path,**info_dic)
    ch_num = mb.insert(tb_name,up_condition)

    ###返回结果
    return ch_num

###获取all_server中的ip
def get_ip_ywsql():
    ###读取配置文件
    prefix_sv = "sv"
    option_sv = "sv_db"
    sv_flag,tb_sv,info_sv = get_conn_info(prefix_sv,option_sv)

    ###查询附加的服务器
    query_sql = "SELECT mapping_ip FROM %s WHERE is_add=1" %(tb_sv,)

    ###连接附加ip数据库进行查询
    mbq = mysqlBase(log_path,**info_sv)
    server_ips = mbq.query(query_sql)
    
    ###处理附加服务器数据
    add_ips = []
    for sip in server_ips:
        if re.search(r'(\d{1,3}.){3}\d{1,3}',sip[0]):
            add_ips.append(sip[0])

    ###返回添加的ip
    return add_ips

###从扫描表中获取信息写入端口信息表
def write_port_info():
    ###端口扫描结果表的连接数据库获取
    prefix_yw = "yw"
    option_yw = "yw_db"
    yw_flag,tb_yw,info_yw = get_conn_info(prefix_yw,option_yw)

    ###格式化时间
    format_today = update_time.strftime('%Y-%m-%d')

    ###查询服务器数量
    snum_sql = "SELECT COUNT(DISTINCT server_ip) AS servers FROM %s WHERE DATE_FORMAT(enter_time,'%%Y-%%m-%%d')='%s'" %(tb_yw,format_today)
    ###查询端口数
    pnum_sql = "SELECT open_port FROM %s WHERE DATE_FORMAT(enter_time,'%%Y-%%m-%%d')='%s'" %(tb_yw,format_today)

    ###连接数据库进行查询
    mbq = mysqlBase(log_path,**info_yw)
    server_num = mbq.query(snum_sql)
    oport_num  = mbq.query(pnum_sql)

    ###处理服务器数量数据
    try:
        servers = server_num[0][0]
    except:
        servers = 0
     
    ###处理端口数据
    all_ports = []
    for oport in oport_num:
        if re.search(r'\d',oport[0]):
            all_ports.append(oport[0])

    ###确认高危端口数量
    wports = len(all_ports)
    
    ###高危端口信息表的连接数据库获取
    prefix_wp = "wp"
    option_wp  = "wp_db"
    wp_flag,tb_wp,info_wp = get_conn_info(prefix_wp,option_wp)

    ###更新选项
    up_condition = {}
    up_condition['servers']    = "%s" %(servers,)
    up_condition['warn_ports'] = "%s" %(wports,)
    up_condition['enter_time'] = "%s" %(update_time,)

    ###写入数据库
    mbi = mysqlBase(log_path,**info_wp)
    in_num = mbi.insert(tb_wp,up_condition)

    ###返回结果
    return in_num

if __name__ == "__main__":
    ###脚本名
    script_name = os.path.basename(__file__)
    sub_name    = script_name.split('.')[0]

    ###日志路径
    log_path = '/log/yunwei/%s.log' %script_name

    ###定义日志标识
    logIns  = log('1006',log_path)
    logMain = log('1006','/log/yunwei/yunwei.log')

    script_info = ' '.join(sys.argv)

    ###脚本排它锁
    exclusiveLock(script_name)

    logMain.writeLog('info','%s start'% script_info)

    ###配置文件路径
    conf_pwd  = os.path.join(os.path.dirname(os.path.realpath(__file__)),'conf')
    conf_main = os.path.join(conf_pwd,'common.conf')
    conf_sub  = os.path.join(conf_pwd,'%s.conf' %sub_name)
    ###时间
    update_time = datetime.datetime.now()

    ###获取扫描的ip
    db_code,all_ip = get_ip_scan()

    ###获取添加的ip
    add_ip = get_ip_ywsql()

    ###合并ip
    all_ip.extend(add_ip)

    ###扫描的端口范围
    option_sm  = "sm_info"
    range_sm   = "black_port"
    all_optu   = parseIni(log_path,conf_sub,option_sm)
    port_range = all_optu.getOption(range_sm)

    ###定制进程数
    pro_num = 4 * cpu_count()
    pool = multiprocessing.Pool(processes = pro_num)
    
    ###并发调用扫描端口函数
    for ip in all_ip:
        pool.apply_async(port_nmap, (ip,port_range, )) 

    pool.close()
    pool.join()

    ###把汇总信息写入port_info表
    num_in = write_port_info()

    ###确认脚本是否成功
    if db_code != 0 or num_in == 0:
        logMain.writeLog('info','%s error end'% script_info)
    else:
        logMain.writeLog('info','%s success end'% script_info)
        print "success"
