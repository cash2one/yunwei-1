#!/usr/bin/env python
#-*- coding:utf-8 -*- 

'''
date: 2016/07/18
role: 解析nginx配置文件，把相关的负载信息写入数据库web_domain_server
usage: config_nginx_new.py
'''
from yunwei.operate.prefix import log,execShell,exclusiveLock
from yunwei.operate.mysql import mysqlBase
from yunwei.getInfo.parser import parseIni,parseNgx
from yunwei.getInfo.connDb import mysqlConn

import os,sys,re,time,datetime,shutil
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

###解析nginx配置文件函数
def get_ngx_info(ngx_path):
    ###解析nginx配置文件
    ngx_pars = parseNgx(log_path,ngx_path)
    all_block = ngx_pars.loadFile()

    ###相关变量
    ups_server = {}
    locup_list = []

    ###遍历列表
    for i in all_block:
        ###确认是字典
        if isinstance(i, dict):
            ###遍历字典
            name_u  = i.get('name',False)
            type_u  = i.get('type',False)
            value_u = i.get('value',False)
            param_u = i.get('param',False)
    
            ###取upstream和大server层
            server_ip = ''
            if name_u == 'upstream' or name_u == 'server':
                upstream_name = param_u

                ###获取server_ip组成列表
                server_list   = []
                location_ups  = {}
                server_str    = 0  
                if type_u == 'block' and isinstance(value_u, list):
                    for j in value_u: 
                        if isinstance(j, dict):
                            name_s  = j.get('name',False)
                            type_s  = j.get('type',False)
                            value_s = j.get('value',False)
                            param_s = j.get('param',False)
                 
                            ###取小server层
                            if type_s == 'item' and name_s == 'server' and isinstance(value_s, list):
                                server_ip = value_s[0]
                                server_list.append(server_ip)

                            listen_port = 0
                            ###取listen层
                            if name_s == 'listen' and isinstance(value_s, list):
                                listen_port = value_s[0]

                            ###取server_name层
                            if name_s == 'server_name' and isinstance(value_s, list):
                                server_name = ','.join(value_s)
                                server_str  = '%s,%s' %(server_name,listen_port)  
                                 

                            ###取location层         
                            if type_s == 'block' and  name_s == 'location' and isinstance(value_s, list):
                                name_l  = j.get('name',False)
                                type_l  = j.get('type',False)
                                value_l = j.get('value',False)
                                param_l = j.get('param',False)

                                ###取proxy_pass层
                                if type_l == 'block' and isinstance(value_l, list):
                                    for k in value_l:
                                        if isinstance(k, dict):
                                            name_p  = k.get('name',False)
                                            type_p  = k.get('type',False)
                                            value_p = k.get('value',False)
                                            if type_p == 'item' and isinstance(value_p, list):
                                                if name_p == 'proxy_pass' or name_p == 'fastcgi_pass':
                                                    location_ups[param_l] = value_p[0] 

                ###组成域名,端口 = {location:pass}
                server_loup   = {server_str:location_ups}

                ###把信息汇总
                if name_u == 'upstream':
                    ups_server[upstream_name] = server_list
                    #ups_server[upstream_name] = ','.join(server_list)   
                elif  name_u == 'server':
                    locup_list.append(server_loup)

    ###数据处理
    domain_uploca = {}
    for locup in locup_list:
        if isinstance(locup, dict):
            ups_location = {}
            for k_s,v_s in locup.items():
                for k_l,v_l in v_s.items():
                    ###如果值和upstream匹配，把键放入列表
                    up_match = re.search(r'(http|https)://(\w+)(/|\$|$)' ,v_l)
                    if up_match:
                        upstream_get = up_match.group(2)
                        if upstream_get not in ups_server.keys():
                            upstream_get = 'empty'

                        ups_location.setdefault(upstream_get,[]).append(k_l)
                            
                    up_match = re.search(r'(http|https)://((\d{1,3}\.){3}\d{1,3}:\d+)(/|\$|$)' ,v_l)
                    if up_match:
                        server_ip = up_match.group(2)
                        upstream_get = 'empty'
                        ups_location.setdefault(upstream_get,[]).append(k_l)
                        ups_server.setdefault(k_l,[]).append(server_ip)

                domain_uploca[k_s] = ups_location

    ###返回
    return (domain_uploca,ups_server)
             
###获取配置文件中nginx的相关目录及匹配模式函数
def write_nginx_data():
    ###错误码
    nginx_flag = 0

    ###nginx项
    option_par  = "nginx_conf"

    ###读取main配置文件获取mail相关变量
    ngx_dir  = None
    math_ngx = None
    try:
        all_optu = parseIni(log_path,conf_sub,option_par)
        try:
            ngx_dir  = all_optu.getOption('conf_path')
            math_ngx = all_optu.getOption('conf_math')
        except:
            nginx_flag = 1
            logIns.writeLog('debug','%s nginx option not exists' %conf_sub)
    except:
        nginx_flag = 2
        logIns.writeLog('debug','%s nginx conf file not exists' %conf_sub)

    ###判断nginx目录
    if not os.path.isdir(ngx_dir):
        nginx_flag = 3
        logIns.writeLog('debug','%s nginx dir not exists' %ngx_dir)

    ###获取本机ip
    local_ip = get_ip_addr('eth0')
    if not local_ip:
        local_ip = get_ip_addr('bond0')

    ###读取mysql连接数据
    prefix_dn = "dn"
    option_dn = "dn_db"
    mc = mysqlConn(log_path) 
    nginx_flag,tb_dn,info_dn = mc.getConn(conf_main,conf_sub,prefix_dn,option_dn)
  
    ###连接数据库
    mbi = mysqlBase(log_path,**info_dn)

    ###清掉本ip所有之前记录
    mbi.delete(tb_dn,"local_ip='%s'" %local_ip)

    ###变量nginx目录逐个解析配置文件
    for ngx_file in os.listdir(ngx_dir):
        if re.search(r'%s$' %math_ngx,ngx_file):
            ###组合成路径
            ngx_path = os.path.join(ngx_dir,ngx_file)
            #domain_name  = ngx_file[:-5]

            ###解析文件，返回upstream,server_ip,location,如：search_group_fullSearch 10.10.225.108:8089,10.10.244.19:8089 /Service/,/v2/starnews
            ###返回两个字典            
            domain_upsloca,ups_server = get_ngx_info(ngx_path) 
     
            for k_domain,v_domain in domain_upsloca.items():
                ###循环信息字典，提前数据
                domain_list = str(k_domain).split(',')
                for domain_name in domain_list:
                    if not re.match(r'\d+$',domain_name):
                        for k_us,v_us in ups_server.items():
                            group_name  = k_us
                            server_info = ','.join(v_us)
                            pass_list   = v_domain.get(k_us,[])  
                 
                            ###如果组匹配/则是location
                            if re.search(r'\/',group_name):
                                pass_list  = [group_name]
                                group_name = 'empty'

                            ###去重
                            pass_list = list(set(pass_list))

                            ###列表转字符串
                            pass_info   = ','.join(pass_list)

                            ###更新选项
                            up_condition = {}
                            up_condition['is_valid']  = '0'

                            ###调用mysql类完成更新
                            if group_name == 'empty':
                                mbi.update(tb_dn,up_condition,"local_ip='%s' and domain_name='%s' and pass_info='%s'" %(local_ip,domain_name,pass_info))
                            else:
                                mbi.update(tb_dn,up_condition,"local_ip='%s' and domain_name='%s' and group_name='%s'" %(local_ip,domain_name,group_name))

                            ###插入选项
                            in_condition = {}
                            in_condition['local_ip']    = local_ip
                            in_condition['domain_name'] = domain_name
                            in_condition['group_name']  = group_name
                            in_condition['server_info'] = server_info
                            in_condition['pass_info']   = pass_info
                            ###调用mysql类完成插入
                            mbi.insert(tb_dn,in_condition)
            
    ###返回错误码
    return nginx_flag                       

if __name__ == "__main__":
    ###脚本名
    script_name = os.path.basename(__file__)
    sub_name    = script_name.split('.')[0]

    ###日志路径
    log_path = '/log/yunwei/%s.log' %script_name

    ###定义日志标识
    logIns  = log('1008',log_path)
    logMain = log('1008','/log/yunwei/yunwei.log')

    script_info = ' '.join(sys.argv)

    ###脚本排它锁
    exclusiveLock(script_name)

    logMain.writeLog('info','%s start'% script_info)

    ###配置文件路径
    conf_pwd  = os.path.join(os.path.dirname(os.path.realpath(__file__)),'conf')
    conf_main = os.path.join(conf_pwd,'common.conf')
    conf_sub  = os.path.join(conf_pwd,'%s.conf' %sub_name)

    ###获取nginx配置信息并写入数据库
    write_code = write_nginx_data()

    ###确认脚本是否成功
    if write_code != 0:
        logMain.writeLog('info','%s error end'% script_info)
    else:
        logMain.writeLog('info','%s success end'% script_info)
        print "success"
