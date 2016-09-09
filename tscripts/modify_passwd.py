#!/usr/bin/env python
#-*- coding:utf-8 -*- 

'''
date: 2016/07/26
role: 修改本机root密码为16位随机密码，并写入数据库passwd_info
usage: modify_passwd.py
'''
from yunwei.operate.prefix import log,execShell,exclusiveLock
from yunwei.operate.mysql import mysqlBase
from yunwei.install.cryptology import cryptoBase
from yunwei.getInfo.parser import parseIni
from yunwei.getInfo.connDb import mysqlConn

import os,sys,re,time,datetime,shutil
import random,string,socket,fcntl,struct
import json

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

###生成随机密码函数
def genPasswd(length):
    chars = string.ascii_letters+string.digits
    source_pd = ''.join([random.choice(chars) for i in range(length)])

    ###返回加密
    cb = cryptoBase(log_path)
    return (source_pd,cb.encrypt_with_certificate(source_pd))

###设置时间窜用户名密码函数
def setLogin(time_string,user_name):
    ###设置系统密码
    secret_dict = {}

    ###生成16位随机密码
    source_pd,sys_pswd = genPasswd(16)
    secret_dict[time_string] = {user_name:sys_pswd}

    ###返回字符串形式
    return (source_pd,json.dumps(secret_dict))

###把密码信息写入数据库函数
def write_pswd_info(user_passwd):
    ###错误码
    pswd_flag = 0

    ###读取mysql连接数据
    prefix_mp = "mp"
    option_mp = "mp_db"
    mc = mysqlConn(log_path) 
    mp_flag,tb_mp,info_mp = mc.getConn(conf_main,conf_sub,prefix_mp,option_mp)
  
    ###错误码跟随
    pswd_flag = mp_flag

    ###连接数据库
    mbm = mysqlBase(log_path,**info_mp)
 
    ###获取服务器ip
    local_ip = get_ip_addr('eth0')
    if not local_ip:
        local_ip = get_ip_addr('bond0')

    ###表字段列表
    passwd_field = ['user_pswd1','user_pswd2','user_pswd3']

    ###查询本机密码信息
    query_sql = 'SELECT user_pswd1,user_pswd2,user_pswd3 FROM %s WHERE server_ip="%s"' %(tb_mp,local_ip)
    local_pswd = mbm.query(query_sql)

    ###判断插入还是更新
    if local_pswd:
        ###获取字段字典
        must_info = local_pswd[0]
        
        ###反转字典，键变成时间
        order_info = {}
        for k,v in zip(passwd_field,must_info):
            if re.search(r'user_pswd',k) and re.search(r'{*}',v):
                v_k = json.loads(v)
                if isinstance(v_k,dict): 
                    v_k = str(v_k.keys()[0])
            else:
                v_k = '0'
          
            order_info[v_k] = k

        ###时间对比，最早的时间替换
        up_field    = order_info.keys()
        order_field = sorted(up_field)[0]
        
        ###字段名
        must_up = order_info.get(order_field)
         
        ###条件组合
        iu_condition = {}
        iu_condition[must_up]      = user_passwd
        iu_condition['enter_time'] = update_time
      
        ###调用mysql类完成更新
        try:
            mbm.update(tb_mp,iu_condition,"server_ip='%s'"%local_ip)
        except:
            logIns.writeLog('error','%s update mysql error' %tb_mp)
            pswd_flag = 2

    else:
        ###插入选项
        in_condition = {}
        in_condition['server_ip']  = local_ip
        in_condition['user_pswd1'] = user_passwd

        ###调用mysql类完成插入
        try:
            mbm.insert(tb_mp,in_condition)
        except:
            logIns.writeLog('error','%s insert mysql error' %tb_mp)    
            pswd_flag = 3

    ###返回错误码
    return pswd_flag

###修改密码函数
def setPasswd(user_name,user_passwd):
    ###错误码
    set_flag = 0 

    ###修改密码
    sys_cmd = "/bin/echo '%s'|/usr/bin/passwd --stdin %s"% (user_passwd,user_name)
    sys_status,sys_result = execShell(sys_cmd)
    if sys_status != 0:
        logIns.writeLog('error','modify %s password error'% k_u)
        set_flag = 1

    return set_flag

if __name__ == "__main__":
    ###脚本名
    script_name = os.path.basename(__file__)
    sub_name    = script_name.split('.')[0]

    ###日志路径
    log_path = '/log/yunwei/%s.log' %script_name

    ###定义日志标识
    logIns  = log('1013',log_path)
    logMain = log('1013','/log/yunwei/yunwei.log')

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

    ###修改root用户的密码信息
    user_name = 'root'

    ###修改用户名密码
    source_pd,user_passwd = setLogin(time_string,user_name)

    ###生成随机密码并写入数据库
    write_code = write_pswd_info(user_passwd)

    ###修改系统密码
    modi_code = write_code
    if write_code == 0:
        modi_code = setPasswd(user_name,source_pd)

    ###确认脚本是否成功
    if modi_code != 0:
        logMain.writeLog('info','%s error end'% script_info)
    else:
        logMain.writeLog('info','%s success end'% script_info)
        print "success"

