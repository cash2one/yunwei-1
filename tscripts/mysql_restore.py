#!/usr/bin/env python
#-*- coding:utf-8 -*- 

'''
date: 2016/08/16
role: 数据库恢复
usage: mysql_restore.py
2016/09/06 如果是innodb引擎的单表恢复,表文件必须存在,否则不支持,考虑使用5.6版本
'''
from yunwei.operate.prefix import log,execShell,exclusiveLock
from yunwei.operate.mysql import mysqlBase
from yunwei.install.cryptology import cryptoBase
from yunwei.getInfo.connDb import mysqlConn
from yunwei.operate.ftp import ftpBase
from yunwei.getInfo.parser import parseIni

import os,sys,re,shutil,datetime,time
import socket,fcntl,struct,base64
from optparse import OptionParser

###参数定义函数
def args_module_des():
    usage  = '''  
    -s(--sevever) 10.6.30.103 -t(--time) 160816-17 -c(--conf) /etc/my.cnf \n
    -t 160816-21\n
    -f /tmp/db_160816-17.tar.gz \n
    -f /tmp/db_160816-17.tar.gz -d db_name -b tb_name\n
    '''

    parser = OptionParser(usage)
    parser.add_option("-s","--server",type="string",default="local",dest="server_entry",
            help="want get server")
    parser.add_option("-t","--time",type="string",default="False",dest="time_entry",
            help="want get time")
    parser.add_option("-f","--file",type="string",default="False",dest="file_entry",
            help="have point file")
    parser.add_option("-c","--conf",type="string",default="/etc/my.cnf",dest="mysql_conf",
            help="mysql config path")
    parser.add_option("-d","--db",type="string",default="",dest="db_name",
            help="db name")
    parser.add_option("-b","--tb",type="string",default="",dest="tb_name",
            help="tb name")

    (options,args) = parser.parse_args()
    ###确认参数是否定义正确
    if (options.time_entry == "False" and options.file_entry == "False"):
        parser.print_usage
        parser.error("-t or -f must defined")

    if (options.time_entry != "False" and options.file_entry != "False"):
        parser.print_usage
        parser.error("-t and -f must defined one")

    ###确认参数个数没有多定义
    if (len(sys.argv) < 2 or len(sys.argv) > 13):
        parser.print_usage
        parser.error("arguments is too much or too little,pls confirmation")

    ###返回值
    return (options.server_entry,options.time_entry,options.file_entry,options.mysql_conf,options.db_name,options.tb_name)

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

###获取mysql连接变量
def get_sql_conf(conf_sql,option_par):
    ###错误码
    sql_flag = 0

    ###读取配置文件获取本地mysql连接变量
    sql_info = {}
    try:
        all_optu = parseIni(log_path,conf_sql,option_par)
        sql_info = all_optu.getOption("db_user","db_pswd")
    except:
        sql_flag = 1
        logIns.writeLog('debug','%s conf file not exists' %conf_sql)

    ###解密sql_pswd
    sql_pswd = sql_info.get('db_pswd', '')
    if sql_pswd:
        try:
            sql_info['db_pswd'] = cb.decrypt_with_certificate(sql_pswd)
        except:
            try:
                sql_info['db_pswd'] = base64.b64decode(sql_pswd)
            except:
                pass

    return (sql_flag,sql_info)

###恢复主体
def xtra_mysql_restore(data_path,mysql_conf,db_name,tb_name):
    ###错误码
    res_flag = 0

    ###建立临时解压目录
    tmp_path = "/data/temp"
    ###如果存在则删除
    if os.path.isdir(tmp_path):
        shutil.rmtree(tmp_path)

    ###建立临时目录
    os.makedirs(tmp_path)

    ###解压文件
    tar_cmd  = "tar -izxvf %s -C %s"% (data_path,tmp_path)
    (get_status_tar,get_output_tar) = execShell(tar_cmd)
    if (get_status_tar != 0):
        logIns.writeLog('error','%s untar error' %data_path)
        res_flag = 1

    ###判断mysql配置文件是否存在
    if not os.path.isfile(mysql_conf):
        logIns.writeLog('error','%s mysql config not exists' % mysql_conf)
        res_flag = 2

    ###获取mysql的数据目录
    all_optu   = parseIni(log_path,mysql_conf,'mysqld')
    mysql_path = all_optu.getOption("datadir")

    if not mysql_path:
        logIns.writeLog('error','%s get mysql datadir error' % mysql_conf)
        res_flag = 3

    ###备份的名字
    backup_mysql = "%s.%s" %(mysql_path,backup_time)

    ###获取mysql的参数选项
    option_par = "xt_sql"
    sql_flag_m,sql_par_m = get_sql_conf(conf_main,option_par)
    sql_flag_s,sql_par_s = get_sql_conf(conf_sub,option_par)

    ###把错误码合并
    if sql_flag_m != 0 and sql_flag_s != 0:
        res_flag = 4

    ###子配置文件更新
    sql_par_m.update(sql_par_s)

    ###获取用户名,密码
    db_user = sql_par_m.get('db_user','')
    db_pswd = sql_par_m.get('db_pswd','')

    ###调用恢复命令
    log_cmd = "innobackupex --user=%s --password='%s' --apply-log %s"% (db_user,db_pswd,tmp_path)
    (get_status_log,get_output_log) = execShell(log_cmd)
    if (get_status_log != 0):
        logIns.writeLog('error','%s restore log error' % get_output_log)
        res_flag = 5

    ###如果是单表恢复
    if tb_name:
        ###确认移动的文件类型
        tb_re = 'cfg|exp|frm|ibd|MYI|MYD'
        ###移动备份文件
        num_file = 0
        tb_dir   = os.path.join(tmp_path,db_name)
        for tb_file in os.listdir(tb_dir):
            if re.search(r'%s\.(%s)' %(tb_name,tb_re),tb_file):
                ###文件数记录
                num_file += 1

        ###判断文件数
        if num_file < 2:
            logIns.writeLog('error','%s restore file not enough' % tb_dir)
            res_flag = 6 
        else:
            for tb_file in os.listdir(tb_dir):
                if re.search(r'%s\.(%s)' %(tb_name,tb_re),tb_file):
                    db_dir  = os.path.join(mysql_path,db_name)
                    tb_path = os.path.join(db_dir,tb_file)
                    tb_back = "%s.%s" %(tb_path,backup_time)
                    ###先备份
                    try:
                        shutil.move(tb_path,tb_back)
                    except:
                        pass
   
                    ###把解压文件拷贝过来
                    tmp_file = os.path.join(tb_dir,tb_file)
                    shutil.copyfile(tmp_file,tb_path)      
    else:             
        ###重命名原有数据库目录
        try:
             shutil.move(mysql_path,backup_mysql)
        except:
            pass

        ###新建mysql数据目录
        os.makedirs(mysql_path)

        ###全恢复
        cop_cmd = "innobackupex --user=%s --password='%s' --copy-back %s"% (db_user,db_pswd,tmp_path)
        (get_status_cop,get_output_cop) = execShell(cop_cmd)
        if (get_status_cop != 0):
            logIns.writeLog('error','%s restore data error' % get_output_cop)
            res_flag = 6

    ###赋权起服务
    chgrp_cmd = "chown -R mysql.mysql %s" %mysql_path
    (get_status_chgrp,get_output_chgrp) = execShell(chgrp_cmd)
    if (get_status_chgrp != 0):
        logIns.writeLog('error','%s chown mysql error' % get_output_chgrp)
        res_flag = 7

    start_cmd = "/etc/init.d/mysql restart"
    (get_status_start,get_output_start) = execShell(start_cmd)
    if (get_status_start != 0):
        logIns.writeLog('error','%s start mysql error' % get_output_start)
        res_flag = 8

    ###返回错误码
    return res_flag

###数据获取函数
def get_restore_data(point_host,point_time,point_rest):
    ###参数注释，point_host:指定主机，point_time:指定日期时间，point_rest:指定恢复的本地路
    ###错误码
    get_flag = 0

    ###如果有指定本地恢复路径则返回
    if point_rest != "False":
        return (get_flag,point_rest)
    
    ###根据ip获取备份机房
    if re.match(r'115.231',point_host):
        idc_get = 'HZ'
    else:
        idc_get = 'BJ'

    ###读取mysql连接数据
    prefix_xt = "xt"
    option_xt = "xt_db"
    mcx = mysqlConn(log_path)
    xt_flag,tb_xt,info_xt = mcx.getConn(conf_main,conf_sub,prefix_xt,option_xt)

    ###错误码跟随
    get_flag = xt_flag

    ###连接数据库
    mbx = mysqlBase(log_path,**info_xt)

    ###查询ftp连接信息
    query_sql = 'SELECT ftp_ip,ftp_user,ftp_pass FROM %s WHERE idc_flag="%s" AND custom_use="%s" LIMIT 1' %(tb_xt,idc_get,defined_use)
    ftp_info  = mbx.query(query_sql)

    ###获取ftp的连接信息
    ftp_conn = {}
    for fi in ftp_info:
        ftp_conn['ftp_host']  = fi[0]
        ftp_conn['ftp_user']  = fi[1]
        encry_pass            = fi[2]

        try:
            ftp_conn['ftp_pswd'] = cb.decrypt_with_certificate(encry_pass)
        except:
            try:
                ftp_conn['ftp_pswd'] = base64.b64decode(encry_pass)
            except:
                ftp_conn['ftp_pswd'] = encry_pass
        break

    ###获取ftp_ip
    ftp_ip = ftp_conn.get('ftp_host','localhost')

    ###获取all_server mysql连接数据
    prefix_sv = "sv"
    option_sv = "sv_db"
    mcs = mysqlConn(log_path)
    sv_flag,tb_sv,info_sv = mcs.getConn(conf_main,conf_sub,prefix_sv,option_sv)

    ###连接yunwei数据库
    mbs = mysqlBase(log_path,**info_sv)
    
    ###查询ftp连接信息
    host_sql = 'SELECT host_name FROM %s WHERE server_ip="%s" OR mapping_ip="%s" LIMIT 1' %(tb_sv,point_host,point_host)
    host_info  = mbx.query(host_sql)

    ###分解主机名
    host_name = []
    for hi in host_info:
        host_name = hi[0].split('-')
        break

    ###分解成三段
    first_dir = os.path.join('/',host_name[0])
    try:
        second_dir = host_name[1]
    except:
        second_dir = ''

    try:
        third_dir = '-'.join(host_name[2:])
    except:
        third_dir = ''

    ###连接成完整目录
    remote_dir  = os.path.join(first_dir,second_dir,third_dir,defined_use,point_time,point_host)
    local_file  = '%s_%s.tar.gz' %(defined_use,point_time)
    remote_path = os.path.join(remote_dir,local_file) 

    ###定义本地下载路径
    local_dir   = '/tmp'
    local_path  = os.path.join(local_dir,local_file)

    ###连接ftp
    try:
        fb = ftpBase(log_path,**ftp_conn)
        fb.downloadFile(local_path,remote_path)
    except:
        get_flag = 1
        logIns.writeLog('error','%s download from %s error' %(remote_path,ftp_ip))

    ###返回错误码
    return (get_flag,local_path)
    
if __name__== "__main__":
    ###脚本名
    script_name = os.path.basename(__file__)
    sub_name    = script_name.split('.')[0]

    ###日志路径
    log_path = '/log/yunwei/%s.log' %script_name

    ###定义日志标识
    logIns  = log('1025',log_path)
    logMain = log('1025','/log/yunwei/yunwei.log')

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
    format_time = update_time.strftime('%y%m%d-%H')
    backup_time = update_time.strftime('%y%m%d%H%M%S')

    ###获取ftp的定义类型
    defined_use  = 'db'

    ###获取本机ip
    local_ip = get_ip_addr('eth0')
    if not local_ip:
        local_ip = get_ip_addr('em3')

    ###导入解密模块
    cb = cryptoBase(log_path)

    ###获取参数
    (accept_server,accept_time,accept_file,mysql_conf,db_name,tb_name) =  args_module_des()

    ###确定获取的数据ip
    if accept_server == 'local':
        point_host = local_ip
    else:
        point_host = accept_server
 
    ###整理恢复的数据库压缩包
    get_code,data_path = get_restore_data(point_host,accept_time,accept_file) 

    ###用xtrabackup完成mysql恢复
    res_code = get_code
    if get_code == 0:
        res_code = xtra_mysql_restore(data_path,mysql_conf,db_name,tb_name)

    ###确认脚本是否成功
    if res_code != 0:
        logMain.writeLog('info','%s error end'% script_info)
    else:
        logMain.writeLog('info','%s success end'% script_info)
        print "success"

