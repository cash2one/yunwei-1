#!/usr/bin/env python
#-*- coding:utf-8 -*- 

'''
date: 2016/08/16
role: 数据库备份
usage: mysql_backup.py
2016/08/31 添加单库单表的备份
'''
from yunwei.operate.prefix import log,execShell,exclusiveLock
from yunwei.operate.mysql import mysqlBase
from yunwei.install.cryptology import cryptoBase
from yunwei.getInfo.connDb import mysqlConn
from yunwei.operate.ftp import ftpBase
from yunwei.getInfo.parser import parseIni

import os,sys,re,shutil,datetime,time
import socket,fcntl,struct,base64
from hashlib import md5
from optparse import OptionParser

###参数定义函数
def args_module_des():
    usage  = '''  
    %prog
    %prog -d test_db -t tb_name
    '''
    parser = OptionParser(usage)
    parser.add_option("-d","--db",type="string",default="",dest="db_name",
                      help="exec db name")
    parser.add_option("-t","--tb",type="string",default="",dest="tb_name",
                      help="exec tb name")

    (options,args) = parser.parse_args()
    ###确认参数是否定义正确
    if not options.db_name and options.tb_name:
        parser.print_usage
        parser.error("-t arguments must depend on -d")

    ###返回值
    return (options.db_name,options.tb_name)


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

###备份主体
def xtra_mysql_backup(db_name,tb_name):
    ###错误码
    xtra_flag = 0

    ###获取mysql的参数选项
    option_par = "xt_sql"
    sql_flag_m,sql_par_m = get_sql_conf(conf_main,option_par)
    sql_flag_s,sql_par_s = get_sql_conf(conf_sub,option_par)

    ###把错误码合并
    if sql_flag_m != 0 and sql_flag_s != 0:
        xtra_flag = 1

    ###子配置文件更新
    sql_par_m.update(sql_par_s)

    ###备份目录定义
    back_dir = "/data/db_back"

    ###建立备份目录
    if not os.path.isdir(back_dir):
        os.makedirs(back_dir)

    ###组成目录路径
    data_path = os.path.join(back_dir,format_time)

    ###如果存在则删除
    if os.path.isdir(data_path):
        shutil.rmtree(data_path)

    ###建立临时目录
    os.makedirs(data_path)

    ###定义备份的名字
    flag_host = "%s_%s"% (defined_use.strip(),format_time.strip())
    log_path  = "%s/back_db.log" %data_path

    ###获取用户名,密码
    db_user = sql_par_m.get('db_user','')
    db_pswd = sql_par_m.get('db_pswd','')

    ###调用备份命令
    if db_name:
        if tb_name:
            pars = '%s.%s' %(db_name,tb_name)
            back_path = "%s/%s_%s.%s.%s.tar.gz"% (data_path,defined_use.strip(),db_name,tb_name,format_time.strip())
        else:
            pars = '%s.*' %(db_name,)
            back_path = "%s/%s_%s.%s.tar.gz"% (data_path,defined_use.strip(),db_name,format_time.strip())

        xtra_cmd = "innobackupex --user=%s --password='%s' --include='%s' --stream=tar %s 2>>%s |gzip 1>%s"% (db_user,db_pswd,pars,data_path,log_path,back_path)
    else:
        back_path = "%s/%s.tar.gz"% (data_path,flag_host)
        xtra_cmd = "innobackupex --user=%s --password='%s' --stream=tar %s 2>>%s |gzip 1>%s"% (db_user,db_pswd,data_path,log_path,back_path)
    (get_status_xtra,get_output_xtra) = execShell(xtra_cmd)
    if (get_status_xtra != 0):
        logIns.writeLog('error','%s exec backup error' %defined_use)
        xtra_flag = 2

    ###如果压缩的文件小于40K，则认为备份失败
    if os.path.isfile(back_path):
        tar_size = os.path.getsize(back_path)
        if tar_size < 40000:
            logIns.writeLog('error','%s backup file too small' %back_path)
            xtra_flag = 3
    else:
        logIns.writeLog('error','%s backup file too small' %back_path)
        xtra_flag = 4
    
    ###确认备份日志的最后三行匹配completed OK
    with open(log_path,'r') as fl:
        last_line = ''.join(fl.readlines()[-3:])
        if not re.search(r'completed\s+OK',last_line,re.I):
            logIns.writeLog('error','%s backup log abnormal' %log_path)
            xtra_flag = 5

    ###返回错误码
    return (xtra_flag,data_path)

###计算md5函数
def md5_file(file_path):
    ###调用MD5函数
    m = md5()
    with open(file_path,'rb') as fr:
        m.update(fr.read())

    ###返回MD5
    return m.hexdigest()

###写入MD5函数
def write_md5(data_path):
    ###错误码
    md5_flag = 0

    ###定义要写的MD5文件列表
    md5_list = []

    ###遍历目录文件
    for file_one in os.listdir(data_path):
        file_path = os.path.join(data_path,file_one)
        ###计算MD5
        md5_str = md5_file(file_path)
        md5_list.append("%s\t%s\n" %(md5_str,file_one))

    ###MD5文件变量定义
    md5_name = "filelist.md5"
    md5_path = os.path.join(data_path,md5_name)

    ###写MD5文件
    try:
        with open(md5_path,'w') as fw:
            fw.writelines(md5_list)
    except:
        logIns.writeLog('error','%s md5 file write error' %md5_path)
        md5_flag = 1

    ###返回错误码
    return md5_flag
    
###上传主体
def ftp_mysql_backup(xtra_path):
    ###错误码
    ftp_flag = 0
    
    ###根据ip获取备份机房
    if re.match(r'115.231',local_ip):
        idc_get = 'HZ'
    else:
        idc_get = 'BJ'

    ###读取mysql连接数据
    prefix_xt = "xt"
    option_xt = "xt_db"
    mcx = mysqlConn(log_path)
    xt_flag,tb_xt,info_xt = mcx.getConn(conf_main,conf_sub,prefix_xt,option_xt)

    ###错误码跟随
    ftp_flag = xt_flag

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
    host_sql = 'SELECT host_name FROM %s WHERE server_ip="%s" OR mapping_ip="%s" LIMIT 1' %(tb_sv,local_ip,local_ip)
    host_info  = mbx.query(host_sql)

    ###分解主机名
    host_name = []
    for hi in host_info:
        host_name = hi[0].split('-')
        break

    ###分解成三段
    first_dir = host_name[0]
    try:
        second_dir = host_name[1]
    except:
        second_dir = ''

    try:
        third_dir = '-'.join(host_name[2:])
    except:
        third_dir = ''

    ###连接成完整目录
    remote_dir = os.path.join(first_dir,second_dir,third_dir,defined_use,format_time,local_ip)

    ###连接ftp
    try:
        fb = ftpBase(log_path,**ftp_conn)
        fb.uploadDir(xtra_path,remote_dir)
    except:
        ftp_flag = 1
        logIns.writeLog('error','%s upload %s error' %(xtra_path,ftp_ip))

    ###返回错误码
    return ftp_flag
    
if __name__== "__main__":
    ###脚本名
    script_name = os.path.basename(__file__)
    sub_name    = script_name.split('.')[0]

    ###日志路径
    log_path = '/log/yunwei/%s.log' %script_name

    ###定义日志标识
    logIns  = log('1024',log_path)
    logMain = log('1024','/log/yunwei/yunwei.log')

    script_info = ' '.join(sys.argv)

    ###脚本排它锁
    exclusiveLock(script_name)

    logMain.writeLog('info','%s start'% script_info)

    ###配置文件路径
    conf_pwd  = os.path.join(os.path.dirname(os.path.realpath(__file__)),'conf')
    conf_main = os.path.join(conf_pwd,'common.conf')
    conf_sub  = os.path.join(conf_pwd,'%s.conf' %sub_name)

    ###参数判断
    db_name,tb_name = args_module_des()

    ###时间
    update_time = datetime.datetime.now()
    format_time = update_time.strftime('%y%m%d-%H')

    ###获取ftp的定义类型
    defined_use  = 'db'

    ###获取本机ip
    local_ip = get_ip_addr('eth0')
    if not local_ip:
        local_ip = get_ip_addr('em3')

    ###导入解密模块
    cb = cryptoBase(log_path)
    
    ###用xtrabackup完成mysql备份
    xtra_code,xtra_path = xtra_mysql_backup(db_name,tb_name)

    ###计算MD5
    md5_code = write_md5(xtra_path)

    ###上传ftp
    ftp_code = xtra_code|md5_code
    if xtra_code == 0:
        ftp_code = ftp_mysql_backup(xtra_path)
    
    ###确认脚本是否成功
    if ftp_code != 0:
        logMain.writeLog('info','%s error end'% script_info)
    else:
        logMain.writeLog('info','%s success end'% script_info)
        print "success"

