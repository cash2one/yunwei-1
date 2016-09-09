#!/usr/bin/env python
#-*- coding:utf-8 -*- 

'''
date: 2016/04/13
role: 上传mysql数据到ftp上
usage: mysql_backup.py 
'''

import logging,os,re,datetime,shutil,time,commands
import socket,fcntl,struct,MySQLdb,sys,base64
from ftplib import FTP
from optparse import OptionParser
from hashlib import md5

###定义日志函数
def write_exec_log(log_level,log_message):
    ###创建一个logger
    logger = logging.getLogger('get_info.logger')
    logger.setLevel(logging.DEBUG)

    ###建立日志目录
    log_dir  = "/data/logs/yunwei"
    log_file = "all_do.log"
    if not os.path.isdir(log_dir):
        os.makedirs(log_dir,mode=0777)

    log_path = os.path.join(log_dir,log_file)

    ###给日志赋权0777
    if os.path.isfile(log_path):
        os.chmod(log_path,0777)

    ###创建一个handler用于写入日志文件
    fh = logging.FileHandler(log_path)
    fh.setLevel(logging.DEBUG)

    ###创建一个handler用于输出到终端
    th = logging.StreamHandler()
    th.setLevel(logging.DEBUG)

    ###定义handler的输出格式
    formatter =logging.Formatter('%(asctime)s  %(name)s  %(levelname)s  %(message)s')
    fh.setFormatter(formatter)
    th.setFormatter(formatter)

    ###给logger添加handler
    logger.addHandler(fh)
    logger.addHandler(th)

    ###记录日志
    level_dic = {'debug':logger.debug,'info':logger.info,'warning':logger.warning,'error':logger.error,'critical':logger.critical}
    level_dic[log_level](log_message)

    ###删除重复记录
    fh.flush()
    logger.removeHandler(fh)

    th.flush()
    logger.removeHandler(th)

###脚本排它锁函数
def script_exclusive_lock(scriptName):
    pid_file  = '/tmp/%s.pid'% scriptName
    lockcount = 0
    while True:
        if os.path.isfile(pid_file):
            ###打开脚本运行进程id文件并读取进程id
            fp_pid     = open(pid_file,'r')
            process_id = fp_pid.readlines()
            fp_pid.close()

            ###判断pid文件取出的是否是数字
            if not process_id:
                break

            if not re.search(r'^\d',process_id[0]):
                break

             ###确认此进程id是否还有进程
            lockcount += 1
            if lockcount > 4:
                write_exec_log('error','2 min after this script is still exists')
                sys.exit(1)
            else:
                if os.popen('/bin/ps %s|grep "%s"'% (process_id[0],scriptName)).readlines():
                    print "The script is running...... ,Please wait for a moment!"
                    time.sleep(30)
                else:
                    os.remove(pid_file)
        else:
            break

    ###把进程号写入文件
    wp_pid = open(pid_file,'w')
    sc_pid = os.getpid()
    wp_pid.write('%s'% sc_pid)
    wp_pid.close()

    ###pid文件赋权
    if os.path.isfile(pid_file):
        os.chmod(pid_file,0777)

###执行shell命令函数
def exec_shell_cmd(exec_cmd):
    ###执行传入的shell命令
    if exec_cmd:
        (cmd_status,cmd_output) = commands.getstatusoutput(exec_cmd)
    else:
        (cmd_status,cmd_output) = (1,'exec cmd is null')
    ###返回执行状态和输出结果
    return (cmd_status,cmd_output)

###检验端口是否开通函数
def check_ip_telnet(ip,port):
    s = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
    try:
        s.connect((ip,int(port)))
        s.shutdown(2)
        return True
    except Exception,e:
        return False

###数据库相关变量函数
def conn_sql_info():
    ###数据库相应变量
    db_host  = ["10.10.184.208"]
    db_user   = "readonly"
    db_passwd = 'readonly'
    db_name   = "yunwei"
    db_port   = 3306
    db_ip     = False

    ###判断端口是否可以连通
    for ip in db_host:
        check_db = check_ip_telnet(ip,db_port)
        if check_db:
            db_ip = ip
            break

    ###如果db_ip为false，则报错
    if not db_ip:
        write_exec_log('error','all db ip can not use')
        sys.exit(1) 

    return db_ip,db_user,db_passwd,db_name,db_port

###数据库查询函数
def get_mysql_data(af_sql):
    ###引入相关数据库所需变量
    (se_host,se_user,se_passwd,se_name,se_port) = conn_sql_info()

    ###连接数据库
    conn = MySQLdb.connect(host=se_host,user=se_user,passwd=se_passwd,db=se_name,port=se_port)
    cursor = conn.cursor(MySQLdb.cursors.DictCursor) #以字典方式返回结果
    cursor.execute(af_sql)

    ###根据查询语句作出查询
    all_data  = cursor.fetchall()

    ###关闭连接
    cursor.close()
    conn.close()

    ###返回是否有查询结果
    return all_data

###上传ftp函数
def ftp_file(dir_up,format_d,ftp_table,project_use,custom_use,ip_flag):
    ###变量注解
    ###dir_up:需要上传的目录，format_d:小时或天日志的目录名, ftp_table:ftp表，project_use:项目名（也是ftp第一层目录名），custom_use:日志类型（ftp上第二层目录名）,ip_flag:本机的eth0地址
    ###取主机名
    local_host  = socket.gethostname()
    host_jifang = re.search(r'\w+-(\w{2,5})\d+',local_host)
    if host_jifang:
        host_flag = host_jifang.group(1)
    else:
        host_flag = 'BJ'

    ###ip_host 共同组成目录名
    local_host = "db" 
    ip_host = "%s_%s" %(ip_flag,local_host)
    
    ###查询ftp相关信息
    query_sql = "select ftp_ip,ftp_user,ftp_pass,project_use,custom_use from %s where project_use='%s' and custom_use='%s'" %(ftp_table,project_use,custom_use)
    info_sel = get_mysql_data(query_sql)
    #print "aaa",query_sql,info_sel    


    ###获取ftp信息
    ftp_host = False
    ftp_user = ""
    fde_pass = ""
    pro_dir  = ""
    ftp_port = 21
    for info_one in info_sel:
        one_host = info_one["ftp_ip"]
        ###判断端口是否可以连通
        check_ftp  = check_ip_telnet(one_host,ftp_port)
        if check_ftp:
            ftp_host = one_host
            ftp_user = info_one["ftp_user"]
            fde_pass = info_one["ftp_pass"]
            pro_dir  = "/%s" %info_one["project_use"]
            break

    ###如果没获取到ftp_ip则报错
    if not ftp_host:
        write_exec_log('error','all ftp ip can not use')
        sys.exit(1)

    ###解密
    ftp_pass = base64.b64decode(fde_pass)
    ###目录结构
    log_last   = os.path.join(pro_dir,custom_use)
    log_remote = os.path.join(log_last,format_d)
    log_host   = os.path.join(log_remote,ip_host)
 
    ###开始连接ftp操作
    ftp=FTP()
    #ftp.set_debuglevel(2)           ###调试模式
    ftp.connect(ftp_host,ftp_port)   ###连接
    ftp.login(ftp_user,ftp_pass)     ###登录
    for dir_mk in [pro_dir,log_last,log_remote,log_host]:
        try:
            ftp.mkd(dir_mk)
        except Exception,e:
            pass

    ###ftp上传相应目录
    for file_do in os.listdir(dir_up):
        ###文件路径
        file_path = os.path.join(dir_up,file_do)

        ###打开文件
        try:
            fp_ftp = open(file_path,'rb')
            ftp.storbinary('STOR %s/%s' %(log_host,file_do),fp_ftp)
        except Exception,e:
            up_cmd = "ncftpput -z -u%s -p%s %s %s %s"% (ftp_user,ftp_pass,ftp_host,log_host,file_path)
            (get_status_up,get_output_up) = exec_shell_cmd(up_cmd)
            ###确认是否执行成功
            if (get_status_up != 0):
                write_exec_log('debug','%s up error'% file_do)
                sys.exit(1)    

    ###退出
    ftp.quit()

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
        write_exec_log('error',"get %s ip error!" % ifname)  
        sys.exit(1)

    return ipaddr

###备份主体
def xtra_mysql_backup():
    ###取当前时间
    date_time_now = time.strftime('%y%m%d-%H')

    ###备份目录定义
    back_dir = "/data/db_back"

    ###建立备份目录
    if not os.path.isdir(back_dir):
        os.makedirs(back_dir)

    ###获取eth0的ip
    local_ip = get_ip_addr("eth0")
   
    ###获取db信息
    server_table = "all_server"
    query_user   = "select user_passwd,project_use from %s where server_ip='%s'" %(server_table,local_ip)
    
    #print "bbb",query_user

    try:
        ###数据库中有记录则取数据库中的项目
        up_info = get_mysql_data(query_user)
        #db_user = up_info[0]["user_passwd"]["ftp"].keys()[0]
        #de_pass = up_info[0]["user_passwd"]["ftp"][db_user]
        db_user = "root"
        de_pass = 'bW9yZXR2c21hclRWQDYwOF84MTA='
        ###获取项目
        project_use = up_info[0]["project_use"]
    except:
        ###用户名 密码
        db_user = "root"
        de_pass = 'bW9yZXR2c21hclRWQDYwOF84MTA='
  
        ###默认项目
        project_use = "Helios"

    ###解密en_pass
    try:
        db_pass = base64.b64decode(de_pass)
    except:
        db_pass = de_pass

    ###判断mysql是否启动
    sql_cmd = '/etc/init.d/mysql status'
    (get_status_sql,get_output_sql) = exec_shell_cmd(sql_cmd)
    if not re.search(r'running|运行',get_output_sql):
        write_exec_log('error','mysql not running')
        sys.exit(1)

    ###获取主机名
    local_host  = socket.gethostname()
    local_host = "db" 


    ###组成目录路径
    data_path = os.path.join(back_dir,date_time_now)

    ###如果存在则删除
    if os.path.isdir(data_path):
        shutil.rmtree(data_path)

    ###建立临时目录
    os.makedirs(data_path)

    ###定义备份的名字
    flag_host = "%s_%s"% (local_host.strip(),date_time_now.strip())
    log_path  = "%s/back_db.log" %data_path 
    back_path = "%s/%s.tar.gz"% (data_path,flag_host)

    ###调用备份命令
    xtra_cmd = "innobackupex --user=%s --password='%s' --stream=tar %s 2>>%s |gzip 1>%s"% (db_user,db_pass,data_path,log_path,back_path)

    (get_status_xtra,get_output_xtra) = exec_shell_cmd(xtra_cmd)
    if (get_status_xtra != 0):
        write_exec_log('error','%s exec backup error' % flag_host)
        sys.exit(1)

    ###读取备份日志，如果有error则报错
    with open(log_path,'r') as fl:
        lines = fl.readlines()

        for line in lines:
            ###不区分error的大小写
            #if re.search(r'error',line,re.I):
             if re.search(r'error',line,re.I) and not re.search(r'stdout',line,re.I): 
              write_exec_log('error','%s backup result error' % flag_host)
              sys.exit(1)

    ###如果压缩的文件小于20K，则认为备份失败
    if os.path.isfile(back_path):
        tar_size = os.path.getsize(back_path)
        if tar_size < 20:
            write_exec_log('error','%s backup result size error' % flag_host)
            sys.exit(1)
    else:
            write_exec_log('error','%s not exists' % flag_host)
            sys.exit(1)
                   
    ###返回备份的db文件
    return (data_path,date_time_now,project_use,local_ip)

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
        write_exec_log('error','%s write md5 error' % md5_path)
        #sys.exit(1)

if __name__ == "__main__":
    ###脚本排它锁
    script_exclusive_lock(os.path.basename(__file__))

    ###数据库备份
    (data_path,format_d,project_use,local_ip) = xtra_mysql_backup()

    ###写入MD5
    write_md5(data_path)

    ###上传数据库
    ftp_table  = "ftp_info"
    custom_use = "db"
    ftp_file(data_path,format_d,ftp_table,project_use,custom_use,local_ip)

    print "success"
