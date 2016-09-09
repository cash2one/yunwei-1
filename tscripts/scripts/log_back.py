#!/usr/bin/env python
#-*- coding:utf-8 -*- 

'''
date: 2016/04/13
role: 上传日志到ftp上
usage: log_back.py -d 1 (1天前的日志备份)
usage: log_back.py -t 1 (2小时前的日志备份)
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

###参数定义函数
def args_module_des():
    usage  = '''  
    %prog -d 1 (1 days before)
    %prog -t 2 (2 hours before)
    '''
    parser = OptionParser(usage)
    parser.add_option("-d","--day",type="int",default=-1,dest="day_log",
                      help="up day log")
    parser.add_option("-t","--time",type="int",default=1,dest="hour_log",
                      help="up hour log")

    (options,args) = parser.parse_args()
    ###确认参数是否定义正确
    if (options.day_log < -1 or options.hour_log < 1):
        parser.print_usage
        write_exec_log('error','arguments defined error!')
        parser.error("arguments defined error!")    

    ###如果定义了天数，不能定义小时
    if (options.day_log !=-1 and options.hour_log !=1):
        parser.print_usage
        write_exec_log('error','arguments day or hour can defined one!')
        parser.error("arguments day or hour can defined one!")

    ###返回值
    return (options.day_log,options.hour_log)

###执行shell命令函数
def exec_shell_cmd(exec_cmd):
    ###执行传入的shell命令
    if exec_cmd:
        (cmd_status,cmd_output) = commands.getstatusoutput(exec_cmd)
    else:
        (cmd_status,cmd_output) = (1,'exec cmd is null')
    ###返回执行状态和输出结果
    return (cmd_status,cmd_output)

###拷贝日志函数
def copy_date_log(data_path,format_d,log_path,log_patt,format_c):
    ###data_path:收集日志的目录，format_d:收集的日期格式，log_path:源日志目录，log_patt:源日志匹配模式
    ###如果没日志目录，则跳过
    if os.path.isdir(log_path):
        ###如果log_patt是传目录
        if log_patt == "dir_up":
            new_dir  = os.path.basename(log_path)
            new_path = os.path.join(data_path,new_dir)
            shutil.copytree(log_path,new_path)
        else:
            for c_log in os.listdir(log_path):
                ###每个文件路径
                c_path = os.path.join(log_path,c_log)
                #if os.path.isfile(c_path) and re.search(r'%s' %format_d,c_log) and re.search(r'%s' %log_patt,c_log):
                if os.path.isfile(c_path) and re.search(r'%s' %log_patt,c_log):
                    if re.search(r'%s' %format_d,c_log) or re.search(r'%s' %format_c,c_log):
                        shutil.copy(c_path,data_path)

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

###文件上传函数
def upload_file(ftp,ftp_user,ftp_pass,ftp_host,log_host,file_path):
    ###上传标识
    up_flag = 0

    ###文件名
    file_do = os.path.basename(file_path)
    ###打开文件
    try:
        fp_ftp = open(file_path,'rb')
        ftp.storbinary('STOR %s/%s' %(log_host,file_do),fp_ftp)
    except Exception,e:
        up_cmd = "ncftpput -z -u%s -p%s %s %s %s"% (ftp_user,ftp_pass,ftp_host,log_host,file_path)
        (get_status_up,get_output_up) = exec_shell_cmd(up_cmd)
        ###确认是否执行成功
        if (get_status_up != 0):
            write_exec_log('debug','%s up error'% file_path)
            up_flag = 1
    ###返回结果
    return up_flag

###目录上传函数
def upload_dir(ftp,ftp_user,ftp_pass,ftp_host,log_host,file_path):
    ###定义上传标识
    ftp_result = 0

    ###远程进入目录
    ftp.cwd(log_host)
    ###循环目录
    for file in os.listdir(file_path):
        src = os.path.join(file_path,file)
        trc = os.path.join(log_host,file)
        ###判断是否有子目录
        if os.path.isfile(src):
            ftp_result = upload_file(ftp,ftp_user,ftp_pass,ftp_host,log_host,src)
            if ftp_result != 0:
                break
        elif os.path.isdir(src):
            ###创建目录
            try:
                ftp.mkd(file)
            except:
                ftp_result = 2
                break
            ###递归
            upload_dir(ftp,ftp_user,ftp_pass,ftp_host,trc,src)
    ftp.cwd('..')
    ###ftp模块上传错误则ncftp上传
    if ftp_result != 0:
        up_cmd = "ncftpput -R -z -u%s -p%s %s %s %s"% (ftp_user,ftp_pass,ftp_host,log_host,file_path)
        (get_status_up,get_output_up) = exec_shell_cmd(up_cmd)
        ###确认是否执行成功
        if (get_status_up != 0):
            write_exec_log('debug','%s up error'% file_path)
            sys.exit(1)

###上传ftp函数
def ftp_file(dir_up,date_time,ftp_table,project_use,custom_use,ip_flag):
    ###变量注解
    ###dir_up:需要上传的目录，date_time:小时或天日志的目录名, ftp_table:ftp表，project_use:项目名（也是ftp第一层目录名），custom_use:日志类型（ftp上第二层目录名）,ip_flag:本机的eth0地址
    ###取主机名
    local_host  = socket.gethostname()
    host_jifang = re.search(r'\w+-(\w{2,5})\d+',local_host)
    if host_jifang:
        host_flag = host_jifang.group(1)
    else:
        host_flag = 'BJ'

    ###ip_host 共同组成目录名
    local_host = "log" ###临时
    ip_host = "%s_%s" %(ip_flag,local_host)

    ###查询ftp相关信息
    query_sql = "select ftp_ip,ftp_user,ftp_pass,project_use,custom_use from %s where project_use='%s' and custom_use='%s'" %(ftp_table,project_use,custom_use)
    info_sel = get_mysql_data(query_sql)

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
    try:
        ftp_pass = base64.b64decode(fde_pass)
    except:
        ftp_pass = fde_pass

    print "aaa",ftp_user,ftp_pass
    ###目录结构
    log_last   = os.path.join(pro_dir,custom_use)
    log_remote = os.path.join(log_last,date_time)
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

        ###确认file_path是否是目录
        if os.path.isdir(file_path):
            real_path = os.path.join(log_host,file_do)
            try:
                ftp.mkd(real_path)
            except Exception,e:
                pass
            upload_dir(ftp,ftp_user,ftp_pass,ftp_host,real_path,file_path)
        elif os.path.isfile(file_path):
            file_flag = upload_file(ftp,ftp_user,ftp_pass,ftp_host,log_host,file_path)
            if file_flag != 0:
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

###整理数据函数
def tidy_data(day_log,hour_log):
    ###日期时间
    date_now = datetime.datetime.now()

    ###自定义log路径及匹配模式,日期的格式定义为2016-04-13这种
    #log_dict = {'/data/webapps':'log\.log_\d{4}(-\d{1,2}){2}','/data/tools':'catalina\.\d{4}(-\d{1,2}){2}','/data/logs/agent':'agent\.log\.\d{4}(-\d{1,2}){2}','/data/test':'dir_up'}    ###日志路径 日志的格式匹配
    #log_dict = {'/usr/local/nginx/conf/online':'dir_up','/usr/local/nginx/conf/vhosts':'dir_up','/data/www/homepage/moretv2.0/attached':'dir_up'}
    log_dict = {'/data/tools/tomcat/logs':'catalina\.\d{4}(-\d{1,2}){2}'}
    
	
    ###log收集目录
    bak_path = '/data/log_back'

    ###建立backup目录
    if not os.path.isdir(bak_path):
        try:
            os.makedirs(bak_path)	    
        except:
            write_exec_log('error',"mkdir %s error!" % bak_path)
            sys.exit(1)

    ###判断是备份天日志还是小时日志
    if day_log != -1:
        ###格式化的日期串
        date_old  = date_now + datetime.timedelta(-day_log)
        format_d  = date_old.strftime('%y-%m-%d')
        format_c  = date_old.strftime('%y%m%d')
        date_time = date_old.strftime('%y%m%d')
    else:
        ###传小时日志
	date_old = date_now + datetime.timedelta(hours=-hour_log)
        format_d = date_old.strftime('%y-%m-%d-%H')
        format_c = date_old.strftime('%y%m%d%H')
        date_time = date_old.strftime('%y%m%d-%H')

    ###组成目录路径
    data_path = os.path.join(bak_path,date_time)

    ###如果存在则删除
    if os.path.isdir(data_path):
        shutil.rmtree(data_path)

    ###建立临时目录
    os.makedirs(data_path)
        
    ###创建目录
    if not os.path.isdir(data_path):
        os.mkdir(data_path)
       
    ###把符合的日志移过来
    for log_path,log_patt in log_dict.items():
        copy_date_log(data_path,format_d,log_path,log_patt,format_c)

    ###获取eth0的ip
    local_ip = get_ip_addr("eth0")

    ###获取项目名
    server_table  = "all_server" 
    query_project = "select project_use from %s where server_ip='%s'" %(server_table,local_ip)
    try:
        ###数据库中有记录则取数据库中的项目
        project_sel = get_mysql_data(query_project)
        project_use = project_sel[0]["project_use"]
    except:
        ###否则都算是wj项目
        project_use = "Helios"
    
    return (data_path,date_time,project_use,local_ip)

###计算md5函数
def md5_file(file_path):
    ###调用MD5函数
    m = md5()
    try:
        with open(file_path,'rb') as fr:
            m.update(fr.read())
    except:
        write_exec_log('error','%s compute md5 error' % file_path)
       # sys.exit(1)
        
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
        if os.path.isfile(file_path):
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
       # sys.exit(1)

if __name__ == "__main__":
    ###脚本排它锁
    script_exclusive_lock(os.path.basename(__file__))

    ###获取参数
    (day_log,hour_log) =  args_module_des()

    ###整理数据
    (data_path,format_d,project_use,local_ip) = tidy_data(day_log,hour_log)

    ###写入MD5
    ###判断文件大小，超过1G则不计算
    log_size = os.path.getsize(data_path)
    if log_size < 1000000:
        write_md5(data_path)

    ###上传日志
    ftp_table  = "ftp_info"
    custom_use = "log"
    ftp_file(data_path,format_d,ftp_table,project_use,custom_use,local_ip)

    print "success"
