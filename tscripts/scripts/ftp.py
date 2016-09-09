#!/usr/bin/env python
#-*- coding:utf-8 -*- 

'''
date: 2016/08/02
role: ftp数据上传下载
usage: ftp.py  
    -u(--user) moretv -s(--super) get/put -f(--file) /tmp/WebServer-BJ01.tar.gz,/root/a.txt
    -u(--user) moretv -s(--super) get/put -d(--file) /wj/log,/tmp/aa
'''

import logging,os,re,datetime,shutil,time,commands
import socket,fcntl,struct,sys,base64
from ftplib import FTP
from optparse import OptionParser

###定义日志函数
def write_exec_log(log_level,log_message):
    ###创建一个logger
    logger = logging.getLogger('get_info.logger')
    logger.setLevel(logging.DEBUG)

    ###建立日志目录
    log_dir  = "/log/yunwei"
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
    -u(--user) ftpuser -s(--super) get/put -f(--file) /tmp/WebServer-BJ01.tar.gz,/root/a.txt
    -u(--user) ftpuser -s(--super) get/put -d(--file) /wj/log,/tmp/aa
    '''

    parser = OptionParser(usage)
    parser.add_option("-u","--user",type="string",default="ftpuser",dest="ftp_user",
            help="ftp user")
    parser.add_option("-s","--super",type="string",default="put",dest="put_get",
            help="want put or get")
    parser.add_option("-f","--file",type="string",default="False",dest="file_path",
            help="want get/put file")
    parser.add_option("-d","--dir",type="string",default="False",dest="dir_path",
            help="want get/put dir")

    (options,args) = parser.parse_args()
    ###确认参数是否定义正确
    if (options.file_path == "False" and options.dir_path == "False"):
        parser.print_usage
        parser.error("-f or -d must defined")

    if (options.put_get != "put" and options.put_get != "get"):
        parser.print_usage
        parser.error("-s must put or get")

    ###确认参数个数没有多定义
    if (len(sys.argv) < 2 or len(sys.argv) > 9):
        parser.print_usage
        parser.error("arguments is too much or too little,pls confirmation")

    ###返回值
    return (options.ftp_user,options.put_get,options.file_path,options.dir_path)

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

###ftp信息函数
def ftp_info(ftp_user):
    ###ftp连接信息
    ftp_hosts = ["10.10.150.244","120.132.92.111"]
    ftp_port  = 21
    ftp_info  = {'migu':'a23ERsfrtwd!','memberinfo':'UXdzZHI0NDNr','ftpuser':'gZy4humhqb5wosUc','moc':'Ie2oiwcsf!s','developer':'Ise39Msp','metisftptest':'oiejWq8S','amstest':'Onsje2ew'}
    ftp_pass  = ftp_info.get(ftp_user,'')

    ###判断端口是否可以连通
    valid_flag = 0
    ftp_host   = ''
    for host in ftp_hosts:
        check_ftp  = check_ip_telnet(host,ftp_port)
        if check_ftp:
           ftp_host   = host
           valid_flag = 1 
           break

    if valid_flag == 0:
        write_exec_log('error',"%s %s connect error"% (ftp_host,ftp_port))
        sys.exit(1)

    return (ftp_host,ftp_port,ftp_user,ftp_pass)

###ftp文件上传或下载函数
def ftp_file(ftp_user,file_do,ftp_way):
    ###ftp信息获取
    ftp_host,ftp_port,ftp_user,ftp_pass = ftp_info(ftp_user)

    ftp=FTP()
    #ftp.set_debuglevel(2)
    ftp.connect(ftp_host,ftp_port)   ###连接
    ftp.login(ftp_user,ftp_pass)     ###登录

    if ftp_way == "get":
        ###删除本地同名文件
        if os.path.isfile(file_do):
            shutil.move(file_do,"%s.%s"% (file_do,"old"))
        try:
            ###打开文件
            fp_ftp = open(file_do,'wb').write
            ftp.retrbinary('RETR %s' % os.path.basename(file_do),fp_ftp)
            ###退出
            ftp.quit()
        except Exception,e:
            down_cmd = "wget ftp://%s:%s@%s/%s -P %s"% (ftp_user,ftp_pass,ftp_host,file_do,os.path.basename(file_do))
            (get_status_down,get_output_down) = exec_shell_cmd(down_cmd)
            ###确认是否执行成功
            if (get_status_down != 0):
                write_exec_log('debug','%s down error'% file_do)
                sys.exit(1)

    elif ftp_way == "put":
        ###打开文件
        try:
            fp_ftp = open(file_do,'rb')
            ftp.storbinary('STOR %s' % os.path.basename(file_do),fp_ftp)
            ###退出
            ftp.quit()
        except Exception,e:
            up_cmd = "ncftpput -z -u%s -p%s %s / %s"% (ftp_user,ftp_pass,ftp_host,file_do)
            (get_status_up,get_output_up) = exec_shell_cmd(up_cmd)
            ###确认是否执行成功
            if (get_status_up != 0):
                write_exec_log('debug','%s up error'% file_do)
                sys.exit(1)

###ftp目录上传或下载函数
def ftp_dir(ftp_user,dir_do,ftp_way):
    ###ftp信息获取
    ftp_host,ftp_port,ftp_user,ftp_pass = ftp_info(ftp_user)

    ftp=FTP()
    #ftp.set_debuglevel(2)           ###调试模式
    ftp.connect(ftp_host,ftp_port)   ###连接
    ftp.login(ftp_user,ftp_pass)     ###登录

    ###ftp上传相应目录
    if ftp_way == "put":
        dir_last = os.path.basename(dir_do)
        try:
            ftp.mkd(dir_last)
        except Exception,e:
            pass

        for file_do in os.listdir(dir_do):
            ###文件路径
            file_path = os.path.join(dir_do,file_do)

            ###打开文件
            try:
                fp_ftp = open(file_path,'rb')
                ftp.storbinary('STOR %s/%s' %(dir_last,file_do),fp_ftp)
            except Exception,e:
                up_cmd = "ncftpput -z -u%s -p%s %s %s %s"% (ftp_user,ftp_pass,ftp_host,dir_last,file_path)
                (get_status_up,get_output_up) = exec_shell_cmd(up_cmd)
                ###确认是否执行成功
                if (get_status_up != 0):
                    write_exec_log('debug','%s up error'% file_do)
                    sys.exit(1)    

    elif ftp_way == "get":
        ###列出远程目录的文件
        all_path = ftp.nlst(dir_do)

        ###本地获取要建立相应目录
        current_dir = os.getcwd()
        want_dir    = os.path.basename(dir_do)
        dir_down = os.path.join(current_dir,want_dir)

        ###删除相应目录
        if os.path.isdir(dir_down):
            shutil.move(dir_down,"%s.%s"% (dir_down,"old"))

        ###建立相应目录
        os.makedirs(dir_down)

        ###ftp下载相应文件
        for file_path in all_path:
            ###获取文件名
            file_name = os.path.basename(file_path)

            ###打开文件
            try:
                fp_ftp = open(os.path.join(dir_down,file_name),'wb').write
                ftp.retrbinary('RETR %s' %file_path,fp_ftp)
            except Exception,e:
                down_cmd = "wget ftp://%s:%s@%s/%s -P %s"% (ftp_user,ftp_pass,ftp_host,dir_do,dir_down)
                (get_status_down,get_output_down) = exec_shell_cmd(down_cmd)
                ###确认是否执行成功
                if (get_status_down != 0):
                    write_exec_log('debug','%s down error'% file_name)
                    sys.exit(1)    

if __name__ == "__main__":
    ###脚本排它锁
    script_exclusive_lock(os.path.basename(__file__))

    ###获取参数
    (ftp_user,put_get,file_path,dir_path) =  args_module_des()

    ###文件处理
    if file_path != "False":
        ###分隔逗号
        file_list = file_path.split(',') 
        for file_do in file_list:
            ftp_file(ftp_user,file_do,put_get)
    
    if dir_path != "False":
        dir_list = dir_path.split(',')
        for dir_do in dir_list:
            ftp_dir(ftp_user,dir_do,put_get)

    print "success"
