#!/usr/bin/env python
#-*- coding:utf-8 -*- 

'''
date: 2015/12/25
role: 删除旧日志文件,日期及匹配模式写死脚本中
usage: del_old_log.py
modify: 2016/06/17 添加删除日志，数据库备份包
'''

import logging,os,re,datetime,time,sys,commands,shutil

###定义日志函数
def write_exec_log(log_level,log_message):
    ###创建一个logger
    logger = logging.getLogger('get_info.logger')
    logger.setLevel(logging.DEBUG)

    ###建立日志目录
    log_dir  = "/data/logs"
    log_file = "del_data.log"
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

###删除函数
def del_log(old_str,nohup_list,**log_dict):
    ###编译正则匹配模式
    pattern = re.compile(r'^(log\.log_\d{4}(-\d{1,2}){2}|\w{1,15}\.log.\d{4}-\d{2}-\d{2}|log\.\d{8}\.log|.*\d{8}\.sql\.tar\.gz|debug\.\d{8}\.log|ip\.log\.\d{4}(-\d{1,2}){2}|catalina\.\d{4}(-\d{1,2}){2}\.out|catalina\.\d{4}(-\d{1,2}){2}\.log|localhost\.\d{4}(-\d{1,2}){2}\.log|.*\.log_\d{8}-\w{5}\d{1}.bz2|.*\.log_\d{8}-\w{5}\d{1}_bak|.*\.log_\d{8}-.*-\d{1}.bz2|.*\.log_\d{8}-.*-\d{1}_bak|.*\.log_\d{8}-.*-\d{1}|error\.log\.\d{4}(-\d{1,2}){2}\.log|\d{8}.txt|agent\.log\.\d{4}(-\d{1,2}){2})$')
    ###遍历
    for k,v in log_dict.items():
        for dirpath,dirnames,filenames in os.walk(v):
            for filename in filenames:
                ###匹配
                file_match = pattern.match(filename)
                if file_match:
                    file_path = os.path.join(dirpath,filename)
                    ###获取文件修改时间
                    stat_info  = os.stat(file_path)
                    file_mtime = time.strftime("%y%m%d%H%M",time.localtime(stat_info.st_mtime))
                    if old_str > file_mtime:
                        os.remove(file_path)
                        write_exec_log('info','删除 %s'% file_path)

            ###找出nodejs的nohup.out
            #nohup_file = 'nohup.out'
            del_flag   = ''
#            if k == 'nodejs' and nohup_file in filenames:
            for nohup_file in [real_file for real_file in nohup_list if real_file in filenames]:
                nohup_path = os.path.join(dirpath,nohup_file)
                with open(nohup_path,'w') as fw:
                    fw.write(del_flag)

                write_exec_log('info','清空 %s'% nohup_path)

###删除备份目录函数
def del_bag(dir_str):
    ###需要遍历的目录列表
    bag_list = ['/data/log_back','/data/db_back']
    
    ###遍历目录
    for bag in bag_list:
      try:
        for dir in os.listdir(bag):
            ###组合成路径
          
            dir_path = os.path.join(bag,dir)

            ###获取目录修改时间
            stat_dir  = os.stat(dir_path)
            dir_mtime = time.strftime("%y%m%d%H",time.localtime(stat_dir.st_mtime))
            
            ###是符合规则的目录
            if os.path.isdir(dir_path) and re.match(r'^(\d{6}|\d{6}-\d{2})$',dir) and dir_str > dir_mtime:
                shutil.rmtree(dir_path)

                write_exec_log('info','删除 %s'% dir_path)
      except:
              write_exec_log('info','test')
    
             
###删除旧日志主体函数
def del_main():
    ###现在日期时间
    date_now = datetime.datetime.now()
    ###3天前日期时间
    date_old = date_now + datetime.timedelta(-3)
    old_str  = date_old.strftime("%y%m%d%H%M")

    ###需要遍历的目录
    log_dict = {'nodejs':'/data/webapps','tomcat':'/data/tools','tomcat_moretv':'/home/moretv/tools/','agent':'/data/logs/agent','ip':'/data/logs/ip','nginx_errlog':'/var/log/nginx/','vod':'/data/log/vod/','user':'/data/logs/user','pm2':'/data/logs/pm2','ms':'/data/logs/ms','login':'/data/logs/login','dbbak':'/data/bak/','normal':'/data/logs','loginlog':'/data/log/login_log','openlogin':'/data/log/openlogin_log','kidslog':'/data/log/kids_log'}

    ###清空目录
    nohup_list = ['nohup.out','uwsgi.log','error.log','livele.letv.log']

    ###删除或清空日志
    del_log(old_str,nohup_list,**log_dict)

    ###删除日志或数据库备份
    #date_dir = date_now + datetime.timedelta(hours=-2)
    date_dir = date_now + datetime.timedelta(-1)
    dir_str  = date_dir.strftime("%y%m%d%H")
    del_bag(dir_str)
    
if __name__ == "__main__":
    ###脚本排它锁
    script_exclusive_lock(os.path.basename(__file__))

    ###脚本主体
    del_main()
    
    print "success"
