#!/usr/bin/env python
# -*- coding:utf-8 -*-
import commands
import logging
import pdb

import signal
import os, sys, time, datetime, shutil, re


reload(sys)
sys.setdefaultencoding("utf-8")



###定义日志函数
def write_exec_log(log_level, log_message):
    ###创建一个logger
    logger = logging.getLogger('get_info.logger')
    logger.setLevel(logging.DEBUG)

    ###建立日志目录
    log_dir = "/home/moretv/yunwei"
    log_file = "service_opera.log"
    if not os.path.isdir(log_dir):
        os.makedirs(log_dir, mode=0777)

    log_path = os.path.join(log_dir, log_file)

    ###给日志赋权0777
    if os.path.isfile(log_path):
        os.chmod(log_path, 0777)

    ###创建一个handler用于写入日志文件
    fh = logging.FileHandler(log_path)
    fh.setLevel(logging.DEBUG)

    ###创建一个handler用于输出到终端
    th = logging.StreamHandler()
    th.setLevel(logging.DEBUG)

    ###定义handler的输出格式
    formatter = logging.Formatter('%(asctime)s  %(name)s  %(levelname)s  %(message)s')
    fh.setFormatter(formatter)
    th.setFormatter(formatter)

    ###给logger添加handler
    logger.addHandler(fh)
    logger.addHandler(th)

    ###记录日志
    level_dic = {'debug': logger.debug, 'info': logger.info, 'warning': logger.warning, 'error': logger.error,
                 'critical': logger.critical}
    level_dic[log_level](log_message)

    ###删除重复记录
    fh.flush()
    logger.removeHandler(fh)

    th.flush()
    logger.removeHandler(th)


###脚本排它锁函数
def script_exclusive_lock(scriptName):
    pid_file = '/tmp/%s.pid' % scriptName
    lockcount = 0
    while True:
        if os.path.isfile(pid_file):
            ###打开脚本运行进程id文件并读取进程id
            fp_pid = open(pid_file, 'r')
            process_id = fp_pid.readlines()
            fp_pid.close()

            ###判断pid文件取出的是否是数字
            if not process_id:
                break

            if not re.search(r'^\d', process_id[0]):
                break

                ###确认此进程id是否还有进程
            lockcount += 1
            if lockcount > 4:
                write_exec_log('error', '2 min after this script is still exists')
                sys.exit(1)
            else:
                if os.popen('/bin/ps %s|grep "%s"' % (process_id[0], scriptName)).readlines():
                    print "The script is running...... ,Please wait for a moment!"
                    time.sleep(30)
                else:
                    os.remove(pid_file)
        else:
            break

    ###把进程号写入文件
    wp_pid = open(pid_file, 'w')
    sc_pid = os.getpid()
    wp_pid.write('%s' % sc_pid)
    wp_pid.close()

    ###pid文件赋权
    if os.path.isfile(pid_file):
        os.chmod(pid_file, 0777)


def exec_shell_cmd(cmd):
    import os
    os.system(cmd)
    return True


def exec_return_cmd(cmd):
    (status, result) = commands.getstatusoutput(cmd)
    if status != 0:
        msg = "Running {0} Filed ,output was {1}".format(cmd, result)
        write_exec_log("error", msg)
        sys.exit(1)
    else:
        # msg = "Run cmd:%s success \n " % cmd
        # write_exec_log("info", msg)
        return result or 0


def get_pid(pro_name):
    cmd = "ps -ef | grep '%s' | grep -Ev 'grep|\.sh'|grep -v 'sh -c'|grep -v 'python' | awk '{print $2}'" % pro_name
    pid = exec_return_cmd(cmd)
    import types
    if type(pid) == type(1):
        return pid
    elif type(pid) == type('a'):
        return pid.split('\n')[0]


def stop_service(db_data):

    #如果/tmp/项目名.pid不存在，则表明服务不存在
    pro_name=db_data.get("publish_name",None)
    pid_file="/tmp/%s.pid"%pro_name
    import pdb;pdb.set_trace()
    if os.path.exists(pid_file):
        return True

    with open(pid_file,'r') as f:
        if f.read():
            try:
                pid_n=int(f.read())
                os.kill(int(pid_n), signal.SIGTERM)
            except:
                write_exec_log("error","Kill project %s Failed"%pro_name)
            finally:
                os.remove(pid_file)


def start_service(db_data):
    publish_name = db_data.get("publish_name", None)
    full_dir = db_data.get("full_dir", None)
    cmd_line = db_data.get("cmd_line", None)
    service_flag = db_data.get("service_flag", None)

    if get_pid(service_flag) != 0:
        error_msg = "进程%s已存在,放弃本次操作 " % publish_name
        write_exec_log("error", error_msg)
        sys.exit(1)
    if full_dir:
        start_cmd = "cd {0} && {1} ".format(full_dir, cmd_line)
    else:
        start_cmd = "{0}".format(cmd_line)

    exec_shell_cmd(start_cmd)
    time.sleep(5)
    i = 0
    while not get_pid(service_flag):
        error_msg = "Start %s Failed " % publish_name
        write_exec_log("error", error_msg)
        i += 1
        if i > 3:
            error_msg = "尝试重启三次，服务无法启动,程序异常退出"
            write_exec_log("error", error_msg)
            sys.exit(1)

    #将PID写入/tmp路径
    pid_file="/tmp/%s.pid"%publish_name
    pid_n=get_pid(service_flag)
    with open(pid_file,'w') as f:
        f.write(str(pid_n))

    return True


def mainproce(pro_opera,db_data):
    ###脚本排它锁
    script_exclusive_lock(os.path.basename(__file__))

    retry_count = 3
    if pro_opera.lower() == "stop":
        stop_service(db_data)
    elif pro_opera.lower() == "start":
        start_service(db_data)
    elif pro_opera.lower() == "restart":
        stop_service(db_data)
        time.sleep(10)
        start_service(db_data)
    sucess_msg = "%s %s 操作完成" % (db_data.get("publish_name", None), pro_opera)
    write_exec_log("info", sucess_msg)


if __name__ == "__main__":
    import pdb
    db_data=dict()
    db_data["full_dir"]=sys.argv[1]
    db_data["service_flag"] = sys.argv[2]
    db_data["cmd_line"] = sys.argv[3]
    pro_opera = sys.argv[4]
    mainproce(pro_opera, db_data)
