#!/usr/bin/env python
#-*- coding:utf-8 -*-

'''
date: 2016/07/12
role: 1.write log  2.exec shell  3.script exclusive lock
usage: 1.d = log() d.writeLog('error','file not exists',log_flag='db[1]',log_file='/var/log/yunwei.log',display=True)  日志等级 日志信息  日志标识（缺省yunwei） 日志保存路径（缺省/var/log/yunwei.log）是否终端显示
       2.execShell('cmd')
       3.exclusiveLock('script name')
notice: four spaces for indentation
'''

import logging,os
import re,time

###写日志类
class log:
    def __init__(self,log_flag='yunwei',log_file='/log/yunwei/yunwei.log',display=False):
        self.logFlag = log_flag
        self.logFile = log_file
        self.display = display
            
    def writeLog(self,log_level,log_message):
        ###创建一个logger
        logger = logging.getLogger(self.logFlag)
        logger.setLevel(logging.DEBUG)

        ###建立日志目录
        log_dir  = os.path.dirname(self.logFile)
        if not os.path.isdir(log_dir):
                os.makedirs(log_dir,mode=0777)

        ###给日志赋权0777
        if os.path.isfile(self.logFile):
            os.chmod(self.logFile,0777)

        ###创建一个handler用于写入日志文件
        fh = logging.FileHandler(self.logFile)
        fh.setLevel(logging.DEBUG)

        if self.display:
            ###创建一个handler用于输出到终端
            th = logging.StreamHandler()
            th.setLevel(logging.DEBUG)

        ###定义handler的输出格式
        formatter =logging.Formatter('%(asctime)s  %(name)s  %(levelname)s  %(message)s')
        fh.setFormatter(formatter)

        ###给logger添加handler
        logger.addHandler(fh)
        if self.display:
            logger.addHandler(th)

        ###记录日志
        level_dic = {'debug':logger.debug,'info':logger.info,'warning':logger.warning,'error':logger.error,'critical':logger.critical}
        level_dic[log_level](log_message)

        ###删除重复记录
        fh.flush()
        logger.removeHandler(fh)

        if self.display:
            th.flush()
            logger.removeHandler(th)

###执行shell命令函数
def execShell(exec_cmd):
    import commands
    ###执行传入的shell命令
    if exec_cmd:
        (cmd_status,cmd_output) = commands.getstatusoutput(exec_cmd)
    else:
        (cmd_status,cmd_output) = (111,'exec cmd is null')

    ###返回执行状态和输出结果
    return (cmd_status,cmd_output)

###脚本排它函数
def exclusiveLock(scriptName):
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
                raise ValueError('111,2 min after this script is still exists')
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

