#!/usr/bin/env python
#-*- coding:utf-8 -*-

'''
date: 2016/08/15
role: 发送邮件
usage: fb = ftpBase(log_path,ftp_host='xxx',ftp_user='xxx',ftp_pswd='xxx',ftp_port = 21,timeout = 60)    实例化
       fb.uploadFile(local_path, remote_path)                    远程目录默认为根目录
       fb.uploadDir(local_dir, remote_dir)                       本地目录默认为当前目录，远程目录默认为根目录
       fb.listDir(remote_dir)                                    远程目录默认为根目录
       fb.findFile(file_name,remote_dir)                         查找匹配文件或目录
'''

from __future__ import absolute_import

from yunwei.operate.prefix import log,execShell
logIns = log('114')

import os,time,re,socket,shutil
from ftplib import FTP

###mail操作类
class ftpBase():
    def __init__(self,log_path,**args):
        ###log_path为日志写入文件
        logIns = log('114',log_path)

        ###获取参数
        self.host = args.get('ftp_host')
        self.user = args.get('ftp_user')
        self.pswd = args.get('ftp_pswd')
        self.port = args.get('ftp_port',21)
        self.timeout = args.get('time_out',60)
   
        ###连接
        try:
            self.ftp = FTP()
            self.ftp.connect(self.host,self.port,self.timeout)
            self.ftp.login(self.user,self.pswd)
        except:
            logIns.writeLog('error','%s connect error' %self.host)
            raise ValueError('114,%s connect error %s'%self.host)

    ###析构函数 
    def __del__(self):
        self.ftp.close()

    def _checkDir(self,check_dir):
        ###确认远程目录是否存在
        try:
            self.ftp.cwd(check_dir)
        except:
            pass

    ###上传文件
    def uploadFile(self, local_path, remote_path='./'):  
        ###判断本地文件
        if not os.path.isfile(local_path):
            logIns.writeLog('error','%s not exists' %self.local_path)
            raise ValueError('114,%s not exists'%self.local_path)

        ###上传的目录
        remote_dir = self.ftp.pwd()
    
        try:
            fp_ftp = open(local_path,'rb')
            self.ftp.storbinary('STOR %s' % remote_path,fp_ftp)
        except Exception,e:
            local_file = os.path.basename(local_path)
            up_cmd = "ncftpput -z -u%s -p%s %s %s %s"% (self.user,self.pswd,self.host,remote_dir,local_path)
            (get_status_up,get_output_up) = execShell(up_cmd)
            ###确认是否执行成功
            if (get_status_up != 0):
                logIns.writeLog('error','%s upload error'% local_path)
                raise ValueError('114,%s upload error'% local_path)
                
    ###上传目录
    def uploadDir(self, local_dir='./', remote_dir='./'):  
        if not os.path.isdir(local_dir):    
            logIns.writeLog('error','%s not exists' %self.local_dir)
            raise ValueError('114,%s not exists'%self.local_dir)

        ###创建目录
        all_dirs = remote_dir.split(os.sep)
        for all_dir in all_dirs:
            try:
                self.ftp.mkd(all_dir)
            except:
                pass
            self._checkDir(all_dir)

        ###进入目录
        self._checkDir(remote_dir)

        ###遍历本地目录
        for file in os.listdir(local_dir):  
            src = os.path.join(local_dir, file)  
            if os.path.isfile(src):  
                self.uploadFile(src, file)  
            elif os.path.isdir(src):  
                try:    
                    self.ftp.mkd(file)    
                except:    
                    pass
                self.uploadDir(src, file)  
        self.ftp.cwd('..') 

    ###打印列表
    def listDir(self,remote_dir='./'):
        self._checkDir(remote_dir)
        return self.ftp.dir()

    ###查找远程目录或文件
    def findFile(self,file_name,remote_dir='./'):
        self._checkDir(remote_dir)
        all_files = self.ftp.nlst()

        ###匹配则输出
        retu_files = []
        for all_file in all_files:
            if re.search(r'%s' %file_name,all_file):
                retu_files.append(all_file)

        ###返回
        return retu_files

    ###下载文件
    def downloadFile(self,local_path, remote_path):
        ###备份旧文件
        if os.path.isfile(local_path):
            shutil.move(local_path,"%s.%s"% (local_path,"old"))    

        ###下载文件
        try:
            fp_ftp = open(local_path,'wb').write
            self.ftp.retrbinary('RETR %s' %remote_path,fp_ftp)
        except:
            down_cmd = "wget ftp://%s:%s@%s/%s -P %s"% (self.user,self.pswd,self.host,remote_path,os.path.dirname(local_path))
            (get_status_down,get_output_down) = execShell(down_cmd)
            ###确认是否执行成功
            if (get_status_down != 0):
                logIns.writeLog('error','%s download error'% remote_path)
                raise ValueError('114,%s download error'% remote_path)

    ###下载目录
    def downloadDir(self,local_dir='./', remote_dir='./'):
        ###创建本地目录
        if not os.path.isdir(local_dir):
            os.makedirs(local_dir)

        ###进入目录
        self._checkDir(remote_dir)

        ###获取远程所有文件
        remote_files = self.listDir(remote_dir)
        for item in remote_files:
            file_type  = item[0]
            file_name  = item[1]
            local_base = os.path.join(local_dir, file_name)
            if file_type == 'd':
                self.downloadDir(local_base, file_name)
            elif filetype == '-':
                self.downloadFile(local_base, file_name)

        self.ftp.cwd('..')  

