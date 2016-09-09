#!/usr/bin/env python
#-*- coding:utf-8 -*-

'''
date: 2016/08/20
role: 压缩解压
usage: cmb = compressBase(log_path)    实例化
       cmb.zipp(source_dir,zipfile_path)
       cmb.tar(source_dir,tarfile_path)
       cmb.unzip(zipfile_path,target_dir)
       cmb.untar(tarfile_path,target_dir)
'''

from __future__ import absolute_import

from yunwei.operate.prefix import log
logIns = log('117')

import os,zipfile,tarfile

###压缩解压操作类
class compressBase:
    def __init__(self,log_path):
        ###log_path为日志写入文件
        logIns = log('117',log_path)
        self.zf = '' 

    ###析构函数 
    def __del__(self):
        try:
            self.zf.close()
        except:
            pass

    ###zip压缩
    def zipp(self,source_dir,zipfile_path):    
        ###判断文件或目录是否存在
        if not os.path.exists(source_dir):
            logIns.writeLog('error','%s not exists' %source_dir)
            raise ValueError('117,%s not exists' %source_dir)

        ###循环把文件加入列表
        file_list = []
        if os.path.isfile(source_dir):
            file_list.append(source_dir)
        else:
            for root, dirs, files in os.walk(source_dir):
                for name in files:
                    file_list.append(os.path.join(root, name))
         
        ###调用zipfile模块
        self.zf = zipfile.ZipFile(zipfile_path, "w", zipfile.zlib.DEFLATED)
        for file_one in file_list:
            arc_name = file_one[len(source_dir):]
            self.zf.write(file_one,arc_name)

    ###解压zip
    def unzip(self,zipfile_path, unzip_dir):
        if not os.path.exists(unzip_dir):
            os.makedirs(unzip_dir, 0777)

        self.zf = zipfile.ZipFile(zipfile_path)
        for name in self.zf.namelist():
            name = name.replace('\\','/')
        
            if name.endswith('/'):
                os.makedirs(os.path.join(unzip_dir, name))
            else:            
                ext_file = os.path.join(unzip_dir, name)
                ext_dir  = os.path.dirname(ext_file)
                if not os.path.exists(ext_dir) : 
                    os.makedirs(ext_dir,0777)

                with open(ext_file, 'wb') as ef:
                    ef.write(self.zf.read(name))

    ###tar压缩
    def tar(self,source_dir,tarfile_path):    
        ###判断文件或目录是否存在
        if not os.path.exists(source_dir):
            logIns.writeLog('error','%s not exists' %source_dir)
            raise ValueError('117,%s not exists' %source_dir)

        ###调用tarfile模块
        self.zf = tarfile.open(tarfile_path, "w:gz")

        ###判断源目录长度
        len_source = len(source_dir)
        
        ###循环把文件加入列表
        for root, dirs, files in os.walk(source_dir):
            for name in files:
                full_path = os.path.join(root,name)
                self.zf.add(full_path,arcname=os.path.join(root[len_source:],name))

    ###解压tar
    def untar(self,tarfile_path, untar_dir):
        if not os.path.exists(untar_dir):
            os.makedirs(untar_dir, 0777)

        try:
            self.zf = tarfile.open(tarfile_path, "r:gz")
            file_names = self.zf.getnames()
            for file_name in file_names:
                self.zf.extract(file_name, untar_dir)
        except Exception, e:
            logIns.writeLog('error','%s untar error' %tarfile_path)
            raise ValueError('error','%s untar error' %tarfile_path)

