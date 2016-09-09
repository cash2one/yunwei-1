#!/usr/bin/env python
#-*- coding:utf-8 -*-

'''
date: 2016/07/12
role: 配置文件解析类 1.ini解析 2.nginx配置文件解析
usage: 1.all_optu = parseIni(conf_file,'Mysql')    all_opdi = all_optu.getOption() or all_opdi = all_optu.getOption("a","b","c") or all_opdi = all_optu.getOption("a")
2.ngx_conf = parseNgx(conf_file) all_block = ngx_conf.loadFile()
notice: 1.2016/07/12   添加获取指定所有的键值,如果只取一个键，则直接返回值
2. 2016/07/18 添加nginx配置文件解析类
'''
from __future__ import absolute_import
import sys,os

#sys.path.append('%s/operate'% os.path.dirname(os.getcwd()))
from yunwei.operate.prefix import log
logIns = log('131')

import ConfigParser

###nginx类文件操作类
class parseNgx:
    def __init__(self,log_path,path):
        ###log_path为日志写入文件
        logIns = log('131',log_path)
        ###配置文件路径
        self.path = path

    def loadFile(self):
        ###判断配置文件path是否存在
        if not os.path.isfile(self.path):
            logIns.writeLog('error','%s not exist'% self.path)
            raise IOError('131,conf file not exist %s'% self.path)

        ###读取配置文件
        conf = ''
        with open(self.path, 'r') as f:
            conf = f.read()

        ###定义初始变量
        self.config = conf
        self.length = len(self.config) - 1
        self.count  = 0

        ###返回列表的字典
        return self.parseBlock()

    def parseBlock(self):
        ###相关变量定义
        re_data  = []
        data_buf = ''
        param_name  = None
        param_value = None

        ###逐个字符遍历
        while self.count < self.length:
            if '\n' == self.config[self.count]:
                if data_buf and param_name:
                    if param_value is None:
                        param_value = []
                    param_value.append(data_buf.strip())
                    data_buf = ''
            elif ' ' == self.config[self.count] or '\t' == self.config[self.count]:
                if not param_name and len(data_buf.strip()) > 0:
                    param_name = data_buf.strip()
                    data_buf   = ''
                else:
                    data_buf += self.config[self.count]
            elif ';' == self.config[self.count]:
                if isinstance(param_value, list):
                    param_value.append(data_buf.strip())
                    re_data.append({'name': param_name, 'value': param_value, 'type': 'item'})
                else:
                    param_value = data_buf.strip()
                    re_data.append({'name': param_name, 'value': param_value.split(' '), 'type': 'item'})

                param_name  = None
                param_value = None
                data_buf = ''
            elif '{' == self.config[self.count]:
                self.count += 1
                ###递归
                data_block  = self.parseBlock()
                re_data.append({'name': param_name, 'param': data_buf.strip(), 'value': data_block, 'type': 'block'})
                param_name  = None
                param_value = None
                data_buf    = ''
            elif '}' == self.config[self.count]:
                self.count += 1
                return re_data
            elif '#' == self.config[self.count]:
                while self.count < self.length and '\n' != self.config[self.count]:
                    self.count += 1
            else:
                data_buf += self.config[self.count]
            self.count += 1

        ###返回结果
        return re_data

###重写ConfigParser类,取消全部转小写
class myrawconf(ConfigParser.RawConfigParser):  
    def __init__(self,defaults=None):  
        ConfigParser.RawConfigParser.__init__(self,defaults=None,allow_no_value=True)  
    def optionxform(self, optionstr):  
        return optionstr  

###重写ConfigParser类,取消全部转小写
class myconf(ConfigParser.ConfigParser):
    def __init__(self,defaults=None):
        ConfigParser.ConfigParser.__init__(self,defaults=None)
    def optionxform(self, optionstr):
        return optionstr

###ini文件操作类
class parseIni:
    def __init__(self,log_path,path,section):
        ###log_path为日志写入文件
        logIns = log('131',log_path)

        self.path = path
        self.sect = section

    def getOption(self,*keys):
        ###判断配置文件path是否存在
        if not os.path.isfile(self.path):
            logIns.writeLog('error','%s not exist'% self.path)
            raise IOError('131,file not exist %s'% self.path) 

        ###解析文件
        try:
            fh_conf = myrawconf()
            fh_conf.read(self.path)
        except:
            try:
                fh_conf = myconf()
                fh_conf.readfp(open(self.path))
            except:
                fh_conf = ConfigParser.ConfigParser()
                fh_conf.read(self.path)
                
        ###确认配置文件有无配置层
        list_section = fh_conf.sections()
        
        res_back = {}
        if self.sect in list_section:
            ###取出section下的所有配置项
            res_list = fh_conf.items(self.sect)

            ###转换成字典
            res_dict = dict(res_list)

            if keys:
                ###取出指定的键
                for k,v in res_dict.items():
                    if k in keys:
                        res_back[k] = v
            else:
                res_back = res_dict
        else:
            logIns.writeLog('error','%s not have %s'% (self.path,self.sect))
            raise ValueError('131,%s not have %s'% (self.path,self.sect))

        ###如果只取一个键，则返回值，其他返回字典
        if keys and len(keys) == 1:
            return res_back.get(keys[0],'')

        return res_back

