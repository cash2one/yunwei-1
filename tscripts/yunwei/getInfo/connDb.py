#!/usr/bin/env python
#-*- coding:utf-8 -*-

'''
date: 2016/07/18
role: 从主配置和本身配置中读取mysql的连接配置
usage: mc = mysqlConn(log_path) mc.getConn(conf_main,conf_sub,prefix_flag,option_par) return db_flag,tb_name,info_dic
'''
from __future__ import absolute_import
import sys,os,base64

from yunwei.getInfo.parser import parseIni
from yunwei.operate.mysql import mysqlBase
from yunwei.install.cryptology import cryptoBase
#sys.path.append('%s/operate'% os.path.dirname(os.getcwd()))
from yunwei.operate.prefix import log
logIns = log('132')

import ConfigParser

###获取mysql连接所需信息类
class mysqlConn:
    def __init__(self,log_path):
        ###log_path为日志写入文件
        logIns = log('132',log_path)
        self.log_path = log_path

    ###获取数据库连接信息
    def _getDb(self,conf_file,option_par,prefix):
        ###退出码
        db_flag = 0

        ###判断配置文件是否存在
        if not os.path.isfile(conf_file):
            db_flag = 1
            return (db_flag,'',{})

        ###解析数据库信息
        all_optu   = parseIni(self.log_path,conf_file,option_par)
        try:
            info_mysql = all_optu.getOption('%s_host' %prefix,'%s_user' %prefix,'%s_passwd' %prefix,'%s_db' %prefix,'%s_tb' %prefix,'%s_port' %prefix,'%s_charset' %prefix)
        except:
            info_mysql = {}

        ###判断配置文件
        db_record = ['%s_host' %prefix,'%s_user' %prefix,'%s_passwd' %prefix,'%s_db' %prefix,'%s_port' %prefix,'%s_charset' %prefix]
        tb_name   = info_mysql.get('%s_tb' %prefix,'')

        ###把符合的信息重新组成字典
        info_dic = {}
        for k,v in info_mysql.items():
            if k in db_record:
                info_dic[k] = v

        ###如果取出的数据为空
        if not info_dic:
            logIns.writeLog('error','get mysql config error for %s'% conf_file)
            db_flag = 2

        return (db_flag,tb_name,info_dic)

    ###获取数据库数据信息
    def getConn(self,conf_main,conf_sub,prefix_flag,option_par):
        ###获取数据库连接
        db_flag_m,tb_name_m,info_dic_m = self._getDb(conf_main,option_par,prefix_flag)
        db_flag_s,tb_name_s,info_dic_s = self._getDb(conf_sub,option_par,prefix_flag)

        ###把错误码合并
        db_flag_h = 0
        if db_flag_m != 0 and db_flag_s != 0:
            db_flag_h = 1
 
        ###把表信息合并
        if tb_name_s:
            tb_name_m = tb_name_s

        ###把数据库连接信息合并
        info_dic_m.update(info_dic_s)

        ###引入解密模块
        cb = cryptoBase(self.log_path)

        ###把字段转换成mysql统一字段
        info_dic_h = {}
        for k,v in info_dic_m.items():
            k_n = k.split('_')[-1]
            ###如果是passwd的需要解密
            if k_n == 'passwd':
                try:
                    v_n = cb.decrypt_with_certificate(v)
                except:
                    v_n = base64.b64decode(v)
            else:
                v_n = v

            ###组成可以连接数据库的字典
            info_dic_h[k_n] = v_n 

        ###返回错误码，表名，连接数据库字典
        return (db_flag_h,tb_name_m,info_dic_h)

