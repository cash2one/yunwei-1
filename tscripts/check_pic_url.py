#!/usr/bin/env python
#-*- coding:utf-8 -*- 

'''
date: 2016/09/01
role: 从check_picture表中获取url,调用ucloud的api判断图片的类型写回pic_level,is_review,is_check字段
usage: check_pic_url.py 
'''
from yunwei.operate.prefix import log,execShell,exclusiveLock
from yunwei.install.cryptology import cryptoBase
from yunwei.operate.mail import mailBase
from yunwei.operate.mysql import mysqlBase
from yunwei.getInfo.parser import parseIni
from yunwei.getInfo.connDb import mysqlConn
from yunwei.operate.api import UcloudApiClient

from multiprocessing import cpu_count
import os,sys,re,time,datetime,shutil
import socket,fcntl,struct,base64,multiprocessing
reload(sys)
sys.setdefaultencoding("utf-8")

###鉴黄调用
def al_check_picture(pic_url,public_key, private_real,tb_cp,info_cp):
    ###连接数据库
    mb = mysqlBase(log_path,**info_cp)

    ###调用API
    ApiClient = UcloudApiClient(base_url, public_key, private_real)

    params   = {"Action":"ImagePornCheck","Image.0":pic_url}
    response = ApiClient.get("/",params)
    ###获取返回结果
    try:
        is_check   = "1"
        pic_level  = response["Result"]["ImageDetail"][0]["Label"]
        pic_review = response["Result"]["ImageDetail"][0]["Review"]
        if pic_review:
            is_review = "1"
        else:
            is_review = "0"
    except:
        pic_level = "2"   ###默认正常图片
        is_review = "0"   ###默认不需要复审
        is_check  = "2"

    ###更新选项
    up_condition = {}
    up_condition['pic_level'] = pic_level  
    up_condition['is_review'] = is_review 
    up_condition['is_check']  = is_check
        
    ###写入数据库
    try:
        mb.update(tb_cp,up_condition,"pic_url='%s'"%pic_url)
    except:
        logIns.writeLog('error','%s update mysql %s error' %(tb_cp,pic_url))

###更新check_picture数据库函数
def up_check_picture():
    ###错误码
    ucp_flag = 0

    ###读数据库
    prefix_cp = "cp"
    option_cp = "cp_db"
    mcc = mysqlConn(log_path)
    cp_flag,tb_cp,info_cp = mcc.getConn(conf_main,conf_sub,prefix_cp,option_cp)
    
    ###查询sql
    cp_sql = "SELECT pic_url FROM %s WHERE is_check=0 AND status=1 GROUP BY pic_url" %(tb_cp,)

    ###连接数据库
    mbc = mysqlBase(log_path,**info_cp)
    cp_query = mbc.query(cp_sql)

    ###配置文件中获取钥匙
    option_key  = "ucloud_key"
    all_key     = parseIni(log_path,conf_sub,option_key)
    public_key  = all_key.getOption('public_key')
    private_key = all_key.getOption('private_key') 

    ###导入解密模块
    cb = cryptoBase(log_path)
    try:
        private_real = cb.decrypt_with_certificate(private_key)
    except:
        private_real = base64.b64decode(private_key)

    ###定制进程数
    pro_num = cpu_count()
    pool = multiprocessing.Pool(processes = pro_num)

    ###并发调用扫描端口函数
    for url in cp_query:
        pool.apply_async(al_check_picture, (url[0],public_key, private_real,tb_cp,info_cp, ))

    pool.close()
    pool.join()

    ###返回错误码
    return ucp_flag
    
if __name__ == "__main__":
    ###脚本名
    script_name = os.path.basename(__file__)
    sub_name    = script_name.split('.')[0]

    ###日志路径
    log_path = '/log/yunwei/%s.log' %script_name

    ###定义日志标识
    logIns  = log('1037',log_path)
    logMain = log('1037','/log/yunwei/yunwei.log')

    script_info = ' '.join(sys.argv)

    ###脚本排它锁
    exclusiveLock(script_name)

    logMain.writeLog('info','%s start'% script_info)

    ###配置文件路径
    conf_pwd  = os.path.join(os.path.dirname(os.path.realpath(__file__)),'conf')
    conf_main = os.path.join(conf_pwd,'common.conf')
    conf_sub  = os.path.join(conf_pwd,'%s.conf' %sub_name)
    base_url  = "https://api.ucloud.cn"

    ###逐个扫描url
    ucp_code = up_check_picture()
    
    ###确认脚本是否成功
    if ucp_code != 0:
        logMain.writeLog('info','%s error end'% script_info)
    else:
        logMain.writeLog('info','%s success end'% script_info)
        print "success"

