#!/usr/bin/env python
#-*- coding:utf-8 -*- 

'''
date: 2016/08/30
role: 从mtv_cms库中的多个表中获取图片url,写入check_picture
usage: get_check_url.py
'''
from yunwei.operate.prefix import log,execShell,exclusiveLock
from yunwei.install.cryptology import cryptoBase
from yunwei.operate.mail import mailBase
from yunwei.operate.mysql import mysqlBase
from yunwei.getInfo.parser import parseIni
from yunwei.getInfo.connDb import mysqlConn

from multiprocessing import cpu_count
import multiprocessing
import os,sys,re,time,datetime,shutil
import socket,fcntl,struct,base64,math
reload(sys)
sys.setdefaultencoding("utf-8")

###写数据库函数
def in_check_picture(title,status,sid,update_time,db_name,tb_name,record_flag,able_urls):
    ###错误码
    icp_flag = 0

    ###写数据库
    prefix_cp = "cp"
    option_cp = "cp_db"
    mcc = mysqlConn(log_path)
    cp_flag,tb_cp,info_cp = mcc.getConn(conf_main,conf_sub,prefix_cp,option_cp)

    ###分表后的表名是tp_cp+record_flag
    record_flag = int(record_flag) + 24
    tb_last = "%s%s" %(tb_cp,record_flag)

    ###连接数据库
    mbc = mysqlBase(log_path,**info_cp)

    ###插入选项
    for url in able_urls:
        in_condition = {}
        in_condition['title']       = title
        in_condition['status']      = status
        in_condition['sid']         = sid
        in_condition['update_time'] = update_time
        in_condition['pic_url']     = url
        in_condition['db_name']     = db_name
        in_condition['tb_name']     = tb_name

        ###调用mysql类完成插入
        try:
            mbc.insert(tb_last,in_condition)
        except:
            logIns.writeLog('error','%s insert mysql error' %tb_last)
            icp_flag = 1

    ###返回错误码
    return icp_flag

###获取需要检查的url信息函数
def get_url_all(db_name,tb_name,record_flag,all_data):
    ###错误码
    gua_flag = 0

    ###获取数据
    for i in all_data:
        title,status,sid,update_time = i[:4]
        ###获取有效url
        able_urls = []
        urls = i[4:]
        for url in urls:
            url_form = str(url)
            able_url = url_form.split('|')
            for i in able_url:
                if re.search(r'http',i):
                    able_urls.append(i.strip())

        ###able_urls去重
        able_urls = list(set(able_urls))

        ###调用in_check_picture函数完成数据库插入
        gua_flag = in_check_picture(title,status,sid,update_time,db_name,tb_name,record_flag,able_urls)

    ###返回错误码
    return gua_flag

###获取含有图片url的各个表中的url
def get_url_data(tb_name,sql_field):
    ###错误码
    gud_flag = 0

    ###读取mtv_cms mysql连接数据
    prefix_bc = "baimao"
    option_bc = "baimao_db"
    mcb = mysqlConn(log_path) 
    bc_flag,tb_bc,info_bc = mcb.getConn(conf_main,conf_sub,prefix_bc,option_bc)
  
    ###sql
    bc_count = "SELECT count(*) FROM %s WHERE status=1" %tb_name

    ###连接数据库
    mbc = mysqlBase(log_path,**info_bc)
    count_query = mbc.query(bc_count)

    ###获取db_name
    db_name = info_bc.get('db',"")
    
    ###获取记录数
    try:
        num_a = int(count_query[0][0]) - 4040000
        #num_a = int(count_query[0][0])
    except:
        num_a = 0

    ###根据记录数分批插入不同的表中
    recods =  500000
    num_c = int(math.ceil(num_a / float(recods)))

    ###定制进程数
    #pro_num = cpu_count()
    #pool = multiprocessing.Pool(processes = pro_num)

    ###分线程
    for n in range(0,num_c):
        ###获取数据的起始值
        if n == 0:
            min_n = 4040001
            #min_n = n * recods
        else:
            min_n = n * recods + 4040001
            #min_n = n * recods + 1

        ###获取分批的记录数
        bc_sql = "SELECT %s FROM %s WHERE status=1 limit %d,%d" %(sql_field,tb_name,min_n,recods)
        #bc_sql = "SELECT %s FROM %s WHERE status=1 limit %d,%d" %(sql_field,tb_name,min_n,recods)
        print "aaa",bc_sql
        bc_query = mbc.query(bc_sql)
    
        ###多进程调用
        pool_flag = get_url_all(db_name,tb_name,n,bc_query)
#        pool_flag = pool.apply_async(get_url_all, (db_name,tb_name,n,bc_query,))
        if pool_flag !=0:
            gud_flag = 1

#    pool.close()
#    pool.join()

    ###返回错误吗
    return gud_flag
        
if __name__ == "__main__":
    ###脚本名
    script_name = os.path.basename(__file__)
    sub_name    = script_name.split('.')[0]

    ###日志路径
    log_path = '/log/yunwei/%s.log' %script_name

    ###定义日志标识
    logIns  = log('1036',log_path)
    logMain = log('1036','/log/yunwei/yunwei.log')

    script_info = ' '.join(sys.argv)

    ###脚本排它锁
    exclusiveLock(script_name)

    logMain.writeLog('info','%s start'% script_info)

    ###配置文件路径
    conf_pwd  = os.path.join(os.path.dirname(os.path.realpath(__file__)),'conf')
    conf_main = os.path.join(conf_pwd,'common.conf')
    conf_sub  = os.path.join(conf_pwd,'%s.conf' %sub_name)

    ###获取配置文件中的表及字段信息
    option_baimao = "baimao_tb"
    all_optu      = parseIni(log_path,conf_sub,option_baimao)
    table_field   = all_optu.getOption()

    ###获取图片url
    fin_code = 0
    for k,v in table_field.items():
        gud_code = get_url_data(k,v)
        if gud_code != 0:
            fin_code = gud_code
    
    ###确认脚本是否成功
    if fin_code != 0:
        logMain.writeLog('info','%s error end'% script_info)
    else:
        logMain.writeLog('info','%s success end'% script_info)
        print "success"

