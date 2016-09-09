#!/usr/bin/env python
#-*- coding:utf-8 -*- 

'''
date: 2016/08/22
role: 判断磁盘及判断版本是否是在跑版本
usage: publish_judge_version.py
'''
from yunwei.operate.prefix import log,execShell,exclusiveLock
from yunwei.install.cryptology import cryptoBase
from yunwei.operate.mysql import mysqlBase
from yunwei.getInfo.parser import parseIni
from yunwei.getInfo.connDb import mysqlConn

from optparse import OptionParser
import os,sys,re,time,datetime,shutil
import statvfs
reload(sys)
sys.setdefaultencoding("utf-8")

###参数定义函数
def args_module_des():
    usage  = '''  
    %prog -p whaley-cms-CMSDataSyncServer -v 20160820D1 -e test -s 200000
    %prog -p whaley-cms-CMSDataSyncServer -v 20160820R1 -e pro  -s 200000
    '''
    parser = OptionParser(usage)
    parser.add_option("-p","--prog",type="string",default=False,dest="pub_prog",
                      help="publish program name")
    parser.add_option("-v","--version",type="string",default=False,dest="pub_ver",
                      help="publish version")
    parser.add_option("-e","--env",type="string",default="test",dest="pub_env",
                      help="publish version")
    parser.add_option("-s","--size",type="int",default=0,dest="pub_size",
                      help="publish version")

    (options,args) = parser.parse_args()
    ###确认参数是否定义正确
    if not options.pub_prog or not options.pub_ver:
        parser.print_usage
        parser.error("arguments defined error!")

    ###返回值
    return (options.pub_prog,options.pub_ver,options.pub_env,options.pub_size)

###获取
def judge_version(pub_id,pub_ver,pub_env):
    ###错误码
    jv_flag = 0 

    ###读取publish_prog mysql连接数据
    prefix_pp = "pp"
    option_pp = "pp_db"
    mcp = mysqlConn(log_path) 
    jv_flag,tb_pp,info_pp = mcp.getConn(conf_main,conf_sub,prefix_pp,option_pp)
  
    ###sql
    pp_sql = "SELECT main_dir FROM %s WHERE publish_name='%s' AND up_env='%s'" %(tb_pp,pub_prog,pub_env)
    ###连接数据库
    mbp = mysqlBase(log_path,**info_pp)
    pp_query = mbp.query(pp_sql)

    ###判断有无数据
    if not pp_query:
        logIns.writeLog('error','get %s %s main_dir error'%(pub_id,pub_env))
        jv_flag = 1

    ###获取在跑版本目录
    try:
        gver_dir = pp_query[0][0]
    except:
        gver_dir = ''

    ###获取在跑的版本
    gver_path = os.path.join(gver_dir,rver_file)

    ###读取文件
    read_ver = ''
    try:
        with open(gver_path,'rb') as gp:
            read_ver = gp.readline()
    except:
        pass

    ###确认在跑版本不是同步版本
    if read_ver and read_ver == pub_ver:
        logIns.writeLog('error','sync version is run version')
        jv_flag = 2
    
    ###返回错误码
    return jv_flag

###判断磁盘空间函数
def judge_size(pub_size):
    ###错误码
    js_flag = 0

    ###可用空间
    vfs = os.statvfs(data_dir) 
    space_size = vfs[statvfs.F_BAVAIL]*vfs[statvfs.F_BSIZE] 

    if space_size < pub_size:
        logIns.writeLog('error','free space not enough')
        js_flag = 1

    ###返回错误码
    return js_flag
    
if __name__ == "__main__":
    ###脚本名
    script_name = os.path.basename(__file__)
    sub_name    = script_name.split('.')[0]

    ###日志路径
    log_path = '/log/yunwei/%s.log' %script_name

    ###定义日志标识
    logIns  = log('1031',log_path)
    logMain = log('1031','/log/yunwei/yunwei.log')

    script_info = ' '.join(sys.argv)

    ###脚本排它锁
    exclusiveLock(script_name)

    logMain.writeLog('info','%s start'% script_info)

    ###配置文件路径
    conf_pwd  = os.path.join(os.path.dirname(os.path.realpath(__file__)),'conf')
    conf_main = os.path.join(conf_pwd,'common.conf')
    conf_sub  = os.path.join(conf_pwd,'%s.conf' %sub_name)

    ###相关变量
    rver_file = "version.txt"
    data_dir  = "/data"

    ###参数判断
    pub_prog,pub_ver,pub_env,pub_size = args_module_des()

    ###判断同步版本是不是在跑版本
    jv_code = judge_version(pub_prog,pub_ver,pub_env)

    ###判断磁盘空间
    js_code = judge_size(pub_size)

    ###确认脚本是否成功
    if jv_code != 0 or js_code != 0:
        logMain.writeLog('info','%s error end'% script_info)
    else:
        logMain.writeLog('info','%s success end'% script_info)
        print "success"
