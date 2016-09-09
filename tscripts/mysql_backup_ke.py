#!/usr/bin/env python
#-*- coding:utf-8 -*- 

'''
date: 2016/08/29
role: 发布系统更新mysql前查询更新mysql的从服务器调用 mysql_backup.py 进行数据库备份
usage: mysql_backup_ke.py -p metis-cms-metis -v 20160820D1 -e test -d db_name -t tb_name -g front
'''
from yunwei.operate.prefix import log,execShell,exclusiveLock
from yunwei.install.cryptology import cryptoBase
from yunwei.operate.mysql import mysqlBase
from yunwei.getInfo.parser import parseIni
from yunwei.getInfo.connDb import mysqlConn
from yunwei.operate.centralization import cenManage

from optparse import OptionParser
import os,sys,re,time,datetime,shutil
import pwd,socket,fcntl,struct
reload(sys)
sys.setdefaultencoding("utf-8")

###参数定义函数
def args_module_des():
    usage  = '''  
    %prog -p metis-cms-metis -v 20160820D1 -e test -d test_db -t tb_name -g front
    '''
    parser = OptionParser(usage)
    parser.add_option("-p","--pro",type="string",default=False,dest="pub_prog",
                      help="exec project")
    parser.add_option("-v","--version",type="string",default=False,dest="pub_ver",
                      help="publish version")
    parser.add_option("-e","--env",type="string",default=False,dest="pub_env",
                      help="exec environment")
    parser.add_option("-d","--db",type="string",default="",dest="db_name",
                      help="exec db name")
    parser.add_option("-t","--tb",type="string",default="",dest="tb_name",
                      help="exec tb name")
    parser.add_option("-g","--flag",type="string",default="",dest="host_flag",
                      help="db host flag")

    (options,args) = parser.parse_args()
    ###确认参数是否定义正确
    if not options.pub_prog or not options.pub_ver or not options.pub_env:
        parser.print_usage
        parser.error("-p -v and -e rguments must defined!")

    ###返回值
    return (options.pub_prog,options.pub_ver,options.pub_env,options.db_name,options.tb_name,options.host_flag)

###获取远程服务器函数
def get_back_ips(pub_prog,pub_ver,pub_env,db_name,host_flag):
    ###错误码
    gbi_flag = 0

    ###读取 mysql连接数据
    prefix_em = "em"
    option_em = "em_db"
    mce = mysqlConn(log_path)
    em_flag,tb_em,info_em = mce.getConn(conf_main,conf_sub,prefix_em,option_em)

    ###查询语句
    em_sql = "SELECT db_host FROM %s WHERE publish_name='%s' AND up_env='%s' AND db_name='%s' AND proj_flag='%s'" %(tb_em,pub_prog,pub_env,db_name,host_flag)

    ###连接数据库
    mbe = mysqlBase(log_path,**info_em)
    em_query = mbe.query(em_sql)

    ###获取mysql主ip
    try:
        ip_master = em_query[0][0]
    except:
        gbi_flag = 1
        logIns.writeLog('error','get %s %s ip error'%(pub_prog,pub_env))
        return gbi_flag,''

    ###获取其中的一个从库
    prefix_si = "si"
    option_si = "si_db"
    mcs = mysqlConn(log_path)
    si_flag,tb_si,info_si = mcs.getConn(conf_main,conf_sub,prefix_si,option_si)

    ###sql
    si_sql = "SELECT db_host FROM %s WHERE is_valid=1 AND master_ip='%s' AND is_slave=1" %(tb_si,ip_master)
 
    ###连接数据库
    mbs = mysqlBase(log_path,**info_si)
    si_query = mbs.query(si_sql)
    
    ###获取mysql从库ip
    ip_slave = ""
    try:
        ip_slave = si_query[0][0]
    except:
        logIns.writeLog('debug','get %s %s slave ip error'%(tb_si,ip_master))    

    ###如果获取从库失败，则在主库备份
    if not ip_slave:
        ip_slave = ip_master
    
    ###返回错误码
    return gbi_flag,ip_slave

###从库执行备份
def exec_slave_backup(slave_ip):
    ###实例化ansible
    center_manage = cenManage(log_path)

    ###拼接参数
    if db_name:
        if tb_name:
            pars = '-d %s -t %s' %(db_name,tb_name)
        else:
            pars = '-d %s' %(db_name,)
    else:
        pars = ''

    ###执行mysql_backup.py
    r_flag,r_out = center_manage.ipAnsible('raw','python /data/tscripts/mysql_backup.py %s' %pars,slave_ip)

    if isinstance(r_out, dict):
        for k,v in r_out.items():
            if not re.search(r'success',v):
                logIns.writeLog('error','have server exec mysql backup error %s'%k)
                r_flag = 1

    ###返回结果码
    return r_flag

if __name__ == "__main__":
    ###脚本名
    script_name = os.path.basename(__file__)
    sub_name    = script_name.split('.')[0]

    ###日志路径
    log_path = '/log/yunwei/%s.log' %script_name

    ###定义日志标识
    logIns  = log('1034',log_path,display=True)
    logMain = log('1034','/log/yunwei/yunwei.log',display=True)

    script_info = ' '.join(sys.argv)

    ###脚本排它锁
    exclusiveLock(script_name)

    logMain.writeLog('info','%s start'% script_info)

    ###配置文件路径
    conf_pwd  = os.path.join(os.path.dirname(os.path.realpath(__file__)),'conf')
    conf_main = os.path.join(conf_pwd,'common.conf')
    conf_sub  = os.path.join(conf_pwd,'%s.conf' %sub_name)

    ###时间
    update_time  = datetime.datetime.now()
    format_date = update_time.strftime('%Y%m%d%H%M%S')
    
    ###参数判断
    pub_prog,pub_ver,pub_env,db_name,tb_name,host_flag = args_module_des()

    ###获取从库ip
    gbi_code,slave_ip = get_back_ips(pub_prog,pub_ver,pub_env,db_name,host_flag)

    ###mysql从库执行
    esb_code = gbi_code
    if gbi_code == 0:
        esb_code = exec_slave_backup(slave_ip)

    ###确认脚本是否成功
    if esb_code != 0:
        logMain.writeLog('info','%s error end'% script_info)
    else:
        logMain.writeLog('info','%s success end'% script_info)
        print "success"
