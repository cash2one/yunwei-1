#!/usr/bin/env python
#-*- coding:utf-8 -*- 

'''
date: 2016/08/31
role: 调用service_operate.py脚本完成进程的重启等功能
usage: service_operate_ke.py
'''
from yunwei.operate.prefix import log,execShell,exclusiveLock
from yunwei.install.cryptology import cryptoBase
from yunwei.operate.mysql import mysqlBase
from yunwei.getInfo.parser import parseIni
from yunwei.getInfo.connDb import mysqlConn
from yunwei.operate.centralization import cenManage

from optparse import OptionParser
import os,sys,re,time,datetime,shutil
import socket,fcntl,struct,json
reload(sys)
sys.setdefaultencoding("utf-8")

###参数定义函数
def args_module_des():
    usage  = '''  
    %prog -p metis-cms-metis -e test -g 1 -a stop/start/restart
    %prog -p metis-cms-metis -e pro  -g 1 -a stop/start/restart
    '''
    parser = OptionParser(usage)
    parser.add_option("-p","--prog",type="string",default=False,dest="pub_prog",
                      help="publish program name")
    parser.add_option("-e","--env",type="string",default="test",dest="pub_env",
                      help="publish environment")
    parser.add_option("-g","--gray",type="int",default=1,dest="pub_gray",
                      help="publish gray")
    parser.add_option("-a","--action",type="string",default="restart",dest="pub_action",
                      help="stop start or restart")
    
    (options,args) = parser.parse_args()
    ###确认参数是否定义正确
    if not options.pub_prog or not options.pub_env:
        parser.print_usage
        parser.error("arguments defined error!")

    ###环境暂只有test和pro
    if options.pub_env != 'test' and options.pub_env != 'pro':
        parser.error("-e must test or pro")

    ###判断灰度值
    if options.pub_gray < 1 or options.pub_gray > 100:
        parser.error("-g must 1-100")

    ###返回值
    return (options.pub_prog,options.pub_env,options.pub_gray,options.pub_action)

###获取远程服务器函数
def get_start_ips(pub_prog,pub_env,pub_gray,host_option):
    ###错误码
    gsi_flag = 0

    ###读取publish_prog mysql连接数据
    prefix_pp = "pp"
    option_pp = "pp_db"
    mcp = mysqlConn(log_path)
    gsi_flag,tb_pp,info_pp = mcp.getConn(conf_main,conf_sub,prefix_pp,option_pp)

    ###sql
    pp_sql = "SELECT up_addr,cmd_dir,service_flag,cmd_line FROM %s WHERE publish_name='%s' AND up_env='%s' AND gray_level='%s'" %(tb_pp,pub_prog,pub_env,pub_gray)

    ###连接数据库
    mbp = mysqlBase(log_path,**info_pp)
    pp_query = mbp.query(pp_sql)

    ###判断有无数据
    if not pp_query:
        logIns.writeLog('error','get %s %s ip error'%(pub_prog,pub_env))
        gsi_flag = 1

    ###创建临时文件目录
    write_dir = '/tmp/yunweitmp'
    if not os.path.isdir(write_dir):
        ###创建目录
        try:
            os.makedirs(write_dir,mode=0777)
        except:
            logIns.writeLog('error','%s mkdir error'% write_dir)
            gsi_flag = 2

    ###临时文件名
    write_file = '%s_start_%s.tmp'%(pub_prog,format_date)

    ###临时文件路径
    write_path = os.path.join(write_dir,write_file)

    ###ini文件的项
    full_dir,service_flag,cmd_line = ["","",""]
    ansible_host = ["[%s]" %host_option]
    for ppq in pp_query:
        up_addr,cmd_dir,service_flag,cmd_line = ppq
        if re.search(r'(\d{1,3}\.){3}\d{1,3}',up_addr):
            ansible_host.append("\n%s" %up_addr)
        ###其他组成dict当参数传给重启脚本
        full_dir     = cmd_dir
        service_flag = service_flag
        cmd_line     = cmd_line

    ###写入临时文件
    with open(write_path,'w') as fw:
        fw.writelines(ansible_host)

    ###返回错误码
    return (gsi_flag,write_path,full_dir,service_flag,cmd_line)

###灰度服务器执行重启脚本
def exec_start_prog(host_ansible,host_option,full_dir,service_flag,cmd_line):
    ###实例化ansible
    center_manage = cenManage(log_path)
    #import pdb;pdb.set_trace()
    ###先拷贝
    center_manage.execAnsible('synchronize','src=/data/tscripts/scripts/service_operate.py dest=/data/tscripts/scripts/service_operate.py compress=yes mode=0777',host_ansible,host_option,10)
    ##import pdb;pdb.set_trace()
    ###执行service_operate.py
    r_flag,r_out = center_manage.execAnsible('raw',"su %s -c \"python /data/tscripts/scripts/service_operate.py %s %s '%s' %s\"" %(user_p,full_dir,service_flag,cmd_line,pub_action),host_ansible,host_option,30)
    if isinstance(r_out, dict):
        for k,v in r_out.items():
            if not re.search(r'success',v):
		print "Ansible return Failed ,message was %s"%v
                logIns.writeLog('error','have server exec restart service error %s'%k)
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
    logIns  = log('1038',log_path,display=True)
    logMain = log('1038','/log/yunwei/yunwei.log',display=True)

    script_info = ' '.join(sys.argv)

    ###脚本排它锁
    exclusiveLock(script_name)

    logMain.writeLog('info','%s start'% script_info)

    ###配置文件路径
    conf_pwd  = os.path.join(os.path.dirname(os.path.realpath(__file__)),'conf')
    conf_main = os.path.join(conf_pwd,'common.conf')
    conf_sub  = os.path.join(conf_pwd,'%s.conf' %sub_name)

    ###时间
    update_time = datetime.datetime.now()
    format_date = update_time.strftime('%Y%m%d%H%M%S')
    
    ###参数判断
    pub_prog,pub_env,pub_gray,pub_action = args_module_des()

    ###ini的选项
    host_option = "%s_%s_start" %(pub_prog,pub_env)
    user_p = "moretv"

    ###获取灰度后的远程服务器ip组成的hosts
    gsi_code,write_path,full_dir,service_flag,cmd_line = get_start_ips(pub_prog,pub_env,pub_gray,host_option)

    ###远程服务器执行软链脚本
    esp_code = gsi_code
    if gsi_code == 0:
        esp_code = exec_start_prog(write_path,host_option,full_dir,service_flag,cmd_line)

    ###确认脚本是否成功
    if esp_code != 0:
        logMain.writeLog('info','%s error end'% script_info)
    else:
        logMain.writeLog('info','%s success end'% script_info)
        print "success"
