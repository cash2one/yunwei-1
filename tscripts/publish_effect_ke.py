#!/usr/bin/env python
#-*- coding:utf-8 -*- 

'''
date: 2016/08/23
role: 版本服务器批量执行publish_effect_version.py完成版本的软连接
usage: publish_effect_ke.py
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
    %prog -p whaley-cms-CMSDataSyncServer -v 20160820D1 -e test -g 1 -a publish
    %prog -p whaley-cms-CMSDataSyncServer -v 20160820R1 -e pro  -g 1 -a rollback
    '''
    parser = OptionParser(usage)
    parser.add_option("-p","--prog",type="string",default=False,dest="pub_prog",
                      help="publish program name")
    parser.add_option("-v","--version",type="string",default=False,dest="pub_ver",
                      help="publish version")
    parser.add_option("-e","--env",type="string",default="test",dest="pub_env",
                      help="publish environment")
    parser.add_option("-g","--gray",type="int",default=1,dest="pub_gray",
                      help="publish gray")
    parser.add_option("-a","--action",type="string",default="publish",dest="pub_action",
                      help="publish or rollback")

    (options,args) = parser.parse_args()
    ###确认参数是否定义正确
    if not options.pub_prog or not options.pub_ver:
        parser.print_usage
        parser.error("arguments defined error!")

    ###判断git或者svn的版本格式 
    ver_match = re.search(r'\d{8}(D|R)\d{1,2}',options.pub_ver)
    if ver_match:
        ###如果是test环境版本D,如果是pro环境版本R
        ver_ident = ver_match.group(1)
        if ver_ident == 'D':
            if options.pub_env != "test" and options.pub_env != "hotfix":
                parser.error("version D must test environment!")
        elif ver_ident == 'R':
            if options.pub_env != "pro":    
                parser.error("version R must pro environment!")
    else:
         if not re.search(r'\d+',options.pub_ver):
             parser.error("-v must like 20160820R1/20160820D1/110!")

    ###环境暂只有test和pro
    if options.pub_env != 'test' and options.pub_env != 'pro' and options.pub_env != 'hotfix':
        parser.error("-e must test or pro")

    ###判断灰度值
    if options.pub_gray < 1 or options.pub_gray > 100:
        parser.error("-g must 1-100")

    ###判断动作是发布还是回退
    if options.pub_action != 'publish' and options.pub_action != 'rollback':
        parser.error("-a must publish or rollback")
    
    ###返回值
    return (options.pub_prog,options.pub_ver,options.pub_env,options.pub_gray,options.pub_action)

###获取远程服务器函数
def get_effect_ips(pub_prog,pub_env,pub_gray,host_option):
    ###错误码
    gei_flag = 0

    ###读取publish_prog mysql连接数据
    prefix_pp = "pp"
    option_pp = "pp_db"
    mcp = mysqlConn(log_path)
    gei_flag,tb_pp,info_pp = mcp.getConn(conf_main,conf_sub,prefix_pp,option_pp)

    ###sql
    pp_sql = "SELECT up_addr FROM %s WHERE publish_name='%s' AND up_env='%s' AND gray_level='%s'" %(tb_pp,pub_prog,pub_env,pub_gray)

    ###连接数据库
    mbp = mysqlBase(log_path,**info_pp)
    pp_query = mbp.query(pp_sql)

    ###判断有无数据
    if not pp_query:
        logIns.writeLog('error','get %s %s ip error'%(pub_prog,pub_env))
        gei_flag = 1

    ###创建临时文件目录
    write_dir = '/tmp/yunweitmp'
    if not os.path.isdir(write_dir):
        ###创建目录
        try:
            os.makedirs(write_dir,mode=0777)
        except:
            logIns.writeLog('error','%s mkdir error'% write_dir)
            gei_flag = 2

    ###临时文件名
    write_file = '%s_%s.tmp'%(pub_prog,format_date)

    ###临时文件路径
    write_path = os.path.join(write_dir,write_file)

    ###ini文件的项
    ansible_host = ["[%s]" %host_option]
    for ip in pp_query:
        if re.search(r'(\d{1,3}\.){3}\d{1,3}',ip[0]):
            ansible_host.append("\n%s" %ip[0])

    ###写入临时文件
    with open(write_path,'w') as fw:
        fw.writelines(ansible_host)

    ###返回错误码
    return (gei_flag,write_path)

###灰度服务器执行软链脚本
def exec_effect_version(host_ansible,host_option):
    ###实例化ansible
    center_manage = cenManage(log_path)

    ###先拷贝
    center_manage.execAnsible('synchronize','src=/data/tscripts/publish_effect_version.py dest=/data/tscripts/publish_effect_version.py compress=yes mode=0777',host_ansible,host_option,10)

    ###执行publish_effect_version.py
    r_flag,r_out = center_manage.execAnsible('raw','python /data/tscripts/publish_effect_version.py -p %s -v %s -e %s -g %s -a %s' %(pub_prog,pub_ver,pub_env,pub_gray,pub_action),host_ansible,host_option,30)

    if isinstance(r_out, dict):
        for k,v in r_out.items():
            if not re.search(r'success',v):
                logIns.writeLog('error','have server exec effect version error %s'%k)
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
    logIns  = log('1033',log_path,display=True)
    logMain = log('1033','/log/yunwei/yunwei.log',display=True)

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
    pub_prog,pub_ver,pub_env,pub_gray,pub_action = args_module_des()

    ###ini的选项
    host_option = "%s_%s_effect" %(pub_prog,pub_env)

    ###获取灰度后的远程服务器ip组成的hosts
    gei_code,write_path = get_effect_ips(pub_prog,pub_env,pub_gray,host_option)

    ###远程服务器执行软链脚本publish_effect_version.py
    eev_code = gei_code
    if gei_code == 0:
        eev_code = exec_effect_version(write_path,host_option)

    ###确认脚本是否成功
    if eev_code != 0:
        logMain.writeLog('info','%s error end'% script_info)
    else:
        logMain.writeLog('info','%s success end'% script_info)
        print "success"
