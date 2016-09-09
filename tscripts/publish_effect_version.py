#!/usr/bin/env python
#-*- coding:utf-8 -*- 

'''
date: 2016/08/22
role: 把服务器的应用环境目录软连接到/data/$project/$version目录,本机执行
usage: publish_effect_version.py
'''
from yunwei.operate.prefix import log,execShell,exclusiveLock
from yunwei.install.cryptology import cryptoBase
from yunwei.operate.mysql import mysqlBase
from yunwei.getInfo.parser import parseIni
from yunwei.getInfo.connDb import mysqlConn

from optparse import OptionParser
import os,sys,re,time,datetime,shutil
import socket,fcntl,struct
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
                      help="publish version")
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
        parser.error("-e must test or pro or hotfix")

    ###判断灰度值
    if options.pub_gray < 1 or options.pub_gray > 100:
        parser.error("-g must 1-100")

    ###判断动作是发布还是回退
    if options.pub_action != 'publish' and options.pub_action != 'rollback':
        parser.error("-a must publish or rollback")
    
    ###返回值
    return (options.pub_prog,options.pub_ver,options.pub_env,options.pub_gray,options.pub_action)

###获取相应网卡的ip函数
def get_ip_addr(ifname):
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        ipaddr = socket.inet_ntoa(fcntl.ioctl(
            s.fileno(),
            0x8915,  # SIOCGIFADDR
            struct.pack('256s', ifname[:15])
        )[20:24])
    except IOError:
        logIns.writeLog('error',"get %s ip error!" % ifname)
        ipaddr = ''

    return ipaddr

###获取灰度IP,前后版本,发布情况等信息
def get_pub_info(pub_prog,pub_ver,pub_env,pub_gray,pub_action):
    ###错误码
    gpi_flag = 0 

    ###读取publish_prog mysql连接数据
    prefix_pp = "pp"
    option_pp = "pp_db"
    mcp = mysqlConn(log_path) 
    gpi_flag,tb_pp,info_pp = mcp.getConn(conf_main,conf_sub,prefix_pp,option_pp)
  
    ###sql
    pp_sql   = "SELECT main_dir,app_dir FROM %s WHERE publish_name='%s' AND up_env='%s' AND gray_level='%s' AND up_addr='%s'" %(tb_pp,pub_prog,pub_env,pub_gray,local_ip)
    gray_sql = "SELECT count(up_addr) FROM %s WHERE publish_name='%s' AND up_env='%s' AND gray_level='%s'" %(tb_pp,pub_prog,pub_env,pub_gray)
    all_sql  = "SELECT count(up_addr) FROM %s WHERE publish_name='%s' AND up_env='%s'" %(tb_pp,pub_prog,pub_env)
    ###连接数据库
    mbp = mysqlBase(log_path,**info_pp)
    pp_query   = mbp.query(pp_sql)
    gray_query = mbp.query(gray_sql)
    all_query  = mbp.query(all_sql)

    ###获取更新目录
    full_dir = ''
    for ppq in pp_query:
        main_dir,app_dir = ppq
        full_dir = os.path.join(main_dir,app_dir)
        break

    ###判读是否获取到完整更新目录
    if not full_dir:
        logIns.writeLog('error','get %s full_dir from %s error'%(pub_prog,pub_env))
        gpi_flag = 1

    ###获取动作代码
    if pub_action == 'publish':
        action_planp = 0
        action_planr = 1
    elif pub_action == 'rollback':
        action_planp = 2
        action_planr = 3

    ###计算百分比
    try:
        gray_percent = gray_query[0][0] * 100 / all_query[0][0]
    except:
        gray_percent = ''
        logIns.writeLog('error','get %s ip percent from %s error'%(pub_prog,pub_env))
        gpi_flag = 2

    ###返回错误码,发布码,回退码,灰度百分比,灰度ip
    return (gpi_flag,action_planp,action_planr,gray_percent,full_dir)

###更新信息写入version_record
def write_info_vr(action_plan,gray_percent,full_dir):
    ###错误码
    wiv_flag = 0

    ###读取version_record mysql连接数据
    prefix_vr = "vr"
    option_vr = "vr_db"
    mcv = mysqlConn(log_path)
    wiv_flag,tb_vr,info_vr = mcv.getConn(conf_main,conf_sub,prefix_vr,option_vr)

    ###连接数据库
    mbv = mysqlBase(log_path,**info_vr)

    ###插入选项
    in_condition = {}
    in_condition['publish_name'] = pub_prog
    in_condition['up_env']       = pub_env
    in_condition['action_plan']  = action_plan
    in_condition['gray_level']   = pub_gray
    in_condition['gray_percent'] = gray_percent
    in_condition['up_addr']      = local_ip

    ###获取之前版本
    rver_path  = os.path.join(full_dir,rver_file)
    ver_before = "0"
    try:
        with open(rver_path,'rb') as rpf:
            ver_before = rpf.readline()
    except:
        pass

    ###前后版本用-连接
    ver_sequ = "%s-%s" %(ver_before,pub_ver)
    in_condition['ver_sequ'] = ver_sequ

    ###插入数据
    try:
        mbv.insert(tb_vr,in_condition)
    except:
        logIns.writeLog('error','%s insert mysql error' %tb_vr)
        wiv_flag = 1

    ###返回错误码
    return wiv_flag

###回退获取上个版本号
def get_last_ver():
    ###错误码
    glv_flag = 0

    ###读取version_record mysql连接数据
    prefix_vr = "vr"
    option_vr = "vr_db"
    mcv = mysqlConn(log_path)
    glv_flag,tb_vr,info_vr = mcv.getConn(conf_main,conf_sub,prefix_vr,option_vr)

    ###sql
    vr_sql = "SELECT ver_sequ FROM %s WHERE publish_name='%s' AND up_env='%s' AND up_addr='%s' AND ver_sequ REGEXP '-%s$' AND action_plan=0" %(tb_vr,pub_prog,pub_env,local_ip,pub_ver)

    ###连接数据库
    mbv = mysqlBase(log_path,**info_vr) 
    vr_query = mbv.query(vr_sql)
 
    ###查询
    try:
        ver_sequ = vr_query[0][0]
    except:
        ver_sequ = ''
        logIns.writeLog('error','%s get last version from %s %s error' %(tb_vr,pub_prog,pub_env))
        glv_flag = 1
    
    ###获取上个版本
    last_ver  = ver_sequ.split('-')[0]
    if not last_ver:
        logIns.writeLog('error','get last version from %s %s error' %(pub_prog,pub_env))
        glv_flag = 2

    last_path = os.path.join(base_dir,pub_prog,last_ver)

    ###返回上个版本
    return (glv_flag,last_path)

###本机建立软连接
def effect_version(pub_action,full_dir):
    ###错误码
    ev_flag = 0

    ###备份目录
    backup_dir = os.path.join(base_dir,"%s.%s" %(pub_ver,format_date))

    ###获取需要软连接的目标目录
    if pub_action == 'publish':
        target_dir = os.path.join(base_dir,pub_prog,pub_ver)
    elif pub_action == 'rollback':  
        ###获取版本记录version_record中的上个版本
        ev_flag,target_dir = get_last_ver()
        if ev_flag != 0:
            return ev_flag

    ###确认目录是否存在
    if not os.path.isdir(target_dir):
        logIns.writeLog('error','last version %s not exists' %(target_dir,))
        ev_flag = 1

    ###建立软连接
    if os.path.islink(full_dir):
        if os.readlink(full_dir) != target_dir:
            os.unlink(full_dir)
        else:
            ###版本已经是在跑版本,返回成功,也打信息
            logIns.writeLog('info','%s is already running' %pub_ver)
    elif os.path.isdir(full_dir):
        shutil.move(full_dir,backup_dir)

    ###建立上层目录
    last_full = os.path.dirname(full_dir)
    if not os.path.isdir(last_full):
        os.makedirs(last_full)
        
    ###开始软链
    try:
        os.symlink(target_dir,full_dir)
    except:
        logIns.writeLog('error','link %s %serror' %(target_dir,full_dir))
        ev_flag = 2

    ###赋用户
    chown_cmd  = "chown -R %s.%s %s"%(user_m,user_m,target_dir)
    (get_status_chown,get_output_chown) = execShell(chown_cmd)
    if (get_status_chown != 0):
        logIns.writeLog('error','chown %s error' %target_dir)
        ev_flag = 3

    ###赋权
    chmod_cmd  = "chmod -R 755 %s"%(target_dir,)
    (get_status_chmod,get_output_chmod) = execShell(chmod_cmd)
    if (get_status_chmod != 0):
        logIns.writeLog('error','chmod %s error' %target_dir)
        ev_flag = 4

    ###返回错误码
    return ev_flag

if __name__ == "__main__":
    ###脚本名
    script_name = os.path.basename(__file__)
    sub_name    = script_name.split('.')[0]

    ###日志路径
    log_path = '/log/yunwei/%s.log' %script_name

    ###定义日志标识
    logIns  = log('1032',log_path,display=True)
    logMain = log('1032','/log/yunwei/yunwei.log',display=True)

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

    ###版本文件名
    base_dir   = "/data/version_repository"
    rver_file  = "version.txt"
    user_m     = "moretv"

    ###获取本机ip
    local_ip = get_ip_addr('eth0')
    if not local_ip:
        local_ip = get_ip_addr('em3')

    ###参数判断
    pub_prog,pub_ver,pub_env,pub_gray,pub_action = args_module_des()

    ###获取项目灰度ip,前后版本,灰度百分比等信息
    gpi_code,action_planp,action_planr,gray_percent,full_dir = get_pub_info(pub_prog,pub_ver,pub_env,pub_gray,pub_action)

    ###把动作前的记录写入version_record
    end_code = gpi_code
    if gpi_code == 0:
        wiv_code = write_info_vr(action_planp,gray_percent,full_dir)
        ###本机版本建立软连接
        end_code = wiv_code
        if wiv_code == 0:
            ev_code = effect_version(pub_action,full_dir)
            ###把动作完成记录写入version_record
            end_code = ev_code 
            if ev_code == 0:
                end_code = write_info_vr(action_planr,gray_percent,full_dir)
 
    ###确认脚本是否成功
    if end_code != 0:
        logMain.writeLog('info','%s error end'% script_info)
    else:
        logMain.writeLog('info','%s success end'% script_info)
        print "success"
