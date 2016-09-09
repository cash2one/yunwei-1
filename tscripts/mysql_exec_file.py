#!/usr/bin/env python
#-*- coding:utf-8 -*-

'''
date: 2016/07/27
role: 从文件中读出sql语句逐条执行,mysql的连接信息是写在配置中的
usage: mysql_exec_file.py  -f filepath
mysql_exec_file.py  -b mysql -p metis-cms-metis -v 20160820D1 -e test -d test_db -f statement_160728.sql -g front
'''

from yunwei.operate.prefix import log,execShell,exclusiveLock
from yunwei.install.cryptology import cryptoBase
from yunwei.operate.mysql import mysqlBase
from yunwei.getInfo.parser import parseIni
from yunwei.getInfo.connDb import mysqlConn

import sys,os,re,time,datetime,shutil
import socket,fcntl,struct,base64
from optparse import OptionParser

###参数定义函数
def args_module_des():
    usage  = '''  
    %prog -f filepath
    %prog -b mysql -p metis-cms-metis -v 20160820D1 -e test -d test_db -f filename -g front
    '''
    parser = OptionParser(usage)
    parser.add_option("-b","--baseget",type="string",default="conf",dest="way_sql",
                      help="get mysql conn way")
    parser.add_option("-f","--file",type="string",default=False,dest="sql_path",
                      help="exec sql file path")
    parser.add_option("-p","--pro",type="string",default=False,dest="project",
                      help="exec project")
    parser.add_option("-v","--version",type="string",default=False,dest="pub_ver",
                      help="publish version")
    parser.add_option("-e","--env",type="string",default=False,dest="environment",
                      help="exec environment")
    parser.add_option("-d","--db",type="string",default="mysql",dest="db_name",
                      help="exec db name")
    parser.add_option("-g","--flag",type="string",default="",dest="host_flag",
                      help="db host flag")

    (options,args) = parser.parse_args()
    ###确认参数是否定义正确
    if options.way_sql != 'mysql' and options.way_sql != 'conf':
        parser.print_usage
        parser.error("-b arguments must mysql or conf!")

    ###如果定义是配置文件方式,必须定义文件路径
    if not options.sql_path:
        parser.print_usage
        parser.error("-f arguments must defined!")

    if options.way_sql == 'mysql':
        if not options.project or not options.environment or not options.db_name:
            parser.print_usage
            parser.error("-p -e and -d rguments must defined!")

    ###返回值
    return (options.way_sql,options.sql_path,options.project,options.pub_ver,options.environment,options.db_name,options.host_flag)

###创建临时文件函数
def create_tmp_path(file_name,befor_after):
    ###错误码
    make_flag = 0

    ###创建临时文件目录
    write_dir = '/tmp/yunweitmp'
    if not os.path.isdir(write_dir):
        ###创建目录
        try:
            os.makedirs(write_dir,mode=0777)
        except:
            logIns.writeLog('error','%s mkdir error'% write_dir)
            make_flag = 1

    ###临时文件名
    write_file = '%s_%s_%s.tmp'% (file_name,befor_after,time_string)

    ###临时文件路径
    write_path = os.path.join(write_dir,write_file)

    ###返回错误码及路径
    return (make_flag,write_path)

###git版本管理工具下载版本函数
def git_get_ver(origin_addr,pub_env,proj_flag):
    ###错误码
    git_flag = 0

    ###确认分支名
    if pub_env == 'pro':
        branch_chose = "master"
    elif pub_env == 'test':
        branch_chose = "test"

    ###下载版本
    logIns.writeLog('info','wget version from %s start' %origin_addr)
    clone_cmd  = "git clone %s %s"%(origin_addr,vm_dir)
    (get_status_clone,get_output_clone) = execShell(clone_cmd)
    if (get_status_clone != 0):
        logIns.writeLog('error','wget version from %s error' %origin_addr)
        git_flag = 1

    ###切换分支
    brance_cmd  = "cd %s && git checkout %s"%(vm_dir,branch_chose)
    (get_status_brance,get_output_brance) = execShell(brance_cmd)
    if (get_status_brance != 0):
        logIns.writeLog('error','change %s branch error' %branch_chose)
        git_flag = 2

    ###获取tags
    tag_cmd  = "cd %s && git tag"%(vm_dir,)
    (get_status_tag,get_output_tag) = execShell(tag_cmd)
    if (get_status_tag != 0):
        logIns.writeLog('error','get %s tag error' %branch_chose)
        git_flag = 3

    ###通过proj_flag和pub_ver获取tag
    pub_tag = ''
    tags    = re.split('\n',get_output_tag)
    for tag in tags:
        if re.search(r'%s\S+%s'%(proj_flag,pub_ver),tag):
            pub_tag = tag
            break

    ###如果没取到指定版本
    if not pub_tag:
        logIns.writeLog('error','choose %s tag error' %pub_ver)
        git_flag = 4

    ###切换tag
    ckout_cmd  = "cd %s && git checkout %s"%(vm_dir,pub_tag)
    (get_status_ckout,get_output_ckout) = execShell(ckout_cmd)
    if (get_status_ckout != 0):
        logIns.writeLog('error','checkout tag %s error' %pub_tag)
        git_flag = 5

    ###返回
    return git_flag

###svn管理工具下载版本函数
def svn_get_ver(origin_addr,pub_env):
    ###错误码
    svn_flag = 0

    ###下载版本
    logIns.writeLog('info','wget version from %s start' %origin_addr)
    export_cmd = "svn export -r %s --force --no-auth-cache --non-interactive %s %s"%(pub_ver,origin_addr,vm_dir)
    (get_status_export,get_output_export) = execShell(export_cmd)
    if (get_status_export != 0):
        logIns.writeLog('error','export  %s version error' %pub_ver)
        svn_flag = 1

    ###返回错误码
    return svn_flag

###获取项目的mysql对应信息函数
def get_pro_mapping(pro_name,env_name,db_name,proj_flag):
    ###错误码
    pro_flag = 0

    ###获取数据库中目标mysql的连接方式
    prefix_em = "em"
    option_em = "em_db"
    mcm = mysqlConn(log_path)
    em_flag,tb_em,info_em = mcm.getConn(conf_main,conf_sub,prefix_em,option_em)

    ###sql
    relation_sql = "SELECT db_host,db_port,origin_way,origin_addr FROM %s WHERE publish_name='%s' AND up_env='%s' AND db_name='%s' AND proj_flag='%s'" %(tb_em,pro_name,env_name,db_name,proj_flag)

    ###连接信息获取库
    mbm = mysqlBase(log_path,**info_em)
    em_query = mbm.query(relation_sql)

    ###判断记录是否异常
    if len(em_query) != 1:
        logIns.writeLog('error','get %s %s record error'%(tb_em,pro_name))
        pro_flag = 1
        
    info_ec = {}
    ###查询
    db_host,db_port,origin_way,origin_addr = ["","","",""]
    for pq in em_query:
        db_host,db_port,origin_way,origin_addr = pq
        break

    ###到sql_authority获取数据库连接信息
    prefix_sa = "sa"
    option_sa = "sa_db"
    mcs = mysqlConn(log_path)
    sa_flag,tb_sa,info_sa = mcs.getConn(conf_main,conf_sub,prefix_sa,option_sa)

    ###sql
    if exec_ip == publish_ip:
        sa_sql = "SELECT db_user,db_pswd FROM %s WHERE db_host='%s' AND db_port='%s' AND source_ip='%s' AND permit_type='all'" %(tb_sa,db_host,db_port,publish_ip)
    else:
        sa_sql  = "SELECT db_user,db_pswd FROM %s WHERE db_host='%s' AND db_port='%s' AND permit_type='all' AND db_user='root'" %(tb_sa,db_host,db_port)
        db_host = "localhost" 

    ###连接信息获取库
    mbs = mysqlBase(log_path,**info_sa)
    sa_query = mbs.query(sa_sql)

    info_ec = {}
    ###获取mysql的连接方式
    try:
        db_user,en_pswd = sa_query[0]
    except:
        logIns.writeLog('error','get %s %s record error'%(tb_sa,db_host))
        print "sa_sql",sa_sql
        pro_flag = 2
        return (pro_flag,{})
        
    ###解密
    try:
        info_ec["passwd"] = cb.decrypt_with_certificate(en_pswd)
    except:
        info_ec["passwd"] = base64.b64decode(en_pswd)

    ###填充info_ec
    info_ec['host'] = db_host
    info_ec['user'] = db_user
    info_ec['db']   = db_name
    info_ec['port'] = db_port
    
    ###确认版本管理工具
    origin_tool = "git"
    try:
        if int(origin_way) == 1:
            origin_tool = "svn"
    except:
        logIns.writeLog('error','get %s origin_way field error'%(tb_em,))
        pro_flag = 2

    ###删除原本版本目录
    if os.path.isdir(vm_dir):
        shutil.rmtree(vm_dir)

    ###根据不同的版本管理工具下载相应版本
    if origin_tool == 'git':
        gv_flag = git_get_ver(origin_addr,env_name,proj_flag)
    elif origin_tool == 'svn':
        gv_flag = svn_get_ver(origin_addr,env_name)

    ###确认版本下载情况
    if gv_flag != 0:
        pro_flag = 3
    else:
        logIns.writeLog('info','get %s version %s end' %(pro_name,pub_ver))

    ###判断是否更新到指定文件
    if not os.path.isfile(sql_path):
        logIns.writeLog('error','%s not exists' %sql_path)
        pro_flag = 4

    ###返回错误码,sql文件路径,要执行数据库的连接信息
    return (pro_flag,info_ec)        
        
###sql文件处理函数
def sqlDeal(file_path):
    ###错误码
    deal_flag = 0

    ###读取文件
    sql_case = []
    try:
        with open(file_path,'r') as sf:
            sql_case = sf.readlines()
    except:
        logIns.writeLog('error','%s not exists'% file_path)
        deal_flag = 1

    ###循环处理文件
    sql_all = []
    for sql_line in sql_case:
	###删除# -- 开始的行并去除空行
        sql_deal = re.sub(r'#.*$|--.*$','',sql_line).strip()
        sql_all.append(sql_deal) 
    
    ###把列表拼接成字符串
    sql_in = ''
    if sql_all:
        sql_single = ''.join(sql_all)
	###去掉/*  */的注释
        sql_in = re.sub(r'\/\*(.*?)\*\/','',sql_single)
    else:
        logIns.writeLog('error','%s is none'% file_path) 
        deal_flag = 2

    ###按;分隔语句
    sql_arr = sql_in.split(';')

    ###创建临时文件
    file_name   = os.path.basename(file_path).split('.')[0]
    befor_after = 'after'
    deal_flag,write_path = create_tmp_path(file_name,befor_after)

    ###逐条语句写入
    try:
        with open(write_path,'w') as wp:
            for sql_one in sql_arr:
                if sql_one:
                    wp.writelines('%s\n'% sql_one)
    except:
        logIns.writeLog('error','%s write sql error'% write_path)
        deal_flag = 3

    return (deal_flag,sql_arr)

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

###数据库执行函数
def sqlExec(way_get,sql_list,**info_ec):
    ###错误码
    exec_flag = 0

    ###读取记录日志的数据库连接数据
    prefix_rd = "rd"
    option_rd = "rd_db"
    mcr = mysqlConn(log_path)
    rd_flag,tb_rd,info_rd = mcr.getConn(conf_main,conf_sub,prefix_rd,option_rd)

    ###错误码跟随
    exec_flag = rd_flag

    ###连接日志库
    try:
        mbr = mysqlBase(log_path,**info_rd)
    except:
        logIns.writeLog('error','%s get log mysql conn info error' %info_rd)
        exec_flag = 1

    ###确认获取要执行mysql的连接信息的方式
    if way_get == 'mysql':
        ###判断option_ec是否为空
        if not info_ec:
            logIns.writeLog('error','%s get record mysql conn info error' %project)
            exec_flag = 2 

    elif way_get == 'conf':
        ###获取配置文件中目标mysql的连接方式
        prefix_ec = "ec"
        option_ec = "ec_db"
        mce = mysqlConn(log_path)
        ec_flag,tb_ec,info_ec = mce.getConn(conf_main,conf_sub,prefix_ec,option_ec)

    ###获取db_ip,db_name
    db_ip   = info_ec.get('host','')
    db_name = info_ec.get('db','')
    db_user = info_ec.get('user','')
    db_port = info_ec.get('port','3306')

    ###连接执行库
    try:
        mbe = mysqlBase(log_path,**info_ec)
    except:
        logIns.writeLog('error','%s get config mysql conn info error' %info_ec)
        exec_flag = 3

    ###引入mysql模块完成mysql语句
    try:
        for sql_exec in sql_list:
            if sql_exec:
                try:
                    ###目标库执行语句
                    mbe.change(sql_exec)
                except:
                    logIns.writeLog('error','%s exec error'% sql_exec)
                    exec_flag = 4

                ###日志记录
                in_condition = {}
                in_condition['exec_ip']   = exec_ip
                in_condition['db_host']   = db_ip
                in_condition['db_user']   = db_user
                in_condition['db_name']   = db_name
                in_condition['db_port']   = db_port
                in_condition['statement'] = sql_exec
                try:
                    mbr.insert(tb_rd,in_condition)
                except:
                    pass
    except:
        logIns.writeLog('error','%s connect mysql or %s exec mysql error'% (ip,db))
        exec_flag = 5

    ###返回错误码
    return exec_flag

if __name__ == "__main__":
    ###脚本名
    script_name = os.path.basename(__file__)
    sub_name    = script_name.split('.')[0]

    ###日志路径
    log_path = '/log/yunwei/%s.log' %script_name

    ###定义日志标识
    logIns  = log('1015',log_path,display=True)
    logMain = log('1015','/log/yunwei/yunwei.log',display=True)

    script_info = ' '.join(sys.argv)

    ###脚本排它锁
    exclusiveLock(script_name)

    ###导入解密模块
    cb = cryptoBase(log_path)

    ###参数判断
    way_get,file_in,project,pub_ver,envi,db_name,host_flag = args_module_des()

    logMain.writeLog('info','%s start'% script_info)

    ###配置文件路径
    conf_pwd  = os.path.join(os.path.dirname(os.path.realpath(__file__)),'conf')
    conf_main = os.path.join(conf_pwd,'common.conf')
    conf_sub  = os.path.join(conf_pwd,'%s.conf' %sub_name)

    ###时间格式化
    update_time = datetime.datetime.now()
    time_string = update_time.strftime('%y%m%d%H%M%S')

    ###文件保存路径
    git_dir    = "/data/git"
    vm_dir     = os.path.join(git_dir,project)
    sql_dir    = os.path.join(vm_dir,"sql")
    sql_path   = os.path.join(sql_dir,file_in)
    publish_ip = "10.19.142.67" 

    ###获取本机ip
    exec_ip = get_ip_addr('eth0')
    if not exec_ip:
        exec_ip = get_ip_addr('em3')

    ###如果way_get是mysql的话file_in从svn或git等版本控制工具中获取
    db_ip   = ''
    info_ec = {}
    if way_get == 'mysql':
        file_code,info_ec = get_pro_mapping(project,envi,db_name,host_flag)
        file_path = sql_path
    else:
        file_path = file_in
        file_code = 0
        
    ###获取sql语句文件,处理好写入临时文件作为对比
    deal_code = file_code
    if file_code == 0:
        logIns.writeLog('info','deal sql file for remove the comment')
        deal_code,sql_list = sqlDeal(file_path)

    ###处理好的sql文件逐条刷入到对应的数据库中
    logIns.writeLog('info','call mysql class to exec')
    exec_code = deal_code
    if deal_code == 0:
        exec_code = sqlExec(way_get,sql_list,**info_ec)
    
    ###确认脚本是否成功
    if exec_code != 0:
        logMain.writeLog('info','%s error end'% script_info)
    else:
        logMain.writeLog('info','%s success end'% script_info)
        print "success"

