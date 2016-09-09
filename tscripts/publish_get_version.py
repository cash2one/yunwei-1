#!/usr/bin/env python
#-*- coding:utf-8 -*- 

'''
date: 2016/08/20
role: 从publish_prog表中获取相应项目的origin_addr,把版本下载到本地git目录,编译解压到/data/$project/$version目录
usage: publish_get_version.py
'''
from yunwei.operate.prefix import log,execShell,exclusiveLock
from yunwei.install.cryptology import cryptoBase
from yunwei.operate.mysql import mysqlBase
from yunwei.operate.compress import compressBase
from yunwei.getInfo.parser import parseIni
from yunwei.getInfo.connDb import mysqlConn

from optparse import OptionParser
import os,sys,re,time,datetime,shutil
import socket,fcntl,struct,base64
reload(sys)
sys.setdefaultencoding("utf-8")

###参数定义函数
def args_module_des():
    usage  = '''  
    %prog -p whaley-cms-CMSDataSyncServer -v 20160820D1 -c 1 -e test
    %prog -p whaley-cms-CMSDataSyncServer -v 20160820R1 -e pro
    '''
    parser = OptionParser(usage)
    parser.add_option("-p","--prog",type="string",default=False,dest="pub_prog",
                      help="publish program name")
    parser.add_option("-v","--version",type="string",default=False,dest="pub_ver",
                      help="publish version")
    parser.add_option("-c","--confversion",type="string",default=False,dest="conf_ver",
                      help="config version")
    parser.add_option("-e","--env",type="string",default="test",dest="pub_env",
                      help="publish version")

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

    ###返回值
    return (options.pub_prog,options.pub_ver,options.pub_env,options.conf_ver)

###git版本管理工具下载版本函数
def git_get_ver(origin_addr,pub_env,proj_flag,ver_path,vm_root,vm_dir,branch_chose):
    ###错误码
    git_flag = 0

    ###下载版本
    logIns.writeLog('info','wget version from %s start' %origin_addr)
    clone_cmd  = "git clone %s %s"%(origin_addr,vm_root)
    (get_status_clone,get_output_clone) = execShell(clone_cmd)
    if (get_status_clone != 0):
        logIns.writeLog('error','wget version from %s error' %origin_addr)
        git_flag = 1

    ###切换分支
    brance_cmd  = "cd %s && git checkout %s"%(vm_root,branch_chose)
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

    ###返回错误码
    return git_flag

###svn管理工具下载版本函数
def svn_get_ver(origin_addr,pub_env,ver_path,vm_dir):
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

###调用编译函数
def compile_ver(app_dir,pub_dir,comp_path,vm_dir,branch_chose):
    ###错误码
    comp_flag = 0

    ###切换分支
    brance_cmd  = "cd %s && git checkout %s"%(vm_dir,branch_chose)
    (get_status_brance,get_output_brance) = execShell(brance_cmd)
    if (get_status_brance != 0):
        logIns.writeLog('error','change %s branch error' %branch_chose)
        comp_flag = 1

    ###调用编译脚本生成zip或tar.gz包
    logIns.writeLog('info','%s compiler start' %comp_path)
    comp_cmd = "cd %s && sh %s"%(vm_dir,comp_file)
    (get_status_comp,get_output_comp) = execShell(comp_cmd)
    if (get_status_comp != 0):
        logIns.writeLog('error','%s compiler error' %comp_path)
        comp_flag = 1

    ###查找编译后的压缩包
    pub_path = ''
    for cf in os.listdir(pub_dir):
        if re.search(r'\.(zip|tar\.gz|war)',cf):
            pub_path = os.path.join(pub_dir,cf)
            break

    ###解压编译文件
    cmb = compressBase(log_path)
    if re.search(r'zip|war',pub_path):
        cmb.unzip(pub_path,ver_path)
    elif re.search(r'tar\.gz',pub_path):
        cmb.untar(pub_path,ver_path)
    else:
        logIns.writeLog('error','%s not have compiler tool' %pub_dir)
        comp_flag = 2

    ###判断目录是否为空
    if not os.listdir(ver_path):
        logIns.writeLog('error','%s version is empty' %ver_path)
        comp_flag = 3

    ###判断编译文件的目录结构
    ver_files = len(os.listdir(ver_path))
    if ver_files < 3:
        for vf in os.listdir(ver_path):
            if vf == app_dir:
                logIns.writeLog('error','%s directory structure error' %ver_path)
                comp_flag = 4

    ###返回错误码
    return comp_flag

###git管理工具下载配置文件函数
def git_get_conf(conf_addr,pub_env):
    ###暂时没添加
    pass

###svn管理工具下载配置文件函数
def svn_get_conf(conf_addr,conf_rver,ver_path):
    ###错误码
    svnc_flag = 0

    ###下载配置文件
    logIns.writeLog('info','%s wget config start' %conf_addr)
    if conf_rver != 'last':
        export_cmd = "svn export -r %s --force --no-auth-cache --non-interactive %s %s"%(conf_rver,conf_addr,ver_path)
    else:
        export_cmd = "svn export --force --no-auth-cache --non-interactive %s %s"%(conf_addr,ver_path)
    (get_status_export,get_output_export) = execShell(export_cmd)
    if (get_status_export != 0):
        logIns.writeLog('error','export conf %s ver error' %conf_rver)
        svnc_flag = 1
  
    ###返回错误码
    return svnc_flag

###获取程序函数
def get_prog_ver(pub_prog,pub_ver,pub_env,conf_ver):
    ###错误码
    gpv_flag = 0 

    ###读取publish_prog mysql连接数据
    prefix_pp = "pp"
    option_pp = "pp_db"
    mcp = mysqlConn(log_path) 
    pp_flag,tb_pp,info_pp = mcp.getConn(conf_main,conf_sub,prefix_pp,option_pp)
  
    ###sql
    pp_sql = "SELECT origin_way,origin_addr,conf_way,conf_addr,proj_flag,app_dir,work_dir FROM %s WHERE publish_name='%s' AND up_env='%s' LIMIT 1" %(tb_pp,pub_prog,pub_env)
    ###连接数据库
    mbp = mysqlBase(log_path,**info_pp)
    pp_query = mbp.query(pp_sql)

    ###查询
    origin_way,origin_addr,conf_way,conf_addr,proj_flag,app_dir,work_dir = ["","","","","","",""]
    for pq in pp_query:
        origin_way,origin_addr,conf_way,conf_addr,proj_flag,app_dir,work_dir = pq
        break

    ###去空行
    origin_addr = origin_addr.strip()
    conf_addr   = conf_addr.strip()
    proj_flag   = proj_flag.strip()
    app_dir     = app_dir.strip()
    work_dir    = work_dir.strip()

    ###确认版本管理工具
    origin_tool = "git"
    try:
        if int(origin_way) == 1:
            origin_tool = "svn"
    except:
        logIns.writeLog('error','get %s origin_way error'%(pub_prog,))
        gpv_flag = 1

    ###确认配置文件管理工具
    conf_tool = "git"
    try:
        if int(conf_way) == 1:
            conf_tool = "svn"
    except:
        logIns.writeLog('error','get %s conf_way error'%(pub_prog,))
        gpv_flag = 2

    ###判断有无数据
    if not origin_addr or not conf_addr:
        logIns.writeLog('error','get %s version addr or conf addr error'%(pub_prog,))
        gpv_flag = 3

    ###拼接本地的项目实际根目录
    vm_root = os.path.join(git_dir,pub_prog)
    vm_dir  = os.path.join(vm_root,work_dir)

    ###删除原本版本目录
    if os.path.isdir(vm_root):
        shutil.rmtree(vm_root)

    ###备份完整的历史版本目录
    if os.path.isdir(ver_path):
        shutil.move(ver_path,bkver_path)

    ###建立目录
    os.makedirs(ver_path)

    ###确认分支名
    if pub_env == 'pro':
        branch_chose = "master"
    elif pub_env == 'test':
        branch_chose = "test"
    elif pub_env == 'hotfix':
        branch_chose = "hotfix-v0.2.1-20160907"

    ###根据不同的版本管理工具下载相应版本
    if origin_tool == 'git':
        gv_flag = git_get_ver(origin_addr,pub_env,proj_flag,ver_path,vm_root,vm_dir,branch_chose)
    elif origin_tool == 'svn':
        gv_flag = svn_get_ver(origin_addr,pub_env,ver_path,vm_root,vm_dir)

    ###确认版本下载情况
    if gv_flag != 0:
        gpv_flag = 4
    else:
        logIns.writeLog('info','get %s version %s end' %(pub_prog,pub_ver))

    ###pub_dir
    pub_dir   = os.path.join(vm_dir,pub_sub)
    comp_path = os.path.join(vm_dir,comp_file)

    ###调用编译模块
    comp_code = compile_ver(app_dir,pub_dir,comp_path,vm_dir,branch_chose)
    if comp_code != 0:
        gpv_flag = 5
    else:
        logIns.writeLog('info','%s compiler end' %comp_path)

    ###判断配置文件的获取版本
    if conf_ver:
        conf_rver  = conf_ver
    else:
        conf_rver  = "last"

    ###根据不同的配置文件管理工具下载相应配置
    if conf_tool == 'git':
        gc_flag = git_get_conf(conf_addr,pub_env,ver_path)
    elif conf_tool == 'svn':
        gc_flag = svn_get_conf(conf_addr,conf_rver,ver_path)

    ###确认配置文件下载情况
    if gc_flag != 0:
        gpv_flag = 6
    else:
        logIns.writeLog('info','get %s conf %s end' %(pub_prog,conf_rver))

    ###写入版本文件
    version_p = os.path.join(ver_path,version_f)
    try:
        with open(version_p,'w') as vp:
            vp.write(pub_ver)
    except:
        logIns.writeLog('error','write %s to %s error'%(pub_ver,version_p))
        gpv_flag = 7
    
    ###返回错误码
    return gpv_flag

if __name__ == "__main__":
    ###脚本名
    script_name = os.path.basename(__file__)
    sub_name    = script_name.split('.')[0]

    ###日志路径
    log_path = '/log/yunwei/%s.log' %script_name

    ###定义日志标识
    logIns  = log('1029',log_path,display=True)
    logMain = log('1029','/log/yunwei/yunwei.log',display=True)

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
    pub_prog,pub_ver,pub_env,conf_ver = args_module_des()

    ###文件保存路径
    base_dir  = "/data/version_repository"
    git_dir   = "/data/git"
    pub_sub   = "target"
    comp_file = "package-test.sh"
    ver_dir   = os.path.join(base_dir,pub_prog)  
    ver_path  = os.path.join(ver_dir,pub_ver)
    bkver_path = os.path.join(ver_dir,"%s.%s" %(pub_ver,format_date))
    version_f  = "version.txt"

    ###获取publish_prog中相应源站方式和地址，调用编译脚本进行编译打包
    gpv_code = get_prog_ver(pub_prog,pub_ver,pub_env,conf_ver)

    ###确认脚本是否成功
    if gpv_code != 0:
        logMain.writeLog('info','%s error end'% script_info)
    else:
        logMain.writeLog('info','%s success end'% script_info)
        print "success"
