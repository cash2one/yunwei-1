#!/usr/bin/env python
#-*- coding:utf-8 -*- 

'''
date: 2016/07/24
role: 从dns_domain_server和cdn_flag表中通过相应api获取完整域名,cname域名,server_ip的一个对应表
usage: domain_cdn_server.py
'''
from yunwei.operate.prefix import log,execShell,exclusiveLock
from yunwei.install.cryptology import cryptoBase
from yunwei.operate.mysql import mysqlBase
from yunwei.operate.api import apiBase
from yunwei.getInfo.parser import parseIni
from yunwei.getInfo.connDb import mysqlConn

import os,sys,re,time,datetime,shutil,re
import socket,fcntl,struct,base64
reload(sys)
sys.setdefaultencoding("utf-8")

###获取cdn提供商的cname标识函数
def get_cdn_mark():
    ###错误码
    cc_flag = 0

    ###读取cdn_flag mysql连接数据
    prefix_cf = "cf"
    option_cf = "cf_db"
    mcf = mysqlConn(log_path)
    cf_flag,tb_cf,info_cf = mcf.getConn(conf_main,conf_sub,prefix_cf,option_cf)

    ###sql
    cf_sql = "SELECT cdn,flag FROM %s WHERE is_valid=1" %tb_cf

    ###连接hms数据库
    mbf = mysqlBase(log_path,**info_cf)
    cf_query = mbf.query(cf_sql)

    if not cf_query or cf_flag != 0:
        logIns.writeLog('error','get data error from %s' %(tb_cf,))
        cc_flag = 2

    ###处理数据
    cdn_mark = {}
    for cdn,flag in cf_query:
        ###获取值
        v_flag =  cdn_mark.get(cdn,'')
        ###如果有值则用|分割
        if v_flag:
            cdn_mark[cdn] = '%s|%s' %(cdn_mark.get(cdn,''),flag)
        else:
            cdn_mark[cdn] = flag

    ###确认结果字典是否为空
    if not cdn_mark:
        logIns.writeLog('error','get cdn and mark error from %s' %(tb_cf,))
        cc_flag = 3

    ###返回错误码,cdn提供商及用|分割的标识字典
    return (cc_flag,cdn_mark)

###api获取源站ip函数
def get_api_ips(cdn,cname_domain):
    ###错误码
    api_flag = 0

    ###引入加密模块
    cb = cryptoBase(log_path)

    ###api项
    option_par = "cdn_api"
    
    ###初始化
    ws_user,ws_pswd,cc_user,cc_pswd = ['','','','']
    ###读取配置文件api的用户名和密码
    try:
        all_optu   = parseIni(log_path,conf_sub,option_par)
        try:
            ws_user = all_optu.getOption('ws_user')
            ws_pswd = all_optu.getOption('ws_pswd')
            cc_user = all_optu.getOption('cc_user')
            cc_pswd = all_optu.getOption('cc_pswd')
        except:
            api_flag = 1
            logIns.writeLog('debug','%s cdn_api option not exists' %conf_sub)
    except:
        api_flag = 2
        logIns.writeLog('debug','%s api conf file not exists' %conf_sub)

    ###解密ws_pswd
    try:
        ws_pswd = cb.decrypt_with_certificate(ws_pswd)
    except:
        ws_pswd = base64.b64decode(ws_pswd)

    ###解密cc_pswd
    try:
       cc_pswd = cb.decrypt_with_certificate(cc_pswd)
    except:
        cc_pswd = base64.b64decode(cc_pswd)

    ###gmt时间格式获取
    gmt_cmd = """date -R -u | awk '{print $1" "$2" "$3" "$4" "$5" GMT"}'"""
    gmt_status,gmt_result = execShell(gmt_cmd)
    if gmt_status != 0:
        logIns.writeLog('error','%s exec error'%gmt_cmd)
        api_flag = 3

    ###网宿api加密方式获取
    mm_cmd = 'echo -en "%s" | openssl dgst -sha1 -hmac %s -binary | openssl enc -base64' %(gmt_result,ws_pswd)
    mm_status,mm_result = execShell(mm_cmd)
    if mm_status != 0:
        logIns.writeLog('error','%s exec error'%mm_cmd)
        api_flag = 4
    
    ###网宿
    if cdn == 'ChinaNetCenter':
        url_b   = 'http://open.chinanetcenter.com/api/si/domain'
        params  = {'domain':cname_domain}
        auth    = (ws_user,mm_result)
        headers = {'Accept-Encoding': '','Date':'%s' %gmt_result,"Accept": "application/json"}
    ###蓝汛
    elif cdn == 'ChinaCache':
        url_b   = 'https://portal-api.chinacache.com:444/public-api/getSourceStationInfo.do'
        params  = {'userName':cc_user,'apiPassword':cc_pswd,'channels':'http://%s' %cname_domain}
        auth    = ()
        headers = {}
    ###其它直接返回
    else:
        return (0,[])

    ###调用api类
    ab = apiBase(log_path,url_b,**params)
    api_return = ab.get(*auth,**headers)

    ###获取源站ip
    ips_list = []
    try:
        if cdn == 'ChinaNetCenter':
            ips_str  = api_return["result"]["domainList"][0]["originIps"]
            ips_list = ips_str.split(';')  
        elif cdn == 'ChinaCache':
            try:
                ips_list = api_return["successChannels"][0]["commonConfigIps"]
            except:
                ips_list = [api_return["successChannels"][0]["origdomain"]]
    except:
        logIns.writeLog('error','%s api return format error'%cdn)
        api_flag = 5

    ###返回源站ip列表
    return (api_flag,ips_list)

###通过api获取cdn提供商的源站ip信息函数
def get_cdn_info(punycode,cname_value):
    ###错误码
    get_flag = 0

    ###初始化
    cdn = ''
    cname_domain = ''
    server_list  = []

    ###获取cdn提供商及cname的mark
    cc_flag,cdn_mark = get_cdn_mark()
    for cdn,mark in cdn_mark.items():
        math_cname = re.search(r'%s'%mark,cname_value)
        if math_cname:
            ###获取真正的cname域名
            #蓝汛
            if cdn == 'ChinaCache':
                cname_prefix = cname_value[:cname_value.find(math_cname.group()) - 1 ]           
                ###有的前缀有.com .cn .net的先去掉
                exclude_str = ['.com','.cn','.net']
                for i in exclude_str:
                    ###替换
                    cname_prefix = cname_prefix.replace(i,'');

                cname_suffix = punycode[punycode.find('.'):]
                cname_domain = ''.join([cname_prefix,cname_suffix])
            #网宿
            elif cdn == 'ChinaNetCenter':
                cname_domain = cname_value[:cname_value.find(math_cname.group()) - 1 ]           

            #其他cdn提供商暂不解析
            else:
                cname_domain = cname_value

            ###根据cname域名调用cdn提供的api查询源站ip列表
            api_code,server_list = get_api_ips(cdn,cname_domain) 
            if api_code != 0:
                logIns.writeLog('error','get %s originIps error from %s' %(cname_domain,cdn))
                get_flag = 1

            ###跳出循环
            break

    ###返回cdn_provider，cname_domain，server_list
    return (get_flag,cdn,cname_value,server_list)
    #return (get_flag,cdn,cname_domain,server_list)
                
###获取内网ip函数
def get_nw_ip(ww_ip,ip_map):
    ###ww_ip:外网ip,ip_map:外网和内网的对应列表
    nw_ip = ''
    for w_ip,n_ip in ip_map:
        if w_ip == ww_ip:
            nw_ip = n_ip
            break
    ###返回内网ip
    return nw_ip

###从dns_domain_server获取相关数据函数
def get_cdn_data():
    ###错误码
    dca_flag = 0

    ###读取dns_domain_server mysql连接数据
    prefix_dns = "dns"
    option_dns = "dns_db"
    mcd = mysqlConn(log_path) 
    dns_flag,tb_dns,info_dns = mcd.getConn(conf_main,conf_sub,prefix_dns,option_dns)
  
    ###sql
    dns_sql = "SELECT name,punycode,type,value FROM %s " %tb_dns

    ###连接hms数据库
    mbd = mysqlBase(log_path,**info_dns)
    dns_query = mbd.query(dns_sql)

    if not dns_query or dns_flag != 0:
        logIns.writeLog('error','get data error from %s' %(tb_dns,))
        dca_flag = 1

    ###先把ip信息表的信息取出来存入列表
    ###读取ucloud_host_instance mysql连接数据
    prefix_uh = "uh"
    option_uh = "uh_db"
    mcu = mysqlConn(log_path)
    uh_flag,tb_uh,info_uh = mcu.getConn(conf_main,conf_sub,prefix_uh,option_uh)

    ###sql
    uh_sql = "SELECT eip,private_ip FROM %s " %tb_uh

    ###连接hms数据库
    mbu = mysqlBase(log_path,**info_uh)
    uh_query = mbu.query(uh_sql)

    if not uh_query or uh_flag != 0:
        logIns.writeLog('error','get ip info error from %s' %(tb_uh,))
        dca_flag = 2

    ###返回的列表[[domain_name,cdn_provider，cname_domain，server_ip],]
    cdn_data = []
    ###逐个处理域名的数据
    for name,punycode,ntype,value in dns_query:
        ###完整域名
        domain_name = ".".join([name,punycode])

        ###A记录直接填ip
        if ntype == 'A':
            server_ip = value
            nw_ip     = get_nw_ip(server_ip,uh_query)
            cdn_data.append([domain_name,'','',server_ip,nw_ip])
        elif ntype == 'CNAME':
            ###获取相关信息
            get_flag,cdn_provider,cname_domain,server_list = get_cdn_info(punycode,value) 
            if get_flag != 0:
                logIns.writeLog('error','get %s originIps error from %s' %(cdn_provider,cname_domain))
                dca_flag = 3 
            else:
                ###如果server_list为空则把记录平移
                if not server_list:
                    cdn_data.append([domain_name,cdn_provider,cname_domain,'',''])

                ###循环所有ip，分成多条记录
                for server_ip in server_list:
                    nw_ip = get_nw_ip(server_ip,uh_query)
                    cdn_data.append([domain_name,cdn_provider,cname_domain,server_ip,nw_ip])

    ###返回结果
    return (dca_flag,cdn_data)

###把数据写入dns_domain_server函数
def write_cdn_yw(cdn_data):
    ###错误码
    cdn_flag = 0

    ###获取dns_domain_server mysql连接数据
    prefix_cdn = "cdn"
    option_cdn = "cdn_db"
    mc = mysqlConn(log_path)
    cdn_flag,tb_cdn,info_cdn = mc.getConn(conf_main,conf_sub,prefix_cdn,option_cdn)

    ###连接yunwei数据库
    mbi = mysqlBase(log_path,**info_cdn)

    ###清空表
    trun_sql = 'truncate table %s' %tb_cdn
    mbi.change(trun_sql)

    ###处理数据后写入yunwei.cdn_domain_server表中
    for record in cdn_data:
        try:
            ###插入选项
            in_condition = {}
            in_condition['domain_name']  = record[0]
            in_condition['cdn_provider'] = record[1]
            in_condition['cname_domain'] = record[2]
            in_condition['server_ip']    = record[3]
            in_condition['nw_ip']        = record[4]
            ###调用mysql类完成插入
            mbi.insert(tb_cdn,in_condition)
        except:
            logIns.writeLog('error','insert dns data %s error' %(tb_cdn,))            
            cdn_flag = 1
            break

    ###返回结果
    return cdn_flag
  
if __name__ == "__main__":
    ###脚本名
    script_name = os.path.basename(__file__)
    sub_name    = script_name.split('.')[0]

    ###日志路径
    log_path = '/log/yunwei/%s.log' %script_name

    ###定义日志标识
    logIns  = log('1012',log_path)
    logMain = log('1012','/log/yunwei/yunwei.log')

    script_info = ' '.join(sys.argv)

    ###脚本排它锁
    exclusiveLock(script_name)

    logMain.writeLog('info','%s start'% script_info)

    ###配置文件路径
    conf_pwd  = os.path.join(os.path.dirname(os.path.realpath(__file__)),'conf')
    conf_main = os.path.join(conf_pwd,'common.conf')
    conf_sub  = os.path.join(conf_pwd,'%s.conf' %sub_name)
 
    ###获取dns_domain_server域名并根据cdn的api逐个解析源站ip
    dca_code,cdn_data = get_cdn_data()

    ###把域名,ip相关信息写入yunwei.cdn_domain_server
    cdn_code = write_cdn_yw(cdn_data)
    
    ###确认脚本是否成功
    if dca_code != 0 or cdn_code != 0:
        logMain.writeLog('info','%s error end'% script_info)
    else:
        logMain.writeLog('info','%s success end'% script_info)
        print "success"

