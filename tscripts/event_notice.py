#!/usr/bin/env python
#-*- coding:utf-8 -*- 

'''
date: 2016/08/18
role: 从event_record数据库中获取相关信息，把根据不同类型并发送通知
usage: event_notice.py
'''
from yunwei.operate.prefix import log,execShell,exclusiveLock
from yunwei.install.cryptology import cryptoBase
from yunwei.operate.mail import mailBase
from yunwei.operate.mysql import mysqlBase
from yunwei.getInfo.parser import parseIni
from yunwei.getInfo.connDb import mysqlConn

import os,sys,re,time,datetime,shutil
import socket,fcntl,struct,base64,xlwt,xlsxwriter
reload(sys)
sys.setdefaultencoding("utf-8")

###order_statistics写入详细信息函数
def into_detail(file_path,list_ids):
    ###错误码
    detail_flag = 0

    ###把ids变成str
    str_ids = ','.join(list_ids)

    ###读取paycms mysql连接数据
    prefix_co = "co"
    option_co = "co_db"
    mcc = mysqlConn(log_path)
    detail_flag,tb_co,info_co = mcc.getConn(conf_main,conf_sub,prefix_co,option_co)

    ###po字段
    po_field = ['orderNo','orderAmount','goodsNo','payMethod','status','notifyUrl','orderBill','qrcode']
    str_po   = ','.join(po_field)

    ###查询语句
    co_sql = "SELECT %s,DATE_FORMAT(overTime,'%%Y-%%m-%%d %%H:%%M:%%S') FROM %s WHERE id in (%s)" %(str_po,tb_co,str_ids)

    ###连接数据库
    mbc = mysqlBase(log_path,**info_co)
    co_query = mbc.query(co_sql)

    ###写入字段头
    po_field.append('overTime\n')
    head_po = '\t'.join(po_field)

    ###把信息写入列表
    payo_list = ["\n\n%s" %head_po]
    for co in co_query:
        co_str = map(str,co)
        line = '\t'.join(co_str)
        payo_list.append("%s\n" %line)

    ###追加到文件
    try:
        with open(file_path,'a') as pof:
            pof.writelines(payo_list)
    except:
        logIns.writeLog('error',"%s append detail error!" %(file_path,))
        detail_flag = 1
        
    ###返回错误码
    return detail_flag

###获取事件库函数
def get_event_data():
    ###错误码
    get_flag = 0

    ###获取事件数据库连接信息
    prefix_er = "er"
    option_er = "er_db"
    mce = mysqlConn(log_path)
    get_flag,tb_er,info_er = mce.getConn(conf_main,conf_sub,prefix_er,option_er)

    ###连接数据库
    mbe = mysqlBase(log_path,**info_er)

    ###查询字段
    all_field = ['local_ip','event_info','event_flag','notice_level']
    str_field = ','.join(all_field) 

    ###sql语句
    er_sql     = "SELECT %s,DATE_FORMAT(enter_time,'%%Y-%%m-%%d') FROM %s WHERE is_inform=0 AND notice_level>0" %(str_field,tb_er)
    events_sql = "SELECT event_flag FROM %s WHERE is_inform=0 AND notice_level>0 GROUP BY event_flag" %tb_er

    ###连接数据库
    mbe = mysqlBase(log_path,**info_er)
    er_query = mbe.query(er_sql)
    es_query = mbe.query(events_sql)

    ###写入文件头
    all_field.append('enter_time\n')
    head_field = '\t'.join(all_field)

    ###根据不同事件类型分别写入不同文件
    event_ident = []
    list_ids    = []
    for es in es_query:
        event_ident.append(es[0])
        ###文件名
        file_name = "%s.%s.txt" %(es[0],format_today)
        file_path = os.path.join(mail_dir,file_name)
        event_list = [head_field,]
        ###同一个事件写入同一列表
        for er in er_query:
            try:
                service_name = er[2] 
            except:
                service_name = ''

            ###判断事件名
            if service_name == es[0]:
                ###并成一行记录
                er_str = map(str, er)
                one_line = '\t'.join(er_str)
                event_list.append("%s\n" %one_line)

            ###如果是order_statistics,获取错误id组成列表
            if service_name == 'order_statistics':
                id_match = re.search(r'ids\s+(\S+)',er[1])
                if id_match:
                    list_ids.append(id_match.group(1))
                  
        ###写文件
        try:
            with open(file_path,'w') as ewf:
                ewf.writelines(event_list)
        except:
            logIns.writeLog('error',"%s write %s error!" %(file_path,es[0]))
            get_flag = 1

        ###如果是order_statistics,则需要从pay数据库中获取详细信息
        if es[0] == 'order_statistics':
            into_flag = into_detail(file_path,list_ids)
            if into_flag != 0:
                logIns.writeLog('error',"%s write %s detail info error!" %(file_path,es[0]))
                get_flag = 2

    ###返回数据
    return (get_flag,event_ident)
    
###邮件配置文件解析函数
def get_mail_conf(conf_mail,option_par):
    ###错误码
    mail_flag = 0
  
    ###读取main配置文件获取mail相关变量
    info_mail = {}
    try:
        all_optu   = parseIni(log_path,conf_mail,option_par)
        try:
            info_mail = all_optu.getOption('mail_host','mail_user','mail_pswd','mail_send','mail_rece')
        except:
            mail_flag = 1
            logIns.writeLog('debug','%s mail option not exists' %conf_mail)
    except:
        mail_flag = 2
        logIns.writeLog('debug','%s mail conf file not exists' %conf_mail)

    ###解密mail_pswd
    mail_pswd = info_mail.get('mail_pswd', False)
    if mail_pswd:
        try:
            info_mail['mail_pswd'] = cb.decrypt_with_certificate(mail_pswd)
        except:
            info_mail['mail_pswd'] = base64.b64decode(mail_pswd)

    return (mail_flag,info_mail)

###发送mail函数
def send_event_mail(rece_addr,event_flag,event_file):
    ###错误码
    send_flag = 0

    ###获取mail的参数选项
    option_par = 'base_mail'
    send_flag_m,mail_par_m = get_mail_conf(conf_main,option_par)
    send_flag_s,mail_par_s = get_mail_conf(conf_sub,option_par)

    ###把错误码合并
    send_flag_h = 0
    if send_flag_m != 0 and send_flag_s != 0:
        send_flag_h = 1

    ###子配置文件更新
    mail_par_m.update(mail_par_s)

    ###更新收件人
    if rece_addr:
        mail_par_m['mail_rece'] = rece_addr

    ###发送的附件
    event_path = os.path.join(mail_dir,event_file)
    path_list  = [event_path,]

    ###实例化mail
    lb = mailBase(log_path,**mail_par_m)

    ###发送的相关变量定义
    subject = u'异常事件警示'
    content = u'%s服务的异常警示，附件%s是异常具体信息' %(event_flag,event_file)
    send_status = lb.sendMail(subject, content, 'plain','utf-8',*path_list)

    ###发送失败返回错误码为2
    if not send_status:
        send_flag = 2

    ###返回错误码
    return send_flag

###发短信函数
def send_event_note(option_par,event_flag,event_file):
    ###目前没用
    return 0

###调用相应工具发送通知函数
def send_event_tool(rece_addr,event_flag,event_file,notice_flag):
    ###错误码
    tool_flag = 0

    ###根据参数通过工具发送通知
    if notice_flag == 1:
        tool_flag = send_event_mail(rece_addr,event_flag,event_file)
    elif notice_flag == 2:
        tool_flag = send_event_note(rece_addr,event_flag,event_file)

    ###返回错误码
    return tool_flag

###通过事件标识名获取配置文件中的通知信息函数
def get_notice_data(event_ident):
    ###获取配置文件的参数
    option_ev = "event_addr"
    all_optu  = parseIni(log_path,conf_sub,option_ev)
    all_adds  = all_optu.getOption()

    ###组成列表返回
    notice_list = []
    for evi in event_ident:
        ###邮件
        rece_addr = all_adds.get("%s_addr" %evi,'')
        if rece_addr:
            event_file  = "%s.%s.txt" %(evi,format_today)      
            notice_flag = 1
            notice_list.append([rece_addr,evi,event_file,notice_flag]) 
        ###短信
        rece_phone = all_adds.get("%s_phone" %evi,'')
        if rece_phone:
            event_file  = "%s.%s.txt" %(evi,format_today)      
            notice_flag = 2
            notice_list.append([rece_addr,evi,event_file,notice_flag]) 

    ###返回[[rece_list,event_flag,event_file,notice_flag],]
    return notice_list

###把事件库的is_inform置1函数
def update_event_record(event_flag):
    ###错误码
    update_flag = 0

    ###获取事件数据库连接信息
    prefix_er = "er"
    option_er = "er_db"
    mce = mysqlConn(log_path)
    get_flag,tb_er,info_er = mce.getConn(conf_main,conf_sub,prefix_er,option_er)

    ###连接数据库
    mbe = mysqlBase(log_path,**info_er)

    ###更新
    up_condition = {}
    up_condition['is_inform'] = '1'
  
    try:
        mbe.update(tb_er,up_condition,"event_flag='%s'"%(event_flag,))
    except:
        logIns.writeLog('error','%s update event_flag error' %tb_er)
        update_flag = 1

    ###返回错误码
    return update_flag

if __name__ == "__main__":
    ###脚本名
    script_name = os.path.basename(__file__)
    sub_name    = script_name.split('.')[0]

    ###日志路径
    log_path = '/log/yunwei/%s.log' %script_name

    ###定义日志标识
    logIns  = log('1028',log_path)
    logMain = log('1028','/log/yunwei/yunwei.log')

    script_info = ' '.join(sys.argv)

    ###脚本排它锁
    exclusiveLock(script_name)

    logMain.writeLog('info','%s start'% script_info)

    ###配置文件路径
    conf_pwd  = os.path.join(os.path.dirname(os.path.realpath(__file__)),'conf')
    conf_main = os.path.join(conf_pwd,'common.conf')
    conf_sub  = os.path.join(conf_pwd,'%s.conf' %sub_name)
    mail_dir  = os.path.join(os.path.dirname(os.path.realpath(__file__)),'mail')

    ###导入解密模块
    cb = cryptoBase(log_path)

    ###时间
    update_time  = datetime.datetime.now()
    format_today = update_time.strftime('%y%m%d%H%M%S')

    ###获取事件数据库数据
    get_code,event_ident = get_event_data()

    ###确认获取的数据
    send_code = get_code
    ###event_ident 事件名
    if event_ident and get_code == 0:
        ###分别获取各个事件的收件人信息,如果是多个通知工具,则插入多条记录
        send_info = get_notice_data(event_ident)

        ###逐条执行
        send_code = 0
        for send_in in send_info:
            ###notice_flag通知工具,1:邮件 2:短信
            rece_addr,event_flag,event_file,notice_flag = send_in
            ###调用相应接口,rece_add:收件人信息 event_file 文件名
            tool_code = send_event_tool(rece_addr,event_flag,event_file,notice_flag) 
            if tool_code == 0:
                ###通知成功后,把时间库的
                send_code = update_event_record(event_flag)
            else:
                send_code = tool_code
    
    ###确认脚本是否成功
    if send_code != 0:
        logMain.writeLog('info','%s error end'% script_info)
    else:
        logMain.writeLog('info','%s success end'% script_info)
        print "success"
