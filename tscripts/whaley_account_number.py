#!/usr/bin/env python
#-*- coding:utf-8 -*- 

'''
date: 2016/08/30
role: 从dolphin_server.whaley_preinstall_authority获取一号多端的情况，把相关信息写入txt并发送邮件
usage: whaley_account_number.py
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

###获取dolphin_server.whaley_preinstall_authority信息函数
def dolphin_txt_data():
    ###读取dolphin_server mysql连接数据
    prefix_ds = "ds"
    option_ds = "ds_db"
    mcd = mysqlConn(log_path) 
    ds_flag,tb_ds,info_ds = mcd.getConn(conf_main,conf_sub,prefix_ds,option_ds)
  
    ###sql
    ds_sql = "SELECT count(whaleyAccount) AS num, whaleyAccount FROM %s GROUP BY whaleyAccount HAVING num > 5 ORDER BY num DESC" %(tb_ds,)
    ###连接数据库
    mbd = mysqlBase(log_path,**info_ds)
    ds_query = mbd.query(ds_sql)

    ###写成列表
    ds_list  = []
    for n in ds_query:
        ds_list.append(u'%s\t%s\n' %(n[0],n[1]))

    ###写入cms文件
    with open(dolphin_path,'w') as cf:
        cf.writelines(ds_list)
            
    ###返回数据
    return ds_list

###邮件配置文件解析函数
def get_mail_conf(conf_mail,option_par):
    ###错误码
    mail_flag = 0
  
    ###导入解密模块
    cb = cryptoBase(log_path)

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
def send_ds_mail():
    ###错误码
    send_flag = 0

    ###判断excel_path是否存在
    path_f  = [dolphin_path,dolphin_excel]
    for i in path_f:
        if not os.path.isfile(i):
            logIns.writeLog('error','%s attachments not exists' %i)
            send_flag = 1
            return send_flag

    ###获取mail的参数选项
    option_par = "ds_mail"
    send_flag_m,mail_par_m = get_mail_conf(conf_main,option_par)
    send_flag_s,mail_par_s = get_mail_conf(conf_sub,option_par)

    ###把错误码合并
    send_flag_h = 0
    if send_flag_m != 0 and send_flag_s != 0:
        send_flag_h = 1

    ###子配置文件更新
    mail_par_m.update(mail_par_s)

    ###实例化mail
    lb = mailBase(log_path,**mail_par_m)

    ###发送的相关变量定义
    subject = u'多账号信息查看'
    content = u'附件%s和%s都是多账号的具体信息,只是保存的文件格式不一样' %(dolphin_file,excel_file)
    send_status = lb.sendMail(subject, content, 'plain','utf-8',*path_f)

    ###发送失败返回错误码为2
    if not send_status:
        send_flag = 2

    ###返回错误码
    return send_flag

###把数据写入excel函数
def write_bd_excel2(which_data):
    ###Create a workbook and add a worksheet.
    workbook = xlsxwriter.Workbook(dolphin_excel)
    worksheet = workbook.add_worksheet()

    ###Add a bold format to use to highlight cells
    bold = workbook.add_format({'bold': 1})

    ###Create a new chart object. In this case an embedded chart
    chart = workbook.add_chart({'type': 'line'})

    ###定义格式
    format_title=workbook.add_format()    #定义format_title格式对象
    format_title.set_border(1)   #定义format_title对象单元格边框加粗(1像素)的格式
    format_title.set_bg_color('#cccccc')   #定义format_title对象单元格背景颜色为
                                       #'#cccccc'的格式
    format_title.set_align('center')    #定义format_title对象单元格居中对齐的格式
    format_title.set_bold()    #定义format_title对象单元格内容加粗的格式

    format_ave=workbook.add_format()    #定义format_ave格式对象
    format_ave.set_border(1)    #定义format_ave对象单元格边框加粗(1像素)的格式
    format_ave.set_num_format('0')   #定义format_ave对象单元格数字类别显示格式


    ###写第一行
    worksheet.write_string('A1','count',format_title)
    worksheet.write_string('B1','whaleyAccount',format_title)

    ###循环写后面几行
    row = 1
    col = 0
    for d in which_data:
        count,account = re.split(r'\t|\n',d)[:2]
        worksheet.set_column(row,col, 14)
        worksheet.write_string(row,col,count,format_ave)
        worksheet.write_string(row,col + 1,account,format_ave)
        #worksheet.write_number(row,col + 1,d[1],format_ave)
        row += 1

    workbook.close()        

if __name__ == "__main__":
    ###脚本名
    script_name = os.path.basename(__file__)
    sub_name    = script_name.split('.')[0]

    ###日志路径
    log_path = '/log/yunwei/%s.log' %script_name

    ###定义日志标识
    logIns  = log('1035',log_path)
    logMain = log('1035','/log/yunwei/yunwei.log')

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
    format_today = update_time.strftime('%y%m%d-%H')

    ###文件保存路径
    txt_dir  = "mail"
    dolphin_file = "Account_info_%s.txt" %format_today
    excel_file   = "Account_info_%s.xlsx" %format_today
    dolphin_path  = os.path.join(os.path.dirname(__file__),txt_dir,dolphin_file)
    dolphin_excel = os.path.join(os.path.dirname(__file__),txt_dir,excel_file) 

    ###获取数据库数据并写入txt
    excel_data = dolphin_txt_data()

    ###把大数据的东西单独在写入excel
    write_bd_excel2(excel_data)

    ###把txt作为副本发送邮件
    mail_code = send_ds_mail()
    
    ###确认脚本是否成功
    if mail_code != 0:
        logMain.writeLog('info','%s error end'% script_info)
    else:
        logMain.writeLog('info','%s success end'% script_info)
        print "success"
