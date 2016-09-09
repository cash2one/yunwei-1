#!/usr/bin/env python
#-*- coding:utf-8 -*- 

'''
date: 2016/07/19
role: 从cms和bigdata数据库中获取播放失败的节目信息，把相关信息写入txt并发送邮件
usage: program_query.py
2016/08/05 添加把bigdata的100条数据写入excel
'''
from yunwei.operate.prefix import log,execShell,exclusiveLock
from yunwei.install.cryptology import cryptoBase
from yunwei.operate.mail import mailBase
from yunwei.operate.mysql import mysqlBase
from yunwei.getInfo.parser import parseIni,parseNgx
from yunwei.getInfo.connDb import mysqlConn

import os,sys,re,time,datetime,shutil
import socket,fcntl,struct,base64,xlwt,xlsxwriter
reload(sys)
sys.setdefaultencoding("utf-8")

###获取配置文件中nginx的相关目录及匹配模式函数
def program_txt_data():
    ###读取cms mysql连接数据
    prefix_cms = "cms"
    option_cms = "cms_db"
    mc = mysqlConn(log_path) 
    cms_flag,tb_cms,info_cms = mc.getConn(conf_main,conf_sub,prefix_cms,option_cms)
  
    ###sql
    cms_sql = "SELECT sid,title,contentType FROM %s WHERE contentType IN ('movie','tv','jilu','sports') AND (source = 'qq' OR source = 'tencent') AND DATE_FORMAT(now(), '%%Y-%%m-%%d') = DATE_FORMAT(updateTime, '%%Y-%%m-%%d')" %(tb_cms,)
    ###连接数据库
    mbc = mysqlBase(log_path,**info_cms)
    cms_query = mbc.query(cms_sql)

    ###写成列表
    cms_list  = []
    sid_list   = []
    cms_length = len(cms_query)
    for n,i in enumerate(cms_query):
        if n == cms_length - 1:
            sid_list.append('%s' %i[0])
            cms_list.append(u'%s\t%s\t%s' %(i[0],i[1],i[2]))
        else:
            sid_list.append('%s\n' %i[0])
            cms_list.append(u'%s\t%s\t%s\n' %(i[0],i[1],i[2]))

    ###写入cms文件
    with open(cms_path,'w') as cf:
        cf.writelines(cms_list)
            
    ###读取cms mysql连接数据
    prefix_big = "bi"
    option_big = "bi_db"
    mb = mysqlConn(log_path)
    big_flag,tb_big,info_big = mb.getConn(conf_main,conf_sub,prefix_big,option_big)

    ###查询sql
    big_sql = "select sid,title,content_type,sum(vv_num) from %s where day = DATE_SUB(CURDATE(),INTERVAL 1 day) and content_type in ('movie','tv','kids','jilu','zongyi','comic','sports') and source = 'tencent' GROUP BY sid,title,content_type ORDER BY 4 desc limit 100" %(tb_big,)
    ###连接数据库
    mbb = mysqlBase(log_path,**info_big)
    big_query = mbb.query(big_sql)

    ###遍历两个结果，取回sid写入txt
    big_list   = []
    big_length = len(big_query)

    ###如果big_query有数据，则在sid_list在一个回车
    if big_length > 0:
        sid_list.append('\n')

    ###循环写入列表
    for n,j in enumerate(big_query):
        if n == big_length - 1: 
            sid_list.append('%s' %j[0])
            big_list.append('%s\t%s\t%s' %(j[0],j[1],j[2]))
        else:
            sid_list.append('%s\n' %j[0])
            big_list.append('%s\t%s\t%s\n' %(j[0],j[1],j[2]))

    ###写入big文件
    with open(big_path,'w') as bf:
        bf.writelines(big_list)
    
    ###写入sid文件
    with open(sid_path,'w') as sf:
        sf.writelines(sid_list)

    ###返回错误码
    return big_query

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
def send_cb_mail():
    ###错误码
    send_flag = 0

    ###判断excel_path,detail_path是否存在
    path_f  = [cms_path,big_path,sid_path,exc_path]
    for path_e in path_f:
        if not os.path.isfile(path_e):
            logIns.writeLog('error','%s attachments not exists' %path_e)
            mail_flag = 1
            return mail_flag

    ###获取mail的参数选项
    option_par = "cb_mail"
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
    subject = u'播放失败查看'
    content = u'附件中为播放的具体信息,%s文件为大数据播放失败次数前100' %exc_file
    send_status = lb.sendMail(subject, content, 'plain','utf-8',*path_f)

    ###发送失败返回错误码为2
    if not send_status:
        send_flag = 2

    ###返回错误码
    return send_flag

###把数据写入excel函数
def write_bd_excel(which_data):

    ###建立文件
    file = xlwt.Workbook(encoding = 'utf-8') 
    table = file.add_sheet('play err', cell_overwrite_ok = True) 

    ###设置样式
    font = xlwt.Font() # Create the Font
    #font.name = 'SimSun'
    font.bold = True
    #font.underline = True
    #font.italic = True

    borders = xlwt.Borders() 
    borders.left = xlwt.Borders.DASHED 
    borders.right = xlwt.Borders.DASHED
    borders.top = xlwt.Borders.DASHED
    borders.bottom = xlwt.Borders.DASHED
    borders.left_colour = 0x40
    borders.right_colour = 0x40
    borders.top_colour = 0x40
    borders.bottom_colour = 0x40
    style = xlwt.XFStyle() # Create Style
    style.borders = borders # Add Borders to Style
    style.font = font

    ###写入首行
    table.write(0, 0, 'sid',style) 
    table.write(0, 1, 'title',style) 
    table.write(0, 2, 'content_type',style) 
    table.write(0, 3, 'vv_num',style) 

    ###设置列宽
    table.col(0).width = 3600
    table.col(1).width = 12000
    table.col(2).width = 3000

    ###循环写后面几行
    row = 1
    col = 0
    for d in which_data:
        table.write(row,col,d[0],style)
        table.write(row,col + 1,d[1],style)
        table.write(row,col + 2,d[2],style)
        table.write(row,col + 3,d[3],style)
        row += 1

    file.save(exc_path)

###把数据写入excel函数
def write_bd_excel2(which_data):
    ###Create a workbook and add a worksheet.
    workbook = xlsxwriter.Workbook(exc_path)
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
    worksheet.write_string('A1','sid',format_title)
    worksheet.write_string('B1','title',format_title)
    worksheet.write_string('C1','content_type',format_title)
    worksheet.write_string('D1','vv_num',format_title)

    ###循环写后面几行
    row = 1
    col = 0
    for d in which_data:
        worksheet.set_column(row,col, 14)
        worksheet.write_string(row,col,d[0],format_ave)
        worksheet.write_string(row,col + 1,d[1],format_ave)
        worksheet.write_string(row,col + 2,d[2],format_ave)
        worksheet.write_number(row,col + 3,d[3],format_ave)
        row += 1

    workbook.close()        

if __name__ == "__main__":
    ###脚本名
    script_name = os.path.basename(__file__)
    sub_name    = script_name.split('.')[0]

    ###日志路径
    log_path = '/log/yunwei/%s.log' %script_name

    ###定义日志标识
    logIns  = log('1009',log_path)
    logMain = log('1009','/log/yunwei/yunwei.log')

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
    format_today = update_time.strftime('%Y-%m-%d')

    ###文件保存路径
    txt_dir  = "mail"
    cms_file = "mtv_program_%s.txt" %format_today
    big_file = "play_error_%s.txt" %format_today
    exc_file = "play_error_%s.xlsx" %format_today
    sid_file = "test.txt"
    #sid_file = "sid_%s.txt" %format_today
    cms_path = os.path.join(os.path.dirname(__file__),txt_dir,cms_file)
    big_path = os.path.join(os.path.dirname(__file__),txt_dir,big_file)
    exc_path = os.path.join(os.path.dirname(__file__),txt_dir,exc_file)
    sid_path = os.path.join(os.path.dirname(__file__),txt_dir,sid_file)

    ###获取数据库数据并写入txt
    excel_data = program_txt_data()

    ###把大数据的东西单独在写入excel
    #write_bd_excel(excel_data)
    write_bd_excel2(excel_data)

    ###把txt作为副本发送邮件
    mail_code = send_cb_mail()
    
    ###确认脚本是否成功
    if mail_code != 0:
        logMain.writeLog('info','%s error end'% script_info)
    else:
        logMain.writeLog('info','%s success end'% script_info)
        print "success"
