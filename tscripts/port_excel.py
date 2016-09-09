#!/usr/bin/env python
#-*- coding:utf-8 -*- 

'''
date: 2016/07/13
role: 从数据库中获取服务器数量及高危端口数量绘制成折线图
usage: port_excel.py
'''
from yunwei.operate.prefix import log,execShell,exclusiveLock
from yunwei.install.cryptology import cryptoBase
from yunwei.operate.mail import mailBase
from yunwei.operate.mysql import mysqlBase
from yunwei.getInfo.parser import parseIni

import os,sys,re,time,datetime,xlsxwriter,base64

###获取数据库连接信息
def get_db_info(conf_file,option_par,prefix):
    ###退出码
    db_flag = 0

    ###判断配置文件是否存在
    if not os.path.isfile(conf_file):
        db_flag = 1
        return (db_flag,'',{})

    ###解析数据库信息
    all_optu   = parseIni(log_path,conf_file,option_par)
    try:
        info_mysql = all_optu.getOption('%s_host' %prefix,'%s_user' %prefix,'%s_passwd' %prefix,'%s_db' %prefix,'%s_tb' %prefix,'%s_port' %prefix,'%s_charset' %prefix)
    except:
        info_mysql = {}

    ###判断配置文件
    db_record = ['%s_host' %prefix,'%s_user' %prefix,'%s_passwd' %prefix,'%s_db' %prefix,'%s_port' %prefix,'%s_charset' %prefix]
    tb_name   = info_mysql.get('%s_tb' %prefix,'')

    ###把符合的信息重新组成字典
    info_dic = {}
    for k,v in info_mysql.items():
        if k in db_record:
            info_dic[k] = v

    ###如果取出的数据为空
    if not info_dic:
        logIns.writeLog('error','get mysql config error for %s'% conf_file)
        db_flag = 2

    return (db_flag,tb_name,info_dic)

###获取数据库数据信息
def get_conn_info(prefix_flag,option_par):
    ###获取数据库连接
    db_flag_m,tb_name_m,info_dic_m = get_db_info(conf_main,option_par,prefix_flag)
    db_flag_s,tb_name_s,info_dic_s = get_db_info(conf_sub,option_par,prefix_flag)

    ###把错误码合并
    db_flag_h = 0
    if db_flag_m != 0 and db_flag_s != 0:
        db_flag_h = 1
 
    ###把表信息合并
    if tb_name_s:
        tb_name_m = tb_name_s

    ###把数据库连接信息合并
    info_dic_m.update(info_dic_s)

    ###把字段转换成mysql统一字段
    info_dic_h = {}
    for k,v in info_dic_m.items():
        k_n = k.split('_')[-1]
        ###如果是passwd的需要解密
        if k_n == 'passwd':
            try:
                v_n = cb.decrypt_with_certificate(v)
            except:
                v_n = base64.b64decode(v)
        else:
            v_n = v

        ###组成可以连接数据库的字典
        info_dic_h[k_n] = v_n 

    ###返回错误码，表名，连接数据库字典
    return (db_flag_h,tb_name_m,info_dic_h)

###从扫描表中获取信息写入端口信息表
def get_port_info():
    ###错误码
    port_flag = 0

    ###高危端口信息表的连接数据库获取
    prefix_wp = "wp"
    option_wp = "wp_db"
    wp_flag,tb_wp,info_wp = get_conn_info(prefix_wp,option_wp)

    ###查询服务器数量及报警端口数量
    query_sql = "SELECT  servers,warn_ports,DATE_FORMAT(enter_time,'%%Y-%%m-%%d') AS time_key FROM %s WHERE enter_time between DATE_SUB(CURDATE(), INTERVAL 30 DAY) AND NOW() GROUP BY enter_time" %(tb_wp,)

    ###写入数据库
    mb = mysqlBase(log_path,**info_wp)
    query_info = mb.query(query_sql)

    ###整理数据到字典中
    ports_info = {}
    for query_data in query_info:
        ports_info[query_data[2]] = [query_data[0],query_data[1]]

    ###把字典变成列表
    order_ports = []
    for k,v in ports_info.items():
        order_ports.append((k, v[0], v[1]))

    ###排序
    ports_sort =sorted(order_ports,key=lambda s: (s[0],s[1],s[2]),reverse=False)

    ###如果没数据返回1
    if not ports_sort:
        logIns.writeLog('error','get data from %s error' %tb_wp)
        port_flag = 1

    ###返回结果
    return (port_flag,ports_sort)

###获取服务器、开放端口的具体数据并写入excel函数
def get_serv_info():
    ###错误码
    serv_flag = 0

    ###高危端口信息表的连接数据库获取
    prefix_yw = "yw"
    option_yw = "yw_db"
    yw_flag,tb_yw,info_yw = get_conn_info(prefix_yw,option_yw)

    ###查询具体服务器开放的端口
    query_sql = "SELECT server_ip,open_port,DATE_FORMAT(enter_time,'%%Y-%%m-%%d %%H:%%i:%%s') AS enter_time FROM %s WHERE open_port regexp '[0-9]' AND DATE_FORMAT(enter_time,'%%Y-%%m-%%d')='%s'" %(tb_yw,format_today)

    ###查询数据库
    mb = mysqlBase(log_path,**info_yw)
    query_info = mb.query(query_sql)

    ###排序
    detail_sort =sorted(query_info,key=lambda s: (s[0],s[2],s[1]),reverse=False)

    ###如果没数据返回1
    if not detail_sort:
        logIns.writeLog('error','get detail data from %s error' %tb_yw)
        serv_flag = 1

    ###返回结果
    return (serv_flag,detail_sort)
    
###把数据写入excel函数
def write_wport_excel(which_data):
    ###Create a workbook and add a worksheet.
    workbook = xlsxwriter.Workbook(excel_path)
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
    worksheet.write_string('A1','date time',format_title)
    worksheet.write_string('B1','server number',format_title)
    worksheet.write_string('C1','port number',format_title)
    
    ###循环写后面几行
    row = 1
    col = 0
    for d in which_data:
        worksheet.set_column(row,col, 14)
        worksheet.write_string(row,col,d[0],format_title)
        worksheet.write_number(row,col + 1,d[1],format_ave)
        worksheet.write_number(row,col + 2,d[2],format_ave)
        row += 1

    ###设置图表
    ###Configure second series. Note use of alternative syntax to define ranges.
    chart.add_series({
        'name':       ['Sheet1', 0, 1],
        'categories': ['Sheet1', 1, 0, row - 1, 0],
        'values':     ['Sheet1', 1, 1, row - 1, 1],
    })

    chart.add_series({
        'name':       ['Sheet1', 0, 2],
        'categories': ['Sheet1', 1, 0, row - 1, 0],
        'values':     ['Sheet1', 1, 2, row - 1, 2],
    })

    ###Add a chart title and some axis labels.
    chart.set_title ({'name': 'server and open port info'})
    chart.set_x_axis({'name': 'date'})
    chart.set_y_axis({'name': 'number'})

    ###Set an Excel chart style. Colors with white outline and shadow.
    chart.set_size({'width': 1000, 'height': 400})    #设置图表大
    chart.set_style(10)

    ###Insert the chart into the worksheet (with an offset).
    worksheet.insert_chart('D2', chart, {'x_offset': 40, 'y_offset': 0})

    workbook.close()

###服务器对应开放端口写入excel函数
def write_detail_excel(ports_data):
    ###Create a workbook and add a worksheet.
    workbook = xlsxwriter.Workbook(detail_path)
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
    worksheet.write_string('A1','server ip',format_title)
    worksheet.write_string('B1','open port',format_title)
    worksheet.write_string('C1','date time',format_title)

    ###循环写后面几行
    row = 1
    col = 0
    for d in ports_data:
        worksheet.set_column(row,col, 20)
        worksheet.write_string(row,col,d[0],format_ave)
        worksheet.write_string(row,col + 1,d[1],format_ave)
        worksheet.write_string(row,col + 2,d[2],format_ave)
        row += 1

    ###关闭
    workbook.close()

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
def send_port_mail():
    ###错误码
    send_flag = 0

    ###判断excel_path,detail_path是否存在
    path_f  = [excel_path,detail_path]
    for path_e in path_f: 
        if not os.path.isfile(path_e):
            logIns.writeLog('error','%s attachments not exists' %path_e)
            mail_flag = 1
            return mail_flag

    ###获取mail的参数选项
    option_par = "yw_mail"
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
    subject = u'扫描端口信息'
    content = u'附件中为端口扫描的具体信息'
    send_status = lb.sendMail(subject, content, 'plain','utf-8',*path_f)

    ###发送失败返回错误码为2
    if not send_status:
        send_flag = 2 

    ###返回错误码
    return send_flag

if __name__ == "__main__":
    ###脚本名
    script_name = os.path.basename(__file__)
    sub_name    = script_name.split('.')[0]

    ###日志路径
    log_path = '/log/yunwei/%s.log' %script_name

    ###定义日志标识
    logIns  = log('1007',log_path)
    logMain = log('1007','/log/yunwei/yunwei.log')

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

    ###导入解密模块
    cb = cryptoBase(log_path)

    ###从数据库中获取数据
    port_code,which_data = get_port_info()
    serv_code,ports_data = get_serv_info()

    ###设置excel目录
    excel_dir  = "mail"
    excel_file = "waning_port_%s.xlsx" %format_today
    excel_path = os.path.join(os.path.dirname(__file__),excel_dir,excel_file)

    ###具体的端口问题excel
    detail_file = "detail_port_%s.xlsx" %format_today
    detail_path = os.path.join(os.path.dirname(__file__),excel_dir,detail_file)
    
    ###生成excel文件
    write_wport_excel(which_data)
    write_detail_excel(ports_data)

    ###发送email,如果有一个数据没取到则不发邮件
    mail_code = 9
    if port_code == 0 and serv_code == 0:
        mail_code = send_port_mail()
    
    ###确认脚本是否成功
    if mail_code != 0:
        logMain.writeLog('info','%s error end'% script_info)
    else:
        logMain.writeLog('info','%s success end'% script_info)
        print "success"

