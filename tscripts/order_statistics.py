#!/usr/bin/env python
#-*- coding:utf-8 -*- 

'''
date: 2016/08/17
role: 从pay_order数据库中获订单支付信息,汇总后写入pay_order_info表中,异常的金额订单信息写入event_info库
usage: order_statistics.py  
'''
from yunwei.operate.prefix import log,execShell,exclusiveLock
from yunwei.operate.mysql import mysqlBase
from yunwei.getInfo.parser import parseIni
from yunwei.getInfo.connDb import mysqlConn

import os,sys,re,time,datetime,shutil
reload(sys)
sys.setdefaultencoding("utf-8")

###获取pay_order相关数据函数
def paycms_order_data(money_range):
    ###错误码
    pay_flag = 0

    ###读取paycms mysql连接数据
    prefix_co = "co"
    option_co = "co_db"
    mcc = mysqlConn(log_path) 
    co_flag,tb_co,info_co = mcc.getConn(conf_main,conf_sub,prefix_co,option_co)
  
    ###错误码跟随
    pay_flag = co_flag

    ###sql
    co_sql = "SELECT orderAmount,id FROM %s WHERE `status` IN (1,2) AND createTime REGEXP  DATE_SUB(curdate(), INTERVAL 1 DAY)" %(tb_co,)
    no_sql = "SELECT COUNT(status) FROM %s WHERE `status` NOT IN (1,2) AND createTime REGEXP  DATE_SUB(curdate(), INTERVAL 1 DAY)" %(tb_co,)

    ###连接数据库
    mbc = mysqlBase(log_path,**info_co)
    co_query = mbc.query(co_sql)
    no_query = mbc.query(no_sql)

    ###获取配置文件中正常金额列表
    moneys = money_range.split(',')
    
    ###把范围的金额整理出来
    nomal_amount = []
    for money in moneys:
        ###正则取出返回的边界
        money_match = re.match(r'(\d+)-(\d+)',money)
        if money_match:
            money_min = int(money_match.group(1))
            money_max = int(money_match.group(2))

            ###添加到需要扫描端口的列表中
            nomal_amount.extend(range(money_min,money_max))

        ###数字直接加入
        elif re.match(r'\d+',money):
            nomal_amount.append(int(money))

    ###整理数据,总额 数量 异常数 异常id
    pay_list  = [0,0,0]
    abnor_ids = [] 
    for cq in co_query:
        order_amount = int(cq[0])
        ###加总额
        pay_list[0] += order_amount
        ###加数量
        pay_list[1] += 1
        ###判断异常订单数
        if order_amount not in nomal_amount:
            pay_list[2] += 1
            abnor_ids.append(str(cq[1]))
    
    ###异常id连接
    abnor_str = ','.join(abnor_ids)            
    pay_list.append(abnor_str)

    ###获取未成功订单数
    try:
        err_num = no_query[0][0]
    except:
        err_num = 0
    
    ###返回
    return (pay_flag,err_num,pay_list)

###写入yunwei.pay_order_info数据库函数        
def write_yo_info(err_num,pay_data):
    ###错误码
    yo_flag = 0

    ###获取yo数据库连接信息
    prefix_yo = "yo"
    option_yo = "yo_db"
    mcy = mysqlConn(log_path)
    yo_flag,tb_yo,info_yo = mcy.getConn(conf_main,conf_sub,prefix_yo,option_yo)

    ###连接数据库
    mby = mysqlBase(log_path,**info_yo)

    ###更新选项
    up_condition = {}
    up_condition['is_valid']  = '0'

    ###调用mysql类完成更新
    mby.update(tb_yo,up_condition,"order_date REGEXP  DATE_SUB(curdate(), INTERVAL 1 DAY)")        

    ###插入选项
    in_condition = {}
    in_condition['total_amount'] = pay_data[0]
    in_condition['succ_num']     = pay_data[1]
    in_condition['err_num']      = err_num
    in_condition['abnormal_num'] = pay_data[2]
    in_condition['order_date']   = format_last
    
    ###调用mysql类完成插入
    try:
        mby.insert(tb_yo,in_condition)
    except:
        logIns.writeLog('error','%s insert mysql error' %tb_yo)
        yo_flag = 1

    ###获取事件数据库连接信息
    prefix_er = "er"
    option_er = "er_db"
    mce = mysqlConn(log_path)
    event_flag,tb_er,info_er = mce.getConn(conf_main,conf_sub,prefix_er,option_er)

    ###连接数据库
    mbe = mysqlBase(log_path,**info_er)
  
    ###mysql服务器ip
    sql_ip = info_er.get('host','')

    ###插入选项
    er_condition = {}
    er_condition['local_ip']     = sql_ip
    er_condition['event_info']   = "amount differ ids %s" %pay_data[3]
    er_condition['event_flag']   = sub_name
    er_condition['notice_level'] = 1
 
    ###如果有异常id则插入
    if pay_data[3]:
        try:
            mbe.insert(tb_er,er_condition)
        except:
            logIns.writeLog('error','%s insert mysql error' %tb_er)
            yo_flag = 2

    ###返回错误码
    return yo_flag

if __name__ == "__main__":
    ###脚本名
    script_name = os.path.basename(__file__)
    sub_name    = script_name.split('.')[0]

    ###日志路径
    log_path = '/log/yunwei/%s.log' %script_name

    ###定义日志标识
    logIns  = log('1027',log_path)
    logMain = log('1027','/log/yunwei/yunwei.log')

    script_info = ' '.join(sys.argv)

    ###脚本排它锁
    exclusiveLock(script_name)

    logMain.writeLog('info','%s start'% script_info)

    ###配置文件路径
    conf_pwd  = os.path.join(os.path.dirname(os.path.realpath(__file__)),'conf')
    conf_main = os.path.join(conf_pwd,'common.conf')
    conf_sub  = os.path.join(conf_pwd,'%s.conf' %sub_name)

    ###日期格式化
    update_time = datetime.datetime.now()
    yester_time = update_time + datetime.timedelta(days=-1)
    format_last = yester_time.strftime('%Y-%m-%d')

    ###获取正常金额范围
    option_mo   = "ok_money"
    range_mo    = "normal_money"
    all_optu    = parseIni(log_path,conf_sub,option_mo)
    money_range = all_optu.getOption(range_mo)

    ###获取数据库数据整理好后返回列表
    pay_code,err_num,pay_data = paycms_order_data(money_range)

    ###写入数据库
    yo_code = pay_code
    if pay_code == 0:
        yo_code = write_yo_info(err_num,pay_data)
    
    ###确认脚本是否成功
    if yo_code != 0:
        logMain.writeLog('info','%s error end'% script_info)
    else:
        logMain.writeLog('info','%s success end'% script_info)
        print "success"
