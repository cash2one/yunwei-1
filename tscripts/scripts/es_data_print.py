#!/usr/bin/env python
#-*- coding:utf-8 -*- 

'''
date: 2016/05/24
role: es内容输出并发送邮件
usage: es_data_print.py
'''
import urllib2,urllib,json,os,re,time,logging,sys
import datetime

import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.header import Header

###定义日志函数
def write_exec_log(log_level,log_message):
    ###创建一个logger
    logger = logging.getLogger('get_info.logger')
    logger.setLevel(logging.DEBUG)

    ###建立日志目录
    log_dir  = "/log/yunwei"
    log_file = "all_do.log"
    if not os.path.isdir(log_dir):
        os.makedirs(log_dir,mode=0777)

    log_path = os.path.join(log_dir,log_file)

    ###给日志赋权0777
    if os.path.isfile(log_path):
        os.chmod(log_path,0777)

    ###创建一个handler用于写入日志文件
    fh = logging.FileHandler(log_path)
    fh.setLevel(logging.DEBUG)

    ###创建一个handler用于输出到终端
    th = logging.StreamHandler()
    th.setLevel(logging.DEBUG)

    ###定义handler的输出格式
    formatter =logging.Formatter('%(asctime)s  %(name)s  %(levelname)s  %(message)s')
    fh.setFormatter(formatter)
    th.setFormatter(formatter)

    ###给logger添加handler
    logger.addHandler(fh)
    logger.addHandler(th)

    ###记录日志
    level_dic = {'debug':logger.debug,'info':logger.info,'warning':logger.warning,'error':logger.error,'critical':logger.critical}
    level_dic[log_level](log_message)

    ###删除重复记录
    fh.flush()
    logger.removeHandler(fh)

    th.flush()
    logger.removeHandler(th)

###脚本排它锁函数
def script_exclusive_lock(scriptName):
    pid_file  = '/tmp/%s.pid'% scriptName
    lockcount = 0
    while True:
        if os.path.isfile(pid_file):
            ###打开脚本运行进程id文件并读取进程id
            fp_pid     = open(pid_file,'r')
            process_id = fp_pid.readlines()
            fp_pid.close()

            ###判断pid文件取出的是否是数字
            if not process_id:
                break

            if not re.search(r'^\d',process_id[0]):
                break

             ###确认此进程id是否还有进程
            lockcount += 1
            if lockcount > 4:
                write_exec_log('error','2 min after this script is still exists')
                sys.exit(1)
            else:
                if os.popen('/bin/ps %s|grep "%s"'% (process_id[0],scriptName)).readlines():
                    print "The script is running...... ,Please wait for a moment!"
                    time.sleep(30)
                else:
                    os.remove(pid_file)
        else:
            break

    ###把进程号写入文件
    wp_pid = open(pid_file,'w')
    sc_pid = os.getpid()
    wp_pid.write('%s'% sc_pid)
    wp_pid.close()

    ###pid文件赋权
    if os.path.isfile(pid_file):
        os.chmod(pid_file,0777)

###数据获取函数
def get_es_data():
    ###日期时间
    date_now  = datetime.datetime.now()
    date_old  = date_now + datetime.timedelta(-1)
    format_d  = date_old.strftime('%Y.%m.%d')

    ###url
    #url = 'http://10.10.88.176:9200/_search'
    url = 'http://10.10.88.176:9200/logstash-%s/_search' %format_d

    ###参数
    data = {"fields":["responsetime","size","http_host","url"],"query":{"bool":{"must":[{"range":{"nginx.size":{"from":"1000000","to":"10000000000"}}},{"range":{"nginx.responsetime":{"from":"5","to":"100"}}}],"must_not":[],"should":[]}},"from":0,"size":10000,"sort":[],"facets":{}}

    data = json.dumps(data)

    ###抓取数据
    try:
        req = urllib2.Request(url, data)
        out = urllib2.urlopen(req)
        out_j =  out.read()
        out_d = json.loads(out_j)
    except:
        write_exec_log('error','%s parse error' %url)
        sys.exit(1)

    ###循环加入列表
    aa = []
    aa_dict = {}
    try:
        for i in out_d['hits']['hits']:
            c = i['fields']
            #aa.append((c['responsetime'][0],c['size'][0],c['http_host'][0],c['url'][0]))
            aa_k = "%s%s" %(c['http_host'][0],c['url'][0])
            aa_v = [c['responsetime'][0],c['size'][0]]
            aa_dict[aa_k] = aa_v
            
    except:
        write_exec_log('error','%s get es url error' %url)
        sys.exit(1)
   
    ###把字典变成列表
    for key, value in aa_dict.items():
        aa.append((value,key))

    ###排序
    bb = sorted(aa,key=lambda s: (s[0][1],s[0][0],s[1]),reverse = True)

    ###bb为空
    if not bb:
        write_exec_log('error','get es data error')
        sys.exit(1)

    ###写文件
    file_path = "/data/tools/mail/es_data.txt"
    with open(file_path,'w') as fw:
        ###输出头
        fw.write('responetime,size,http_host/url\n')
        for j in bb:
            #fw.write('%s,%s,%s/%s\n' % (j[0], j[1] ,j[2],j[3]))
            fw.write('%s,%s,%s\n' % (j[0][0],j[0][1],j[1]))

    ###返回文件路径
    return file_path

###发送邮件函数
def mail_data(es_path):
    ###判断邮件文件是否已经存在
    if not os.path.isfile(es_path):
        write_exec_log('error','%s not exists' %es_path)
        sys.exit(1)

    ###邮件服务器变量
    mail_host="106.75.16.248"  #设置服务器
    mail_user="jaunt"    #用户名
    mail_pass="jaunt@whaley"   #口令 

    sender = '89263580@qq.com'
    #receivers = ['lujian@ztgame.com'] 
    receivers = ['zhang.qiangjun@whaley.cn','xu.weiyi@whaley.cn','dong.jinfeng@whaley.cn'] 

    ###创建一个带附件的实例
    message = MIMEMultipart()
    subject = 'Elasticsearch data'
    message['Subject'] = Header(subject, 'utf-8')

    ###添加发件人和收件人的显示
    msg['From'] = sender
    msg['To']   = ";".join(receivers)

    ###邮件正文内容
    message.attach(MIMEText('附件为Elasticsearch数据', 'plain', 'utf-8'))

    ###构造附件
    att1 = MIMEText(open(es_path, 'rb').read(), 'base64', 'utf-8')
    att1["Content-Type"] = 'application/octet-stream'

    ###邮件中显示什么名字
    att1["Content-Disposition"] = 'attachment; filename="es_data.txt"'
    message.attach(att1)


    try:
        smtpObj = smtplib.SMTP(mail_host)
        smtpObj.starttls()
        smtpObj.login(mail_user,mail_pass)
        smtpObj.sendmail(sender, receivers, message.as_string())
    except smtplib.SMTPException:
        write_exec_log('error','%s send mail error' %es_path)
        sys.exit(1)

if __name__ == "__main__":
    ###脚本排它锁
    script_exclusive_lock(os.path.basename(__file__))

    ###获取数据
    data_path = get_es_data()

    ###发邮件
    mail_data(data_path)

    print "success"
