#!/usr/bin/env python
#-*- coding:utf-8 -*- 

from yunwei.operate.prefix import log,execShell,exclusiveLock
from yunwei.install.cryptology import cryptoBase
from yunwei.operate.compress import compressBase
from yunwei.getInfo.parser import parseIni
from yunwei.getInfo.connDb import mysqlConn
from yunwei.operate.api import apiBase
from yunwei.operate.centralization import cenManage   
import hashlib

import os,sys




if __name__ == "__main__":
    ###脚本名
    script_name = os.path.basename(__file__)
    sub_name    = script_name.split('.')[0]

    ###日志路径
    log_path = '/log/yunwei/%s.log' %script_name

    ###定义日志标识
    logIns  = log('1010',log_path,display=True)
    logMain = log('1010','/log/yunwei/yunwei.log',display=True)

    script_info = ' '.join(sys.argv)

    ###脚本排它锁
    exclusiveLock(script_name)

    logMain.writeLog('info','%s start'% script_info)

    ###配置文件路径
    conf_pwd  = os.path.join(os.path.dirname(os.path.realpath(__file__)),'conf')
    conf_main = os.path.join(conf_pwd,'common.conf')
    conf_sub  = os.path.join(conf_pwd,'%s.conf' %sub_name)

    center_manage = cenManage(log_path)
    slave_ip = "10.10.220.229"
    r_flag,r_out = center_manage.ipAnsible('cron',"name='for test' cron_file='test ansible' state=absent",slave_ip)
    print "aaa",r_out
    #r_flag,r_out = center_manage.ipAnsible('cron',"name=chec minute=5 job='crontab -l >>/root/123'",slave_ip)


#    mysql_conf = '/tmp/my.cnf'
#    option_baimao = "baimao_tb"
#    all_optu      = parseIni(log_path,mysql_conf,option_baimao)
#    table_field   = all_optu.getOption()
#    print "aaa",table_field
#    all_optu   = parseIni(log_path,mysql_conf,'Mysqld')
#    mysql_path = all_optu.getOption("datadir")
#    print "aaa",mysql_path

'''
    write_event_info()

log_path = "/tmp/aaa"

public_key = 'ucloudma.kai@moretv.com.cn1355800414123959271'

def _verfy_ac(private_key, params):
    items=params.items()

    items.sort()


    params_data = "";
    for key, value in items:
        params_data = params_data + str(key) + str(value)
    params_data = params_data + private_key
    print params_data

    sign = hashlib.sha1()
    sign.update(params_data)
    signature = sign.hexdigest()

    return signature

pic_url = 'https://img1.doubanio.com/img/celebrity/large/1430735201.9.jpg'
params = {"Action":"ImagePornCheck","Image.0":pic_url,"PublicKey":"ucloudma.kai@moretv.com.cn1355800414123959271"}

sig = _verfy_ac('cd295132434ad835baa118f4da33e44bb7a419ac',params)

url_b   = 'https://api.ucloud.cn'
params  = {'Action':'ImagePornCheck','PublicKey':public_key,'Signature':sig,'Image.0':'%s' %pic_url}
auth    = ()
headers = {}

###调用api类
ab = apiBase(log_path,url_b,**params)
api_return = ab.get(*auth,**headers)

print "aaa",api_return


#cmb = compressBase(log_path)
#cmb.zipp('/data/git/aaa/src','/data/git/aaa/src.zip')
#cmb.unzip('/data/git/aaa/build/compiler.zip','/data/aaa/20160704/')
#cmb.tar('/tmp/test/apache-maven-3.3.9','/data/git/aaa.tar.gz')
#cmb.untar('/data/git/aaa.tar.gz','/data/git/test')
#import tarfile
#import os
#import tarfile
 
#创建压缩包名
 
#tar = tarfile.open("/data/git/aaa.tar.gz","w:gz")
 
#创建压缩包
#aaa = len("/tmp/test/apache-maven-3.3.9")
#print "aaa",aaa
#for root,dir,files in os.walk("/tmp/test/apache-maven-3.3.9"):
#    for file in files:
#        fullpath = os.path.join(root,file)
#        tar.add(fullpath,arcname=os.path.join(root[aaa:],file))
 
#tar.close()
'''
