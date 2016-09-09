#!/usr/bin/env python
#-*- coding:utf-8 -*-

'''
date: 2016/07/22
role: api的使用
usage: ab = apiBase(url,**params)    实例化
       ab.get(*auth,**headers)
'''

from __future__ import absolute_import

from yunwei.operate.prefix import log
logIns = log('116')

import hashlib,json,httplib,urlparse,urllib
import sys,warnings
try:
    import requests
except ImportError, e:
    logIns.writeLog('error','requests import error')
    raise ImportError('116,No module named requests')

###api操作类
class apiBase(object):
    ###获取url,params
    def __init__(self,log_path,url,**params):
        ###log_path为日志写入文件
        logIns = log('116',log_path)

        ###获取参数
        self.url    = url
        self.params = params

    ###方法
    def get(self,*auth,**headers):
        ###解析
        try:
            rg = requests.get(self.url, params=self.params,auth=auth,headers=headers)
        except:
            logIns.writeLog('error','%s url parse error' %self.url)
            raise ImportError('116,%s url parse error' %self.url)

        ###判断返回码
        if not rg.status_code == 200:
            logIns.writeLog('error','%s url return error,code is %s' %(self.url,rg.status_code))    
            raise ImportError('116,%s url return error,code is %s' %(self.url,rg.status_code))

        ###返回结果
        try:
            return rg.json()
        except:
            logIns.writeLog('error','%s url not return json' %self.url)
            raise ImportError('116,%s url not return json' %self.url)

###ucloud签名算法
def _verfy_ac(private_key, params):
    items = params.items()
    items.sort()

    params_data = ""
    for key, value in items:
        params_data = params_data + str(key) + str(value)

    params_data = params_data+private_key

    '''use sha1 to encode keys'''
    hash_new = hashlib.sha1()
    hash_new.update(params_data)
    hash_value = hash_new.hexdigest()
    return hash_value

###连接
class UConnection(object):
    def __init__(self, base_url):
        self.base_url = base_url
        o = urlparse.urlsplit(base_url)
        if o.scheme == 'https':
            self.conn = httplib.HTTPSConnection(o.netloc)
        else:
            self.conn = httplib.HTTPConnection(o.netloc)

    def __del__(self):
        self.conn.close()

    def get(self, resouse, params):
        resouse += "?" + urllib.urlencode(params)
        self.conn.request("GET", resouse)
        response = json.loads(self.conn.getresponse().read())
        return response

class UcloudApiClient(object):
    # 添加 设置 数据中心和  zone 参数
    def __init__(self, base_url, public_key, private_key):
        self.g_params = {}
        self.g_params['PublicKey'] = public_key
        self.private_key = private_key
        self.conn = UConnection(base_url)

    def get(self, uri, params):
        # print params
        _params = dict(self.g_params, **params)
        _params["Signature"] = _verfy_ac(self.private_key, _params)
        return self.conn.get(uri, _params)

