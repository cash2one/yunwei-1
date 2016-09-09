#!/usr/bin/env python
#-*- coding:utf-8 -*-

'''
date: 2015/10/16
role: 集中化管理工具的使用 1.ansible
usage: m = cenManage(log_path)    实例化
       m.execAnsible(m_name,m_args,h_list,p_server,fork_num=1)
       m.execSalt(xx,xx,xx)
'''

from __future__ import absolute_import
import re

from yunwei.operate.prefix import log
logIns = log('113')

###集中化管理工具操作类
class cenManage:
    def __init__(self,log_path):
        ###log_path为日志写入文件
        logIns = log('113',log_path)

    ###调用ansible的API函数
    def execAnsible(self,m_name,m_args,h_list,p_server,fork_num=1,priKeyFile='/etc/ansible/id_dsa'):
        try:
            import ansible.runner
        except ImportError, e:
            logIns.writeLog('error','ansible.runner import error')
            raise ImportError('113,No module named ansible.runner')

        ###退出码
        api_code = 0

        ###ansible API
        results = ansible.runner.Runner(
            module_name = m_name,
            module_args = m_args,
            host_list = h_list,
            pattern = p_server,
            forks = fork_num,
            private_key_file = priKeyFile,
        ).run()

        ###没匹配的服务器
        out = {}
        if results is None:
            logIns.writeLog('error','%s not match %s'% (h_list,p_server))
            api_code = 1
        else:
            ###过滤执行结果
            for (hostname,result) in results['contacted'].items():
                if not 'failed' in result:
                    err = result.get('stderr',None)
                    rc  = result.get('rc',None)
                    if not err:
                        out[hostname] = result.get('stdout',result)
                    else:
                        if not re.search(r'success',err) and rc != 0:
                            logIns.writeLog('error','%s>>>%s'% (hostname,result))
                            out[hostname] = 'connect_ng'
                            api_code = 2
                else:
                    logIns.writeLog('error','%s>>>%s'% (hostname,result))
                    out[hostname] = 'connect_ng'
                    api_code = 3
            ###过滤连接结果
            for (hostname,result) in results['dark'].items():
                logIns.writeLog('error','%s>>>%s'% (hostname,result))
                out[hostname] = 'connect_ng'
                api_code = 4
      
        return (api_code,out)

    ###调用ansible的API函数参数直接为ip
    def ipAnsible(self,m_name,m_args,ips,fork_num=1,priKeyFile='/etc/ansible/id_dsa'):
        try:
            import ansible.runner
            import ansible.inventory
        except ImportError, e:
            logIns.writeLog('error','ansible.runner import error')
            raise ImportError('113,No module named ansible.runner')

        ###退出码
        ia_code = 0

        ###ip地址转换
        web_inventory = ansible.inventory.Inventory([ips])

        ###ansible API
        results = ansible.runner.Runner(
            module_name = m_name,
            module_args = m_args,
            inventory = web_inventory,
            forks = fork_num,
            private_key_file = priKeyFile,
        ).run()

        ###没匹配的服务器
        out = {}
        if results is None:
            logIns.writeLog('error','%s not match'% (ips,))
            ia_code = 1
        else:
            ###过滤执行结果
            for (hostname,result) in results['contacted'].items():
                if not 'failed' in result:
                    err = result.get('stderr',None)
                    rc  = result.get('rc',None)
                    if not err:
                        out[hostname] = result.get('stdout',result)
                    else:
                        if not re.search(r'success',err) and rc != 0:
                            logIns.writeLog('error','%s>>>%s'% (hostname,result))
                            out[hostname] = 'connect_ng'
                            ia_code = 2
                else:
                    logIns.writeLog('error','%s>>>%s'% (hostname,result))
                    out[hostname] = 'connect_ng'
                    ia_code = 3
            ###过滤连接结果
            for (hostname,result) in results['dark'].items():
                logIns.writeLog('error','%s>>>%s'% (hostname,result))
                out[hostname] = 'connect_ng'
                ia_code = 4
      
        return (ia_code,out)


