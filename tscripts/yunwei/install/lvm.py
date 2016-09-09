#!/usr/bin/env python
#-*- coding:utf-8 -*-

'''
date: 2015/08/21
role:  lvm相关操作类
usage: m = mysqlBase(host='xxx',db='xxx',user='xxx',pwd='xxx')    实例化
       m.insert('core',{'host_name':'ccc','process_name':'ddd','ip_addr':'192.168.136.41','status':'4'})
notice: Support for Linux LVM2
'''

from __future__ import absolute_import

from yunwei.operate.prefix import log,execShell
logIns = log('121')

import re,sys,os

###lvm操作类
class lvmBase():
    ###判断lvm版本
    def __init__(self,log_path):
        ###log_path为日志写入文件
        logIns = log('121',log_path)
        lvm_ver = 'lvm version'

        ###如果lvm的版本不是2则退出
        ver_status,ver_res = execShell(lvm_ver)
        if not re.search(r'LVM\s*version:\s*2',ver_res):
            logIns.writeLog('error','lvm version must lvm2')
            raise TypeError('121,lvm version must lvm2')


    ###pvdisplay
    def pvdisplay(self,pvname=''):
        ret = {}

        ###执行命令
        cmd = 'pvdisplay -c %s'% pvname
        cmd_status,cmd_res = execShell(cmd)
        if cmd_status != 0:
            return {}
 
        out = cmd_res.splitlines()
        for line in out:
            comps = line.strip().split(':')
            if len(comps) > 8:
                ret['pvName'] = comps[0]
                ret['vgName'] = comps[1]
                ret['pvSize'] = comps[2]
                ret['totalPE'] = comps[8]

        return ret

    ###vgdisplay
    def vgdisplay(self,vgname=''):
        ret = {}

        ###执行命令
        cmd = 'vgdisplay -c %s'% vgname
        cmd_status,cmd_res = execShell(cmd)
        if cmd_status != 0:
            return {}
 
        out = cmd_res.splitlines()
        for line in out:
            comps = line.strip().split(':')
            if len(comps) > 15:
                ret['vgName'] = comps[0]
                ret['vgSize'] = comps[11]
                ret['totalPE'] = comps[13]
                ret['allocPE'] = comps[14]
                ret['freePE'] = comps[15]

        return ret

    ###lvdisplay
    def lvdisplay(self,lvname=''):
        ret = {}

        ###执行命令
        cmd = 'lvdisplay -c %s'% lvname
        cmd_status,cmd_res = execShell(cmd)
        if cmd_status != 0:
            return {}
 
        out = cmd_res.splitlines()
        for line in out:
            comps = line.strip().split(':')
            if len(comps) > 6:
                ret['lvName'] = comps[0]
                ret['vgName'] = comps[1]
                ret['lvSize'] = comps[6]

        return ret

    ###pvcreate
    def pvcreate(self,devices):
        ###判断是否指定device
        if not devices:
            logIns.writeLog('error','pvcreate not point device')
            raise AttributeError('121,pvcreate not point device')
            

        ###分隔多个磁盘
        device_list = []
        for device in devices.split(','):
            ###验证是否存在device
            if not os.path.exists(device):
                logIns.writeLog('debug','pvcreate not found device %s'% device)
            device_list.append(device)

        if not device_list:
            raise AttributeError('121,pvcreate not point device')

        ###拼接所有有效device
        device_valid = ','.join(device_list)

        ###执行pvcreate命令
        cmd = 'pvcreate -f %s'% device_valid 
        cmd_status,cmd_res = execShell(cmd)

        return cmd_res

    ###pvremove
    def pvremove(self,devices):
        ###判断是否指定device
        if not devices:
            logIns.writeLog('error','pvremove not point device')
            raise AttributeError('121,pvremove not point device')

        ###分隔多个磁盘
        device_list = []
        for device in devices.split(','):
            ###验证是否存在device
            if not os.path.exists(device):
                logIns.writeLog('debug','pvremove not found device %s'% device)
            device_list.append(device)

        if not device_list:
            raise AttributeError('121,pvremove not point device')

        ###拼接所有有效device
        device_valid = ','.join(device_list)

        ###执行pvcreate命令
        cmd = 'pvremove -f %s'% device_valid 
        cmd_status,cmd_res = execShell(cmd)

        return cmd_res
        
    ###vgcreate
    def vgcreate(self,vgname,devices):
        ###判断是否指定device
        if not vgname or not devices:
            logIns.writeLog('error','vgcreate not point vgname or device')
            raise AttributeError('121,vgcreate not point vgname or device')

        ###分隔多个磁盘
        device_list = []
        for device in devices.split(','):
            ###验证是否存在device
            if not os.path.exists(device):
                logIns.writeLog('debug','vgcreate not found device %s'% device)
            device_list.append(device)

        if not device_list:
            raise AttributeError('121,vgcreate not point device')

        ###拼接所有有效device
        device_valid = ','.join(device_list)

        ###执行vgcreate命令
        cmd = 'vgcreate -y %s %s'% (vgname,device_valid)
      
        cmd_status,cmd_res = execShell(cmd)

        return cmd_res

    ###vgremove
    def vgremove(self,vgname):
        if not vgname:
            logIns.writeLog('error','vgremove not point vgname')
            raise AttributeError('121,vgremove not point vgname')

        ###执行vgremove命令
        cmd = 'vgremove -f %s'% vgname
        cmd_status,cmd_res = execShell(cmd)

        return cmd_res

    ###vgextend
    def vgextend(self,vgname,devices):
        ###判断是否指定device
        if not vgname or not devices:
            logIns.writeLog('error','vgextend not point vgname or device')
            raise AttributeError('121,vgextend not point vgname or device')

        ###分隔多个磁盘
        device_list = []
        for device in devices.split(','):
            ###验证是否存在device
            if not os.path.exists(device):
                logIns.writeLog('debug','vgextend not found device %s'% device)
            device_list.append(device)

        if not device_list:
            raise AttributeError('121,vgextend not point device')

        ###拼接所有有效device
        device_valid = ','.join(device_list)

        ###执行vgextend命令
        cmd = 'vgextend -f %s %s'% (vgname,device_valid)
      
        cmd_status,cmd_res = execShell(cmd)

        return cmd_res

    ###lvcreate
    def lvcreate(self,lvname,vgname,size=None, extents=None):
        ###判断是否有vgname
        if not lvname or not vgname:
            logIns.writeLog('error','lvcreate not point lvname or vgname')
            raise AttributeError('121,lvcreate not point lvname or vgname')

        ###判断是否同时指定-l -L
        if size and extents:
            logIns.writeLog('error','lvcreate point -l -L at the same time')
            raise AttributeError('121,lvcreate can not point -l -L at the same time')

        ###执行lvcreate命令
        cmd = ''
        if size:
            cmd = 'lvcreate -n %s -L %s %s'% (lvname,size,vgname)
        elif extents:
            cmd = 'lvcreate -n %s -l %sFREE %s'% (lvname,extents,vgname)
        else:
            logIns.writeLog('error','lvcreate must point one of -l -L ')
            raise AttributeError('121,lvcreate must point one of -l -L')
            
        cmd_status,cmd_res = execShell(cmd)

        return cmd_res

    ###lvremove
    def lvremove(self,lvpath):
        if not lvpath:
            logIns.writeLog('error','lvremove not point lvpath')
            raise AttributeError('121,lvremove not point lvpath')

        ###执行lvremove命令
        cmd = 'lvremove -f %s'% lvpath
        cmd_status,cmd_res = execShell(cmd)

        return cmd_res

    ###lvresize
    def lvresize(self,lvpath,size=None,extents=None):
        ###判断是否同时指定-l -L
        if size and extents:
            logIns.writeLog('error','lvresize point -l -L at the same time')
            raise AttributeError('121,lvresize can not point -l -L at the same time')

        ###判断是否指定lvpath
        if not lvpath:
            logIns.writeLog('error','lvresize not point device')
            raise AttributeError('121,lvcreate not point device')

        ###执行lvresize命令
        cmd = ''
        if size:
            cmd = 'lvresize -f -L %s %s'% (lvname,size,lvpath)
        elif extends:
            cmd = 'lvresize -f -l %sFREE %s'% (lvname,extends,lvpath)
        else:
            logIns.writeLog('error','lvresize must point one of -l -L ')
            raise AttributeError('121,lvresize must point one of -l -L')

        cmd_status,cmd_res = execShell(cmd)

        return cmd_res

