# -*- coding: utf-8 -*-
"""
Created on Sat Oct 16 14:03:51 2021

@author: Lenovo
"""

import requests
import re
import random


def CHECK_IP(proxy,headers):
    try:
        proxies=proxy
        url='https://www.ipip.net/'
        r = requests.get(url, headers=headers, proxies=proxy,timeout=3)   #通过来自IPIP的反馈判断IP是否有效
        r.raise_for_status()
    except:
        print("Checked proxy"+str(proxies)+" and it's invalid!")
        return True
    else:
        print("Checked proxy"+str(proxies)+" and it's valid.")
        return True



def GET_IP():
    ip_list_all=[]
    for page in range(1):
        url = 'http://www.66ip.cn/%d.html' % page    #代理IP获取
        headers={'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.132 Safari/537.36'}
        response= requests.get(url,headers=headers)   
        ip_list_address=re.findall(r'<td>(\d+\.\d+\.\d+\.\d+)</td><td>\d+</td>',response.text)    #用正则表达式从text里爬ip，分别得到IP的三部分
        ip_list_port=re.findall(r'<td>\d+\.\d+\.\d+\.\d+</td><td>(\d+)</td>',response.text)   
        for n in range(len(ip_list_address)):
            ip_integrated={"https:": ip_list_address[n] + ":" + ip_list_port[n]}
            if CHECK_IP(ip_integrated,headers) == True:
                ip_list_all.append(ip_integrated)
    return ip_list_all
#"https://"+

#ip_list_protest[n].lower():ip_list_protest[n].lower()+'://'+ip_list_address[n]+':'+ip_list_port[n]

#proxy=IP_RANDOM_WITHCHECK()  #随机抽取IP并检查   
#print(proxy)

#//<td>58.255.6.183</td><td>9999</td>