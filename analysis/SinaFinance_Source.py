# -*- coding: utf-8 -*-

import pandas as pd
import numpy as np
from data_source import local_source
from tqdm import tqdm as pb

import requests
from lxml import etree
from lxml.html import fromstring, tostring
import re
import random
import time

from analysis import MyRandomIP
from analysis import TopicSentenceExtraction


def SetEncodings(response):
    encodings = requests.utils.get_encodings_from_content(response.text)
    if encodings:
        encoding = encodings[0]
    else:
        encoding = response.apparent_encoding
    response.encoding = encoding
    return response
    

def Crawler_SinaFinance_Report(init_url, proxy_list, headers, min_date, max_date, result_name='SinaFinance_news_temp.xlsx'):
    reports_tags_all = []
    print("正在检索相关研报：")
    response = requests.get(init_url, headers=headers,proxies=random.choice(proxy_list))
    response = SetEncodings(response)
    tree = etree.HTML(response.text)
    try:  #防止只有一页的情况
        reports_max_page_num = str(tree.xpath("//a[contains(text(),'最末页')]/@onclick")[0])
        reports_max_page_num = re.findall(r'[0-9]+',reports_max_page_num)[0]
    except:
        pass
    stop_flag = 0
    reports_current_page_num = 1
    while stop_flag == 0:
        url_page = init_url + '&p=' + str(reports_current_page_num)
        response_page = requests.get(url_page, headers=headers,proxies=random.choice(proxy_list))
        response_page = SetEncodings(response_page)
        tree_page = etree.HTML(response_page.text)
        tree_page = tree_page.xpath("//div[contains(@class,'main')]/table/descendant::tr")
        reports_tags_page = []
        for reports_single in tree_page:
            reports_text = tostring(reports_single, encoding='utf-8').decode('utf-8')
            try: #过滤掉不是新闻, 从而不符合格式的节点
                reports_url = 'http:' + re.findall(r'//.+?phtml',reports_text)[0]
                reports_date = float(''.join(re.findall(r'[0-9]{4}-[0-9]{2}-[0-9]{2}',reports_text)[0].split('-')))
                reports_title = re.findall(r'title="(.+?)"',reports_text)[0]
                reports_tags_page.append((reports_title, reports_date, reports_url))
            except:
                pass
        reports_current_page_num = reports_current_page_num + 1
        dates_page = [i[1] for i in reports_tags_page]
        if max(dates_page) <= min_date: #消息出现顺序从日期晚到日期早, 如果整页最大日期都比需求最小日期小, 就没必要继续爬了
            stop_flag = 1
        else:
            reports_tags_page_included = [i for i in reports_tags_page if (i[1]>min_date) and (i[1]<max_date)]
            reports_tags_all = reports_tags_all + reports_tags_page_included     

    print("研报检索完毕, 开始爬取研报内容；")
    reports_text_all= []
    for report in pb(reports_tags_all, desc='Please wait', colour='#ffffff'):
        url_reports = report[2]
        response_reports = requests.get(url_reports, headers=headers,proxies=random.choice(proxy_list))
        response_reports = SetEncodings(response_reports)
        tree_reports = etree.HTML(response_reports.text)
        tree_reports = tree_reports.xpath("//div[contains(@class,'blk_container')]/descendant::text()")
        text_org_reports = ''
        for line in tree_reports:
            text_org_reports = text_org_reports + str(line)
        reports_text_all.append(text_org_reports)
        time.sleep(1)
    print("研报内容爬取完毕。")    
    
    result = pd.DataFrame([ [i[0] for i in reports_tags_all], [int(i[1]) for i in reports_tags_all], reports_text_all, [i[2] for i in reports_tags_all] ]).T
    result.columns = ['TITLE', 'DATE', 'CONTENT', 'URL']
    #result.to_excel(result_name, encoding='utf-8-sig',index=False)
    return result
    

def SinaFinance_Initializer(ts_code, min_date, max_date, proxy_list):
    headers={"User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:93.0) Gecko/20100101 Firefox/93.0"}
 
    ts_code_url = ts_code.split('.')[1].lower()+ts_code.split('.')[0]
    url_stock = 'https://finance.sina.com.cn/realstock/company/'+ts_code_url+'/nc.shtml'
    response_0 = requests.get(url_stock, headers=headers,proxies=random.choice(proxy_list))
    response_0 = SetEncodings(response_0)
    tree_0 = etree.HTML(response_0.text)

    url_corp_report = tree_0.xpath("//a[contains(text(),'公司研究')]/@href")[0]
    #url_ind_report = tree_0.xpath("//a[contains(text(),'行业研究')]/@href")[0]

    text_org_list_corp_report = Crawler_SinaFinance_Report(init_url=url_corp_report, proxy_list=proxy_list, headers=headers, min_date=min_date, max_date=max_date, result_name='SinaFinance_news_temp_corp.xlsx')
    #text_org_list_ind_report = Crawler_SinaFinance_Report(init_url=url_ind_report, proxy_list=proxy_list, headers=headers, min_date=min_date, max_date=max_date, result_name='SinaFinance_news_temp_ind.xlsx')

    text_org_list_corp_report['TOPIC']=np.nan
    text_all=''
    for i in text_org_list_corp_report.index:
        content = text_org_list_corp_report.loc[i,'CONTENT']
        text_org_list_corp_report.loc[i,'TOPIC'] = list(TopicSentenceExtraction.generate_summary(text_org=content, sentence_num=1))
        text_all = text_all + content

    summary_all=TopicSentenceExtraction.generate_summary(text_org=text_all, sentence_num=5)

    for i in summary_all: print(i)


if __name__ == '__main__':
    ts_code = '000002.SZ'
    min_date = 20220101
    max_date = 20220401
    proxy_list=MyRandomIP.GET_IP()
    print("IP池获取完成。")
    SinaFinance_Initializer(ts_code=ts_code,min_date=min_date,max_date=max_date,proxy_list=proxy_list)

