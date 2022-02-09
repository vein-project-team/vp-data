# -*- coding: utf-8 -*-

import pandas as pd
import numpy as np
from data_source import local_source

def get_stock_type(stock):
    block = ''
    market = ''
    description = ''
    if stock[0:2] in ['00','30','20']:
        market = 'SZ'
        if stock[0:3] in ['000','001']: 
            block = '主板'
        if stock[0:3] == '002':
            block = '中小板'
        if stock[0:2] == '30':
            block = '创业板'
        if stock[0:2] == '20':
            block = 'B股'
       
    elif stock[0:2] in ['60','68','90']:
        market = 'SH'
        if stock[0:2] == '60':
            block = '主板'
        if stock[0:2] == '68':
            block = '科创板'            
            if stock[0:3] == '689':           
                description = '存托凭证'
        if stock[0:2] == '90':
            block = 'B股'
                
    elif stock[0:2] in ['43','83','87']:
        market = 'BJ'
        if stock[0:3] == '002':
            block = '老三板'
        else: block = '新三版'
        
    return [block, market, description]
    

def pct_chg_ranking(date,condition):  #input: 日期, 筛选条件(单个产业名、板块名或市场名); output： 当日涨幅榜;
    stock_name_list = local_source.get_stock_list(cols='TS_CODE,NAME,INDUSTRY').set_index("TS_CODE")  
    quotations_piece = local_source.get_quotations_daily(condition='TRADE_DATE='+date)
    quotations_piece = quotations_piece.set_index("TS_CODE")
    quotations_piece = pd.merge(quotations_piece,stock_name_list,how='left',left_index=True,right_index=True) #从股名表填充股名
    quotations_piece["BLOCK"]=np.nan
    quotations_piece["MARKET"]=np.nan
    for stock in quotations_piece.index:
        stock_type=get_stock_type(stock)
        quotations_piece.loc[stock,"BLOCK"]=stock_type[0]
        quotations_piece.loc[stock,"MARKET"]=stock_type[1]
    quotations_piece['PCT_CHG'] = quotations_piece['CHANGE'] / quotations_piece['OPEN'] #计算涨幅
    quotations_piece=quotations_piece[["NAME","PCT_CHG","INDUSTRY","BLOCK","MARKET"]]
    if condition==None:
        pass
    elif condition in quotations_piece["INDUSTRY"].unique():
        quotations_piece=quotations_piece[quotations_piece["INDUSTRY"]==condition]
    elif condition in quotations_piece["BLOCK"].unique():
        quotations_piece=quotations_piece[quotations_piece["BLOCK"]==condition]
    elif condition in quotations_piece["MARKET"].unique():
        quotations_piece=quotations_piece[quotations_piece["MARKET"]==condition]
    else:
        print("无效的筛选条件。请输入单个产业名、板块名或市场名。")
    quotations_piece.sort_values(by='PCT_CHG', ascending=False, inplace=True)
    return quotations_piece

#test=pct_chg_ranking(date='20220208',condition='科创板')

def get_limit_up_continuous(date): #获取一个日期下连续涨停的个股
    date_list = local_source.get_limits_statistic(cols='TRADE_DATE').loc[:,'TRADE_DATE'].drop_duplicates().sort_values().reset_index(drop=True)
    stock_name_list = local_source.get_stock_list(cols='TS_CODE,NAME,INDUSTRY').set_index("TS_CODE")  
    limit_up_last = local_source.get_limits_statistic(condition='TRADE_DATE =' + date)
    limit_up_last = list(limit_up_last[limit_up_last['LIMIT_TYPE']=='U'].loc[:,'TS_CODE'])
    limit_up_last = [i for i in limit_up_last if stock_name_list.loc[i,'NAME'][0:2] not in ['ST','*S']]  #去除ST股
    output_list=[]
    for stock in limit_up_last:   #判断今日涨停个股是否是连续涨停
        i = 0
        limit_up_before_last = limit_up_last
        while stock in limit_up_before_last:
            i = i + 1
            date_before_last = date_list.iloc[-1-i]
            limit_up_before_last = local_source.get_limits_statistic(condition='TRADE_DATE ='+date_before_last)
            limit_up_before_last = list(limit_up_before_last[limit_up_before_last['LIMIT_TYPE']=='U'].loc[:,'TS_CODE'])    
        if i==1:
            text='{date}当日涨停'.format(date=date)
        if i>1:
            text='截至{date}日连续涨停{i}次'.format(date=date,i=i)
        output_list.append([stock,text])
    return output_list
            

def get_limit_up_period(date,period,times): #判断从date往前period交易日内是否有至少times个涨停
    date_list = local_source.get_limits_statistic(cols='TRADE_DATE').loc[:,'TRADE_DATE'].drop_duplicates().sort_values().reset_index(drop=True)
    stock_name_list = local_source.get_stock_list(cols='TS_CODE,NAME,INDUSTRY').set_index("TS_CODE")  
    date_last_index=date_list[date_list==date].index[0]    #获取输入日期在日期表的索引, 取它及它之前的日期共period天
    date_needed=list(date_list.iloc[date_last_index-period+1:date_last_index+1])
    limit_up_all=[]
    for d in date_needed:
        limit_up_piece = local_source.get_limits_statistic(condition='TRADE_DATE =' + d)
        limit_up_piece = list(limit_up_piece[limit_up_piece['LIMIT_TYPE']=='U'].loc[:,'TS_CODE'])
        limit_up_piece = [i for i in limit_up_piece if stock_name_list.loc[i,'NAME'][0:2] not in ['ST','*S']]  #去除ST股    
        limit_up_all=limit_up_all+limit_up_piece
    output_list=[]
    for stock in set(limit_up_all):
        times_stock=limit_up_all.count(stock)
        if times_stock >= times:
            text="{date}当日及之前,共{period}个交易日内涨停{times_stock}次".format(date=date,period=period,times_stock=times_stock)
            output_list.append([stock,text])
    return output_list

#test=get_limit_up_period(date="20220208",period=10,times=3)
