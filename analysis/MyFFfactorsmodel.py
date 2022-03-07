# -*- coding: utf-8 -*-
"""
Created on Wed Mar  2 19:20:31 2022

@author: Lenovo
"""
import pandas as pd
import numpy as np
from data_source import local_source
from tqdm import tqdm as pb
import datetime



def date_delta_calculator(date, diff_days=-180):  #input: YYYYMMDD   #暂时未使用
    date_type = type(date)
    date = datetime.datetime.strptime(str(date), "%Y%m%d")
    date = date + datetime.timedelta(days = diff_days)
    date = date.strftime("%Y%m%d")
    if date_type == int: date = int(date)
    return date

def tradedate_delta_calculator(date, date_list, diff_days=-180): #input: YYYYMMDD #暂时未使用
    date = date_delta_calculator(date=date, diff_days = diff_days)
    while date not in date_list.values:
        date = date_delta_calculator(date=date, diff_days = -1)
    return date

def panel_initializer(series1, series2): #将time_list与entity_list合成为panel形式的空dataframe #暂时未使用
    if series1.name == None:
        series1.name = "name1"
    if series2.name == None:
        series2.name = "name2"
    series1_name = series1.name
    series2_name = series2.name
    series1 = pd.DataFrame(series1)
    for i in series2.values:
        series1[i]=i
    series1.set_index(series1_name, inplace=True)
    result = pd.DataFrame(series1.stack())
    result.reset_index(inplace=True)
    result = result[[series1_name, 'level_1']]
    result.rename(columns={'level_1':series2_name},inplace=True)
    return result

def fill_financial_data_to_daily(data_fs, date_list): #data的日期列名应为TRADE_DATE或ANN_DATE
    date_list.rename("TRADE_DATE",inplace=True)
    date_list = date_list.astype(int)
    try:
        data_fs.rename(columns={'ANN_DATE':'TRADE_DATE'},inplace=True)
    except:
        pass
    data_fs["TRADE_DATE"] = data_fs["TRADE_DATE"].astype(int)
    data_fs.drop_duplicates(subset=["TRADE_DATE"],inplace=True)
    result = pd.merge(date_list, data_fs, on=['TRADE_DATE'], how="left")
    result = result.fillna(method='ffill',axis=0)
    return result

def construct_FSdata_for_FFmodels():
    date_list = local_source.get_indices_daily(cols='TRADE_DATE',condition='INDEX_CODE = "000001.SH"')["TRADE_DATE"].astype(int)
    stock_list = local_source.get_stock_list(cols="TS_CODE")
    try:
        result_existed = pd.read_csv("FSdata_for_FFmodelsdaily.csv")
        result_existed["TRADE_DATE"] = result_existed["TRADE_DATE"].astype(int)        
        result_existed.set_index("TRADE_DATE",inplace=True)
        date_list_existed = pd.Series(result_existed.index.drop_duplicates())
        date_list_update = date_list[~date_list.isin(date_list_existed)]
        if len(date_list_update)==0:
            print("The corresponding daily FSdata for FFmodels needs not to be updated.")
            result_existed.reset_index(inplace=True)
            return result_existed
        else:
            print("The corresponding daily FSdata for FFmodels needs to be updated.")
            first_date_update = date_list_update[0]
    except:
        print("The corresponding daily FSdata for FFmodels is missing.")
    date_list_update = date_list
    first_date_update=0
        
    close_added = local_source.get_quotations_daily(cols='TRADE_DATE,TS_CODE,CLOSE', condition = "TRADE_DATE>="+str(first_date_update))
    total_shares_added = local_source.get_stock_indicators_daily(cols='TRADE_DATE,TS_CODE,TOTAL_SHARE', condition = "TRADE_DATE>="+str(first_date_update))
        
    balancesheet_added=0
    incomestatement_added=0
    for ts_code in pb(stock_list["TS_CODE"], desc='filling financial data', colour='#ffffff'):
        balancesheet_chosen = local_source.get_balance_sheets(cols='ANN_DATE,TS_CODE,TOTAL_ASSETS,TOTAL_HLDR_EQY_INC_MIN_INT',condition='TS_CODE = '+'"'+ts_code+'" and ANN_DATE >='+str(first_date_update))
        balancesheet_chosen.rename(columns={'TOTAL_HLDR_EQY_INC_MIN_INT':'BV'},inplace=True)
        balancesheet_chosen = fill_financial_data_to_daily(data_fs=balancesheet_chosen, date_list=date_list)
        incomestatement_chosen = local_source.get_income_statements(cols='ANN_DATE,TS_CODE,EBIT',condition='TS_CODE = '+'"'+ts_code+'" and ANN_DATE >='+str(first_date_update))
        incomestatement_chosen = fill_financial_data_to_daily(data_fs=incomestatement_chosen, date_list=date_list)
           
        if first_date_update !=0:
            balancesheet_chosen = balancesheet_chosen.fillna(method='bfill',axis=0)
            incomestatement_chosen = incomestatement_chosen.fillna(method='bfill',axis=0)
        
        if type(balancesheet_added)==int:
            balancesheet_added = balancesheet_chosen 
        else:
            balancesheet_added = pd.concat([balancesheet_added,balancesheet_chosen],axis=0)
        if type(incomestatement_added)==int:
            incomestatement_added = incomestatement_chosen 
        else:
            incomestatement_added = pd.concat([incomestatement_added,incomestatement_chosen],axis=0)
        
    result_added = pd.merge(close_added, total_shares_added, on=['TRADE_DATE','TS_CODE'], how="left") 
    result_added["TRADE_DATE"] = result_added["TRADE_DATE"].astype(int)
    result_added = pd.merge(result_added, balancesheet_added, on=['TRADE_DATE','TS_CODE'], how="left") 
    result_added = pd.merge(result_added, incomestatement_added, on=['TRADE_DATE','TS_CODE'], how="left")     
    result_added = result_added.applymap(lambda x: np.nan if x=="NULL" else x)
    
    result_added["TOTAL_MV"] = result_added["TOTAL_SHARE"] * result_added["CLOSE"]
    result_added["BM"] = result_added["BV"] / result_added["TOTAL_MV"]
    result_added["INV"] = (result_added["TOTAL_ASSETS"]-result_added["TOTAL_ASSETS"].shift())/result_added["TOTAL_ASSETS"].shift()

    
    result_added = result_added[["TRADE_DATE","TS_CODE","TOTAL_SHARE","BV","TOTAL_MV","BM","INV", "EBIT"]]
    try:
        result = pd.concat([result_existed, result_added],axis=0)
    except:
        result = result_added
    result.to_csv("FSdata_for_FFmodelsdaily.csv",encoding='utf-8-sig',index=False)
    return result


def calculate_average_return(choice_matrix,close_matrix):  #输入选股矩阵(binary),收盘价矩阵,均为日期(YYYYMMDD)*股票(代码)的矩阵
    close_matrix = choice_matrix * close_matrix
    returns = close_matrix.pct_change().shift(-1).sum(axis=1)
    return returns

def calculate_MVweighted_average_return(choice_matrix,close_matrix,mv_matrix):  #输入选股矩阵(binary),收盘价矩阵,市值矩阵,均为日期(YYYYMMDD)*股票(代码)的矩阵
    weights = choice_matrix * mv_matrix
    weights = weights.div(weights.sum(axis=1), axis='rows')
    returns = (close_matrix.loc[weights.index].pct_change().shift(-1) * weights).sum(axis=1)
    return returns

def degenerate_dailydata_to_monthlydata(data_matrix):   #输入日期(YYYYMMDD)*股票(代码)的矩阵
    data_matrix.index = data_matrix.index // 100
    data_matrix = data_matrix[~data_matrix.index.duplicated(keep='first')]
    return data_matrix


def construct_FFfactors_daily():
    FSdata_for_FFmodel=construct_FSdata_for_FFmodels()

    data_close = local_source.get_quotations_daily(cols="TS_CODE, TRADE_DATE, CLOSE")
    data_close["TRADE_DATE"] = data_close["TRADE_DATE"].astype(int)
    data_close = data_close.set_index(["TRADE_DATE","TS_CODE"])
    data_close = data_close.unstack()
    data_close.columns = pd.DataFrame(data_close.columns)[0].apply(lambda x: x[1])
    
    data_mv = FSdata_for_FFmodel[["TS_CODE","TRADE_DATE","TOTAL_MV"]]
    data_mv = data_mv.set_index(["TRADE_DATE","TS_CODE"])
    data_mv = data_mv.unstack()
    data_mv.columns = pd.DataFrame(data_mv.columns)[0].apply(lambda x: x[1])
    
    data_bm = FSdata_for_FFmodel[["TS_CODE","TRADE_DATE","BM"]]
    data_bm = data_bm.set_index(["TRADE_DATE","TS_CODE"])
    data_bm = data_bm.unstack()
    data_bm.columns = pd.DataFrame(data_bm.columns)[0].apply(lambda x: x[1])
    
    data_ebit = FSdata_for_FFmodel[["TS_CODE","TRADE_DATE","EBIT"]]
    data_ebit = data_ebit.set_index(["TRADE_DATE","TS_CODE"])
    data_ebit = data_ebit.unstack()
    data_ebit.columns = pd.DataFrame(data_ebit.columns)[0].apply(lambda x: x[1])
    
    data_inv = FSdata_for_FFmodel[["TS_CODE","TRADE_DATE","INV"]]
    data_inv = data_inv.set_index(["TRADE_DATE","TS_CODE"])
    data_inv = data_inv.unstack()
    data_inv.columns = pd.DataFrame(data_inv.columns)[0].apply(lambda x: x[1])
    
    data_bv = FSdata_for_FFmodel[["TS_CODE","TRADE_DATE","BV"]]
    data_bv = data_bv.set_index(["TRADE_DATE","TS_CODE"])
    data_bv = data_bv.unstack()
    data_bv.columns = pd.DataFrame(data_bv.columns)[0].apply(lambda x: x[1])
        
    data_past_return = data_close.copy().fillna(method='ffill',axis=0)
    data_past_return = (data_past_return.shift(60)-data_past_return.shift(360)) / data_past_return.shift(360)
    
    #选择满足条件的股票组合
    B=data_mv.apply(lambda x:x>=x.quantile(0.5),axis=1)
    S=data_mv.apply(lambda x:x<x.quantile(0.5),axis=1)
    
    H=data_bm.apply(lambda x:x>= x.quantile(0.7),axis=1)
    M=data_bm.apply(lambda x:(x>=x.quantile(0.3))&(x<x.quantile(0.7)),axis=1)
    L=data_bm.apply(lambda x:x<x.quantile(0.3),axis=1)
    
    R=data_ebit.apply(lambda x:x>=x.quantile(0.7),axis=1)
    W=data_ebit.apply(lambda x:x<x.quantile(0.3),axis=1)
    
    A=data_inv.apply(lambda x:x>=x.quantile(0.7),axis=1)
    C=data_inv.apply(lambda x:x<x.quantile(0.3),axis=1)
    
    U=data_past_return.apply(lambda x:x>=x.quantile(0.7),axis=1)
    D=data_past_return.apply(lambda x:x<x.quantile(0.3),axis=1)
    
    #构建不同的股票组合
    BH = B & H
    BM = B & M
    BL = B & L
    SH = S & H
    SM = S & M
    SL = S & L

    BR = R & B
    BW = B & W
    SR = S & R 
    SW = R & S
    
    BC = B & C   
    BA = B & A
    SC = S & C
    SA = S & A

    BU = B & U   
    BD = B & D
    SU = S & U
    SD = S & D    
        
    #计算市值加权收益率   
    ret_BH=calculate_MVweighted_average_return(choice_matrix=BH, close_matrix=data_close, mv_matrix=data_mv)
    ret_BM=calculate_MVweighted_average_return(choice_matrix=BM, close_matrix=data_close, mv_matrix=data_mv)
    ret_BL=calculate_MVweighted_average_return(choice_matrix=BL, close_matrix=data_close, mv_matrix=data_mv)
    ret_SH=calculate_MVweighted_average_return(choice_matrix=SH, close_matrix=data_close, mv_matrix=data_mv)
    ret_SM=calculate_MVweighted_average_return(choice_matrix=SM, close_matrix=data_close, mv_matrix=data_mv)
    ret_SL=calculate_MVweighted_average_return(choice_matrix=SL, close_matrix=data_close, mv_matrix=data_mv)
    ret_BR=calculate_MVweighted_average_return(choice_matrix=BR, close_matrix=data_close, mv_matrix=data_mv)
    ret_BW=calculate_MVweighted_average_return(choice_matrix=BW, close_matrix=data_close, mv_matrix=data_mv)
    ret_SR=calculate_MVweighted_average_return(choice_matrix=SR, close_matrix=data_close, mv_matrix=data_mv)
    ret_SW=calculate_MVweighted_average_return(choice_matrix=SW, close_matrix=data_close, mv_matrix=data_mv)
    ret_BC=calculate_MVweighted_average_return(choice_matrix=BC, close_matrix=data_close, mv_matrix=data_mv)
    ret_BA=calculate_MVweighted_average_return(choice_matrix=BA, close_matrix=data_close, mv_matrix=data_mv)
    ret_SC=calculate_MVweighted_average_return(choice_matrix=SC, close_matrix=data_close, mv_matrix=data_mv)
    ret_SA=calculate_MVweighted_average_return(choice_matrix=SA, close_matrix=data_close, mv_matrix=data_mv)
    ret_BU=calculate_MVweighted_average_return(choice_matrix=BU, close_matrix=data_close, mv_matrix=data_mv)
    ret_BD=calculate_MVweighted_average_return(choice_matrix=BD, close_matrix=data_close, mv_matrix=data_mv)
    ret_SU=calculate_MVweighted_average_return(choice_matrix=SU, close_matrix=data_close, mv_matrix=data_mv)
    ret_SD=calculate_MVweighted_average_return(choice_matrix=SD, close_matrix=data_close, mv_matrix=data_mv)
 
    #计算因子
    SMB=(ret_SH+ret_SM+ret_SL)/3-(ret_BH+ret_BM+ret_BL)/3
    HML=(ret_SH+ret_BH)/2-(ret_SL+ret_BL)/2
    RMW=(ret_SR+ret_BR)/2-(ret_SW+ret_BW)/2
    CMA=(ret_SC+ret_BC)/2-(ret_SA+ret_BA)/2
    UMD=(ret_SU+ret_BU)/2-(ret_SD+ret_BD)/2
    
    result = pd.concat([SMB, HML, RMW, CMA, UMD],axis=1)
    result.columns = ["SMB","HML", "RMW", "CMA", "UMD"]
    result = result.shift(1)
    result.replace(0,np.nan, inplace=True)
    result.to_csv("FFfactors_daily.csv",encoding='utf-8-sig')


def construct_FFfactors_monthly():
    FSdata_for_FFmodel=construct_FSdata_for_FFmodels()

    data_close = local_source.get_quotations_daily(cols="TS_CODE, TRADE_DATE, CLOSE")
    data_close["TRADE_DATE"] = data_close["TRADE_DATE"].astype(int)
    data_close = data_close.set_index(["TRADE_DATE","TS_CODE"])
    data_close = data_close.unstack()
    data_close.columns = pd.DataFrame(data_close.columns)[0].apply(lambda x: x[1])
    
    data_mv = FSdata_for_FFmodel[["TS_CODE","TRADE_DATE","TOTAL_MV"]]
    data_mv = data_mv.set_index(["TRADE_DATE","TS_CODE"])
    data_mv = data_mv.unstack()
    data_mv.columns = pd.DataFrame(data_mv.columns)[0].apply(lambda x: x[1])
    
    data_bm = FSdata_for_FFmodel[["TS_CODE","TRADE_DATE","BM"]]
    data_bm = data_bm.set_index(["TRADE_DATE","TS_CODE"])
    data_bm = data_bm.unstack()
    data_bm.columns = pd.DataFrame(data_bm.columns)[0].apply(lambda x: x[1])
    
    data_ebit = FSdata_for_FFmodel[["TS_CODE","TRADE_DATE","EBIT"]]
    data_ebit = data_ebit.set_index(["TRADE_DATE","TS_CODE"])
    data_ebit = data_ebit.unstack()
    data_ebit.columns = pd.DataFrame(data_ebit.columns)[0].apply(lambda x: x[1])
    
    data_inv = FSdata_for_FFmodel[["TS_CODE","TRADE_DATE","INV"]]
    data_inv = data_inv.set_index(["TRADE_DATE","TS_CODE"])
    data_inv = data_inv.unstack()
    data_inv.columns = pd.DataFrame(data_inv.columns)[0].apply(lambda x: x[1])
    
    data_bv = FSdata_for_FFmodel[["TS_CODE","TRADE_DATE","BV"]]
    data_bv = data_bv.set_index(["TRADE_DATE","TS_CODE"])
    data_bv = data_bv.unstack()
    data_bv.columns = pd.DataFrame(data_bv.columns)[0].apply(lambda x: x[1])
        
    data_past_return = data_close.copy().fillna(method='ffill',axis=0)
    data_past_return = (data_past_return.shift(60)-data_past_return.shift(360)) / data_past_return.shift(360)
    
    #改月度数据
    data_close = degenerate_dailydata_to_monthlydata(data_close)
    data_mv = degenerate_dailydata_to_monthlydata(data_mv)
    data_bm = degenerate_dailydata_to_monthlydata(data_bm)
    data_ebit = degenerate_dailydata_to_monthlydata(data_ebit)
    data_inv = degenerate_dailydata_to_monthlydata(data_inv)
    data_bv = degenerate_dailydata_to_monthlydata(data_bv)
    data_past_return = degenerate_dailydata_to_monthlydata(data_past_return)        
    
    #选择满足条件的股票组合
    B=data_mv.apply(lambda x:x>=x.quantile(0.5),axis=1)
    S=data_mv.apply(lambda x:x<x.quantile(0.5),axis=1)
    
    H=data_bm.apply(lambda x:x>= x.quantile(0.7),axis=1)
    M=data_bm.apply(lambda x:(x>=x.quantile(0.3))&(x<x.quantile(0.7)),axis=1)
    L=data_bm.apply(lambda x:x<x.quantile(0.3),axis=1)
    
    R=data_ebit.apply(lambda x:x>=x.quantile(0.7),axis=1)
    W=data_ebit.apply(lambda x:x<x.quantile(0.3),axis=1)
    
    A=data_inv.apply(lambda x:x>=x.quantile(0.7),axis=1)
    C=data_inv.apply(lambda x:x<x.quantile(0.3),axis=1)
    
    U=data_past_return.apply(lambda x:x>=x.quantile(0.7),axis=1)
    D=data_past_return.apply(lambda x:x<x.quantile(0.3),axis=1)
    
    #构建不同的股票组合
    BH = B & H
    BM = B & M
    BL = B & L
    SH = S & H
    SM = S & M
    SL = S & L

    BR = R & B
    BW = B & W
    SR = S & R 
    SW = R & S
    
    BC = B & C   
    BA = B & A
    SC = S & C
    SA = S & A

    BU = B & U   
    BD = B & D
    SU = S & U
    SD = S & D    
       
    #计算市值加权收益率   
    ret_BH=calculate_MVweighted_average_return(choice_matrix=BH, close_matrix=data_close, mv_matrix=data_mv)
    ret_BM=calculate_MVweighted_average_return(choice_matrix=BM, close_matrix=data_close, mv_matrix=data_mv)
    ret_BL=calculate_MVweighted_average_return(choice_matrix=BL, close_matrix=data_close, mv_matrix=data_mv)
    ret_SH=calculate_MVweighted_average_return(choice_matrix=SH, close_matrix=data_close, mv_matrix=data_mv)
    ret_SM=calculate_MVweighted_average_return(choice_matrix=SM, close_matrix=data_close, mv_matrix=data_mv)
    ret_SL=calculate_MVweighted_average_return(choice_matrix=SL, close_matrix=data_close, mv_matrix=data_mv)
    ret_BR=calculate_MVweighted_average_return(choice_matrix=BR, close_matrix=data_close, mv_matrix=data_mv)
    ret_BW=calculate_MVweighted_average_return(choice_matrix=BW, close_matrix=data_close, mv_matrix=data_mv)
    ret_SR=calculate_MVweighted_average_return(choice_matrix=SR, close_matrix=data_close, mv_matrix=data_mv)
    ret_SW=calculate_MVweighted_average_return(choice_matrix=SW, close_matrix=data_close, mv_matrix=data_mv)
    ret_BC=calculate_MVweighted_average_return(choice_matrix=BC, close_matrix=data_close, mv_matrix=data_mv)
    ret_BA=calculate_MVweighted_average_return(choice_matrix=BA, close_matrix=data_close, mv_matrix=data_mv)
    ret_SC=calculate_MVweighted_average_return(choice_matrix=SC, close_matrix=data_close, mv_matrix=data_mv)
    ret_SA=calculate_MVweighted_average_return(choice_matrix=SA, close_matrix=data_close, mv_matrix=data_mv)
    ret_BU=calculate_MVweighted_average_return(choice_matrix=BU, close_matrix=data_close, mv_matrix=data_mv)
    ret_BD=calculate_MVweighted_average_return(choice_matrix=BD, close_matrix=data_close, mv_matrix=data_mv)
    ret_SU=calculate_MVweighted_average_return(choice_matrix=SU, close_matrix=data_close, mv_matrix=data_mv)
    ret_SD=calculate_MVweighted_average_return(choice_matrix=SD, close_matrix=data_close, mv_matrix=data_mv)
     
    #计算因子
    SMB=(ret_SH+ret_SM+ret_SL)/3-(ret_BH+ret_BM+ret_BL)/3
    HML=(ret_SH+ret_BH)/2-(ret_SL+ret_BL)/2
    RMW=(ret_SR+ret_BR)/2-(ret_SW+ret_BW)/2
    CMA=(ret_SC+ret_BC)/2-(ret_SA+ret_BA)/2
    UMD=(ret_SU+ret_BU)/2-(ret_SD+ret_BD)/2
    
    result = pd.concat([SMB, HML, RMW, CMA, UMD],axis=1)
    result.columns = ["SMB","HML", "RMW", "CMA", "UMD"]
    result = result.shift(1)
    result.replace(0,np.nan, inplace=True)
    result.to_csv("FFfactors_monthly.csv",encoding='utf-8-sig')


'''
stock_list = local_source.get_stock_list(cols="TS_CODE,NAME")
index_list = local_source.get_indices_daily(cols="INDEX_CODE, TRADE_DATE, CLOSE")
index_list = index_list[index_list["INDEX_CODE"]=="000001.SH"].sort_values(by="TRADE_DATE", ascending=True) 
index_list["CHANGE"] = (index_list["CLOSE"]-index_list["CLOSE"].shift(1)) / index_list["CLOSE"].shift(1)
date_list = index_list["TRADE_DATE"].astype(int)

rf_change_list = pd.read_csv("rf_changes.csv", index_col=0)
'''
