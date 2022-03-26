# -*- coding: utf-8 -*-


import pandas as pd
import numpy as np
from data_source import local_source
from analysis import value_strategy_tests
from tqdm import tqdm as pb


def fill_financial_data_to_daily_single_stock(data_fs, date_list): #data的日期列名应为TRADE_DATE或ANN_DATE
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
        balancesheet_chosen = fill_financial_data_to_daily_single_stock(data_fs=balancesheet_chosen, date_list=date_list)
        incomestatement_chosen = local_source.get_income_statements(cols='ANN_DATE,TS_CODE,EBIT',condition='TS_CODE = '+'"'+ts_code+'" and ANN_DATE >='+str(first_date_update))
        incomestatement_chosen = fill_financial_data_to_daily_single_stock(data_fs=incomestatement_chosen, date_list=date_list)
           
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


def construct_FFfactors(freq='daily'):
    FSdata_for_FFmodel=construct_FSdata_for_FFmodels()

    data_close = local_source.get_quotations_daily(cols="TS_CODE, TRADE_DATE, CLOSE")
    data_close = value_strategy_tests.panel_to_matrix_data(data_close,"CLOSE")
    
    data_mv = FSdata_for_FFmodel[["TS_CODE","TRADE_DATE","TOTAL_MV"]]
    data_mv = value_strategy_tests.panel_to_matrix_data(data_mv,"TOTAL_MV")
    
    data_bm = FSdata_for_FFmodel[["TS_CODE","TRADE_DATE","BM"]]
    data_bm = value_strategy_tests.panel_to_matrix_data(data_bm,"BM")
    
    data_ebit = FSdata_for_FFmodel[["TS_CODE","TRADE_DATE","EBIT"]]
    data_ebit = value_strategy_tests.panel_to_matrix_data(data_ebit,"EBIT")
    
    data_inv = FSdata_for_FFmodel[["TS_CODE","TRADE_DATE","INV"]]
    data_inv = value_strategy_tests.panel_to_matrix_data(data_inv,"INV")
    
    data_bv = FSdata_for_FFmodel[["TS_CODE","TRADE_DATE","BV"]]
    data_bv = value_strategy_tests.panel_to_matrix_data(data_bv,"BV")
        
    data_past_return = data_close.copy().fillna(method='ffill',axis=0)
    data_past_return = (data_past_return.shift(60)-data_past_return.shift(360)) / data_past_return.shift(360)

    if freq == 'monthly': #改月度数据
        data_close = value_strategy_tests.degenerate_dailydata_to_monthlydata(data_close)
        data_mv = value_strategy_tests.degenerate_dailydata_to_monthlydata(data_mv)
        data_bm = value_strategy_tests.degenerate_dailydata_to_monthlydata(data_bm)
        data_ebit = value_strategy_tests.degenerate_dailydata_to_monthlydata(data_ebit)
        data_inv = value_strategy_tests.degenerate_dailydata_to_monthlydata(data_inv)
        data_bv = value_strategy_tests.degenerate_dailydata_to_monthlydata(data_bv)
        data_past_return = value_strategy_tests.degenerate_dailydata_to_monthlydata(data_past_return)    
    
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
    ret_BH = value_strategy_tests.calculate_MVweighted_average_return(choice_matrix=BH, close_matrix=data_close, mv_matrix=data_mv)
    ret_BM = value_strategy_tests.calculate_MVweighted_average_return(choice_matrix=BM, close_matrix=data_close, mv_matrix=data_mv)
    ret_BL = value_strategy_tests.calculate_MVweighted_average_return(choice_matrix=BL, close_matrix=data_close, mv_matrix=data_mv)
    ret_SH = value_strategy_tests.calculate_MVweighted_average_return(choice_matrix=SH, close_matrix=data_close, mv_matrix=data_mv)
    ret_SM = value_strategy_tests.calculate_MVweighted_average_return(choice_matrix=SM, close_matrix=data_close, mv_matrix=data_mv)
    ret_SL = value_strategy_tests.calculate_MVweighted_average_return(choice_matrix=SL, close_matrix=data_close, mv_matrix=data_mv)
    ret_BR = value_strategy_tests.calculate_MVweighted_average_return(choice_matrix=BR, close_matrix=data_close, mv_matrix=data_mv)
    ret_BW = value_strategy_tests.calculate_MVweighted_average_return(choice_matrix=BW, close_matrix=data_close, mv_matrix=data_mv)
    ret_SR = value_strategy_tests.calculate_MVweighted_average_return(choice_matrix=SR, close_matrix=data_close, mv_matrix=data_mv)
    ret_SW = value_strategy_tests.calculate_MVweighted_average_return(choice_matrix=SW, close_matrix=data_close, mv_matrix=data_mv)
    ret_BC = value_strategy_tests.calculate_MVweighted_average_return(choice_matrix=BC, close_matrix=data_close, mv_matrix=data_mv)
    ret_BA = value_strategy_tests.calculate_MVweighted_average_return(choice_matrix=BA, close_matrix=data_close, mv_matrix=data_mv)
    ret_SC = value_strategy_tests.calculate_MVweighted_average_return(choice_matrix=SC, close_matrix=data_close, mv_matrix=data_mv)
    ret_SA = value_strategy_tests.calculate_MVweighted_average_return(choice_matrix=SA, close_matrix=data_close, mv_matrix=data_mv)
    ret_BU = value_strategy_tests.calculate_MVweighted_average_return(choice_matrix=BU, close_matrix=data_close, mv_matrix=data_mv)
    ret_BD = value_strategy_tests.calculate_MVweighted_average_return(choice_matrix=BD, close_matrix=data_close, mv_matrix=data_mv)
    ret_SU = value_strategy_tests.calculate_MVweighted_average_return(choice_matrix=SU, close_matrix=data_close, mv_matrix=data_mv)
    ret_SD = value_strategy_tests.calculate_MVweighted_average_return(choice_matrix=SD, close_matrix=data_close, mv_matrix=data_mv)
 
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
    result.to_csv("FFfactors_{freq}.csv".format(freq=freq),encoding='utf-8-sig')








'''
stock_list = local_source.get_stock_list(cols="TS_CODE,NAME")
index_list = local_source.get_indices_daily(cols="INDEX_CODE, TRADE_DATE, CLOSE")
index_list = index_list[index_list["INDEX_CODE"]=="000001.SH"].sort_values(by="TRADE_DATE", ascending=True) 
index_list["CHANGE"] = (index_list["CLOSE"]-index_list["CLOSE"].shift(1)) / index_list["CLOSE"].shift(1)
date_list = index_list["TRADE_DATE"].astype(int)


#rf_change_list = pd.read_csv("rf_changes.csv", index_col=0)

'''

