# -*- coding: utf-8 -*-

import pandas as pd
import numpy as np
from scipy import stats
from linearmodels import FamaMacBeth
from decimal import Decimal 
from data_source import local_source
from tqdm import tqdm as pb
import datetime



def DataFrame_Updater(df_old, df_new, by_list):  #以by_list为主键, 用df_new中数据更新df_old中数据
    return pd.concat(df_old, df_new).drop_duplicates(by_list,keep='last')


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


def panel_to_matrix_data(panel_data, var_name, index_name="TRADE_DATE", columns_name="TS_CODE"):
    panel_data_ = panel_data[[columns_name, index_name, var_name]]
    panel_data_.loc[:,index_name] = panel_data_.loc[:,index_name].astype(int)
    panel_data_ = panel_data_.set_index([index_name,columns_name])
    matrix_data = panel_data_.unstack()
    matrix_data.columns = pd.DataFrame(matrix_data.columns)[0].apply(lambda x: x[1])    
    return matrix_data  


def matrix_to_panel_data(matrix_data, var_name, index_name="TRADE_DATE", columns_name="TS_CODE"):
    panel_data = matrix_data.stack().reset_index(name=var_name)
    panel_data.columns = [index_name,columns_name,var_name]
    panel_data = panel_data[[columns_name,index_name,var_name]]
    return panel_data


def date_delta_calculator(date, diff_days=-180):  #input: YYYYMMDD   #暂时未使用
    date_type = type(date)
    date = datetime.datetime.strptime(str(date), "%Y%m%d")
    date = date + datetime.timedelta(days = diff_days)
    date = date.strftime("%Y%m%d")
    if date_type == int: date = int(date)
    return date


def date_delta_calculator2(date, date_list, diff_days=-180): #input: YYYYMMDD  #暂时未使用
    #在date_delta_calculator的基础上添加自动调整使偏移后的日期为交易日    
    date = date_delta_calculator(date=date, diff_days = diff_days)
    if diff_days<0:
        while date not in date_list.values:
            if date<min(date_list): return min(date_list)
            date = date_delta_calculator(date=date, diff_days = -1)
    if diff_days>0:
        while date not in date_list.values:
            if date>max(date_list): return max(date_list)
            date = date_delta_calculator(date=date, diff_days = 1)
    return date


def degenerate_dailydata_to_monthlydata(data, data_type='matrix'):   #matrix: 输入日期(YYYYMMDD)*股票(代码)的矩阵; panel: 输入含有日期TRADE_DATE与个体TS_CODE列的dataframe.
    data_ = data.copy()
    if data_type == 'matrix':
        data_.index = data_.index // 100
        data_ = data_[~data_.index.duplicated(keep='last')]
    if data_type == 'panel':
        data_["TRADE_DATE"] = data_["TRADE_DATE"]//100
        data_ = data_.drop_duplicates(["TS_CODE","TRADE_DATE"], keep='last')
        data_ = data_.reset_index(drop=True)
    return data_

def calculate_pctchange_bystock(df,var_name='CLOSE', result_name='PCT_CHANGE'):
    df_=panel_to_matrix_data(df, var_name = var_name)
    df_ = df_.pct_change()
    df_ = matrix_to_panel_data(df_, var_name = result_name)
    return df_
    

def calculate_average_return(choice_matrix,close_matrix):  #输入选股矩阵(binary),收盘价矩阵,均为日期(YYYYMMDD)*股票(代码)的矩阵
    #close_matrix = choice_matrix * close_matrix  
    #returns = close_matrix.pct_change().shift(-1).sum(axis=1)  #以上为错误的写法,会出现inf; 先处理权重 inf部分权重会为0
    weights = choice_matrix.div(choice_matrix.sum(axis=1), axis='rows')
    returns = (close_matrix.pct_change().shift(-1) * weights).sum(axis=1)     
    return returns


def calculate_MVweighted_average_return(choice_matrix,close_matrix,mv_matrix):  #输入选股矩阵(binary),收盘价矩阵,市值矩阵,均为日期(YYYYMMDD)*股票(代码)的矩阵
    weights = choice_matrix * mv_matrix
    weights = weights.div(weights.sum(axis=1), axis='rows')
    returns = (close_matrix.loc[weights.index].pct_change().shift(-1) * weights).sum(axis=1)
    return returns


def delete_ST(df):
    stock_name_list = local_source.get_stock_list(cols="TS_CODE, NAME")
    stock_name_list["ST_FLAG"]=['ST' in stock_name_list["NAME"][x] for x in stock_name_list.index]
    ST_stock_list = list(stock_name_list[stock_name_list["ST_FLAG"]==1]["TS_CODE"])
    df = df[ [item not in ST_stock_list for item in df["TS_CODE"]] ]
    return df


def delete_FinanceCorps(df):
    delete_corp_list = ['银行','证券','多元金融']
    stock_industry_list = local_source.get_stock_list(cols="TS_CODE, INDUSTRY")
    stock_industry_list['FC_FLAG']=[stock_industry_list["INDUSTRY"][x] in delete_corp_list for x in stock_industry_list.index]
    FC_stock_list = list(stock_industry_list[stock_industry_list["FC_FLAG"]==1]["TS_CODE"])
    df = df[ [item not in FC_stock_list for item in df["TS_CODE"]] ]    
    return df


def Z_standardization(df, input_name_list, input_ascending, output_name): #input_list如["账面市值比A","现金方差"], input_ascending如["True","False"], output_name为输出指标名如"Safety"
    df_ = df.copy()
    input_num = 0
    df_[output_name] = 0
    for input_name in input_name_list:
        df_[input_name+"_normalized"] = (df_[input_name]-df_[input_name].mean())/df_[input_name].mean()
        df_[output_name] = df_[output_name] + df_[input_name+"_normalized"]
        df_.drop(input_name+"_normalized", axis=1, inplace=True)
        input_num = input_num + 1  
    return df_


def Z_standardization_of_rank(df,input_name_list, input_ascending, output_name):  #input_list如["账面市值比A","现金方差"], input_ascending如["True","False"], output_name为输出指标名如"Safety"
    df_=df.copy()
    input_num=0
    df_[output_name]=0
    for input_name in input_name_list:
        df_["rank_"+input_name]=df_[input_name].rank(ascending=input_ascending[input_num])
        df_["rank_"+input_name+"_normalized"]=(df_["rank_"+input_name]-df_["rank_"+input_name].mean())/df_["rank_"+input_name].mean()
        df_[output_name]=df_[output_name]+df_["rank_"+input_name+"_normalized"]
        df_.drop("rank_"+input_name+"_normalized", axis=1, inplace=True)
        input_num = input_num + 1  
    return df_


def get_annualized_income_statements(cols, condition):
    data_season1 = local_source.get_income_statements(cols=cols, condition=condition+' and END_TYPE=1')
    data_season1 = data_season1.applymap(lambda x: np.nan if x=="NULL" else x)
    data_season1 = pd.concat([4*data_season1.loc[:,x] if type(data_season1.loc[:,x][0])!=str else data_season1.loc[:,x] for x in data_season1.columns], axis=1)
    data_season2 = local_source.get_income_statements(cols=cols, condition=condition+' and END_TYPE=2')
    data_season2 = data_season2.applymap(lambda x: np.nan if x=="NULL" else x)
    data_season2 = pd.concat([2*data_season2.loc[:,x] if type(data_season2.loc[:,x][0])!=str else data_season2.loc[:,x] for x in data_season2.columns], axis=1)
    data_season3 = local_source.get_income_statements(cols=cols, condition=condition+' and END_TYPE=3')
    data_season3 = data_season3.applymap(lambda x: np.nan if x=="NULL" else x)
    data_season3 = pd.concat([(4/3)*data_season3.loc[:,x] if type(data_season3.loc[:,x][0])!=str else data_season3.loc[:,x] for x in data_season3.columns], axis=1)
    data_season4 = local_source.get_income_statements(cols=cols, condition=condition+' and END_TYPE=4')
    data_season4 = data_season4.applymap(lambda x: np.nan if x=="NULL" else x)
    data_season_all = pd.concat([data_season1, data_season2, data_season3, data_season4], axis=0)
    #data_season_all = data_season_all.sort_values(by=["TS_CODE","END_DATE"])
    return data_season_all
    

def fill_financial_data_to_daily_ann_date_basis(data_fs, date_list=0):
    if type(date_list)==int:
        date_list = local_source.get_indices_daily(cols='TRADE_DATE',condition='INDEX_CODE = "000001.SH"')["TRADE_DATE"].astype(int)
    try:
        data_fs.rename(columns={'ANN_DATE':'TRADE_DATE'},inplace=True)
    except:
        pass
    data_fs["TRADE_DATE"]=data_fs["TRADE_DATE"].astype(int)
    data_fs=data_fs.sort_values(by=['TRADE_DATE','END_DATE'], ascending=False) #对于同一天一次补发先前多年财务报表的情况,
    data_fs=data_fs.drop_duplicates(["TS_CODE","TRADE_DATE"], keep='first')   #应选择最新的财务报表。
    data_fs_daily = 0
    for entry in pb(data_fs.columns.drop(["TRADE_DATE","TS_CODE"]), desc='filling financial data to daily', colour='#ffffff'):
        data_fs_daily_piece = pd.merge(date_list, data_fs, on=["TRADE_DATE"], how='left')
        data_fs_daily_piece = data_fs_daily_piece.applymap(lambda x: np.nan if x=="NULL" else x)  
        data_fs_daily_piece = panel_to_matrix_data(data_fs_daily_piece, entry, index_name="TRADE_DATE", columns_name="TS_CODE")
        data_fs_daily_piece.drop(np.nan, axis=1, inplace=True)
        data_fs_daily_piece = data_fs_daily_piece.fillna(method='ffill',axis=0)
        data_fs_daily_piece = matrix_to_panel_data(data_fs_daily_piece, entry, index_name="TRADE_DATE", columns_name="TS_CODE")
        if type(data_fs_daily) == int:
            data_fs_daily = data_fs_daily_piece
        else:
            data_fs_daily = pd.merge(data_fs_daily, data_fs_daily_piece, on=["TRADE_DATE","TS_CODE"], how='left')  
    return data_fs_daily    


def fill_financial_data_to_daily_end_date_basis(data_fs, delay_month=2):
    #构建每个年月与延迟后月的最后一个交易日的关系
    date_list = local_source.get_indices_daily(cols='TRADE_DATE',condition='INDEX_CODE = "000001.SH"')["TRADE_DATE"].sort_values(ascending=True).astype(int)
    month_list = pd.Series((date_list//100).unique())
    last_day_of_month_list = pd.Series([date_list[(date_list//100)==month].iloc[-1] for month in month_list])
    last_day_of_month_list = last_day_of_month_list.shift(-1*delay_month)
    last_day_of_month_dict =  {i:j for i,j in zip(month_list, last_day_of_month_list)}    
    #填充TRADE_DATE为延迟后月的最后一个交易日
    data_fs["END_DATE"]=data_fs["END_DATE"].astype(int)
    data_fs["END_DATE_YM"]=data_fs["END_DATE"]//100
    data_fs["END_DATE_ADJUSTED"]=0
    for ym in data_fs["END_DATE_YM"].unique():
        try:                                        #可能报错的原因：该企业的财务报表日期早于最早交易日期
            data_fs.loc[data_fs["END_DATE_YM"]==ym, 'END_DATE_ADJUSTED'] = last_day_of_month_dict[ym]
        except:
            pass
    data_fs["END_DATE_ADJUSTED"]=data_fs["END_DATE_ADJUSTED"].astype(int)
    data_fs["TRADE_DATE"]=data_fs["END_DATE_ADJUSTED"]
    data_fs.drop("END_DATE_YM", axis=1, inplace=True)
    #将数据填到每天
    data_fs_daily = 0
    for entry in pb(data_fs.columns.drop(["TRADE_DATE","TS_CODE"]), desc='filling financial data to daily', colour='#ffffff'):
        data_fs_daily_piece = pd.merge(date_list, data_fs, on=["TRADE_DATE"], how='left')
        data_fs_daily_piece = data_fs_daily_piece.applymap(lambda x: np.nan if x=="NULL" else x)  
        data_fs_daily_piece = panel_to_matrix_data(data_fs_daily_piece, entry, index_name="TRADE_DATE", columns_name="TS_CODE")
        data_fs_daily_piece.drop(np.nan, axis=1, inplace=True)
        data_fs_daily_piece = data_fs_daily_piece.fillna(method='ffill',axis=0)
        data_fs_daily_piece = matrix_to_panel_data(data_fs_daily_piece, entry, index_name="TRADE_DATE", columns_name="TS_CODE")
        if type(data_fs_daily) == int:
            data_fs_daily = data_fs_daily_piece
        else:
            data_fs_daily = pd.merge(data_fs_daily, data_fs_daily_piece, on=["TRADE_DATE","TS_CODE"], how='left')  
    return data_fs_daily  
    

def MyCensor(df, var_list, quantile=0.01): #缩尾处理, var_list为要进行处理的变量名列表
    df_ = df.copy()
    for var_name in var_list:
        threshold_max = df_[var_name].quantile(1-quantile)
        threshold_min = df_[var_name].quantile(quantile)
        df_[var_name]=[i if (i<threshold_max) else threshold_max for i in df_[var_name]]
        df_[var_name]=[i if (i>threshold_min) else threshold_min for i in df_[var_name]]
    return df_


def CountingStars(p):
    if p<=0.01: return "***"
    if p<=0.05: return "**"
    if p<=0.1: return "*"
    else: return ""


def stock_selection_by_var(df, var_name, pct=0.2, Type='best', freq='daily', start_date=20200101, end_date=20201231):
    #输入的df要与freq匹配
    df["TRADE_DATE"] = df["TRADE_DATE"].astype(int)
    df_ = df[df["TRADE_DATE"]>=start_date]
    df_ = df_[df_["TRADE_DATE"]<=end_date]
    if len(df_)==0:
        print("选择日期区间内无数据！")
        return 0
    df_ = df_.drop_duplicates(["TS_CODE","TRADE_DATE"], keep='last')  #keep选last是因为财报数据在同trade_date报出多个财报时, 年报(数据最全)会是最后一个;
    df_ = df_[["TS_CODE","TRADE_DATE",var_name]]     
    value_mat = panel_to_matrix_data(df_, var_name)      
    if Type == 'best':
        choice_mat = value_mat.apply(lambda x:(x>=x.quantile(1-pct)), axis=1)
    if Type == 'worse':
        choice_mat = value_mat.apply(lambda x:(x<=x.quantile(pct)), axis=1) 
    return choice_mat


def univariate_test_for_returns(df, var_name, mv_weighted=False, freq='daily', start_date=20200101, end_date=20201231):
    #输入的df要与freq匹配
    #date_list = local_source.get_indices_daily(cols='TRADE_DATE',condition='INDEX_CODE = "000001.SH" and TRADE_DATE>='+str(start_date)+' and TRADE_DATE<='+str(end_date))["TRADE_DATE"].sort_values(ascending=True).astype(int)  
    df["TRADE_DATE"] = df["TRADE_DATE"].astype(int)
    df_ = df[df["TRADE_DATE"]>=start_date]
    df_ = df_[df_["TRADE_DATE"]<=end_date]
    if len(df_)==0:
        print("选择日期区间内无数据！")
        return 0
    df_ = df_.drop_duplicates(["TS_CODE","TRADE_DATE"], keep='last')  #keep选last是因为财报数据在同trade_date报出多个财报时, 年报(数据最全)会是最后一个;
    df_ = df_[["TS_CODE","TRADE_DATE",var_name]] 
    
    if freq == 'daily':
        data_close = local_source.get_quotations_daily(cols="TS_CODE, TRADE_DATE, CLOSE", condition='TRADE_DATE>=' + str(start_date) + ' and TRADE_DATE<=' + str(end_date))
        data_close["TRADE_DATE"] = data_close["TRADE_DATE"].astype(int)
        data_close = data_close.drop_duplicates(["TS_CODE","TRADE_DATE"], keep='last')
        data_mv = local_source.get_stock_indicators_daily(cols='TS_CODE, TRADE_DATE, TOTAL_SHARE', condition='TRADE_DATE>=' + str(start_date) + ' and TRADE_DATE<=' + str(end_date))
        data_mv["TRADE_DATE"] = data_mv["TRADE_DATE"].astype(int)  
    
    if freq == 'monthly':
        data_close = local_source.get_quotations_daily(cols="TS_CODE, TRADE_DATE, CLOSE", condition='TRADE_DATE>=' + str(start_date) + '01' + ' and TRADE_DATE<=' + str(end_date) + '31')
        data_close["TRADE_DATE"] = data_close["TRADE_DATE"].astype(int)
        data_close = data_close.drop_duplicates(["TS_CODE","TRADE_DATE"], keep='last')
        data_close = degenerate_dailydata_to_monthlydata(data_close, data_type='panel')
        data_mv = local_source.get_stock_indicators_daily(cols='TS_CODE, TRADE_DATE, TOTAL_SHARE', condition='TRADE_DATE>=' + str(start_date) + '01' + ' and TRADE_DATE<=' + str(end_date) +'31')
        data_mv["TRADE_DATE"] = data_mv["TRADE_DATE"].astype(int)  
        data_mv = degenerate_dailydata_to_monthlydata(data_mv, data_type='panel')
        
    data_merged = pd.merge(data_close, data_mv, on=["TS_CODE","TRADE_DATE"],how='left')
    data_merged["TOTAL_MV"] = data_merged["TOTAL_SHARE"] * data_merged["CLOSE"]
    data_merged.drop("TOTAL_SHARE", axis=1, inplace=True)
    data_merged = pd.merge(data_merged, df_, on=["TS_CODE","TRADE_DATE"],how='left')  

    data_close = panel_to_matrix_data(data_merged, "CLOSE")      
    data_mv = panel_to_matrix_data(data_merged, "TOTAL_MV")        
    value_mat = panel_to_matrix_data(data_merged, var_name)
    
    choice_mat_q1=value_mat.apply(lambda x:(x<=x.quantile(0.2)),axis=1)
    choice_mat_q2=value_mat.apply(lambda x:(x>x.quantile(0.2))&(x<=x.quantile(0.4)),axis=1)
    choice_mat_q3=value_mat.apply(lambda x:(x>x.quantile(0.4))&(x<=x.quantile(0.6)),axis=1)
    choice_mat_q4=value_mat.apply(lambda x:(x>x.quantile(0.6))&(x<=x.quantile(0.8)),axis=1)
    choice_mat_q5=value_mat.apply(lambda x:(x>x.quantile(0.8)),axis=1)

    if mv_weighted == False:
        ret_q1 = calculate_average_return(choice_matrix=choice_mat_q1, close_matrix=data_close)
        ret_q2 = calculate_average_return(choice_matrix=choice_mat_q2, close_matrix=data_close)
        ret_q3 = calculate_average_return(choice_matrix=choice_mat_q3, close_matrix=data_close)
        ret_q4 = calculate_average_return(choice_matrix=choice_mat_q4, close_matrix=data_close)
        ret_q5 = calculate_average_return(choice_matrix=choice_mat_q5, close_matrix=data_close)
    if mv_weighted == True:
        ret_q1 = calculate_MVweighted_average_return(choice_matrix=choice_mat_q1, close_matrix=data_close, mv_matrix=data_mv)
        ret_q2 = calculate_MVweighted_average_return(choice_matrix=choice_mat_q2, close_matrix=data_close, mv_matrix=data_mv)
        ret_q3 = calculate_MVweighted_average_return(choice_matrix=choice_mat_q3, close_matrix=data_close, mv_matrix=data_mv)
        ret_q4 = calculate_MVweighted_average_return(choice_matrix=choice_mat_q4, close_matrix=data_close, mv_matrix=data_mv)
        ret_q5 = calculate_MVweighted_average_return(choice_matrix=choice_mat_q5, close_matrix=data_close, mv_matrix=data_mv)
    ret_q5minusq1 = ret_q5-ret_q1
    
    t_q1=stats.ttest_1samp(ret_q1, 0) #statistic, pvalue, count
    t_q2=stats.ttest_1samp(ret_q2, 0)
    t_q3=stats.ttest_1samp(ret_q3, 0)
    t_q4=stats.ttest_1samp(ret_q4, 0)
    t_q5=stats.ttest_1samp(ret_q5, 0)
    t_q5minusq1=stats.ttest_1samp(ret_q5minusq1, 0)
    
    result = pd.DataFrame(index=["statistic","p-value","Obs."],columns=["q1","q2","q3","q4","q5","diff[q5-q1]"])
    statistics = [t_q1.statistic,t_q2.statistic,t_q3.statistic,t_q4.statistic,t_q5.statistic,t_q5minusq1.statistic]
    statistics = [ str(Decimal("%.4f" % float(i))) if np.isnan(i)==False else '' for i in statistics ]
    pvalues = [t_q1.pvalue,t_q2.pvalue,t_q3.pvalue,t_q4.pvalue,t_q5.pvalue,t_q5minusq1.pvalue]
    result.loc["statistic",:] = statistics
    result.loc["p-value",:] = pvalues   
    result.loc["Obs.",:] = [len(ret_q1),len(ret_q2),len(ret_q3),len(ret_q4),len(ret_q5),len(ret_q5minusq1)] 
    print(result)
    return ret_q1, ret_q2, ret_q3, ret_q4, ret_q5


def univariate_test_for_returns_2(df, var_name, mv_weighted=False, freq='daily', start_date=20200101, end_date=20201231):  
    #备用代码, 暂不使用, 输入df需要包含return列
    df["TRADE_DATE"] = df["TRADE_DATE"].astype(int)
    df_ = df[df["TRADE_DATE"]>=start_date]
    df_ = df_[df_["TRADE_DATE"]<=end_date]
    if len(df_)==0:
        print("选择日期区间内无数据！")
        return 0
    df_ = df_.drop_duplicates(["TS_CODE","TRADE_DATE"], keep='last')
    
    if freq == 'daily':
        data_close = local_source.get_quotations_daily(cols="TS_CODE, TRADE_DATE, CLOSE", condition='TRADE_DATE>=' + str(start_date) + ' and TRADE_DATE<=' + str(end_date))
        data_close["TRADE_DATE"] = data_close["TRADE_DATE"].astype(int)
        data_close = data_close.drop_duplicates(["TS_CODE","TRADE_DATE"], keep='last')
        data_mv = local_source.get_stock_indicators_daily(cols='TS_CODE, TRADE_DATE, TOTAL_SHARE', condition='TRADE_DATE>=' + str(start_date) + ' and TRADE_DATE<=' + str(end_date))
        data_mv["TRADE_DATE"] = data_mv["TRADE_DATE"].astype(int)  
    
    if freq == 'monthly':
        data_close = local_source.get_quotations_daily(cols="TS_CODE, TRADE_DATE, CLOSE", condition='TRADE_DATE>=' + str(start_date) + '01' + ' and TRADE_DATE<=' + str(end_date) + '31')
        data_close["TRADE_DATE"] = data_close["TRADE_DATE"].astype(int)
        data_close = data_close.drop_duplicates(["TS_CODE","TRADE_DATE"], keep='last')
        data_close = degenerate_dailydata_to_monthlydata(data_close, data_type='panel')
        data_mv = local_source.get_stock_indicators_daily(cols='TS_CODE, TRADE_DATE, TOTAL_SHARE', condition='TRADE_DATE>=' + str(start_date) + '01' + ' and TRADE_DATE<=' + str(end_date) +'31')
        data_mv["TRADE_DATE"] = data_mv["TRADE_DATE"].astype(int)  
        data_mv = degenerate_dailydata_to_monthlydata(data_mv, data_type='panel')
        
    data_merged = pd.merge(data_close, data_mv, on=["TS_CODE","TRADE_DATE"],how='left')
    data_merged["TOTAL_MV"] = data_merged["TOTAL_SHARE"] * data_merged["CLOSE"]
    data_merged.drop("TOTAL_SHARE", axis=1, inplace=True)
    data_merged = pd.merge(data_merged, df_, on=["TS_CODE","TRADE_DATE"],how='left')  
    
    date_list = data_merged["TRADE_DATE"].unique()
    if mv_weighted == False:
        avg_ret_q1_list=[]
        avg_ret_q2_list=[]
        avg_ret_q3_list=[]
        avg_ret_q4_list=[]
        avg_ret_q5_list=[]
        
        for date in pb(date_list, desc='please wait', colour='#ffffff'):
            data_1m=data_merged[data_merged["TRADE_DATE"]==date]
            data_1m=data_1m.sort_values(by=['test'],ascending=True)
            q1=int(len(data_1m)*(1/5))
            q2=int(len(data_1m)*(2/5))
            q3=int(len(data_1m)*(3/5))
            q4=int(len(data_1m)*(4/5))
            data_1m_q1=data_1m.iloc[0:q1,:]
            data_1m_q2=data_1m.iloc[q1:q2,:]
            data_1m_q3=data_1m.iloc[q2:q3,:]
            data_1m_q4=data_1m.iloc[q3:q4,:]
            data_1m_q5=data_1m.iloc[q4:len(data_1m),:]
            avg_ret_q1=data_1m_q1.loc[:,"PCT_CHANGE"].mean()
            avg_ret_q2=data_1m_q2.loc[:,"PCT_CHANGE"].mean()
            avg_ret_q3=data_1m_q3.loc[:,"PCT_CHANGE"].mean()
            avg_ret_q4=data_1m_q4.loc[:,"PCT_CHANGE"].mean()
            avg_ret_q5=data_1m_q5.loc[:,"PCT_CHANGE"].mean()
            avg_ret_q1_list.append(avg_ret_q1)
            avg_ret_q2_list.append(avg_ret_q2)
            avg_ret_q3_list.append(avg_ret_q3)
            avg_ret_q4_list.append(avg_ret_q4)
            avg_ret_q5_list.append(avg_ret_q5)    
        
        avg_ret_q5minusq1_list=list(np.array(avg_ret_q5_list)-np.array(avg_ret_q1_list))   
        avg_ret_q1_list=list(pd.Series(avg_ret_q1_list).dropna())
        avg_ret_q2_list=list(pd.Series(avg_ret_q2_list).dropna())
        avg_ret_q3_list=list(pd.Series(avg_ret_q3_list).dropna())
        avg_ret_q4_list=list(pd.Series(avg_ret_q4_list).dropna())
        avg_ret_q5_list=list(pd.Series(avg_ret_q5_list).dropna())    
        avg_ret_q5minusq1_list=list(pd.Series(avg_ret_q5minusq1_list).dropna())
        print(np.array(avg_ret_q1_list).mean(),np.array(avg_ret_q2_list).mean(),np.array(avg_ret_q3_list).mean(),np.array(avg_ret_q4_list).mean(),np.array(avg_ret_q5_list).mean(),np.array(avg_ret_q5minusq1_list).mean())   
    
        t_q1=stats.ttest_1samp(avg_ret_q1_list,0)
        t_q2=stats.ttest_1samp(avg_ret_q2_list,0)
        t_q3=stats.ttest_1samp(avg_ret_q3_list,0)
        t_q4=stats.ttest_1samp(avg_ret_q4_list,0)
        t_q5=stats.ttest_1samp(avg_ret_q5_list,0)
        t_q5minusq1=stats.ttest_1samp(avg_ret_q5minusq1_list,0)
        print(t_q1,t_q2,t_q3,t_q4,t_q5,t_q5minusq1)
    
    if mv_weighted == True:
        avg_ret_q1_w_list=[]
        avg_ret_q2_w_list=[]
        avg_ret_q3_w_list=[]
        avg_ret_q4_w_list=[]
        avg_ret_q5_w_list=[]
    
        for date in pb(date_list, desc='please wait', colour='#ffffff'):
            data_1m=data_merged[data_merged["TRADE_DATE"]==date]
            data_1m=data_1m.sort_values(by=['test'],ascending=True)
            q1=int(len(data_1m)*(1/5))
            q2=int(len(data_1m)*(2/5))
            q3=int(len(data_1m)*(3/5))
            q4=int(len(data_1m)*(4/5))
            data_1m_q1=data_1m.iloc[0:q1,:]
            data_1m_q2=data_1m.iloc[q1:q2,:]
            data_1m_q3=data_1m.iloc[q2:q3,:]
            data_1m_q4=data_1m.iloc[q3:q4,:]
            data_1m_q5=data_1m.iloc[q4:len(data_1m),:]
            avg_ret_q1_w = np.ma.average(np.ma.array(data_1m_q1.loc[:,"PCT_CHANGE"].values, mask=data_1m_q1.loc[:,"PCT_CHANGE"].isnull().values), weights=data_1m_q1.loc[:,"TOTAL_MV"])
            avg_ret_q2_w = np.ma.average(np.ma.array(data_1m_q2.loc[:,"PCT_CHANGE"].values, mask=data_1m_q2.loc[:,"PCT_CHANGE"].isnull().values), weights=data_1m_q2.loc[:,"TOTAL_MV"])            
            avg_ret_q3_w = np.ma.average(np.ma.array(data_1m_q3.loc[:,"PCT_CHANGE"].values, mask=data_1m_q3.loc[:,"PCT_CHANGE"].isnull().values), weights=data_1m_q3.loc[:,"TOTAL_MV"])            
            avg_ret_q4_w = np.ma.average(np.ma.array(data_1m_q4.loc[:,"PCT_CHANGE"].values, mask=data_1m_q4.loc[:,"PCT_CHANGE"].isnull().values), weights=data_1m_q4.loc[:,"TOTAL_MV"])            
            avg_ret_q5_w = np.ma.average(np.ma.array(data_1m_q5.loc[:,"PCT_CHANGE"].values, mask=data_1m_q5.loc[:,"PCT_CHANGE"].isnull().values), weights=data_1m_q5.loc[:,"TOTAL_MV"])            
            avg_ret_q1_w_list.append(avg_ret_q1_w)
            avg_ret_q2_w_list.append(avg_ret_q2_w)
            avg_ret_q3_w_list.append(avg_ret_q3_w)
            avg_ret_q4_w_list.append(avg_ret_q4_w)
            avg_ret_q5_w_list.append(avg_ret_q5_w)
        
        avg_ret_q5minusq1_w_list=list(np.array(avg_ret_q5_w_list)-np.array(avg_ret_q1_w_list))
        avg_ret_q1_w_list=list(pd.Series(avg_ret_q1_w_list).dropna())
        avg_ret_q2_w_list=list(pd.Series(avg_ret_q2_w_list).dropna())
        avg_ret_q3_w_list=list(pd.Series(avg_ret_q3_w_list).dropna())
        avg_ret_q4_w_list=list(pd.Series(avg_ret_q4_w_list).dropna())
        avg_ret_q5_w_list=list(pd.Series(avg_ret_q5_w_list).dropna())
        avg_ret_q5minusq1_w_list=list(pd.Series(avg_ret_q5minusq1_w_list).dropna())
        print(np.array(avg_ret_q1_w_list).mean(),np.array(avg_ret_q2_w_list).mean(),np.array(avg_ret_q3_w_list).mean(),np.array(avg_ret_q4_w_list).mean(),np.array(avg_ret_q5_w_list).mean(),np.array(avg_ret_q5minusq1_w_list).mean())
        
        t_q1_w=stats.ttest_1samp(avg_ret_q1_w_list,0)
        t_q2_w=stats.ttest_1samp(avg_ret_q2_w_list,0)
        t_q3_w=stats.ttest_1samp(avg_ret_q3_w_list,0)
        t_q4_w=stats.ttest_1samp(avg_ret_q4_w_list,0)
        t_q5_w=stats.ttest_1samp(avg_ret_q5_w_list,0)
        t_q5minusq1_w=stats.ttest_1samp(avg_ret_q5minusq1_w_list,0)
        print(t_q1_w,t_q2_w,t_q3_w,t_q4_w,t_q5_w,t_q5minusq1_w)


def fm_summary(reg_result_list):
    #输入: FamaMacBeth的model.fit的结果组成的列表
    #输出: 学术格式的结果表格
    reg_result_df_rows = []
    reg_result_processed = []
    for reg_result in reg_result_list:
        #系数保留四位并添加*
        reg_result_params = [ str(Decimal("%.4f" % float(i))) if np.isnan(i)==False else '' for i in reg_result.params ]
        reg_result_params = [ reg_result_params[i]+CountingStars(reg_result.pvalues[i]) for i in range(len(reg_result_params)) ] 
        reg_result_params = pd.Series(reg_result_params, name=reg_result.params.name, index=reg_result.params.index)
        #t值保留两位并添加( )
        reg_result_tstats = ['(' + str(Decimal("%.2f" % float(i))) +')' if np.isnan(i)==False else '' for i in reg_result.tstats ]
        reg_result_tstats = pd.Series(reg_result_tstats, name=reg_result.tstats.name, index=reg_result.tstats.index)
        #统计量, 及保留小数位
        rsquared = "%.4f" % float(reg_result.rsquared_overall)
        obs = reg_result.nobs
        meanobs = "%.2f" % float(reg_result.time_info['mean'])
        #整理结果
        reg_result_processed.append([reg_result_params, reg_result_tstats, rsquared, obs, meanobs]) 
        #保存变量名用来生成表格的行
        reg_result_df_rows = reg_result_df_rows + list(reg_result_params.index)
    
    reg_result_df_rows = list(set(reg_result_df_rows))
    reg_result_df_rows = [[i,""] for i in reg_result_df_rows]
    reg_result_df_rows = sum(reg_result_df_rows, [])
    reg_result_df_rows = reg_result_df_rows + ['Avg. R Square', 'Total Obs.', 'Avg. Obs.']
    reg_result_table = pd.DataFrame(index=reg_result_df_rows)
    
    for i in range(len(reg_result_processed)):
        for entry in reg_result_processed[i][0].index:
               reg_result_table.loc[entry, i] = reg_result_processed[i][0].loc[entry]
               reg_result_table.iloc[reg_result_table.index.get_loc(entry)+1, i] = reg_result_processed[i][1].loc[entry]
        reg_result_table.loc['Avg. R Square', i] = reg_result_processed[i][2]
        reg_result_table.loc['Total Obs.', i] = reg_result_processed[i][3]
        reg_result_table.loc['Avg. Obs.', i] = reg_result_processed[i][4]
    reg_result_table.replace(np.nan, '', inplace=True)
    reg_result_table.columns = [i+1 for i in range(len(reg_result_table.columns))]    #调整列名为1,2,...    
    return reg_result_table


def Fama_MacBeth_reg(df,y_name,x_name_list):
    #输入: dataframe, 其中'TRADE_DATE'为YYYYMMMDD格式的日期, 'TS_CODE'为股票代码;
    #      被解释变量名y_name, 解释变量名列表x_name_list
    #输出: Fama-MacBeth回归结果
    df_=df.copy()
    df_["TRADE_DATE"] = pd.to_datetime(df_["TRADE_DATE"])
    df_.set_index(["TS_CODE","TRADE_DATE"],inplace=True)  #FamaMacBeth函数格式要求multi-index: 股票代码+datetime格式日期
    formula = y_name + '~' + "+".join(x_name_list) + "+1" 
    model = FamaMacBeth.from_formula(formula, data=df_)
    reg_result = model.fit(cov_type= 'kernel',debiased = False, bandwidth = 4) #cov_type设定表示输出Newey-West调整后的结果; bandwidth为Newey-West滞后阶数, 选取方式为lag = 4(T/100) ^ (2/9);
    return reg_result


def value_strategy_tests_monthly(df, factor_name_list, input_list, input_ascending_list=0, test_freq='monthly', standardization_type = 'default'):
    #本函数是该文件中所有函数的总结, 外部调用
    #factor_name_list格式: [factor1, factor2]
    #input_list格式: [[A,B,C],[D,E]]
    #input_ascending格式: [[True,True,False],[True,True]]
    #test_freq是进行检验的频率, 输入数据的频率只允许daily    
    data0 = df.copy()
    #删除ST股
    data0 = delete_FinanceCorps(data0)
    #删除金融业公司
    data0 = delete_ST(data0)
    
    if test_freq == 'monthly':
        data0 = degenerate_dailydata_to_monthlydata(data0, data_type='panel')  
    start_date = np.nanmin(data0["TRADE_DATE"])
    end_date = np.nanmax(data0["TRADE_DATE"])
    
    for factor_num in range(len(factor_name_list)):
        if input_ascending_list == 0: 
            input_ascending = [True for i in range(len(input_list[factor_num]))]
        else: 
            input_ascending = input_ascending_list[factor_num]
        #对一个factor的各组成部分标准化
        if standardization_type == 'default':
            data0 = Z_standardization(data0, input_name_list=input_list[factor_num], input_ascending=input_ascending, output_name=factor_name_list[factor_num])
        elif standardization_type == 'rank':
            data0 = Z_standardization_of_rank(data0, input_name_list=input_list[factor_num], input_ascending=input_ascending, output_name=factor_name_list[factor_num])
        #使用t-test检验该factor的收益显著性
        print("等额加权下对因子{factor_name}的t检验结果:".format(factor_name=factor_name_list[factor_num]))
        test = univariate_test_for_returns(data0, var_name=factor_name_list[factor_num], mv_weighted=False, freq=test_freq, start_date=start_date, end_date=end_date)
        
    #对所有factor的总标准化和t-test
    if standardization_type == 'default':
        data0 = Z_standardization(data0, input_name_list=factor_name_list, input_ascending=[True for i in range(len(input_list))], output_name='Overall_Normalization_Result')
    elif standardization_type == 'rank':
        data0 = Z_standardization_of_rank(data0, input_name_list=factor_name_list, input_ascending=[True for i in range(len(input_list))], output_name='Overall_Normalization_Result')
    print("等额加权下对全部因子标准化后的t检验结果:")
    test = univariate_test_for_returns(data0, var_name='Overall_Normalization_Result', mv_weighted=False, freq=test_freq, start_date=start_date, end_date=end_date)
    
    #Fama-MecBeth检验准备,往dataframe中导入相关数据
    data_close = local_source.get_quotations_daily(cols='TRADE_DATE, TS_CODE, CLOSE', condition="TRADE_DATE>=" + str(start_date) + " and TRADE_DATE<=" + str(end_date))
    data_close["TRADE_DATE"]=data_close["TRADE_DATE"].astype(int)
    if test_freq == 'monthly':    
        data_close = degenerate_dailydata_to_monthlydata(data_close, data_type='panel')
    data0 = pd.merge(data_close, data0, on=["TS_CODE","TRADE_DATE"], how='left')
    data_return = calculate_pctchange_bystock(data_close)
    data0 = pd.merge(data0, data_return, on=["TS_CODE","TRADE_DATE"], how='left')
    
    #进行Fama-MacBeth检验
    fm_result_list=[]
    #test = value_strategy_funcs.Fama_MacBeth_reg(data0,'PCT_CHANGE', ['Overall_Normalization_Result']) #这个会报错, 原因未知
    #fm_result_list.append(test)
    for factor_name in factor_name_list:
        test = Fama_MacBeth_reg(data0,'PCT_CHANGE', [factor_name])
        fm_result_list.append(test)
    test = Fama_MacBeth_reg(data0,'PCT_CHANGE', factor_name_list)
    fm_result_list.append(test)
    print(fm_summary(fm_result_list))

    #输出根据这种策略选出的一定比例的股票
    choice_matrix = stock_selection_by_var(data0, var_name='Overall_Normalization_Result', pct=0.2, Type='best', start_date=start_date, end_date=end_date)
    return choice_matrix




#a=pd.Series([1,2,np.nan,5])
#b=pd.Series([1,2,3,4])
#c=np.ma.average(np.ma.array(a.values, mask=a.isnull().values), weights=b)







