# -*- coding: utf-8 -*-

import numpy as np
import pandas as pd
from data_source import local_source
from analysis import alphas_base
from analysis import value_strategy_funcs


def EmbeddedFactors_allstocks(factor_name, start_date, end_date, monthly_data_adjustment="ann_date_basis"):  #输入"#"+内置因子名可以引用对应的因子。
    #预设项目的格式为"#"+名称
    balance_sheets_factors = [i for i in local_source.get_balance_sheets(condition='TS_CODE="?"').columns][7:]
    income_statements_factors = [i for i in local_source.get_income_statements(condition='TS_CODE="?"').columns][7:]
    statement_of_cash_flows_factors = [i for i in local_source.get_cash_flows(condition='TS_CODE="?"').columns][7:]
    financial_indicators_factors = [i for i in local_source.get_financial_indicators(condition='TS_CODE="?"').columns][3:]
    income_forecasts_factors = [i for i in local_source.get_income_forecasts(condition='TS_CODE="?"').columns][4:]
    
    if factor_name[1:] in balance_sheets_factors:
        print("您输入了一个资产负债表中的项目。")
        df = local_source.get_balance_sheets(cols='TS_CODE, ANN_DATE, END_DATE, '+factor_name[1:], condition="REPORT_TYPE=1" + " and END_DATE>=" + str(start_date) + " and END_DATE<=" + str(end_date))        
        df = df.applymap(lambda x: np.nan if x=="NULL" else x)
        if monthly_data_adjustment == "ann_date_basis":
            df = value_strategy_funcs.fill_financial_data_to_daily_ann_date_basis(df) 
            #将财务数据向后填充至每天； 即使准备用monthly的数据, 也需要用daily填充。
        else:
            df = value_strategy_funcs.fill_financial_data_to_daily_end_date_basis(df, month=monthly_data_adjustment)
        df.drop(columns=['END_DATE'], inplace=True)
        return df
        
    elif factor_name[1:] in income_statements_factors:
        print("您输入了一个损益表中的项目。")
        df = local_source.get_income_statements(cols='TS_CODE, ANN_DATE, END_DATE, '+factor_name[1:], condition='REPORT_TYPE=1' + " and END_DATE>=" + str(start_date) + " and END_DATE<=" + str(end_date))        
        df = df.applymap(lambda x: np.nan if x=="NULL" else x)
        if monthly_data_adjustment == "ann_date_basis":
            df = value_strategy_funcs.fill_financial_data_to_daily_ann_date_basis(df) 
        else:
            df = value_strategy_funcs.fill_financial_data_to_daily_end_date_basis(df, month=monthly_data_adjustment)
        df.drop(columns=['END_DATE'], inplace=True)
        return df
    
    elif factor_name[1:] in statement_of_cash_flows_factors:
        print("您输入了一个现金流量表中的项目。")
        df = local_source.get_cash_flows(cols='TS_CODE, ANN_DATE, END_DATE, '+factor_name[1:], condition='REPORT_TYPE=1' + " and END_DATE>=" + str(start_date) + " and END_DATE<=" + str(end_date))        
        df = df.applymap(lambda x: np.nan if x=="NULL" else x)
        if monthly_data_adjustment == "ann_date_basis":
            df = value_strategy_funcs.fill_financial_data_to_daily_ann_date_basis(df) 
        else:
            df = value_strategy_funcs.fill_financial_data_to_daily_end_date_basis(df, month=monthly_data_adjustment)
        df.drop(columns=['END_DATE'], inplace=True)
        return df
    
    elif factor_name[1:] in financial_indicators_factors:
        print("您输入了一个财务指标。")
        df = local_source.get_financial_indicators(cols='TS_CODE, ANN_DATE, END_DATE, '+factor_name[1:], condition="END_DATE>=" + str(start_date) + " and END_DATE<=" + str(end_date))        
        df = df.applymap(lambda x: np.nan if x=="NULL" else x)
        if monthly_data_adjustment == "ann_date_basis":
            df = value_strategy_funcs.fill_financial_data_to_daily_ann_date_basis(df) 
        else:
            df = value_strategy_funcs.fill_financial_data_to_daily_end_date_basis(df, month=monthly_data_adjustment)
        df.drop(columns=['END_DATE'], inplace=True)
        return df

    elif factor_name[1:] in income_forecasts_factors:
        print("您输入了一个企业预测的财务指标。")        
        df = local_source.get_income_forecasts(cols='TS_CODE, ANN_DATE, END_DATE, '+factor_name[1:], condition="END_DATE>=" + str(start_date) + " and END_DATE<=" + str(end_date))              
        df = df.applymap(lambda x: np.nan if x=="NULL" else x)
        if monthly_data_adjustment == "ann_date_basis":
            df = value_strategy_funcs.fill_financial_data_to_daily_ann_date_basis(df) 
        else:
            df = value_strategy_funcs.fill_financial_data_to_daily_end_date_basis(df, month=monthly_data_adjustment)
        df.drop(columns=['END_DATE'], inplace=True)
        return df
        
    elif factor_name[0:6]=='#Alpha':
        print("您输入了一个Alpha101中的因子。")
        df = alphas_base.get_Alpha101_allstocks(alpha_name=factor_name[1:], start_date=start_date, end_date=end_date)
        return df
    
        
    elif factor_name[0:10]=='#GTJAalpha':
        print("您输入了一个国泰君安Alpha191中的因子。")
        df = alphas_base.get_GTJAalphas_allstocks(alpha_name=factor_name[1:], start_date=start_date, end_date=end_date)
        return df        
    
    #未来在这里加入Myalphas的判定...
    else: 
        print("未从预设因子中找到输入的因子！")
        return 0



def add_factor(df_added, df=0): 
    if type(df)==int:
        return df_added
    else:
        return pd.merge(df, df_added, on=["TS_CODE","TRADE_DATE"], how='left')


