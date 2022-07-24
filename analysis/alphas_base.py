import os
import numpy as np
import pandas as pd
from numpy import abs
from numpy import log
from numpy import sign
from scipy.stats import rankdata
import scipy as sp
import statsmodels.api as sm
from data_source import local_source
from tqdm import tqdm as pb


# region Auxiliary functions
def ts_sum(df, window=10):
    """
    Wrapper function to estimate rolling sum.
    :param df: a pandas DataFrame.
    :param window: the rolling window.
    :return: a pandas DataFrame with the time-series sum over the past 'window' days.
    """
    
    return df.rolling(window).sum()

def sma(df, window=10): #simple moving average
    """
    Wrapper function to estimate SMA.
    :param df: a pandas DataFrame.
    :param window: the rolling window.
    :return: a pandas DataFrame with the time-series SMA over the past 'window' days.
    """
    return df.rolling(window).mean()

def ema(df, n, m): #exponential moving average
    """
    Wrapper function to estimate EMA.
    :param df: a pandas DataFrame.
    :return: ema_{t}=(m/n)*a_{t}+((n-m)/n)*ema_{t-1}
    """   
    result = df.copy()
    for i in range(1,len(df)):
        result.iloc[i] = (m*df.iloc[i-1] + (n-m)*result.iloc[i-1]) / n
        if result.iloc[i] != result.iloc[i]:
            result.iloc[i] = 0
    return result

def wma(df, n):
    """
    Wrapper function to estimate WMA.
    :param df: a pandas DataFrame.
    :return: wma_{t}=0.9*a_{t}+1.8*a_{t-1}+...+0.9*n*a_{t-n+1}
    """   
    weights = pd.Series(0.9*np.flipud(np.arange(1,n+1)))
    result = pd.Series(np.nan, index=df.index)
    for i in range(n-1,len(df)):
        result.iloc[i]= sum(df[i-n+1:i+1].reset_index(drop=True)*weights.reset_index(drop=True))
    return result

def stddev(df, window=10):
    """
    Wrapper function to estimate rolling standard deviation.
    :param df: a pandas DataFrame.
    :param window: the rolling window.
    :return: a pandas DataFrame with the time-series min over the past 'window' days.
    """
    return df.rolling(window).std()

def correlation(x, y, window=10):
    """
    Wrapper function to estimate rolling corelations.
    :param df: a pandas DataFrame.
    :param window: the rolling window.
    :return: a pandas DataFrame with the time-series min over the past 'window' days.
    """
    return x.rolling(window).corr(y)

def covariance(x, y, window=10):
    """
    Wrapper function to estimate rolling covariance.
    :param df: a pandas DataFrame.
    :param window: the rolling window.
    :return: a pandas DataFrame with the time-series min over the past 'window' days.
    """
    return x.rolling(window).cov(y)

def rank(df):
    """
    Wrapper function for ranking.
    :param df: a pandas DataFrame.
    :return: a pandas DataFrame with rank along columns.
    """
    #return df.rank(axis=1, pct=True)
    return df.rank(pct=True)

def rolling_rank(df, window=1):
    """
    Wrapper function to sort a vector by rolling.
    :param df: a pandas DataFrame.
    :param window: the rolling window.
    :return: a pandas Series of the maximum value over past window days.
    """
    return df.rolling(window).apply(max)

def ts_rank_(na):
    """
    Auxiliary function to be used in pd.rolling_apply
    :param na: numpy array.
    :return: a pandas Series of the rank of the values in the array.
    """
    return pd.Series(rankdata(na))

def ts_rank_last(na):
    """
    Auxiliary function to be used in pd.rolling_apply
    :param na: numpy array.
    :return: The rank of the last value in the array.
    """
    return rankdata(na)[-1]

def ts_rank(df, window=10):
    """
    Wrapper function to estimate rolling rank.
    :param df: a pandas DataFrame.
    :param window: the rolling window.
    :return: a pandas DataFrame with the time-series rank over the past window days.
    """
    return df.rolling(window).apply(ts_rank_last)

def product(na):
    """
    Auxiliary function to be used in pd.rolling_apply
    :param na: numpy array.
    :return: The product of the values in the array.
    """
    return np.prod(na)

def rolling_product(df, window=10):
    """
    Wrapper function to estimate rolling product.
    :param df: a pandas DataFrame.
    :param window: the rolling window.
    :return: a pandas DataFrame with the time-series product over the past 'window' days.
    """
    return df.rolling(window).apply(product)

def ts_min(df, window=10):
    """
    Wrapper function to estimate rolling min.
    :param df: a pandas DataFrame.
    :param window: the rolling window.
    :return: a pandas DataFrame with the time-series min over the past 'window' days.
    """
    return df.rolling(window).min()

def ts_max(df, window=10):
    """
    Wrapper function to estimate rolling min.
    :param df: a pandas DataFrame.
    :param window: the rolling window.
    :return: a pandas DataFrame with the time-series max over the past 'window' days.
    """
    return df.rolling(window).max()

def delta(df, period=1):
    """
    Wrapper function to estimate difference.
    :param df: a pandas DataFrame.
    :param period: the difference grade.
    :return: a pandas DataFrame with today’s value minus the value 'period' days ago.
    """
    return df.diff(period)

def delay(df, period=1):
    """
    Wrapper function to estimate lag.
    :param df: a pandas DataFrame.
    :param period: the lag grade.
    :return: a pandas DataFrame with lagged time series
    """
    return df.shift(period)

def scale(df, k=1):
    """
    Scaling time serie.
    :param df: a pandas DataFrame.
    :param k: scaling factor.
    :return: a pandas DataFrame rescaled df such that sum(abs(df)) = k
    """
    return df.mul(k).div(np.abs(df).sum())

def ts_argmax(df, window=10):
    """
    Wrapper function to estimate which day ts_max(df, window) occurred on
    :param df: a pandas DataFrame.
    :param window: the rolling window.
    :return: well.. that :)
    """
    return df.rolling(window).apply(np.argmax) + 1 

def ts_argmin(df, window=10):
    """
    Wrapper function to estimate which day ts_min(df, window) occurred on
    :param df: a pandas DataFrame.
    :param window: the rolling window.
    :return: well.. that :)
    """
    return df.rolling(window).apply(np.argmin) + 1

def decay_linear(df, period=10):
    """
    Linear weighted moving average implementation.
    :param df: a pandas DataFrame.
    :param period: the LWMA period
    :return: a pandas DataFrame with the LWMA.
    """
    try:
        df = df.to_frame()  #Series is not supported for the calculations below.
    except:
        pass
    # Clean data
    if df.isnull().values.any():
        df.fillna(method='ffill', inplace=True)
        df.fillna(method='bfill', inplace=True)
        df.fillna(value=0, inplace=True)
    na_lwma = np.zeros_like(df)
    na_lwma[:period, :] = df.iloc[:period, :] 
    na_series = df.values

    divisor = period * (period + 1) / 2
    y = (np.arange(period) + 1) * 1.0 / divisor
    # Estimate the actual lwma with the actual close.
    # The backtest engine should assure to be snooping bias free.
    for row in range(period - 1, df.shape[0]):
        x = na_series[row - period + 1: row + 1, :]
        na_lwma[row, :] = (np.dot(x.T, y))
    return pd.DataFrame(na_lwma, index=df.index).iloc[:,0]  

def highday(df, n): #计算df前n期时间序列中最大值距离当前时点的间隔
    result = pd.Series(np.nan, index=df.index)
    for i in range(n,len(df)):
        result.iloc[i]= i - df[i-n:i].idxmax()
    return result

def lowday(df, n): #计算df前n期时间序列中最小值距离当前时点的间隔
    result = pd.Series(np.nan, index=df.index)
    for i in range(n,len(df)):
        result.iloc[i]= i - df[i-n:i].idxmin()
    return result    



def daily_panel_csv_initializer(csv_name):  #not used now
    if os.path.exists(csv_name)==False:
        stock_list=local_source.get_stock_list(cols='TS_CODE,INDUSTRY')
        date_list=local_source.get_indices_daily(cols='TRADE_DATE',condition='INDEX_CODE = "000001.SH"')
        dataset=0
        for date in date_list["TRADE_DATE"]:
            stock_list[date]=stock_list["INDUSTRY"]
        stock_list.drop("INDUSTRY",axis=1,inplace=True)
        stock_list.set_index("TS_CODE", inplace=True)
        dataset = pd.DataFrame(stock_list.stack())
        dataset.reset_index(inplace=True)
        dataset.columns=["TS_CODE","TRADE_DATE","INDUSTRY"]
        dataset.to_csv(csv_name,encoding='utf-8-sig',index=False)
    else:
        dataset=pd.read_csv(csv_name)
    return dataset



def IndustryAverage_vwap():          
    stock_list=local_source.get_stock_list(cols='TS_CODE,INDUSTRY').set_index("TS_CODE")
    industry_list=stock_list["INDUSTRY"].drop_duplicates()
    date_list=local_source.get_indices_daily(cols='TRADE_DATE',condition='INDEX_CODE = "000001.SH"')["TRADE_DATE"].astype(int)
    
    #check for building/updating/reading dataset
    try:
        result_industryaveraged_df = pd.read_csv("analysis/IndustryAverage_Data_vwap.csv")
        result_industryaveraged_df["TRADE_DATE"] = result_industryaveraged_df["TRADE_DATE"].astype(int)        
        result_industryaveraged_df.set_index("TRADE_DATE",inplace=True)
        date_list_existed = pd.Series(result_industryaveraged_df.index)
        date_list_update = date_list[~date_list.isin(date_list_existed)]
        if len(date_list_update)==0:
            print("The corresponding industry average vwap data needs not to be updated.")
            return result_industryaveraged_df
        else:
            print("The corresponding industry average vwap data needs to be updated.")
            first_date_update = date_list_update.iloc[0]
    except:
        print("The corresponding industry average vwap data is missing.")
        result_industryaveraged_df=pd.DataFrame(index=date_list,columns=industry_list)
        date_list_update = date_list
        first_date_update=0
    
    #building/updating dataset
    result_unaveraged_industry=0 
    for industry in pb(industry_list, desc='Please wait', colour='#ffffff'):
        stock_list_industry=stock_list[stock_list["INDUSTRY"]==industry]
        #calculating unindentralized data
        for ts_code in stock_list_industry.index:
            quotations_daily_chosen=local_source.get_quotations_daily(cols='TRADE_DATE,TS_CODE,OPEN,CLOSE,LOW,HIGH,VOL,CHANGE,AMOUNT',condition='TS_CODE = '+'"'+ts_code+'"').sort_values(by="TRADE_DATE", ascending=True) 
            quotations_daily_chosen["TRADE_DATE"]=quotations_daily_chosen["TRADE_DATE"].astype(int)
            quotations_daily_chosen=quotations_daily_chosen.applymap(lambda x: np.nan if x=="NULL" else x)
            
            try:    #valid only in updating
                index_first_date_needed = date_list_existed[date_list_existed.values == first_date_update].index[0]
                first_date_needed = date_list_existed.loc[index_first_date_needed]
                quotations_daily_chosen = quotations_daily_chosen[quotations_daily_chosen["TRADE_DATE"]>=first_date_needed]
            except:
                pass
 
            VWAP = (quotations_daily_chosen['AMOUNT']*1000)/(quotations_daily_chosen['VOL']*100+1) 
            result_unaveraged_piece = VWAP
            
            result_unaveraged_piece.rename("VWAP_UNAVERAGED",inplace=True)
            result_unaveraged_piece = pd.DataFrame(result_unaveraged_piece)
            result_unaveraged_piece.insert(loc=0,column='INDUSTRY',value=industry)
            result_unaveraged_piece.insert(loc=0,column='TRADE_DATE',value=quotations_daily_chosen["TRADE_DATE"])
            result_unaveraged_piece.insert(loc=0,column='TS_CODE',value=ts_code)
            
            result_unaveraged_piece = result_unaveraged_piece[result_unaveraged_piece["TRADE_DATE"]>=first_date_update] #to lower the memory needed
            
            if type(result_unaveraged_industry)==int:
                result_unaveraged_industry=result_unaveraged_piece
            else:
                result_unaveraged_industry=pd.concat([result_unaveraged_industry,result_unaveraged_piece],axis=0)    
        
        #indentralizing data
        for date in date_list_update:
            try:   #to prevent the case that the stock is suspended, so that there's no data for the stock at some dates
                result_piece=result_unaveraged_industry[result_unaveraged_industry["TRADE_DATE"]==date]              
                value=result_piece["VWAP_UNAVERAGED"].mean()
                result_industryaveraged_df.loc[date,industry]=value
            except:
                pass

        result_unaveraged_industry=0
        
    result_industryaveraged_df.to_csv("analysis/IndustryAverage_Data_vwap.csv",encoding='utf-8-sig')           
    return result_industryaveraged_df

def IndustryAverage_close():          
    stock_list=local_source.get_stock_list(cols='TS_CODE,INDUSTRY').set_index("TS_CODE")
    industry_list=stock_list["INDUSTRY"].drop_duplicates()
    date_list=local_source.get_indices_daily(cols='TRADE_DATE',condition='INDEX_CODE = "000001.SH"')["TRADE_DATE"].astype(int)
    
    #check for building/updating/reading dataset
    try:
        result_industryaveraged_df = pd.read_csv("analysis/IndustryAverage_Data_close.csv")
        result_industryaveraged_df["TRADE_DATE"] = result_industryaveraged_df["TRADE_DATE"].astype(int)        
        result_industryaveraged_df.set_index("TRADE_DATE",inplace=True)
        date_list_existed = pd.Series(result_industryaveraged_df.index)
        date_list_update = date_list[~date_list.isin(date_list_existed)]
        if len(date_list_update)==0:
            print("The corresponding industry average close data needs not to be updated.")
            return result_industryaveraged_df
        else:
            print("The corresponding industry average close data needs to be updated.")
            first_date_update = date_list_update.iloc[0]
    except:
        print("The corresponding industry average close data is missing.")
        result_industryaveraged_df=pd.DataFrame(index=date_list,columns=industry_list)
        date_list_update = date_list
        first_date_update=0
    
    #building/updating dataset
    result_unaveraged_industry=0 
    for industry in pb(industry_list, desc='Please wait', colour='#ffffff'):
        stock_list_industry=stock_list[stock_list["INDUSTRY"]==industry]
        #calculating unindentralized data
        for ts_code in stock_list_industry.index:
            quotations_daily_chosen=local_source.get_quotations_daily(cols='TRADE_DATE,TS_CODE,OPEN,CLOSE,LOW,HIGH,VOL,CHANGE,AMOUNT',condition='TS_CODE = '+'"'+ts_code+'"').sort_values(by="TRADE_DATE", ascending=True) 
            quotations_daily_chosen["TRADE_DATE"]=quotations_daily_chosen["TRADE_DATE"].astype(int)
            quotations_daily_chosen=quotations_daily_chosen.applymap(lambda x: np.nan if x=="NULL" else x)
            
            try:    #valid only in updating
                index_first_date_needed = date_list_existed[date_list_existed.values == first_date_update].index[0]
                first_date_needed = date_list_existed.loc[index_first_date_needed]
                quotations_daily_chosen = quotations_daily_chosen[quotations_daily_chosen["TRADE_DATE"]>=first_date_needed]
            except:
                pass
   
            CLOSE = quotations_daily_chosen['CLOSE']  
            result_unaveraged_piece = CLOSE
            
            result_unaveraged_piece.rename("CLOSE_UNAVERAGED",inplace=True)
            result_unaveraged_piece = pd.DataFrame(result_unaveraged_piece)
            result_unaveraged_piece.insert(loc=0,column='INDUSTRY',value=industry)
            result_unaveraged_piece.insert(loc=0,column='TRADE_DATE',value=quotations_daily_chosen["TRADE_DATE"])
            result_unaveraged_piece.insert(loc=0,column='TS_CODE',value=ts_code)
            
            result_unaveraged_piece = result_unaveraged_piece[result_unaveraged_piece["TRADE_DATE"]>=first_date_update] #to lower the memory needed
            
            if type(result_unaveraged_industry)==int:
                result_unaveraged_industry=result_unaveraged_piece
            else:
                result_unaveraged_industry=pd.concat([result_unaveraged_industry,result_unaveraged_piece],axis=0)    
        
        #indentralizing data
        for date in date_list_update:
            try:   #to prevent the case that the stock is suspended, so that there's no data for the stock at some dates
                result_piece=result_unaveraged_industry[result_unaveraged_industry["TRADE_DATE"]==date]              
                value=result_piece["CLOSE_UNAVERAGED"].mean()
                result_industryaveraged_df.loc[date,industry]=value
            except:
                pass

        result_unaveraged_industry=0
        
    result_industryaveraged_df.to_csv("analysis/IndustryAverage_Data_close.csv",encoding='utf-8-sig')           
    return result_industryaveraged_df

def IndustryAverage_low():          
    stock_list=local_source.get_stock_list(cols='TS_CODE,INDUSTRY').set_index("TS_CODE")
    industry_list=stock_list["INDUSTRY"].drop_duplicates()
    date_list=local_source.get_indices_daily(cols='TRADE_DATE',condition='INDEX_CODE = "000001.SH"')["TRADE_DATE"].astype(int)
    
    #check for building/updating/reading dataset
    try:
        result_industryaveraged_df = pd.read_csv("analysis/IndustryAverage_Data_low.csv")
        result_industryaveraged_df["TRADE_DATE"] = result_industryaveraged_df["TRADE_DATE"].astype(int)        
        result_industryaveraged_df.set_index("TRADE_DATE",inplace=True)
        date_list_existed = pd.Series(result_industryaveraged_df.index)
        date_list_update = date_list[~date_list.isin(date_list_existed)]
        if len(date_list_update)==0:
            print("The corresponding industry average low data needs not to be updated.")
            return result_industryaveraged_df
        else:
            print("The corresponding industry average low data needs to be updated.")
            first_date_update = date_list_update.iloc[0]
    except:
        print("The corresponding industry average low data is missing.")
        result_industryaveraged_df=pd.DataFrame(index=date_list,columns=industry_list)
        date_list_update = date_list
        first_date_update=0
    
    #building/updating dataset
    result_unaveraged_industry=0 
    for industry in pb(industry_list, desc='Please wait', colour='#ffffff'):
        stock_list_industry=stock_list[stock_list["INDUSTRY"]==industry]
        #calculating unindentralized data
        for ts_code in stock_list_industry.index:
            quotations_daily_chosen=local_source.get_quotations_daily(cols='TRADE_DATE,TS_CODE,OPEN,CLOSE,LOW,HIGH,VOL,CHANGE,AMOUNT',condition='TS_CODE = '+'"'+ts_code+'"').sort_values(by="TRADE_DATE", ascending=True) 
            quotations_daily_chosen["TRADE_DATE"]=quotations_daily_chosen["TRADE_DATE"].astype(int)
            quotations_daily_chosen=quotations_daily_chosen.applymap(lambda x: np.nan if x=="NULL" else x)
            
            try:    #valid only in updating
                index_first_date_needed = date_list_existed[date_list_existed.values == first_date_update].index[0]
                first_date_needed = date_list_existed.loc[index_first_date_needed]
                quotations_daily_chosen = quotations_daily_chosen[quotations_daily_chosen["TRADE_DATE"]>=first_date_needed]
            except:
                pass
 
            LOW = quotations_daily_chosen['LOW']  
            result_unaveraged_piece = LOW
            
            result_unaveraged_piece.rename("LOW_UNAVERAGED",inplace=True)
            result_unaveraged_piece = pd.DataFrame(result_unaveraged_piece)
            result_unaveraged_piece.insert(loc=0,column='INDUSTRY',value=industry)
            result_unaveraged_piece.insert(loc=0,column='TRADE_DATE',value=quotations_daily_chosen["TRADE_DATE"])
            result_unaveraged_piece.insert(loc=0,column='TS_CODE',value=ts_code)
            
            result_unaveraged_piece = result_unaveraged_piece[result_unaveraged_piece["TRADE_DATE"]>=first_date_update] #to lower the memory needed
            
            if type(result_unaveraged_industry)==int:
                result_unaveraged_industry=result_unaveraged_piece
            else:
                result_unaveraged_industry=pd.concat([result_unaveraged_industry,result_unaveraged_piece],axis=0)    
        
        #indentralizing data
        for date in date_list_update:
            try:   #to prevent the case that the stock is suspended, so that there's no data for the stock at some dates
                result_piece=result_unaveraged_industry[result_unaveraged_industry["TRADE_DATE"]==date]              
                value=result_piece["LOW_UNAVERAGED"].mean()
                result_industryaveraged_df.loc[date,industry]=value
            except:
                pass

        result_unaveraged_industry=0
        
    result_industryaveraged_df.to_csv("analysis/IndustryAverage_Data_low.csv",encoding='utf-8-sig')           
    return result_industryaveraged_df

def IndustryAverage_volume():          
    stock_list=local_source.get_stock_list(cols='TS_CODE,INDUSTRY').set_index("TS_CODE")
    industry_list=stock_list["INDUSTRY"].drop_duplicates()
    date_list=local_source.get_indices_daily(cols='TRADE_DATE',condition='INDEX_CODE = "000001.SH"')["TRADE_DATE"].astype(int)
    
    #check for building/updating/reading dataset
    try:
        result_industryaveraged_df = pd.read_csv("analysis/IndustryAverage_Data_volume.csv")
        result_industryaveraged_df["TRADE_DATE"] = result_industryaveraged_df["TRADE_DATE"].astype(int)        
        result_industryaveraged_df.set_index("TRADE_DATE",inplace=True)
        date_list_existed = pd.Series(result_industryaveraged_df.index)
        date_list_update = date_list[~date_list.isin(date_list_existed)]
        if len(date_list_update)==0:
            print("The corresponding industry average volume data needs not to be updated.")
            return result_industryaveraged_df
        else:
            print("The corresponding industry average volume data needs to be updated.")
            first_date_update = date_list_update.iloc[0]
    except:
        print("The corresponding industry average volume data is missing.")
        result_industryaveraged_df=pd.DataFrame(index=date_list,columns=industry_list)
        date_list_update = date_list
        first_date_update=0
    
    #building/updating dataset
    result_unaveraged_industry=0 
    for industry in pb(industry_list, desc='Please wait', colour='#ffffff'):
        stock_list_industry=stock_list[stock_list["INDUSTRY"]==industry]
        #calculating unindentralized data
        for ts_code in stock_list_industry.index:
            quotations_daily_chosen=local_source.get_quotations_daily(cols='TRADE_DATE,TS_CODE,OPEN,CLOSE,LOW,HIGH,VOL,CHANGE,AMOUNT',condition='TS_CODE = '+'"'+ts_code+'"').sort_values(by="TRADE_DATE", ascending=True) 
            quotations_daily_chosen["TRADE_DATE"]=quotations_daily_chosen["TRADE_DATE"].astype(int)
            quotations_daily_chosen=quotations_daily_chosen.applymap(lambda x: np.nan if x=="NULL" else x)
            
            try:    #valid only in updating
                index_first_date_needed = date_list_existed[date_list_existed.values == first_date_update].index[0]
                first_date_needed = date_list_existed.loc[index_first_date_needed]
                quotations_daily_chosen = quotations_daily_chosen[quotations_daily_chosen["TRADE_DATE"]>=first_date_needed]
            except:
                pass
 
            VOLUME = quotations_daily_chosen['VOL']*100   
            result_unaveraged_piece = VOLUME
            
            result_unaveraged_piece.rename("VOLUME_UNAVERAGED",inplace=True)
            result_unaveraged_piece = pd.DataFrame(result_unaveraged_piece)
            result_unaveraged_piece.insert(loc=0,column='INDUSTRY',value=industry)
            result_unaveraged_piece.insert(loc=0,column='TRADE_DATE',value=quotations_daily_chosen["TRADE_DATE"])
            result_unaveraged_piece.insert(loc=0,column='TS_CODE',value=ts_code)
            
            result_unaveraged_piece = result_unaveraged_piece[result_unaveraged_piece["TRADE_DATE"]>=first_date_update] #to lower the memory needed
            
            if type(result_unaveraged_industry)==int:
                result_unaveraged_industry=result_unaveraged_piece
            else:
                result_unaveraged_industry=pd.concat([result_unaveraged_industry,result_unaveraged_piece],axis=0)    
        
        #indentralizing data
        for date in date_list_update:
            try:   #to prevent the case that the stock is suspended, so that there's no data for the stock at some dates
                result_piece=result_unaveraged_industry[result_unaveraged_industry["TRADE_DATE"]==date]              
                value=result_piece["VOLUME_UNAVERAGED"].mean()
                result_industryaveraged_df.loc[date,industry]=value
            except:
                pass

        result_unaveraged_industry=0
        
    result_industryaveraged_df.to_csv("analysis/IndustryAverage_Data_volume.csv",encoding='utf-8-sig')           
    return result_industryaveraged_df

def IndustryAverage_adv(num):         
    stock_list=local_source.get_stock_list(cols='TS_CODE,INDUSTRY').set_index("TS_CODE")
    industry_list=stock_list["INDUSTRY"].drop_duplicates()
    date_list=local_source.get_indices_daily(cols='TRADE_DATE',condition='INDEX_CODE = "000001.SH"')["TRADE_DATE"].astype(int)
    
    #check for building/updating/reading dataset
    try:
        result_industryaveraged_df = pd.read_csv("analysis/IndustryAverage_Data_adv{num}.csv".format(num=num))
        result_industryaveraged_df["TRADE_DATE"] = result_industryaveraged_df["TRADE_DATE"].astype(int)        
        result_industryaveraged_df.set_index("TRADE_DATE",inplace=True)
        date_list_existed = pd.Series(result_industryaveraged_df.index)
        date_list_update = date_list[~date_list.isin(date_list_existed)]
        if len(date_list_update)==0:
            print("The corresponding industry average adv{num} data needs not to be updated.".format(num=num))
            return result_industryaveraged_df
        else:
            print("The corresponding industry average adv{num} data needs to be updated.".format(num=num))
            first_date_update = date_list_update.iloc[0]
    except:
        print("The corresponding industry average adv{num} data is missing.".format(num=num))
        result_industryaveraged_df=pd.DataFrame(index=date_list,columns=industry_list)
        date_list_update = date_list
        first_date_update=0
    
    #building/updating dataset
    result_unaveraged_industry=0 
    for industry in pb(industry_list, desc='Please wait', colour='#ffffff'):
        stock_list_industry=stock_list[stock_list["INDUSTRY"]==industry]
        #calculating unindentralized data
        for ts_code in stock_list_industry.index:
            quotations_daily_chosen=local_source.get_quotations_daily(cols='TRADE_DATE,TS_CODE,OPEN,CLOSE,LOW,HIGH,VOL,CHANGE,AMOUNT',condition='TS_CODE = '+'"'+ts_code+'"').sort_values(by="TRADE_DATE", ascending=True) 
            quotations_daily_chosen["TRADE_DATE"]=quotations_daily_chosen["TRADE_DATE"].astype(int)
            quotations_daily_chosen=quotations_daily_chosen.applymap(lambda x: np.nan if x=="NULL" else x)
            
            try:    #valid only in updating
                index_first_date_needed = date_list_existed[date_list_existed.values == first_date_update].index[0]
                first_date_needed = date_list_existed.loc[index_first_date_needed]
                quotations_daily_chosen = quotations_daily_chosen[quotations_daily_chosen["TRADE_DATE"]>=first_date_needed]
            except:
                pass
 
            VOLUME = quotations_daily_chosen['VOL']*100  
            result_unaveraged_piece = sma(VOLUME, num)
            
            result_unaveraged_piece.rename("ADV{num}_UNAVERAGED".format(num=num),inplace=True)
            result_unaveraged_piece = pd.DataFrame(result_unaveraged_piece)
            result_unaveraged_piece.insert(loc=0,column='INDUSTRY',value=industry)
            result_unaveraged_piece.insert(loc=0,column='TRADE_DATE',value=quotations_daily_chosen["TRADE_DATE"])
            result_unaveraged_piece.insert(loc=0,column='TS_CODE',value=ts_code)
            
            result_unaveraged_piece = result_unaveraged_piece[result_unaveraged_piece["TRADE_DATE"]>=first_date_update] #to lower the memory needed
            
            if type(result_unaveraged_industry)==int:
                result_unaveraged_industry=result_unaveraged_piece
            else:
                result_unaveraged_industry=pd.concat([result_unaveraged_industry,result_unaveraged_piece],axis=0)    
        
        #indentralizing data
        for date in date_list_update:
            try:   #to prevent the case that the stock is suspended, so that there's no data for the stock at some dates
                result_piece=result_unaveraged_industry[result_unaveraged_industry["TRADE_DATE"]==date]              
                value=result_piece["ADV{num}_UNAVERAGED".format(num=num)].mean()
                result_industryaveraged_df.loc[date,industry]=value
            except:
                pass

        result_unaveraged_industry=0
        
    result_industryaveraged_df.to_csv("analysis/IndustryAverage_Data_adv{num}.csv".format(num=num),encoding='utf-8-sig')           
    return result_industryaveraged_df

#(correlation(delta(close, 1), delta(delay(close, 1), 1), 250) *delta(close, 1)) / close
def IndustryAverage_PreparationForAlpha048():          
    stock_list=local_source.get_stock_list(cols='TS_CODE,INDUSTRY').set_index("TS_CODE")
    industry_list=stock_list["INDUSTRY"].drop_duplicates()
    date_list=local_source.get_indices_daily(cols='TRADE_DATE',condition='INDEX_CODE = "000001.SH"')["TRADE_DATE"].astype(int)
    
    #check for building/updating/reading dataset
    try:
        result_industryaveraged_df = pd.read_csv("analysis/IndustryAverage_Data_PreparationForAlpha048.csv")
        result_industryaveraged_df["TRADE_DATE"] = result_industryaveraged_df["TRADE_DATE"].astype(int)        
        result_industryaveraged_df.set_index("TRADE_DATE",inplace=True)
        date_list_existed = pd.Series(result_industryaveraged_df.index)
        date_list_update = date_list[~date_list.isin(date_list_existed)]
        if len(date_list_update)==0:
            print("The corresponding industry average data for alpha048 needs not to be updated.")
            return result_industryaveraged_df
        else:
            print("The corresponding industry average data for alpha048 needs to be updated.")
            first_date_update = date_list_update.iloc[0]
    except:
        print("The corresponding industry average dataset for alpha048 is missing.")
        result_industryaveraged_df=pd.DataFrame(index=date_list,columns=industry_list)
        date_list_update = date_list
        first_date_update=0
    
    #building/updating dataset
    result_unaveraged_industry=0 
    for industry in pb(industry_list, desc='Please wait', colour='#ffffff'):
        stock_list_industry=stock_list[stock_list["INDUSTRY"]==industry]
        #calculating unindentralized data
        for ts_code in stock_list_industry.index:
            quotations_daily_chosen=local_source.get_quotations_daily(cols='TRADE_DATE,TS_CODE,OPEN,CLOSE,LOW,HIGH,VOL,CHANGE,AMOUNT',condition='TS_CODE = '+'"'+ts_code+'"').sort_values(by="TRADE_DATE", ascending=True) 
            quotations_daily_chosen["TRADE_DATE"]=quotations_daily_chosen["TRADE_DATE"].astype(int)
            quotations_daily_chosen=quotations_daily_chosen.applymap(lambda x: np.nan if x=="NULL" else x)
            
            try:    #valid only in updating
                index_first_date_needed = date_list_existed[date_list_existed.values == first_date_update].index[0]
                first_date_needed = date_list_existed.loc[index_first_date_needed]
                quotations_daily_chosen = quotations_daily_chosen[quotations_daily_chosen["TRADE_DATE"]>=first_date_needed]
            except:
                pass
 
            CLOSE = quotations_daily_chosen['CLOSE'] 
            result_unaveraged_piece = (correlation(delta(CLOSE, 1), delta(delay(CLOSE, 1), 1), 250) *delta(CLOSE, 1)) / CLOSE
            
            result_unaveraged_piece.rename("PREPARATION_FOR_ALPHA048_UNAVERAGED",inplace=True)
            result_unaveraged_piece = pd.DataFrame(result_unaveraged_piece)
            result_unaveraged_piece.insert(loc=0,column='INDUSTRY',value=industry)
            result_unaveraged_piece.insert(loc=0,column='TRADE_DATE',value=quotations_daily_chosen["TRADE_DATE"])
            result_unaveraged_piece.insert(loc=0,column='TS_CODE',value=ts_code)
            
            result_unaveraged_piece = result_unaveraged_piece[result_unaveraged_piece["TRADE_DATE"]>=first_date_update] #to lower the memory needed
            
            if type(result_unaveraged_industry)==int:
                result_unaveraged_industry=result_unaveraged_piece
            else:
                result_unaveraged_industry=pd.concat([result_unaveraged_industry,result_unaveraged_piece],axis=0)    
        
        #indentralizing data
        for date in date_list_update:
            try:   #to prevent the case that the stock is suspended, so that there's no data for the stock at some dates
                result_piece=result_unaveraged_industry[result_unaveraged_industry["TRADE_DATE"]==date]              
                value=result_piece["PREPARATION_FOR_ALPHA048_UNAVERAGED"].mean()
                result_industryaveraged_df.loc[date,industry]=value
            except:
                pass

        result_unaveraged_industry=0
        
    result_industryaveraged_df.to_csv("analysis/IndustryAverage_Data_PreparationForAlpha048.csv",encoding='utf-8-sig')           
    return result_industryaveraged_df

#(vwap * 0.728317) + (vwap *(1 - 0.728317))
def IndustryAverage_PreparationForAlpha059():          
    stock_list=local_source.get_stock_list(cols='TS_CODE,INDUSTRY').set_index("TS_CODE")
    industry_list=stock_list["INDUSTRY"].drop_duplicates()
    date_list=local_source.get_indices_daily(cols='TRADE_DATE',condition='INDEX_CODE = "000001.SH"')["TRADE_DATE"].astype(int)
    
    #check for building/updating/reading dataset
    try:
        result_industryaveraged_df = pd.read_csv("analysis/IndustryAverage_Data_PreparationForAlpha059.csv")
        result_industryaveraged_df["TRADE_DATE"] = result_industryaveraged_df["TRADE_DATE"].astype(int)        
        result_industryaveraged_df.set_index("TRADE_DATE",inplace=True)
        date_list_existed = pd.Series(result_industryaveraged_df.index)
        date_list_update = date_list[~date_list.isin(date_list_existed)]
        if len(date_list_update)==0:
            print("The corresponding industry average data for alpha059 needs not to be updated.")
            return result_industryaveraged_df
        else:
            print("The corresponding industry average data for alpha059 needs to be updated.")
            first_date_update = date_list_update.iloc[0]
    except:
        print("The corresponding industry average dataset for alpha059 is missing.")
        result_industryaveraged_df=pd.DataFrame(index=date_list,columns=industry_list)
        date_list_update = date_list
        first_date_update=0
    
    #building/updating dataset
    result_unaveraged_industry=0 
    for industry in pb(industry_list, desc='Please wait', colour='#ffffff'):
        stock_list_industry=stock_list[stock_list["INDUSTRY"]==industry]
        #calculating unindentralized data
        for ts_code in stock_list_industry.index:
            quotations_daily_chosen=local_source.get_quotations_daily(cols='TRADE_DATE,TS_CODE,OPEN,CLOSE,LOW,HIGH,VOL,CHANGE,AMOUNT',condition='TS_CODE = '+'"'+ts_code+'"').sort_values(by="TRADE_DATE", ascending=True) 
            quotations_daily_chosen["TRADE_DATE"]=quotations_daily_chosen["TRADE_DATE"].astype(int)
            quotations_daily_chosen=quotations_daily_chosen.applymap(lambda x: np.nan if x=="NULL" else x)
            
            try:    #valid only in updating
                index_first_date_needed = date_list_existed[date_list_existed.values == first_date_update].index[0]
                first_date_needed = date_list_existed.loc[index_first_date_needed]
                quotations_daily_chosen = quotations_daily_chosen[quotations_daily_chosen["TRADE_DATE"]>=first_date_needed]
            except:
                pass
 
            VWAP = (quotations_daily_chosen['AMOUNT']*1000)/(quotations_daily_chosen['VOL']*100+1) 
            result_unaveraged_piece = (VWAP * 0.728317) + (VWAP *(1 - 0.728317))

            result_unaveraged_piece.rename("PREPARATION_FOR_ALPHA059_UNAVERAGED",inplace=True)
            result_unaveraged_piece = pd.DataFrame(result_unaveraged_piece)
            result_unaveraged_piece.insert(loc=0,column='INDUSTRY',value=industry)
            result_unaveraged_piece.insert(loc=0,column='TRADE_DATE',value=quotations_daily_chosen["TRADE_DATE"])
            result_unaveraged_piece.insert(loc=0,column='TS_CODE',value=ts_code)
            
            result_unaveraged_piece = result_unaveraged_piece[result_unaveraged_piece["TRADE_DATE"]>=first_date_update] #to lower the memory needed
            
            if type(result_unaveraged_industry)==int:
                result_unaveraged_industry=result_unaveraged_piece
            else:
                result_unaveraged_industry=pd.concat([result_unaveraged_industry,result_unaveraged_piece],axis=0)    
        
        #indentralizing data
        for date in date_list_update:
            try:   #to prevent the case that the stock is suspended, so that there's no data for the stock at some dates
                result_piece=result_unaveraged_industry[result_unaveraged_industry["TRADE_DATE"]==date]              
                value=result_piece["PREPARATION_FOR_ALPHA059_UNAVERAGED"].mean()
                result_industryaveraged_df.loc[date,industry]=value
            except:
                pass

        result_unaveraged_industry=0
        
    result_industryaveraged_df.to_csv("analysis/IndustryAverage_Data_PreparationForAlpha059.csv",encoding='utf-8-sig')           
    return result_industryaveraged_df

#(close * 0.60733) + (open * (1 - 0.60733))
def IndustryAverage_PreparationForAlpha079():          
    stock_list=local_source.get_stock_list(cols='TS_CODE,INDUSTRY').set_index("TS_CODE")
    industry_list=stock_list["INDUSTRY"].drop_duplicates()
    date_list=local_source.get_indices_daily(cols='TRADE_DATE',condition='INDEX_CODE = "000001.SH"')["TRADE_DATE"].astype(int)
    
    #check for building/updating/reading dataset
    try:
        result_industryaveraged_df = pd.read_csv("analysis/IndustryAverage_Data_PreparationForAlpha079.csv")
        result_industryaveraged_df["TRADE_DATE"] = result_industryaveraged_df["TRADE_DATE"].astype(int)        
        result_industryaveraged_df.set_index("TRADE_DATE",inplace=True)
        date_list_existed = pd.Series(result_industryaveraged_df.index)
        date_list_update = date_list[~date_list.isin(date_list_existed)]
        if len(date_list_update)==0:
            print("The corresponding industry average data for alpha079 needs not to be updated.")
            return result_industryaveraged_df
        else:
            print("The corresponding industry average data for alpha079 needs to be updated.")
            first_date_update = date_list_update.iloc[0]
    except:
        print("The corresponding industry average dataset for alpha079 is missing.")
        result_industryaveraged_df=pd.DataFrame(index=date_list,columns=industry_list)
        date_list_update = date_list
        first_date_update=0
    
    #building/updating dataset
    result_unaveraged_industry=0 
    for industry in pb(industry_list, desc='Please wait', colour='#ffffff'):
        stock_list_industry=stock_list[stock_list["INDUSTRY"]==industry]
        #calculating unindentralized data
        for ts_code in stock_list_industry.index:
            quotations_daily_chosen=local_source.get_quotations_daily(cols='TRADE_DATE,TS_CODE,OPEN,CLOSE,LOW,HIGH,VOL,CHANGE,AMOUNT',condition='TS_CODE = '+'"'+ts_code+'"').sort_values(by="TRADE_DATE", ascending=True) 
            quotations_daily_chosen["TRADE_DATE"]=quotations_daily_chosen["TRADE_DATE"].astype(int)
            quotations_daily_chosen=quotations_daily_chosen.applymap(lambda x: np.nan if x=="NULL" else x)
            
            try:    #valid only in updating
                index_first_date_needed = date_list_existed[date_list_existed.values == first_date_update].index[0]
                first_date_needed = date_list_existed.loc[index_first_date_needed]
                quotations_daily_chosen = quotations_daily_chosen[quotations_daily_chosen["TRADE_DATE"]>=first_date_needed]
            except:
                pass
 
            OPEN = quotations_daily_chosen['OPEN']
            CLOSE = quotations_daily_chosen['CLOSE']
            result_unaveraged_piece = (CLOSE * 0.60733) + (OPEN * (1 - 0.60733))

            result_unaveraged_piece.rename("PREPARATION_FOR_ALPHA079_UNAVERAGED",inplace=True)
            result_unaveraged_piece = pd.DataFrame(result_unaveraged_piece)
            result_unaveraged_piece.insert(loc=0,column='INDUSTRY',value=industry)
            result_unaveraged_piece.insert(loc=0,column='TRADE_DATE',value=quotations_daily_chosen["TRADE_DATE"])
            result_unaveraged_piece.insert(loc=0,column='TS_CODE',value=ts_code)
            
            result_unaveraged_piece = result_unaveraged_piece[result_unaveraged_piece["TRADE_DATE"]>=first_date_update] #to lower the memory needed
            
            if type(result_unaveraged_industry)==int:
                result_unaveraged_industry=result_unaveraged_piece
            else:
                result_unaveraged_industry=pd.concat([result_unaveraged_industry,result_unaveraged_piece],axis=0)    
        
        #indentralizing data
        for date in date_list_update:
            try:   #to prevent the case that the stock is suspended, so that there's no data for the stock at some dates
                result_piece=result_unaveraged_industry[result_unaveraged_industry["TRADE_DATE"]==date]              
                value=result_piece["PREPARATION_FOR_ALPHA079_UNAVERAGED"].mean()
                result_industryaveraged_df.loc[date,industry]=value
            except:
                pass

        result_unaveraged_industry=0
        
    result_industryaveraged_df.to_csv("analysis/IndustryAverage_Data_PreparationForAlpha079.csv",encoding='utf-8-sig')           
    return result_industryaveraged_df

#((open * 0.868128) + (high * (1 - 0.868128))
def IndustryAverage_PreparationForAlpha080():          
    stock_list=local_source.get_stock_list(cols='TS_CODE,INDUSTRY').set_index("TS_CODE")
    industry_list=stock_list["INDUSTRY"].drop_duplicates()
    date_list=local_source.get_indices_daily(cols='TRADE_DATE',condition='INDEX_CODE = "000001.SH"')["TRADE_DATE"].astype(int)
    
    #check for building/updating/reading dataset
    try:
        result_industryaveraged_df = pd.read_csv("analysis/IndustryAverage_Data_PreparationForAlpha080.csv")
        result_industryaveraged_df["TRADE_DATE"] = result_industryaveraged_df["TRADE_DATE"].astype(int)        
        result_industryaveraged_df.set_index("TRADE_DATE",inplace=True)
        date_list_existed = pd.Series(result_industryaveraged_df.index)
        date_list_update = date_list[~date_list.isin(date_list_existed)]
        if len(date_list_update)==0:
            print("The corresponding industry average data for alpha080 needs not to be updated.")
            return result_industryaveraged_df
        else:
            print("The corresponding industry average data for alpha080 needs to be updated.")
            first_date_update = date_list_update.iloc[0]
    except:
        print("The corresponding industry average dataset for alpha080 is missing.")
        result_industryaveraged_df=pd.DataFrame(index=date_list,columns=industry_list)
        date_list_update = date_list
        first_date_update=0
    
    #building/updating dataset
    result_unaveraged_industry=0 
    for industry in pb(industry_list, desc='Please wait', colour='#ffffff'):
        stock_list_industry=stock_list[stock_list["INDUSTRY"]==industry]
        #calculating unindentralized data
        for ts_code in stock_list_industry.index:
            quotations_daily_chosen=local_source.get_quotations_daily(cols='TRADE_DATE,TS_CODE,OPEN,CLOSE,LOW,HIGH,VOL,CHANGE,AMOUNT',condition='TS_CODE = '+'"'+ts_code+'"').sort_values(by="TRADE_DATE", ascending=True) 
            quotations_daily_chosen["TRADE_DATE"]=quotations_daily_chosen["TRADE_DATE"].astype(int)
            quotations_daily_chosen=quotations_daily_chosen.applymap(lambda x: np.nan if x=="NULL" else x)
            
            try:    #valid only in updating
                index_first_date_needed = date_list_existed[date_list_existed.values == first_date_update].index[0]
                first_date_needed = date_list_existed.loc[index_first_date_needed]
                quotations_daily_chosen = quotations_daily_chosen[quotations_daily_chosen["TRADE_DATE"]>=first_date_needed]
            except:
                pass
 
            OPEN = quotations_daily_chosen['OPEN']
            HIGH = quotations_daily_chosen['HIGH']
            result_unaveraged_piece = (OPEN * 0.868128) + (HIGH * (1 - 0.868128))

            result_unaveraged_piece.rename("PREPARATION_FOR_ALPHA080_UNAVERAGED",inplace=True)
            result_unaveraged_piece = pd.DataFrame(result_unaveraged_piece)
            result_unaveraged_piece.insert(loc=0,column='INDUSTRY',value=industry)
            result_unaveraged_piece.insert(loc=0,column='TRADE_DATE',value=quotations_daily_chosen["TRADE_DATE"])
            result_unaveraged_piece.insert(loc=0,column='TS_CODE',value=ts_code)
            
            result_unaveraged_piece = result_unaveraged_piece[result_unaveraged_piece["TRADE_DATE"]>=first_date_update] #to lower the memory needed
            
            if type(result_unaveraged_industry)==int:
                result_unaveraged_industry=result_unaveraged_piece
            else:
                result_unaveraged_industry=pd.concat([result_unaveraged_industry,result_unaveraged_piece],axis=0)    
        
        #indentralizing data
        for date in date_list_update:
            try:   #to prevent the case that the stock is suspended, so that there's no data for the stock at some dates
                result_piece=result_unaveraged_industry[result_unaveraged_industry["TRADE_DATE"]==date]              
                value=result_piece["PREPARATION_FOR_ALPHA080_UNAVERAGED"].mean()
                result_industryaveraged_df.loc[date,industry]=value
            except:
                pass

        result_unaveraged_industry=0
        
    result_industryaveraged_df.to_csv("analysis/IndustryAverage_Data_PreparationForAlpha080.csv",encoding='utf-8-sig')           
    return result_industryaveraged_df

#((low * 0.721001) + (vwap * (1 - 0.721001))
def IndustryAverage_PreparationForAlpha097():          
    stock_list=local_source.get_stock_list(cols='TS_CODE,INDUSTRY').set_index("TS_CODE")
    industry_list=stock_list["INDUSTRY"].drop_duplicates()
    date_list=local_source.get_indices_daily(cols='TRADE_DATE',condition='INDEX_CODE = "000001.SH"')["TRADE_DATE"].astype(int)
    
    #check for building/updating/reading dataset
    try:
        result_industryaveraged_df = pd.read_csv("analysis/IndustryAverage_Data_PreparationForAlpha097.csv")
        result_industryaveraged_df["TRADE_DATE"] = result_industryaveraged_df["TRADE_DATE"].astype(int)        
        result_industryaveraged_df.set_index("TRADE_DATE",inplace=True)
        date_list_existed = pd.Series(result_industryaveraged_df.index)
        date_list_update = date_list[~date_list.isin(date_list_existed)]
        if len(date_list_update)==0:
            print("The corresponding industry average data for alpha097 needs not to be updated.")
            return result_industryaveraged_df
        else:
            print("The corresponding industry average data for alpha097 needs to be updated.")
            first_date_update = date_list_update.iloc[0]
    except:
        print("The corresponding industry average dataset for alpha097 is missing.")
        result_industryaveraged_df=pd.DataFrame(index=date_list,columns=industry_list)
        date_list_update = date_list
        first_date_update=0
    
    #building/updating dataset
    result_unaveraged_industry=0 
    for industry in pb(industry_list, desc='Please wait', colour='#ffffff'):
        stock_list_industry=stock_list[stock_list["INDUSTRY"]==industry]
        #calculating unindentralized data
        for ts_code in stock_list_industry.index:
            quotations_daily_chosen=local_source.get_quotations_daily(cols='TRADE_DATE,TS_CODE,OPEN,CLOSE,LOW,HIGH,VOL,CHANGE,AMOUNT',condition='TS_CODE = '+'"'+ts_code+'"').sort_values(by="TRADE_DATE", ascending=True) 
            quotations_daily_chosen["TRADE_DATE"]=quotations_daily_chosen["TRADE_DATE"].astype(int)
            quotations_daily_chosen=quotations_daily_chosen.applymap(lambda x: np.nan if x=="NULL" else x)
            
            try:    #valid only in updating
                index_first_date_needed = date_list_existed[date_list_existed.values == first_date_update].index[0]
                first_date_needed = date_list_existed.loc[index_first_date_needed]
                quotations_daily_chosen = quotations_daily_chosen[quotations_daily_chosen["TRADE_DATE"]>=first_date_needed]
            except:
                pass
 
            LOW = quotations_daily_chosen['LOW']
            VWAP = (quotations_daily_chosen['AMOUNT']*1000)/(quotations_daily_chosen['VOL']*100+1) 
            result_unaveraged_piece = (LOW * 0.721001) + (VWAP * (1 - 0.721001))

            result_unaveraged_piece.rename("PREPARATION_FOR_ALPHA097_UNAVERAGED",inplace=True)
            result_unaveraged_piece = pd.DataFrame(result_unaveraged_piece)
            result_unaveraged_piece.insert(loc=0,column='INDUSTRY',value=industry)
            result_unaveraged_piece.insert(loc=0,column='TRADE_DATE',value=quotations_daily_chosen["TRADE_DATE"])
            result_unaveraged_piece.insert(loc=0,column='TS_CODE',value=ts_code)
            
            result_unaveraged_piece = result_unaveraged_piece[result_unaveraged_piece["TRADE_DATE"]>=first_date_update] #to lower the memory needed
            
            if type(result_unaveraged_industry)==int:
                result_unaveraged_industry=result_unaveraged_piece
            else:
                result_unaveraged_industry=pd.concat([result_unaveraged_industry,result_unaveraged_piece],axis=0)    
        
        #indentralizing data
        for date in date_list_update:
            try:   #to prevent the case that the stock is suspended, so that there's no data for the stock at some dates
                result_piece=result_unaveraged_industry[result_unaveraged_industry["TRADE_DATE"]==date]              
                value=result_piece["PREPARATION_FOR_ALPHA097_UNAVERAGED"].mean()
                result_industryaveraged_df.loc[date,industry]=value
            except:
                pass

        result_unaveraged_industry=0
        
    result_industryaveraged_df.to_csv("analysis/IndustryAverage_Data_PreparationForAlpha097.csv",encoding='utf-8-sig')           
    return result_industryaveraged_df

#rank(((((close - low) - (high -close)) / (high - low)) * volume))
def IndustryAverage_PreparationForAlpha100_1():          
    stock_list=local_source.get_stock_list(cols='TS_CODE,INDUSTRY').set_index("TS_CODE")
    industry_list=stock_list["INDUSTRY"].drop_duplicates()
    date_list=local_source.get_indices_daily(cols='TRADE_DATE',condition='INDEX_CODE = "000001.SH"')["TRADE_DATE"].astype(int)
    
    #check for building/updating/reading dataset
    try:
        result_industryaveraged_df = pd.read_csv("analysis/IndustryAverage_Data_PreparationForAlpha100_1.csv")
        result_industryaveraged_df["TRADE_DATE"] = result_industryaveraged_df["TRADE_DATE"].astype(int)        
        result_industryaveraged_df.set_index("TRADE_DATE",inplace=True)
        date_list_existed = pd.Series(result_industryaveraged_df.index)
        date_list_update = date_list[~date_list.isin(date_list_existed)]
        if len(date_list_update)==0:
            print("The corresponding industry average data for alpha100_1 needs not to be updated.")
            return result_industryaveraged_df
        else:
            print("The corresponding industry average data for alpha100_1 needs to be updated.")
            first_date_update = date_list_update.iloc[0]
    except:
        print("The corresponding industry average dataset for alpha100_1 is missing.")
        result_industryaveraged_df=pd.DataFrame(index=date_list,columns=industry_list)
        date_list_update = date_list
        first_date_update=0
    
    #building/updating dataset
    result_unaveraged_industry=0 
    for industry in pb(industry_list, desc='Please wait', colour='#ffffff'):
        stock_list_industry=stock_list[stock_list["INDUSTRY"]==industry]
        #calculating unindentralized data
        for ts_code in stock_list_industry.index:
            quotations_daily_chosen=local_source.get_quotations_daily(cols='TRADE_DATE,TS_CODE,OPEN,CLOSE,LOW,HIGH,VOL,CHANGE,AMOUNT',condition='TS_CODE = '+'"'+ts_code+'"').sort_values(by="TRADE_DATE", ascending=True) 
            quotations_daily_chosen["TRADE_DATE"]=quotations_daily_chosen["TRADE_DATE"].astype(int)
            quotations_daily_chosen=quotations_daily_chosen.applymap(lambda x: np.nan if x=="NULL" else x)
            
            try:    #valid only in updating
                index_first_date_needed = date_list_existed[date_list_existed.values == first_date_update].index[0]
                first_date_needed = date_list_existed.loc[index_first_date_needed]
                quotations_daily_chosen = quotations_daily_chosen[quotations_daily_chosen["TRADE_DATE"]>=first_date_needed]
            except:
                pass

            HIGH = quotations_daily_chosen['HIGH']
            LOW = quotations_daily_chosen['LOW']
            CLOSE = quotations_daily_chosen['CLOSE']
            VOLUME = quotations_daily_chosen['VOL']*100 
            result_unaveraged_piece = rank(((((CLOSE - LOW) - (HIGH -CLOSE)) / (HIGH - LOW)) * VOLUME))

            result_unaveraged_piece.rename("PREPARATION_FOR_ALPHA100_1_UNAVERAGED",inplace=True)
            result_unaveraged_piece = pd.DataFrame(result_unaveraged_piece)
            result_unaveraged_piece.insert(loc=0,column='INDUSTRY',value=industry)
            result_unaveraged_piece.insert(loc=0,column='TRADE_DATE',value=quotations_daily_chosen["TRADE_DATE"])
            result_unaveraged_piece.insert(loc=0,column='TS_CODE',value=ts_code)
            
            result_unaveraged_piece = result_unaveraged_piece[result_unaveraged_piece["TRADE_DATE"]>=first_date_update] #to lower the memory needed
            
            if type(result_unaveraged_industry)==int:
                result_unaveraged_industry=result_unaveraged_piece
            else:
                result_unaveraged_industry=pd.concat([result_unaveraged_industry,result_unaveraged_piece],axis=0)    
        
        #indentralizing data
        for date in date_list_update:
            try:   #to prevent the case that the stock is suspended, so that there's no data for the stock at some dates
                result_piece=result_unaveraged_industry[result_unaveraged_industry["TRADE_DATE"]==date]              
                value=result_piece["PREPARATION_FOR_ALPHA100_1_UNAVERAGED"].mean()
                result_industryaveraged_df.loc[date,industry]=value
            except:
                pass

        result_unaveraged_industry=0
        
    result_industryaveraged_df.to_csv("analysis/IndustryAverage_Data_PreparationForAlpha100_1.csv",encoding='utf-8-sig')           
    return result_industryaveraged_df

#(correlation(close, rank(adv20), 5) - rank(ts_argmin(close, 30))) 
def IndustryAverage_PreparationForAlpha100_2():          
    stock_list=local_source.get_stock_list(cols='TS_CODE,INDUSTRY').set_index("TS_CODE")
    industry_list=stock_list["INDUSTRY"].drop_duplicates()
    date_list=local_source.get_indices_daily(cols='TRADE_DATE',condition='INDEX_CODE = "000001.SH"')["TRADE_DATE"].astype(int)
    
    #check for building/updating/reading dataset
    try:
        result_industryaveraged_df = pd.read_csv("analysis/IndustryAverage_Data_PreparationForAlpha100_2.csv")
        result_industryaveraged_df["TRADE_DATE"] = result_industryaveraged_df["TRADE_DATE"].astype(int)        
        result_industryaveraged_df.set_index("TRADE_DATE",inplace=True)
        date_list_existed = pd.Series(result_industryaveraged_df.index)
        date_list_update = date_list[~date_list.isin(date_list_existed)]
        if len(date_list_update)==0:
            print("The corresponding industry average data for alpha100_2 needs not to be updated.")
            return result_industryaveraged_df
        else:
            print("The corresponding industry average data for alpha100_2 needs to be updated.")
            first_date_update = date_list_update.iloc[0]
    except:
        print("The corresponding industry average dataset for alpha100_2 is missing.")
        result_industryaveraged_df=pd.DataFrame(index=date_list,columns=industry_list)
        date_list_update = date_list
        first_date_update=0
    
    #building/updating dataset
    result_unaveraged_industry=0 
    for industry in pb(industry_list, desc='Please wait', colour='#ffffff'):
        stock_list_industry=stock_list[stock_list["INDUSTRY"]==industry]
        #calculating unindentralized data
        for ts_code in stock_list_industry.index:
            quotations_daily_chosen=local_source.get_quotations_daily(cols='TRADE_DATE,TS_CODE,OPEN,CLOSE,LOW,HIGH,VOL,CHANGE,AMOUNT',condition='TS_CODE = '+'"'+ts_code+'"').sort_values(by="TRADE_DATE", ascending=True) 
            quotations_daily_chosen["TRADE_DATE"]=quotations_daily_chosen["TRADE_DATE"].astype(int)
            quotations_daily_chosen=quotations_daily_chosen.applymap(lambda x: np.nan if x=="NULL" else x)
            
            try:    #valid only in updating
                index_first_date_needed = date_list_existed[date_list_existed.values == first_date_update].index[0] -30
                first_date_needed = date_list_existed.loc[index_first_date_needed]
                quotations_daily_chosen = quotations_daily_chosen[quotations_daily_chosen["TRADE_DATE"]>=first_date_needed]
            except:
                pass

            CLOSE = quotations_daily_chosen['CLOSE']
            VOLUME = quotations_daily_chosen['VOL']*100 
            adv20 = sma(VOLUME, 30)      
            result_unaveraged_piece = (correlation(CLOSE, rank(adv20), 5) - rank(ts_argmin(CLOSE, 30)))

            result_unaveraged_piece.rename("PREPARATION_FOR_ALPHA100_2_UNAVERAGED",inplace=True)
            result_unaveraged_piece = pd.DataFrame(result_unaveraged_piece)
            result_unaveraged_piece.insert(loc=0,column='INDUSTRY',value=industry)
            result_unaveraged_piece.insert(loc=0,column='TRADE_DATE',value=quotations_daily_chosen["TRADE_DATE"])
            result_unaveraged_piece.insert(loc=0,column='TS_CODE',value=ts_code)
            
            result_unaveraged_piece = result_unaveraged_piece[result_unaveraged_piece["TRADE_DATE"]>=first_date_update] #to lower the memory needed
            
            if type(result_unaveraged_industry)==int:
                result_unaveraged_industry=result_unaveraged_piece
            else:
                result_unaveraged_industry=pd.concat([result_unaveraged_industry,result_unaveraged_piece],axis=0)    
        
        #indentralizing data
        for date in date_list_update:
            try:   #to prevent the case that the stock is suspended, so that there's no data for the stock at some dates
                result_piece=result_unaveraged_industry[result_unaveraged_industry["TRADE_DATE"]==date]            
                value=result_piece["PREPARATION_FOR_ALPHA100_2_UNAVERAGED"].mean()
                result_industryaveraged_df.loc[date,industry]=value
            except:
                pass

        result_unaveraged_industry=0
        
    result_industryaveraged_df.to_csv("analysis/IndustryAverage_Data_PreparationForAlpha100_2.csv",encoding='utf-8-sig')           
    return result_industryaveraged_df




class Alphas(object):
    def __init__(self, ts_code, start_date=20210101, end_date=20211231):
        
        self.ts_code = ts_code
        self.start_date = start_date
        self.end_date = end_date
        
        if ts_code == "All":  #需要同时计算多股的alpha时, 将数据一次性取出以加快运行速度
            quotations_daily=local_source.get_quotations_daily(cols='TRADE_DATE,TS_CODE,OPEN,CLOSE,LOW,HIGH,VOL,CHANGE,AMOUNT').sort_values(by="TRADE_DATE", ascending=True) 
            stock_indicators_daily=local_source.get_stock_indicators_daily(cols='TRADE_DATE,TS_CODE,TOTAL_SHARE').sort_values(by="TRADE_DATE", ascending=True) 
        else:
            quotations_daily=local_source.get_quotations_daily(cols='TRADE_DATE,TS_CODE,OPEN,CLOSE,LOW,HIGH,VOL,CHANGE,AMOUNT',condition="TS_CODE = " + "'" + ts_code + "'").sort_values(by="TRADE_DATE", ascending=True) 
            stock_indicators_daily=local_source.get_stock_indicators_daily(cols='TRADE_DATE,TS_CODE,TOTAL_SHARE',condition="TS_CODE = " + "'" + ts_code + "'").sort_values(by="TRADE_DATE", ascending=True) 
        
        self.stock_data=pd.merge(quotations_daily, stock_indicators_daily,on=['TRADE_DATE','TS_CODE'],how="left") 
        self.stock_data=self.stock_data.applymap(lambda x: np.nan if x=="NULL" else x)
        self.stock_data["TOTAL_MV"]=self.stock_data["TOTAL_SHARE"]*self.stock_data["CLOSE"]
        self.stock_data["TRADE_DATE"]=self.stock_data["TRADE_DATE"].astype(int)


    def initializer(self, ts_code_chosen=0):
        if self.ts_code == 'All':
            self.stock_data_chosen = self.stock_data[self.stock_data["TS_CODE"]==ts_code_chosen].reset_index(drop=True)
        else:
            self.stock_data_chosen = self.stock_data
            ts_code_chosen = self.ts_code
            
        self.open = self.stock_data_chosen['OPEN'] 
        self.high = self.stock_data_chosen['HIGH'] 
        self.low = self.stock_data_chosen['LOW']   
        self.close = self.stock_data_chosen['CLOSE'] 
        self.volume = self.stock_data_chosen['VOL']*100
        self.returns = self.stock_data_chosen['CHANGE'] / self.stock_data_chosen['OPEN']  
        self.vwap = (self.stock_data_chosen['AMOUNT']*1000)/(self.stock_data_chosen['VOL']*100+1) 
        self.cap = self.stock_data_chosen['TOTAL_MV']
        self.industry = local_source.get_stock_list(cols='TS_CODE,INDUSTRY', condition='TS_CODE = '+'"'+ts_code_chosen+'"')['INDUSTRY'].iloc[0]
        self.available_dates = self.stock_data_chosen["TRADE_DATE"]
        self.output_dates = self.stock_data_chosen[(self.stock_data_chosen["TRADE_DATE"]>=self.start_date)*(self.stock_data_chosen["TRADE_DATE"]<=self.end_date)]["TRADE_DATE"]
        start_available_date = self.output_dates.iloc[0]  #这个是交易日
        end_available_date = self.output_dates.iloc[-1]        
        self.start_date_index = self.stock_data_chosen["TRADE_DATE"][self.stock_data_chosen["TRADE_DATE"].values == start_available_date].index[0]
        self.end_date_index = self.stock_data_chosen["TRADE_DATE"][self.stock_data_chosen["TRADE_DATE"].values == end_available_date].index[0] +1
        
         
    # Alpha#1	 (rank(Ts_ArgMax(SignedPower(((returns < 0) ? stddev(returns, 20) : close), 2.), 5)) -0.5)
    def alpha001(self):
        inner = self.close
        inner[self.returns < 0] = stddev(self.returns, 20)
        alpha = rank(ts_argmax(inner ** 2, 5))
        return alpha[self.start_date_index:self.end_date_index]
    
    # Alpha#2	 (-1 * correlation(rank(delta(log(volume), 2)), rank(((close - open) / open)), 6))
    def alpha002(self):
        df = -1 * correlation(rank(delta(log(self.volume), 2)), rank((self.close - self.open) / self.open), 6)
        alpha = df.replace([-np.inf, np.inf], 0).fillna(value=0)
        return alpha[self.start_date_index:self.end_date_index]
    
    # Alpha#3	 (-1 * correlation(rank(open), rank(volume), 10))
    def alpha003(self):
        df = -1 * correlation(rank(self.open), rank(self.volume), 10)
        alpha =  df.replace([-np.inf, np.inf], 0).fillna(value=0)
        return alpha[self.start_date_index:self.end_date_index]
    
    # Alpha#4	 (-1 * Ts_Rank(rank(low), 9))
    def alpha004(self):
        alpha =  -1 * ts_rank(rank(self.low), 9)
        return alpha[self.start_date_index:self.end_date_index]
    
    # Alpha#5	 (rank((open - (sum(vwap, 10) / 10))) * (-1 * abs(rank((close - vwap)))))
    def alpha005(self):
        alpha = (rank((self.open - (sum(self.vwap, 10) / 10))) * (-1 * abs(rank((self.close - self.vwap)))))
        return alpha[self.start_date_index:self.end_date_index]
    
    # Alpha#6	 (-1 * correlation(open, volume, 10))
    def alpha006(self):
        df = -1 * correlation(self.open, self.volume, 10)
        alpha = df.replace([-np.inf, np.inf], 0).fillna(value=0)
        return alpha[self.start_date_index:self.end_date_index]
    
    # Alpha#7	 ((adv20 < volume) ? ((-1 * ts_rank(abs(delta(close, 7)), 60)) * sign(delta(close, 7))) : (-1* 1))
    def alpha007(self):
        adv20 = sma(self.volume, 20)
        alpha = -1 * ts_rank(abs(delta(self.close, 7)), 60) * sign(delta(self.close, 7))
        alpha[adv20 >= self.volume] = -1
        return alpha[self.start_date_index:self.end_date_index]
    
    # Alpha#8	 (-1 * rank(((sum(open, 5) * sum(returns, 5)) - delay((sum(open, 5) * sum(returns, 5)),10))))
    def alpha008(self):
        alpha = -1 * (rank(((ts_sum(self.open, 5) * ts_sum(self.returns, 5)) - delay((ts_sum(self.open, 5) * ts_sum(self.returns, 5)), 10))))
        return alpha[self.start_date_index:self.end_date_index]
    
    # Alpha#9	 ((0 < ts_min(delta(close, 1), 5)) ? delta(close, 1) : ((ts_max(delta(close, 1), 5) < 0) ?delta(close, 1) : (-1 * delta(close, 1))))
    def alpha009(self):
        delta_close = delta(self.close, 1)
        cond_1 = ts_min(delta_close, 5) > 0
        cond_2 = ts_max(delta_close, 5) < 0
        alpha = -1 * delta_close
        alpha[cond_1 | cond_2] = delta_close
        return alpha[self.start_date_index:self.end_date_index]
    
    # Alpha#10	 rank(((0 < ts_min(delta(close, 1), 4)) ? delta(close, 1) : ((ts_max(delta(close, 1), 4) < 0)? delta(close, 1) : (-1 * delta(close, 1)))))
    def alpha010(self):
        delta_close = delta(self.close, 1)
        cond_1 = ts_min(delta_close, 4) > 0
        cond_2 = ts_max(delta_close, 4) < 0
        alpha = -1 * delta_close
        alpha[cond_1 | cond_2] = delta_close
        return alpha[self.start_date_index:self.end_date_index]
    
    # Alpha#11	 ((rank(ts_max((vwap - close), 3)) + rank(ts_min((vwap - close), 3))) *rank(delta(volume, 3)))
    def alpha011(self):
        alpha = ((rank(ts_max((self.vwap - self.close), 3)) + rank(ts_min((self.vwap - self.close), 3))) *rank(delta(self.volume, 3)))
        return alpha[self.start_date_index:self.end_date_index]
    
    # Alpha#12	 (sign(delta(volume, 1)) * (-1 * delta(close, 1)))
    def alpha012(self):
        alpha = sign(delta(self.volume, 1)) * (-1 * delta(self.close, 1))
        return alpha[self.start_date_index:self.end_date_index]
    
    # Alpha#13	 (-1 * rank(covariance(rank(close), rank(volume), 5)))
    def alpha013(self):
        alpha = -1 * rank(covariance(rank(self.close), rank(self.volume), 5))
        return alpha[self.start_date_index:self.end_date_index]
    
    # Alpha#14	 ((-1 * rank(delta(returns, 3))) * correlation(open, volume, 10))
    def alpha014(self):
        df = correlation(self.open, self.volume, 10)
        df = df.replace([-np.inf, np.inf], 0).fillna(value=0)
        alpha = -1 * rank(delta(self.returns, 3)) * df
        return alpha[self.start_date_index:self.end_date_index]
    
    # Alpha#15	 (-1 * sum(rank(correlation(rank(high), rank(volume), 3)), 3))
    def alpha015(self):
        df = correlation(rank(self.high), rank(self.volume), 3)
        df = df.replace([-np.inf, np.inf], 0).fillna(value=0)
        alpha = -1 * ts_sum(rank(df), 3)
        return alpha[self.start_date_index:self.end_date_index]
    
    # Alpha#16	 (-1 * rank(covariance(rank(high), rank(volume), 5)))
    def alpha016(self):
        alpha = -1 * rank(covariance(rank(self.high), rank(self.volume), 5))
        return alpha[self.start_date_index:self.end_date_index]
    
    # Alpha#17	 (((-1 * rank(ts_rank(close, 10))) * rank(delta(delta(close, 1), 1))) *rank(ts_rank((volume / adv20), 5)))
    def alpha017(self):
        adv20 = sma(self.volume, 20)
        alpha = -1 * (rank(ts_rank(self.close, 10)) * rank(delta(delta(self.close, 1), 1)) * rank(ts_rank((self.volume / adv20), 5)))
        return alpha[self.start_date_index:self.end_date_index]
    
    # Alpha#18	 (-1 * rank(((stddev(abs((close - open)), 5) + (close - open)) + correlation(close, open,10))))
    def alpha018(self):
        df = correlation(self.close, self.open, 10)
        df = df.replace([-np.inf, np.inf], 0).fillna(value=0)
        alpha = -1 * (rank((stddev(abs((self.close - self.open)), 5) + (self.close - self.open)) + df))
        return alpha[self.start_date_index:self.end_date_index]
    
    # Alpha#19	 ((-1 * sign(((close - delay(close, 7)) + delta(close, 7)))) * (1 + rank((1 + sum(returns,250)))))
    def alpha019(self):
        alpha = ((-1 * sign((self.close - delay(self.close, 7)) + delta(self.close, 7))) * (1 + rank(1 + ts_sum(self.returns, 250))))
        return alpha[self.start_date_index:self.end_date_index]
    
    # Alpha#20	 (((-1 * rank((open - delay(high, 1)))) * rank((open - delay(close, 1)))) * rank((open -delay(low, 1))))
    def alpha020(self):
        alpha = -1 * (rank(self.open - delay(self.high, 1)) * rank(self.open - delay(self.close, 1)) * rank(self.open - delay(self.low, 1)))
        return alpha[self.start_date_index:self.end_date_index]
    
    # Alpha#21	 ((((sum(close, 8) / 8) + stddev(close, 8)) < (sum(close, 2) / 2)) ? (-1 * 1) : (((sum(close,2) / 2) < ((sum(close, 8) / 8) - stddev(close, 8))) ? 1 : (((1 < (volume / adv20)) || ((volume /adv20) == 1)) ? 1 : (-1 * 1))))
    def alpha021(self):
        cond_1 = sma(self.close, 8) + stddev(self.close, 8) < sma(self.close, 2)
        cond_2 = sma(self.volume, 20) / self.volume < 1
        alpha = pd.DataFrame(np.ones_like(self.close), index=self.close.index)
        #alpha = pd.DataFrame(np.ones_like(self.close), index=self.close.index, columns=self.close.columns)
        alpha[cond_1 | cond_2] = -1
        return alpha[self.start_date_index:self.end_date_index]
    
    # Alpha#22	 (-1 * (delta(correlation(high, volume, 5), 5) * rank(stddev(close, 20))))
    def alpha022(self):
        df = correlation(self.high, self.volume, 5)
        df = df.replace([-np.inf, np.inf], 0).fillna(value=0)
        alpha = -1 * delta(df, 5) * rank(stddev(self.close, 20))
        return alpha[self.start_date_index:self.end_date_index]
    
    # Alpha#23	 (((sum(high, 20) / 20) < high) ? (-1 * delta(high, 2)) : 0)
    def alpha023(self):
        cond = sma(self.high, 20) < self.high
        alpha = pd.DataFrame(np.zeros_like(self.close),index=self.close.index,columns=['close'])
        alpha.at[cond,'close'] = -1 * delta(self.high, 2).fillna(value=0)
        return alpha[self.start_date_index:self.end_date_index]
    
    # Alpha#24	 ((((delta((sum(close, 100) / 100), 100) / delay(close, 100)) < 0.05) ||((delta((sum(close, 100) / 100), 100) / delay(close, 100)) == 0.05)) ? (-1 * (close - ts_min(close,100))) : (-1 * delta(close, 3)))
    def alpha024(self):
        cond = delta(sma(self.close, 100), 100) / delay(self.close, 100) <= 0.05
        alpha = -1 * delta(self.close, 3)
        alpha[cond] = -1 * (self.close - ts_min(self.close, 100))
        return alpha[self.start_date_index:self.end_date_index]
    
    # Alpha#25	 rank(((((-1 * returns) * adv20) * vwap) * (high - close)))
    def alpha025(self):
        adv20 = sma(self.volume, 20)
        alpha = rank(((((-1 * self.returns) * adv20) * self.vwap) * (self.high - self.close)))
        return alpha[self.start_date_index:self.end_date_index]
    
    # Alpha#26	 (-1 * ts_max(correlation(ts_rank(volume, 5), ts_rank(high, 5), 5), 3))
    def alpha026(self):
        df = correlation(ts_rank(self.volume, 5), ts_rank(self.high, 5), 5)
        df = df.replace([-np.inf, np.inf], 0).fillna(value=0)
        alpha = -1 * ts_max(df, 3)
        return alpha[self.start_date_index:self.end_date_index]
    
    # Alpha#27	 ((0.5 < rank((sum(correlation(rank(volume), rank(vwap), 6), 2) / 2.0))) ? (-1 * 1) : 1)
    def alpha027(self): #there maybe problems
        alpha = rank((sma(correlation(rank(self.volume), rank(self.vwap), 6), 2) / 2.0))
        alpha[alpha > 0.5] = -1
        alpha[alpha <= 0.5]=1
        return alpha[self.start_date_index:self.end_date_index]  
    
    # Alpha#28	 scale(((correlation(adv20, low, 5) + ((high + low) / 2)) - close))
    def alpha028(self):
        adv20 = sma(self.volume, 20)
        df = correlation(adv20, self.low, 5)
        df = df.replace([-np.inf, np.inf], 0).fillna(value=0)
        alpha = scale(((df + ((self.high + self.low) / 2)) - self.close))
        return alpha[self.start_date_index:self.end_date_index]
    
    # Alpha#29	 (min(product(rank(rank(scale(log(sum(ts_min(rank(rank((-1 * rank(delta((close - 1),5))))), 2), 1))))), 1), 5) + ts_rank(delay((-1 * returns), 6), 5))
    def alpha029(self):
        alpha = (ts_min(rolling_product(rank(rank(scale(log(ts_sum(ts_min(rank(rank((-1 * rank(delta((self.close - 1), 5))))), 2), 1))))), 1), 5) + ts_rank(delay((-1 * self.returns), 6), 5))
        return alpha[self.start_date_index:self.end_date_index]

    # Alpha#30	 (((1.0 - rank(((sign((close - delay(close, 1))) + sign((delay(close, 1) - delay(close, 2)))) +sign((delay(close, 2) - delay(close, 3)))))) * sum(volume, 5)) / sum(volume, 20))
    def alpha030(self):
        delta_close = delta(self.close, 1)
        inner = sign(delta_close) + sign(delay(delta_close, 1)) + sign(delay(delta_close, 2))
        alpha = ((1.0 - rank(inner)) * ts_sum(self.volume, 5)) / ts_sum(self.volume, 20)
        return alpha[self.start_date_index:self.end_date_index]
    
    # Alpha#31	 ((rank(rank(rank(decay_linear((-1 * rank(rank(delta(close, 10)))), 10)))) + rank((-1 *delta(close, 3)))) + sign(scale(correlation(adv20, low, 12))))
    def alpha031(self):
        adv20 = sma(self.volume, 20)
        df = correlation(adv20, self.low, 12).replace([-np.inf, np.inf], 0).fillna(value=0)         
        p1=rank(rank(rank(decay_linear((-1 * rank(rank(delta(self.close, 10)))), 10)))) 
        p2=rank((-1 * delta(self.close, 3)))
        p3=sign(scale(df))
        print(p1)
        alpha = p1+p2+p3
        return alpha[self.start_date_index:self.end_date_index]
    
    # Alpha#32	 (scale(((sum(close, 7) / 7) - close)) + (20 * scale(correlation(vwap, delay(close, 5),230))))
    def alpha032(self):
        alpha = scale(((sma(self.close, 7) / 7) - self.close)) + (20 * scale(correlation(self.vwap, delay(self.close, 5),230)))
        return alpha[self.start_date_index:self.end_date_index]
    
    # Alpha#33	 rank((-1 * ((1 - (open / close))^1)))
    def alpha033(self):
        alpha = rank(-1 + (self.open / self.close))
        return alpha[self.start_date_index:self.end_date_index]
    
    # Alpha#34	 rank(((1 - rank((stddev(returns, 2) / stddev(returns, 5)))) + (1 - rank(delta(close, 1)))))
    def alpha034(self):
        inner = stddev(self.returns, 2) / stddev(self.returns, 5)
        inner = inner.replace([-np.inf, np.inf], 1).fillna(value=1)
        alpha = rank(2 - rank(inner) - rank(delta(self.close, 1)))
        return alpha[self.start_date_index:self.end_date_index]
    
    # Alpha#35	 ((Ts_Rank(volume, 32) * (1 - Ts_Rank(((close + high) - low), 16))) * (1 -Ts_Rank(returns, 32)))
    def alpha035(self):
        alpha = ((ts_rank(self.volume, 32) * (1 - ts_rank(self.close + self.high - self.low, 16))) * (1 - ts_rank(self.returns, 32)))
        return alpha[self.start_date_index:self.end_date_index]
            
    # Alpha#36	 (((((2.21 * rank(correlation((close - open), delay(volume, 1), 15))) + (0.7 * rank((open- close)))) + (0.73 * rank(Ts_Rank(delay((-1 * returns), 6), 5)))) + rank(abs(correlation(vwap,adv20, 6)))) + (0.6 * rank((((sum(close, 200) / 200) - open) * (close - open)))))
    def alpha036(self):
        adv20 = sma(self.volume, 20)
        alpha = (((((2.21 * rank(correlation((self.close - self.open), delay(self.volume, 1), 15))) + (0.7 * rank((self.open- self.close)))) + (0.73 * rank(ts_rank(delay((-1 * self.returns), 6), 5)))) + rank(abs(correlation(self.vwap,adv20, 6)))) + (0.6 * rank((((sma(self.close, 200) / 200) - self.open) * (self.close - self.open)))))
        return alpha[self.start_date_index:self.end_date_index]
    
    # Alpha#37	 (rank(correlation(delay((open - close), 1), close, 200)) + rank((open - close)))
    def alpha037(self):
        alpha = rank(correlation(delay(self.open - self.close, 1), self.close, 200)) + rank(self.open - self.close)
        return alpha[self.start_date_index:self.end_date_index]
    
    # Alpha#38	 ((-1 * rank(Ts_Rank(close, 10))) * rank((close / open)))
    def alpha038(self):
        inner = self.close / self.open
        inner = inner.replace([-np.inf, np.inf], 1).fillna(value=1)
        alpha = -1 * rank(ts_rank(self.open, 10)) * rank(inner)
        return alpha[self.start_date_index:self.end_date_index]
    
    # Alpha#39	 ((-1 * rank((delta(close, 7) * (1 - rank(decay_linear((volume / adv20), 9)))))) * (1 +rank(sum(returns, 250))))
    def alpha039(self):
        adv20 = sma(self.volume, 20)
        alpha = ((-1 * rank(delta(self.close, 7) * (1 - rank(decay_linear((self.volume / adv20), 9))))) * (1 + rank(sma(self.returns, 250))))
        return alpha[self.start_date_index:self.end_date_index]
    
    # Alpha#40	 ((-1 * rank(stddev(high, 10))) * correlation(high, volume, 10))
    def alpha040(self):
        alpha = -1 * rank(stddev(self.high, 10)) * correlation(self.high, self.volume, 10)
        return alpha[self.start_date_index:self.end_date_index]
    
    # Alpha#41	 (((high * low)^0.5) - vwap)
    def alpha041(self):
        alpha = pow((self.high * self.low),0.5) - self.vwap
        return alpha[self.start_date_index:self.end_date_index]
    
    # Alpha#42	 (rank((vwap - close)) / rank((vwap + close)))
    def alpha042(self):
        alpha = rank((self.vwap - self.close)) / rank((self.vwap + self.close))
        return alpha[self.start_date_index:self.end_date_index]
    
    # Alpha#43	 (ts_rank((volume / adv20), 20) * ts_rank((-1 * delta(close, 7)), 8))
    def alpha043(self):
        adv20 = sma(self.volume, 20)
        alpha = ts_rank(self.volume / adv20, 20) * ts_rank((-1 * delta(self.close, 7)), 8)
        return alpha[self.start_date_index:self.end_date_index]
    
    # Alpha#44	 (-1 * correlation(high, rank(volume), 5))
    def alpha044(self):
        df = correlation(self.high, rank(self.volume), 5)
        df = df.replace([-np.inf, np.inf], 0).fillna(value=0)
        alpha = -1 * df
        return alpha[self.start_date_index:self.end_date_index]
    
    # Alpha#45	 (-1 * ((rank((sum(delay(close, 5), 20) / 20)) * correlation(close, volume, 2)) *rank(correlation(sum(close, 5), sum(close, 20), 2))))
    def alpha045(self):
        df = correlation(self.close, self.volume, 2)
        df = df.replace([-np.inf, np.inf], 0).fillna(value=0)
        alpha = -1 * (rank(sma(delay(self.close, 5), 20)) * df * rank(correlation(ts_sum(self.close, 5), ts_sum(self.close, 20), 2)))
        return alpha[self.start_date_index:self.end_date_index]
    
    # Alpha#46	 ((0.25 < (((delay(close, 20) - delay(close, 10)) / 10) - ((delay(close, 10) - close) / 10))) ?(-1 * 1) : (((((delay(close, 20) - delay(close, 10)) / 10) - ((delay(close, 10) - close) / 10)) < 0) ? 1 :((-1 * 1) * (close - delay(close, 1)))))
    def alpha046(self):
        inner = ((delay(self.close, 20) - delay(self.close, 10)) / 10) - ((delay(self.close, 10) - self.close) / 10)
        alpha = (-1 * delta(self.close))
        alpha[inner < 0] = 1
        alpha[inner > 0.25] = -1
        return alpha[self.start_date_index:self.end_date_index]

    # Alpha#47	 ((((rank((1 / close)) * volume) / adv20) * ((high * rank((high - close))) / (sum(high, 5) /5))) - rank((vwap - delay(vwap, 5))))
    def alpha047(self):
        adv20 = sma(self.volume, 20)
        alpha = ((((rank((1 / self.close)) * self.volume) / adv20) * ((self.high * rank((self.high - self.close))) / (sma(self.high, 5) /5))) - rank((self.vwap - delay(self.vwap, 5))))
        return alpha[self.start_date_index:self.end_date_index]
    
    # Alpha#48	 (indneutralize(((correlation(delta(close, 1), delta(delay(close, 1), 1), 250) *delta(close, 1)) / close), IndClass.subindustry) / sum(((delta(close, 1) / delay(close, 1))^2), 250))
    def alpha048(self):
        indaverage_data = IndustryAverage_PreparationForAlpha048()
        indaverage_data = indaverage_data[indaverage_data.index.isin(self.available_dates)]        
        indaverage_data = indaverage_data[self.industry]
        indaverage_data = indaverage_data.reset_index(drop=True)
        unindneutralized_data = (correlation(delta(self.close, 1), delta(delay(self.close, 1), 1), 250) *delta(self.close, 1)) / self.close 
        indneutralized_data = unindneutralized_data - indaverage_data
        alpha = indneutralized_data / sma(((delta(self.close, 1) / delay(self.close, 1))**2), 250)
        return alpha[self.start_date_index:self.end_date_index]
    
    # Alpha#49	 (((((delay(close, 20) - delay(close, 10)) / 10) - ((delay(close, 10) - close) / 10)) < (-1 *0.1)) ? 1 : ((-1 * 1) * (close - delay(close, 1))))
    def alpha049(self):
        inner = (((delay(self.close, 20) - delay(self.close, 10)) / 10) - ((delay(self.close, 10) - self.close) / 10))
        alpha = (-1 * delta(self.close))
        alpha[inner < -0.1] = 1
        return alpha[self.start_date_index:self.end_date_index]
    
    # Alpha#50	 (-1 * ts_max(rank(correlation(rank(volume), rank(vwap), 5)), 5))
    def alpha050(self):
        alpha = (-1 * ts_max(rank(correlation(rank(self.volume), rank(self.vwap), 5)), 5))
        return alpha[self.start_date_index:self.end_date_index]
    
    # Alpha#51	 (((((delay(close, 20) - delay(close, 10)) / 10) - ((delay(close, 10) - close) / 10)) < (-1 *0.05)) ? 1 : ((-1 * 1) * (close - delay(close, 1))))
    def alpha051(self):
        inner = (((delay(self.close, 20) - delay(self.close, 10)) / 10) - ((delay(self.close, 10) - self.close) / 10))
        alpha = (-1 * delta(self.close))
        alpha[inner < -0.05] = 1
        return alpha[self.start_date_index:self.end_date_index]
    
    # Alpha#52	 ((((-1 * ts_min(low, 5)) + delay(ts_min(low, 5), 5)) * rank(((sum(returns, 240) -sum(returns, 20)) / 220))) * ts_rank(volume, 5))
    def alpha052(self):
        alpha = (((-1 * delta(ts_min(self.low, 5), 5)) * rank(((ts_sum(self.returns, 240) - ts_sum(self.returns, 20)) / 220))) * ts_rank(self.volume, 5))
        return alpha[self.start_date_index:self.end_date_index]
    
    # Alpha#53	 (-1 * delta((((close - low) - (high - close)) / (close - low)), 9))
    def alpha053(self):
        inner = (self.close - self.low).replace(0, 0.0001)
        alpha = -1 * delta((((self.close - self.low) - (self.high - self.close)) / inner), 9)
        return alpha[self.start_date_index:self.end_date_index]
    
    # Alpha#54	 ((-1 * ((low - close) * (open^5))) / ((low - high) * (close^5)))
    def alpha054(self):
        inner = (self.low - self.high).replace(0, -0.0001)
        alpha = -1 * (self.low - self.close) * (self.open ** 5) / (inner * (self.close ** 5))
        return alpha[self.start_date_index:self.end_date_index]
    
    # Alpha#55	 (-1 * correlation(rank(((close - ts_min(low, 12)) / (ts_max(high, 12) - ts_min(low,12)))), rank(volume), 6))
    def alpha055(self):
        divisor = (ts_max(self.high, 12) - ts_min(self.low, 12)).replace(0, 0.0001)
        inner = (self.close - ts_min(self.low, 12)) / (divisor)
        df = correlation(rank(inner), rank(self.volume), 6)
        alpha = -1 * df.replace([-np.inf, np.inf], 0).fillna(value=0)
        return alpha[self.start_date_index:self.end_date_index]
    
    # Alpha#56	 (0 - (1 * (rank((sum(returns, 10) / sum(sum(returns, 2), 3))) * rank((returns * cap)))))
    def alpha056(self):
        alpha = (0 - (1 * (rank((sma(self.returns, 10) / sma(sma(self.returns, 2), 3))) * rank((self.returns * self.cap)))))
        return alpha[self.start_date_index:self.end_date_index]
    
    # Alpha#57	 (0 - (1 * ((close - vwap) / decay_linear(rank(ts_argmax(close, 30)), 2))))
    def alpha057(self):
        alpha = (0 - (1 * ((self.close - self.vwap) / decay_linear(rank(ts_argmax(self.close, 30)), 2))))
        return alpha[self.start_date_index:self.end_date_index]
    
    # Alpha#58	 (-1 * Ts_Rank(decay_linear(correlation(IndNeutralize(vwap, IndClass.sector), volume,3.92795), 7.89291), 5.50322))
    def alpha058(self):
        indaverage_vwap = IndustryAverage_vwap()
        indaverage_vwap = indaverage_vwap[indaverage_vwap.index.isin(self.available_dates)]        
        indaverage_vwap = indaverage_vwap[self.industry]
        indaverage_vwap = indaverage_vwap.reset_index(drop=True)
        indneutralized_vwap = self.vwap - indaverage_vwap
        alpha = (-1 * ts_rank(decay_linear(correlation(indneutralized_vwap, self.volume, 4), 8), 6))
        return alpha[self.start_date_index:self.end_date_index]    
    
    # Alpha#59	 (-1 * Ts_Rank(decay_linear(correlation(IndNeutralize(((vwap * 0.728317) + (vwap *(1 - 0.728317))), IndClass.industry), volume, 4.25197), 16.2289), 8.19648))
    def alpha059(self):
        indaverage_data = IndustryAverage_PreparationForAlpha059()
        indaverage_data = indaverage_data[indaverage_data.index.isin(self.available_dates)]        
        indaverage_data = indaverage_data[self.industry]
        indaverage_data = indaverage_data.reset_index(drop=True)
        unindneutralized_data = (self.vwap * 0.728317) + (self.vwap *(1 - 0.728317))
        indneutralized_data = unindneutralized_data - indaverage_data
        alpha = (-1 * ts_rank(decay_linear(correlation(indneutralized_data, self.volume, 4), 16), 8))
        return alpha[self.start_date_index:self.end_date_index]           
    
    # Alpha#60	 (0 - (1 * ((2 * scale(rank(((((close - low) - (high - close)) / (high - low)) * volume)))) -scale(rank(ts_argmax(close, 10))))))
    def alpha060(self):
        divisor = (self.high - self.low).replace(0, 0.0001)
        inner = ((self.close - self.low) - (self.high - self.close)) * self.volume / divisor
        alpha = - ((2 * scale(rank(inner))) - scale(rank(ts_argmax(self.close, 10))))
        return alpha[self.start_date_index:self.end_date_index]
    
	# Alpha#61	 (rank((vwap - ts_min(vwap, 16.1219))) < rank(correlation(vwap, adv180, 17.9282)))
    def alpha061(self):
        adv180 = sma(self.volume, 180)
        alpha = (rank((self.vwap - ts_min(self.vwap, 16))) < rank(correlation(self.vwap, adv180, 18)))
        alpha = alpha * 1
        return alpha[self.start_date_index:self.end_date_index]
    
	# Alpha#62	 ((rank(correlation(vwap, sum(adv20, 22.4101), 9.91009)) < rank(((rank(open) +rank(open)) < (rank(((high + low) / 2)) + rank(high))))) * -1)
    def alpha062(self):
        adv20 = sma(self.volume, 20)
        alpha = ((rank(correlation(self.vwap, sma(adv20, 22), 10)) < rank(((rank(self.open) +rank(self.open)) < (rank(((self.high + self.low) / 2)) + rank(self.high))))) * -1)
        return alpha[self.start_date_index:self.end_date_index]
    
    # Alpha#63	 ((rank(decay_linear(delta(IndNeutralize(close, IndClass.industry), 2.25164), 8.22237))- rank(decay_linear(correlation(((vwap * 0.318108) + (open * (1 - 0.318108))), sum(adv180,37.2467), 13.557), 12.2883))) * -1)
    def alpha063(self):
        indaverage_close = IndustryAverage_close()
        indaverage_close = indaverage_close[indaverage_close.index.isin(self.available_dates)]        
        indaverage_close = indaverage_close[self.industry]
        indaverage_close = indaverage_close.reset_index(drop=True)
        indneutralized_close = self.close - indaverage_close
        adv180 = sma(self.volume, 180)
        alpha = ((rank(decay_linear(delta(indneutralized_close, 2), 8))- rank(decay_linear(correlation(((self.vwap * 0.318108) + (self.open * (1 - 0.318108))), sma(adv180, 38), 14), 12))) * -1)
        return alpha[self.start_date_index:self.end_date_index]
    
    # Alpha#64	 ((rank(correlation(sum(((open * 0.178404) + (low * (1 - 0.178404))), 12.7054),sum(adv120, 12.7054), 16.6208)) < rank(delta(((((high + low) / 2) * 0.178404) + (vwap * (1 -0.178404))), 3.69741))) * -1)
    def alpha064(self):
        adv120 = sma(self.volume, 120)
        alpha = ((rank(correlation(sma(((self.open * 0.178404) + (self.low * (1 - 0.178404))), 13),sma(adv120, 13), 17)) < rank(delta(((((self.high + self.low) / 2) * 0.178404) + (self.vwap * (1 -0.178404))), 3.69741))) * -1)
        return alpha[self.start_date_index:self.end_date_index]
    
    # Alpha#65	 ((rank(correlation(((open * 0.00817205) + (vwap * (1 - 0.00817205))), sum(adv60,8.6911), 6.40374)) < rank((open - ts_min(open, 13.635)))) * -1)
    def alpha065(self):
        adv60 = sma(self.volume, 60)
        alpha = ((rank(correlation(((self.open * 0.00817205) + (self.vwap * (1 - 0.00817205))), sma(adv60,9), 6)) < rank((self.open - ts_min(self.open, 14)))) * -1)
        return alpha[self.start_date_index:self.end_date_index]
    
    # Alpha#66	 ((rank(decay_linear(delta(vwap, 3.51013), 7.23052)) + Ts_Rank(decay_linear(((((low* 0.96633) + (low * (1 - 0.96633))) - vwap) / (open - ((high + low) / 2))), 11.4157), 6.72611)) * -1)
    def alpha066(self):
        alpha = ((rank(decay_linear(delta(self.vwap, 4), 7)) + ts_rank(decay_linear(((((self.low* 0.96633) + (self.low * (1 - 0.96633))) - self.vwap) / (self.open - ((self.high + self.low) / 2))), 11), 7)) * -1)
        return alpha[self.start_date_index:self.end_date_index]
    
    # Alpha#67	 ((rank((high - ts_min(high, 2.14593)))^rank(correlation(IndNeutralize(vwap,IndClass.sector), IndNeutralize(adv20, IndClass.subindustry), 6.02936))) * -1)
    def alpha067(self):
        indaverage_adv20 = IndustryAverage_adv(20)
        indaverage_adv20 = indaverage_adv20[indaverage_adv20.index.isin(self.available_dates)]        
        indaverage_adv20 = indaverage_adv20[self.industry]
        indaverage_adv20 = indaverage_adv20.reset_index(drop=True)   
        adv20 = sma(self.volume, 20)        
        indneutralized_adv20 = adv20 - indaverage_adv20
        indaverage_vwap = IndustryAverage_vwap()
        indaverage_vwap = indaverage_vwap[indaverage_vwap.index.isin(self.available_dates)]        
        indaverage_vwap = indaverage_vwap[self.industry]
        indaverage_vwap = indaverage_vwap.reset_index(drop=True)
        indneutralized_vwap = self.vwap - indaverage_vwap 
        alpha = rank((self.high - ts_min(self.high, 2))) ** rank(correlation(indneutralized_vwap, indneutralized_adv20, 6)) * -1
        return alpha[self.start_date_index:self.end_date_index]
        
    # Alpha#68	 ((Ts_Rank(correlation(rank(high), rank(adv15), 8.91644), 13.9333) <rank(delta(((close * 0.518371) + (low * (1 - 0.518371))), 1.06157))) * -1)
    def alpha068(self):
        adv15 = sma(self.volume, 15)
        alpha = ((ts_rank(correlation(rank(self.high), rank(adv15), 9), 14) <rank(delta(((self.close * 0.518371) + (self.low * (1 - 0.518371))), 1.06157))) * -1)
        return alpha[self.start_date_index:self.end_date_index]
    
    # Alpha#69	 ((rank(ts_max(delta(IndNeutralize(vwap, IndClass.industry), 2.72412),4.79344))^Ts_Rank(correlation(((close * 0.490655) + (vwap * (1 - 0.490655))), adv20, 4.92416),9.0615)) * -1)
    def alpha069(self):
        indaverage_vwap = IndustryAverage_vwap()
        indaverage_vwap = indaverage_vwap[indaverage_vwap.index.isin(self.available_dates)]        
        indaverage_vwap = indaverage_vwap[self.industry]
        indaverage_vwap = indaverage_vwap.reset_index(drop=True)
        indneutralized_vwap = self.vwap - indaverage_vwap        
        adv20 = sma(self.volume, 20)
        alpha = ((rank(ts_max(delta(indneutralized_vwap, 3),5)) ** ts_rank(correlation(((self.close * 0.490655) + (self.vwap * (1 - 0.490655))), adv20, 5),9)) * -1)
        return alpha[self.start_date_index:self.end_date_index]
    
    # Alpha#70	 ((rank(delta(vwap, 1.29456))^Ts_Rank(correlation(IndNeutralize(close,IndClass.industry), adv50, 17.8256), 17.9171)) * -1)
    def alpha070(self):
        indaverage_close = IndustryAverage_close()
        indaverage_close = indaverage_close[indaverage_close.index.isin(self.available_dates)]        
        indaverage_close = indaverage_close[self.industry]
        indaverage_close = indaverage_close.reset_index(drop=True)
        indneutralized_close = self.close - indaverage_close   
        adv50 = sma(self.volume, 50) 
        alpha = (rank(delta(self.vwap, 1)) ** ts_rank(correlation(indneutralized_close, adv50, 18), 18)) * -1
        return alpha[self.start_date_index:self.end_date_index]
    
    # Alpha#71	 max(Ts_Rank(decay_linear(correlation(Ts_Rank(close, 3.43976), Ts_Rank(adv180,12.0647), 18.0175), 4.20501), 15.6948), Ts_Rank(decay_linear((rank(((low + open) - (vwap +vwap)))^2), 16.4662), 4.4388))
    def alpha071(self):
        adv180 = sma(self.volume, 180)
        p1=ts_rank(decay_linear(correlation(ts_rank(self.close, 3), ts_rank(adv180,12), 18), 4), 16)
        p2=ts_rank(decay_linear((rank(((self.low + self.open) - (self.vwap +self.vwap))).pow(2)), 16), 4)
        df=pd.DataFrame({'p1':p1,'p2':p2})
        df.at[df['p1']>=df['p2'],'max']=df['p1']
        df.at[df['p2']>=df['p1'],'max']=df['p2']
        alpha = df['max']
        #alpha = max(ts_rank(decay_linear(correlation(ts_rank(self.close, 3), ts_rank(adv180,12), 18).to_frame(), 4).CLOSE, 16), ts_rank(decay_linear((rank(((self.low + self.open) - (self.vwap +self.vwap))).pow(2)).to_frame(), 16).CLOSE, 4))
        return alpha[self.start_date_index:self.end_date_index]
    
    # Alpha#72	 (rank(decay_linear(correlation(((high + low) / 2), adv40, 8.93345), 10.1519)) /rank(decay_linear(correlation(Ts_Rank(vwap, 3.72469), Ts_Rank(volume, 18.5188), 6.86671),2.95011)))
    def alpha072(self):
        adv40 = sma(self.volume, 40)
        alpha = (rank(decay_linear(correlation(((self.high + self.low) / 2), adv40, 9).to_frame(), 10)) /rank(decay_linear(correlation(ts_rank(self.vwap, 4), ts_rank(self.volume, 19), 7).to_frame(),3)))
        return alpha[self.start_date_index:self.end_date_index]
    
    # Alpha#73	 (max(rank(decay_linear(delta(vwap, 4.72775), 2.91864)),Ts_Rank(decay_linear(((delta(((open * 0.147155) + (low * (1 - 0.147155))), 2.03608) / ((open *0.147155) + (low * (1 - 0.147155)))) * -1), 3.33829), 16.7411)) * -1)
    def alpha073(self):
        p1=rank(decay_linear(delta(self.vwap, 5).to_frame(), 3))
        p2=ts_rank(decay_linear(((delta(((self.open * 0.147155) + (self.low * (1 - 0.147155))), 2) / ((self.open *0.147155) + (self.low * (1 - 0.147155)))) * -1).to_frame(), 3), 17)
        df=pd.DataFrame({'p1':p1,'p2':p2})
        df.at[df['p1']>=df['p2'],'max']=df['p1']
        df.at[df['p2']>=df['p1'],'max']=df['p2']
        alpha = -1 * df['max']
        return alpha[self.start_date_index:self.end_date_index]
   
    # Alpha#74	 ((rank(correlation(close, sum(adv30, 37.4843), 15.1365)) <rank(correlation(rank(((high * 0.0261661) + (vwap * (1 - 0.0261661)))), rank(volume), 11.4791)))* -1)
    def alpha074(self):
        adv30 = sma(self.volume, 30)
        alpha = ((rank(correlation(self.close, sma(adv30, 37), 15)) <rank(correlation(rank(((self.high * 0.0261661) + (self.vwap * (1 - 0.0261661)))), rank(self.volume), 11)))* -1)
        return alpha[self.start_date_index:self.end_date_index]
    
    # Alpha#75	 (rank(correlation(vwap, volume, 4.24304)) < rank(correlation(rank(low), rank(adv50),12.4413)))
    def alpha075(self):
        adv50 = sma(self.volume, 50)
        alpha = (rank(correlation(self.vwap, self.volume, 4)) < rank(correlation(rank(self.low), rank(adv50),12)))
        alpha = alpha * 1
        return alpha[self.start_date_index:self.end_date_index]
    
    # Alpha#76	 (max(rank(decay_linear(delta(vwap, 1.24383), 11.8259)),Ts_Rank(decay_linear(Ts_Rank(correlation(IndNeutralize(low, IndClass.sector), adv81,8.14941), 19.569), 17.1543), 19.383)) * -1)
    def alpha076(self):
        indaverage_low = IndustryAverage_low()
        indaverage_low = indaverage_low[indaverage_low.index.isin(self.available_dates)]        
        indaverage_low = indaverage_low[self.industry]
        indaverage_low = indaverage_low.reset_index(drop=True)
        indneutralized_low = self.low - indaverage_low    
        adv81 = sma(self.volume, 81)
        p1 = rank(decay_linear(delta(self.vwap.to_frame(), 1), 12))
        p2 = ts_rank(decay_linear(ts_rank(correlation(indneutralized_low, adv81, 8).to_frame(), 20), 17), 19)     
        df=pd.DataFrame({'p1':p1,'p2':p2})
        df.at[df['p1']>=df['p2'],'max']=df['p1']
        df.at[df['p2']>=df['p1'],'max']=df['p2']
        alpha = -1 * df['max']
        return alpha[self.start_date_index:self.end_date_index]        
        
    # Alpha#77	 min(rank(decay_linear(((((high + low) / 2) + high) - (vwap + high)), 20.0451)),rank(decay_linear(correlation(((high + low) / 2), adv40, 3.1614), 5.64125)))
    def alpha077(self):
        adv40 = sma(self.volume, 40)
        p1=rank(decay_linear(((((self.high + self.low) / 2) + self.high) - (self.vwap + self.high)).to_frame(), 20))
        p2=rank(decay_linear(correlation(((self.high + self.low) / 2), adv40, 3).to_frame(), 6))
        df=pd.DataFrame({'p1':p1,'p2':p2})
        df.at[df['p1']>=df['p2'],'min']=df['p2']
        df.at[df['p2']>=df['p1'],'min']=df['p1']
        alpha = df['min']
        return alpha[self.start_date_index:self.end_date_index]
    
    # Alpha#78	 (rank(correlation(sum(((low * 0.352233) + (vwap * (1 - 0.352233))), 19.7428),sum(adv40, 19.7428), 6.83313))^rank(correlation(rank(vwap), rank(volume), 5.77492)))
    def alpha078(self):
        adv40 = sma(self.volume, 40)
        alpha = (rank(correlation(ts_sum(((self.low * 0.352233) + (self.vwap * (1 - 0.352233))), 20),ts_sum(adv40,20), 7)).pow(rank(correlation(rank(self.vwap), rank(self.volume), 6))))
        return alpha[self.start_date_index:self.end_date_index]
    
    # Alpha#79	 (rank(delta(IndNeutralize(((close * 0.60733) + (open * (1 - 0.60733))),IndClass.sector), 1.23438)) < rank(correlation(Ts_Rank(vwap, 3.60973), Ts_Rank(adv150,9.18637), 14.6644)))
    def alpha079(self):
        indaverage_data = IndustryAverage_PreparationForAlpha079()
        indaverage_data = indaverage_data[indaverage_data.index.isin(self.available_dates)]        
        indaverage_data = indaverage_data[self.industry]
        indaverage_data = indaverage_data.reset_index(drop=True)
        unindneutralized_data = (self.close * 0.60733) + (self.open * (1 - 0.60733))
        indneutralized_data = unindneutralized_data - indaverage_data       
        adv150 = sma(self.volume, 150) 
        alpha = (rank(delta(indneutralized_data, 1)) < rank(correlation(ts_rank(self.vwap, 4), ts_rank(adv150, 9), 15))) *1 
        return alpha[self.start_date_index:self.end_date_index]
    
    # Alpha#80	 ((rank(Sign(delta(IndNeutralize(((open * 0.868128) + (high * (1 - 0.868128))),IndClass.industry), 4.04545)))^Ts_Rank(correlation(high, adv10, 5.11456), 5.53756)) * -1)
    def alpha080(self):
        indaverage_data = IndustryAverage_PreparationForAlpha080()
        indaverage_data = indaverage_data[indaverage_data.index.isin(self.available_dates)]        
        indaverage_data = indaverage_data[self.industry]
        indaverage_data = indaverage_data.reset_index(drop=True)
        unindneutralized_data = (self.open * 0.868128) + (self.high * (1 - 0.868128))
        indneutralized_data = unindneutralized_data - indaverage_data   
        adv10 = sma(self.volume, 10)
        alpha = rank(sign(delta(indneutralized_data, 4))) ** (ts_rank(correlation(self.high, adv10, 5), 6)) * -1
        return alpha[self.start_date_index:self.end_date_index]
   
    # Alpha#81	 ((rank(Log(product(rank((rank(correlation(vwap, sum(adv10, 49.6054),8.47743))^4)), 14.9655))) < rank(correlation(rank(vwap), rank(volume), 5.07914))) * -1)
    def alpha081(self):
        adv10 = sma(self.volume, 10)
        alpha = ((rank(log(rolling_product(rank((rank(correlation(self.vwap, ts_sum(adv10, 50),8)).pow(4))), 15))) < rank(correlation(rank(self.vwap), rank(self.volume), 5))) * -1)
        return alpha[self.start_date_index:self.end_date_index]
    
    # Alpha#82	 (min(rank(decay_linear(delta(open, 1.46063), 14.8717)),Ts_Rank(decay_linear(correlation(IndNeutralize(volume, IndClass.sector), ((open * 0.634196) +(open * (1 - 0.634196))), 17.4842), 6.92131), 13.4283)) * -1)
    def alpha082(self):
        indaverage_volume = IndustryAverage_volume()
        indaverage_volume = indaverage_volume[indaverage_volume.index.isin(self.available_dates)]        
        indaverage_volume = indaverage_volume[self.industry]
        indaverage_volume = indaverage_volume.reset_index(drop=True)
        indneutralized_volume = self.volume - indaverage_volume      
        p1 = rank(decay_linear(delta(self.open, 1).to_frame(), 15))
        p2 = ts_rank(decay_linear(correlation(indneutralized_volume, ((self.open * 0.634196)+(self.open * (1 - 0.634196))), 17).to_frame(), 7), 13)
        df=pd.DataFrame({'p1':p1,'p2':p2})
        df.at[df['p1']>=df['p2'],'min']=df['p2']
        df.at[df['p2']>=df['p1'],'min']=df['p1']
        alpha = -1 * df['min']
        return alpha[self.start_date_index:self.end_date_index]                
    
    # Alpha#83	 ((rank(delay(((high - low) / (sum(close, 5) / 5)), 2)) * rank(rank(volume))) / (((high -low) / (sum(close, 5) / 5)) / (vwap - close)))
    def alpha083(self):
        alpha = ((rank(delay(((self.high - self.low) / (ts_sum(self.close, 5) / 5)), 2)) * rank(rank(self.volume))) / (((self.high -self.low) / (ts_sum(self.close, 5) / 5)) / (self.vwap - self.close)))
        return alpha[self.start_date_index:self.end_date_index]
    
    # Alpha#84	 SignedPower(Ts_Rank((vwap - ts_max(vwap, 15.3217)), 20.7127), delta(close,4.96796))
    def alpha084(self):
        alpha = pow(ts_rank((self.vwap - ts_max(self.vwap, 15)), 21), delta(self.close,5))
        return alpha[self.start_date_index:self.end_date_index]
    
    # Alpha#85	 (rank(correlation(((high * 0.876703) + (close * (1 - 0.876703))), adv30,9.61331))^rank(correlation(Ts_Rank(((high + low) / 2), 3.70596), Ts_Rank(volume, 10.1595),7.11408)))
    def alpha085(self):
        adv30 = sma(self.volume, 30)
        alpha = (rank(correlation(((self.high * 0.876703) + (self.close * (1 - 0.876703))), adv30,10)).pow(rank(correlation(ts_rank(((self.high + self.low) / 2), 4), ts_rank(self.volume, 10),7))))
        return alpha[self.start_date_index:self.end_date_index]
    
    # Alpha#86	 ((Ts_Rank(correlation(close, sum(adv20, 14.7444), 6.00049), 20.4195) < rank(((open+ close) - (vwap + open)))) * -1)
    def alpha086(self):
        adv20 = sma(self.volume, 20)
        alpha = ((ts_rank(correlation(self.close, sma(adv20, 15), 6), 20) < rank(((self.open+ self.close) - (self.vwap +self.open)))) * -1)
        return alpha[self.start_date_index:self.end_date_index]
    
    # Alpha#87	 (max(rank(decay_linear(delta(((close * 0.369701) + (vwap * (1 - 0.369701))),1.91233), 2.65461)), Ts_Rank(decay_linear(abs(correlation(IndNeutralize(adv81,IndClass.industry), close, 13.4132)), 4.89768), 14.4535)) * -1)
    def alpha087(self): 
        indaverage_adv81 = IndustryAverage_adv(81)
        indaverage_adv81 = indaverage_adv81[indaverage_adv81.index.isin(self.available_dates)]        
        indaverage_adv81 = indaverage_adv81[self.industry]
        indaverage_adv81 = indaverage_adv81.reset_index(drop=True)   
        adv81 = sma(self.volume, 81)        
        indneutralized_adv81 = adv81 - indaverage_adv81     
        p1 = rank(decay_linear(delta(((self.close * 0.369701) + (self.vwap * (1 - 0.369701))),2).to_frame(), 3))
        p2 = ts_rank(decay_linear(abs(correlation(indneutralized_adv81, self.close, 13)), 5), 14)       
        df=pd.DataFrame({'p1':p1,'p2':p2})
        df.at[df['p1']>=df['p2'],'max']=df['p1']
        df.at[df['p2']>=df['p1'],'max']=df['p2']
        alpha = -1 * df['max']
        return alpha[self.start_date_index:self.end_date_index]   

    # Alpha#88	 min(rank(decay_linear(((rank(open) + rank(low)) - (rank(high) + rank(close))),8.06882)), Ts_Rank(decay_linear(correlation(Ts_Rank(close, 8.44728), Ts_Rank(adv60,20.6966), 8.01266), 6.65053), 2.61957))
    def alpha088(self):
        adv60 = sma(self.volume, 60)
        p1=rank(decay_linear(((rank(self.open) + rank(self.low)) - (rank(self.high) + rank(self.close))).to_frame(),8))
        p2=ts_rank(decay_linear(correlation(ts_rank(self.close, 8), ts_rank(adv60,21), 8).to_frame(), 7), 3)
        df=pd.DataFrame({'p1':p1,'p2':p2})
        df.at[df['p1']>=df['p2'],'min']=df['p2']
        df.at[df['p2']>=df['p1'],'min']=df['p1']
        alpha = df['min']
        return alpha[self.start_date_index:self.end_date_index]
   
    # Alpha#89	 (Ts_Rank(decay_linear(correlation(((low * 0.967285) + (low * (1 - 0.967285))), adv10,6.94279), 5.51607), 3.79744) - Ts_Rank(decay_linear(delta(IndNeutralize(vwap,IndClass.industry), 3.48158), 10.1466), 15.3012))
    def alpha089(self):
        indaverage_vwap = IndustryAverage_vwap()
        indaverage_vwap = indaverage_vwap[indaverage_vwap.index.isin(self.available_dates)]        
        indaverage_vwap = indaverage_vwap[self.industry]
        indaverage_vwap = indaverage_vwap.reset_index(drop=True)
        indneutralized_vwap = self.vwap - indaverage_vwap 
        adv10 = sma(self.volume, 10)
        alpha = ts_rank(decay_linear(correlation(((self.low * 0.967285) + (self.low * (1 - 0.967285))), adv10, 7), 6), 4) - ts_rank(decay_linear(delta(indneutralized_vwap, 10)), 15)
        return alpha[self.start_date_index:self.end_date_index]
    
    # Alpha#90	 ((rank((close - ts_max(close, 4.66719)))^Ts_Rank(correlation(IndNeutralize(adv40,IndClass.subindustry), low, 5.38375), 3.21856)) * -1)
    def alpha090(self):
        indaverage_adv40 = IndustryAverage_adv(40)
        indaverage_adv40 = indaverage_adv40[indaverage_adv40.index.isin(self.available_dates)]        
        indaverage_adv40 = indaverage_adv40[self.industry]
        indaverage_adv40 = indaverage_adv40.reset_index(drop=True)   
        adv40 = sma(self.volume, 40)        
        indneutralized_adv40 = adv40 - indaverage_adv40  
        alpha = ((rank((self.close - ts_max(self.close, 5))) ** ts_rank(correlation(indneutralized_adv40, self.low, 5), 3)) * -1)
        return alpha[self.start_date_index:self.end_date_index]
     
    # Alpha#91	 ((Ts_Rank(decay_linear(decay_linear(correlation(IndNeutralize(close,IndClass.industry), volume, 9.74928), 16.398), 3.83219), 4.8667) -rank(decay_linear(correlation(vwap, adv30, 4.01303), 2.6809))) * -1)
    def alpha091(self):
        indaverage_close = IndustryAverage_close()
        indaverage_close = indaverage_close[indaverage_close.index.isin(self.available_dates)]        
        indaverage_close = indaverage_close[self.industry]
        indaverage_close = indaverage_close.reset_index(drop=True)
        indneutralized_close = self.close - indaverage_close              
        adv30 = sma(self.volume, 30)        
        alpha = ((ts_rank(decay_linear(decay_linear(correlation(indneutralized_close, self.volume, 10), 16), 4), 5) -rank(decay_linear(correlation(self.vwap, adv30, 4), 3))) * -1)
        return alpha[self.start_date_index:self.end_date_index]

    # Alpha#92	 min(Ts_Rank(decay_linear(((((high + low) / 2) + close) < (low + open)), 14.7221),18.8683), Ts_Rank(decay_linear(correlation(rank(low), rank(adv30), 7.58555), 6.94024),6.80584))
    def alpha092(self):
        adv30 = sma(self.volume, 30)
        p1=ts_rank(decay_linear(((((self.high + self.low) / 2) + self.close) < (self.low + self.open)).to_frame(), 15),19)
        p2=ts_rank(decay_linear(correlation(rank(self.low), rank(adv30), 8).to_frame(), 7),7)
        df=pd.DataFrame({'p1':p1,'p2':p2})
        df.at[df['p1']>=df['p2'],'min']=df['p2']
        df.at[df['p2']>=df['p1'],'min']=df['p1']
        alpha = df['min']
        return alpha[self.start_date_index:self.end_date_index]
    
    # Alpha#93	 (Ts_Rank(decay_linear(correlation(IndNeutralize(vwap, IndClass.industry), adv81,17.4193), 19.848), 7.54455) / rank(decay_linear(delta(((close * 0.524434) + (vwap * (1 -0.524434))), 2.77377), 16.2664)))
    def alpha093(self):
        indaverage_vwap = IndustryAverage_vwap()
        indaverage_vwap = indaverage_vwap[indaverage_vwap.index.isin(self.available_dates)]        
        indaverage_vwap = indaverage_vwap[self.industry]
        indaverage_vwap = indaverage_vwap.reset_index(drop=True)
        indneutralized_vwap = self.vwap - indaverage_vwap      
        adv81 = sma(self.volume, 81)
        alpha = (ts_rank(decay_linear(correlation(indneutralized_vwap, adv81, 17).to_frame(), 20), 8) / rank(decay_linear(delta(((self.close * 0.524434) + (self.vwap * (1 -0.524434))), 3).to_frame(), 16)))
        return alpha[self.start_date_index:self.end_date_index]
    
    # Alpha#94	 ((rank((vwap - ts_min(vwap, 11.5783)))^Ts_Rank(correlation(Ts_Rank(vwap,19.6462), Ts_Rank(adv60, 4.02992), 18.0926), 2.70756)) * -1)
    def alpha094(self):
        adv60 = sma(self.volume, 60)
        alpha = ((rank((self.vwap - ts_min(self.vwap, 12))).pow(ts_rank(correlation(ts_rank(self.vwap,20), ts_rank(adv60, 4), 18), 3)) * -1))
        return alpha[self.start_date_index:self.end_date_index]
    
    # Alpha#95	 (rank((open - ts_min(open, 12.4105))) < Ts_Rank((rank(correlation(sum(((high + low)/ 2), 19.1351), sum(adv40, 19.1351), 12.8742))^5), 11.7584))
    def alpha095(self):
        adv40 = sma(self.volume, 40)
        alpha = (rank((self.open - ts_min(self.open, 12))) < ts_rank((rank(correlation(sma(((self.high + self.low)/ 2), 19), sma(adv40, 19), 13)).pow(5)), 12))
        alpha = alpha * 1
        return alpha[self.start_date_index:self.end_date_index]
    
    # Alpha#96	 (max(Ts_Rank(decay_linear(correlation(rank(vwap), rank(volume), 3.83878),4.16783), 8.38151), Ts_Rank(decay_linear(Ts_ArgMax(correlation(Ts_Rank(close, 7.45404),Ts_Rank(adv60, 4.13242), 3.65459), 12.6556), 14.0365), 13.4143)) * -1)
    def alpha096(self):
        adv60 = sma(self.volume, 60)
        p1=ts_rank(decay_linear(correlation(rank(self.vwap), rank(self.volume).to_frame(), 4),4), 8)
        p2=ts_rank(decay_linear(ts_argmax(correlation(ts_rank(self.close, 7),ts_rank(adv60, 4), 4), 13).to_frame(), 14), 13)
        df=pd.DataFrame({'p1':p1,'p2':p2})
        df.at[df['p1']>=df['p2'],'max']=df['p1']
        df.at[df['p2']>=df['p1'],'max']=df['p2']
        alpha = -1*df['max']
        #alpha = (max(ts_rank(decay_linear(correlation(rank(self.vwap), rank(self.volume).to_frame(), 4),4).CLOSE, 8), ts_rank(decay_linear(ts_argmax(correlation(ts_rank(self.close, 7),ts_rank(adv60, 4), 4), 13).to_frame(), 14).CLOSE, 13)) * -1)
        return alpha[self.start_date_index:self.end_date_index]
    
    # Alpha#97	 ((rank(decay_linear(delta(IndNeutralize(((low * 0.721001) + (vwap * (1 - 0.721001))),IndClass.industry), 3.3705), 20.4523)) - Ts_Rank(decay_linear(Ts_Rank(correlation(Ts_Rank(low,7.87871), Ts_Rank(adv60, 17.255), 4.97547), 18.5925), 15.7152), 6.71659)) * -1)
    def alpha097(self):
        indaverage_data = IndustryAverage_PreparationForAlpha097()
        indaverage_data = indaverage_data[indaverage_data.index.isin(self.available_dates)]        
        indaverage_data = indaverage_data[self.industry]
        indaverage_data = indaverage_data.reset_index(drop=True)
        unindneutralized_data = (self.low * 0.721001) + (self.vwap * (1 - 0.721001))
        indneutralized_data = unindneutralized_data - indaverage_data   
        adv60 = sma(self.volume, 60)
        alpha = ((rank(decay_linear(delta(indneutralized_data, 3).to_frame(), 20)) - ts_rank(decay_linear(ts_rank(correlation(ts_rank(self.low,8), ts_rank(adv60, 17), 5), 19).to_frame(), 16), 7)) * -1)
        return alpha[self.start_date_index:self.end_date_index]
    
    # Alpha#98	 (rank(decay_linear(correlation(vwap, sum(adv5, 26.4719), 4.58418), 7.18088)) -rank(decay_linear(Ts_Rank(Ts_ArgMin(correlation(rank(open), rank(adv15), 20.8187), 8.62571),6.95668), 8.07206)))
    def alpha098(self):
        adv5 = sma(self.volume, 5)
        adv15 = sma(self.volume, 15)
        alpha = (rank(decay_linear(correlation(self.vwap, sma(adv5, 26), 5).to_frame(), 7)) -rank(decay_linear(ts_rank(ts_argmin(correlation(rank(self.open), rank(adv15), 21), 9),7).to_frame(), 8)))
        return alpha[self.start_date_index:self.end_date_index]
    
    # Alpha#99	 ((rank(correlation(sum(((high + low) / 2), 19.8975), sum(adv60, 19.8975), 8.8136)) <rank(correlation(low, volume, 6.28259))) * -1)
    def alpha099(self):
        adv60 = sma(self.volume, 60)
        alpha = ((rank(correlation(ts_sum(((self.high + self.low) / 2), 20), ts_sum(adv60, 20), 9)) <rank(correlation(self.low, self.volume, 6))) * -1)
        return alpha[self.start_date_index:self.end_date_index]
    
    # Alpha#100	 (0 - (1 * (((1.5 * scale(indneutralize(indneutralize(rank(((((close - low) - (high -close)) / (high - low)) * volume)), IndClass.subindustry), IndClass.subindustry))) -scale(indneutralize((correlation(close, rank(adv20), 5) - rank(ts_argmin(close, 30))),IndClass.subindustry))) * (volume / adv20))))
    def alpha100(self):
        indaverage_data_1 = IndustryAverage_PreparationForAlpha100_1()
        indaverage_data_1 = indaverage_data_1[indaverage_data_1.index.isin(self.available_dates)]        
        indaverage_data_1 = indaverage_data_1[self.industry]
        indaverage_data_1 = indaverage_data_1.reset_index(drop=True)
        unindneutralized_data_1 = rank(((((self.close - self.low) - (self.high - self.close)) / (self.high - self.low)) * self.volume))
        indneutralized_data_1 = unindneutralized_data_1 - indaverage_data_1  #there's a problem in calculation here.         
        indaverage_data_2 = IndustryAverage_PreparationForAlpha100_2()
        indaverage_data_2 = indaverage_data_2[indaverage_data_2.index.isin(self.available_dates)]        
        indaverage_data_2 = indaverage_data_2[self.industry]
        indaverage_data_2 = indaverage_data_2.reset_index(drop=True)
        adv20 = sma(self.volume, 20)
        unindneutralized_data_2 = (correlation(self.close, rank(adv20), 5) - rank(ts_argmin(self.close, 30)))
        indneutralized_data_2 = unindneutralized_data_2 - indaverage_data_2 
        alpha = (0 - (1 * (((1.5 * scale(indneutralized_data_1))-scale(indneutralized_data_2)) * (self.volume / adv20))))
        return alpha[self.start_date_index:self.end_date_index]

    # Alpha#101	 ((close - open) / ((high - low) + .001))
    def alpha101(self):
        alpha = (self.close - self.open) /((self.high - self.low) + 0.001)
        return alpha[self.start_date_index:self.end_date_index]



class GTJAalphas(object):
    def __init__(self, ts_code, start_date=20210101, end_date=20211231):
        
        self.ts_code = ts_code
        self.start_date = start_date
        self.end_date = end_date
        
        if ts_code == "All":  #需要同时计算多股的alpha时, 将数据一次性取出以加快运行速度
            quotations_daily=local_source.get_quotations_daily(cols='TRADE_DATE,TS_CODE,OPEN,CLOSE,LOW,HIGH,VOL,CHANGE,AMOUNT').sort_values(by="TRADE_DATE", ascending=True) 
            stock_indicators_daily=local_source.get_stock_indicators_daily(cols='TRADE_DATE,TS_CODE,TOTAL_SHARE').sort_values(by="TRADE_DATE", ascending=True) 
        else:
            quotations_daily=local_source.get_quotations_daily(cols='TRADE_DATE,TS_CODE,OPEN,CLOSE,LOW,HIGH,VOL,CHANGE,AMOUNT',condition="TS_CODE = " + "'" + ts_code + "'").sort_values(by="TRADE_DATE", ascending=True) 
            stock_indicators_daily=local_source.get_stock_indicators_daily(cols='TRADE_DATE,TS_CODE,TOTAL_SHARE',condition="TS_CODE = " + "'" + ts_code + "'").sort_values(by="TRADE_DATE", ascending=True) 
        
        self.stock_data=pd.merge(quotations_daily, stock_indicators_daily,on=['TRADE_DATE','TS_CODE'],how="left") 
        self.stock_data=self.stock_data.applymap(lambda x: np.nan if x=="NULL" else x)
        self.stock_data["TOTAL_MV"]=self.stock_data["TOTAL_SHARE"]*self.stock_data["CLOSE"]
        self.stock_data["TRADE_DATE"]=self.stock_data["TRADE_DATE"].astype(int)


    def initializer(self, ts_code_chosen=0):
        if self.ts_code == 'All':
            self.stock_data_chosen = self.stock_data[self.stock_data["TS_CODE"]==ts_code_chosen].reset_index(drop=True)
        else:
            self.stock_data_chosen = self.stock_data
            ts_code_chosen = self.ts_code
            
        self.open = self.stock_data_chosen['OPEN'] 
        self.high = self.stock_data_chosen['HIGH'] 
        self.low = self.stock_data_chosen['LOW']   
        self.close = self.stock_data_chosen['CLOSE'] 
        self.volume = self.stock_data_chosen['VOL']*100 
        self.amount = self.stock_data_chosen["AMOUNT"]
        self.returns = self.stock_data_chosen['CHANGE'] / self.stock_data_chosen['OPEN']  
        self.vwap = (self.stock_data_chosen['AMOUNT']*1000)/(self.stock_data_chosen['VOL']*100+1) 
        self.cap = self.stock_data_chosen['TOTAL_MV']
        self.industry = local_source.get_stock_list(cols='TS_CODE,INDUSTRY', condition='TS_CODE = '+'"'+ts_code_chosen+'"')['INDUSTRY'].iloc[0]
        self.available_dates = self.stock_data_chosen["TRADE_DATE"]
        self.output_dates = self.stock_data_chosen[(self.stock_data_chosen["TRADE_DATE"]>=self.start_date)*(self.stock_data_chosen["TRADE_DATE"]<=self.end_date)]["TRADE_DATE"]
        start_available_date = self.output_dates.iloc[0]  #这个是交易日
        end_available_date = self.output_dates.iloc[-1]        
        self.start_date_index = self.stock_data_chosen["TRADE_DATE"][self.stock_data_chosen["TRADE_DATE"].values == start_available_date].index[0]
        self.end_date_index = self.stock_data_chosen["TRADE_DATE"][self.stock_data_chosen["TRADE_DATE"].values == end_available_date].index[0] +1
        
        if ts_code_chosen[-2:]=='SZ': index_code = "399001.SZ"
        else: index_code = "000001.SH"
        indices_daily_chosen=local_source.get_indices_daily(cols='TRADE_DATE,INDEX_CODE,OPEN,CLOSE',condition='INDEX_CODE = '+'"'+index_code+'"').sort_values(by="TRADE_DATE", ascending=True)
        indices_daily_chosen=indices_daily_chosen.applymap(lambda x: np.nan if x=="NULL" else x)
        indices_daily_chosen["TRADE_DATE"]=indices_daily_chosen["TRADE_DATE"].astype(int)
        indices_daily_chosen = pd.merge(self.stock_data_chosen["TRADE_DATE"], indices_daily_chosen, on=['TRADE_DATE'], how="left")
        self.benchmarkindexopen = indices_daily_chosen['OPEN'] 
        self.benchmarkindexclose = indices_daily_chosen['CLOSE'] 
    
    #Alpha1 (-1 * CORR(RANK(DELTA(LOG(VOLUME), 1)), RANK(((CLOSE - OPEN) / OPEN)), 6))
    def GTJAalpha001(self):
        alpha = -1 * correlation(rank(delta(np.log(self.volume),1)),rank(((self.close-self.open)/self.open)), 6)
        return alpha[self.start_date_index:self.end_date_index]
    
    #Alpha2 (-1 * DELTA((((CLOSE - LOW) - (HIGH - CLOSE)) / (HIGH - LOW)), 1))
    def GTJAalpha002(self):
        alpha = -1 * delta((((self.close - self.low) - (self.high - self.close)) / (self.high - self.low)), 1)
        return alpha[self.start_date_index:self.end_date_index]
    
    #Alpha3 SUM((CLOSE=DELAY(CLOSE,1)?0:CLOSE-(CLOSE>DELAY(CLOSE,1)?MIN(LOW,DELAY(CLOSE,1)):MAX(HIGH,DELAY(CLOSE,1)))),6)
    def GTJAalpha003(self):
        delay1 = self.close.shift()        
        condition1 = (self.close > delay1)
        inner1_true = np.minimum(self.low, delay1)
        inner1_false = np.maximum(self.low, delay1)
        inner1 = pd.Series(np.where(condition1, inner1_true, inner1_false))
        condition2 = (self.close == delay1)
        inner2_true = pd.Series(np.zeros(len(condition2)))
        inner2_false = self.close - inner1
        inner2 = pd.Series(np.where(condition2, inner2_true, inner2_false))
        alpha = ts_sum(inner2, 6)
        return alpha[self.start_date_index:self.end_date_index]
    
    #Alpha4 ((((SUM(CLOSE, 8) / 8) + STD(CLOSE, 8)) < (SUM(CLOSE, 2) / 2)) ? (-1 * 1) : (((SUM(CLOSE, 2) / 2) < ((SUM(CLOSE, 8) / 8) - STD(CLOSE, 8))) ? 1 : (((1 < (VOLUME / MEAN(VOLUME,20))) || ((VOLUME / MEAN(VOLUME,20)) == 1)) ? 1 : (-1 * 1))))
    def GTJAalpha004(self):
        condition1 = ((1 < (self.volume / sma(self.volume,20))) | ((self.volume / sma(self.volume,20)) == 1))
        inner1_true = pd.Series(np.ones(len(condition1)))
        inner1_false = -1 * pd.Series(np.ones(len(condition1)))
        inner1 = pd.Series(np.where(condition1, inner1_true, inner1_false))
        condition2 = ((ts_sum(self.close, 2) / 2) < ((ts_sum(self.close, 8) / 8) - stddev(self.close, 8)))
        inner2_true = -1 * pd.Series(np.ones(len(condition2)))
        inner2_false = inner1
        alpha = pd.Series(np.where(condition2, inner2_true, inner2_false))
        return alpha[self.start_date_index:self.end_date_index]
    
    #Alpha5 (-1 * TSMAX(CORR(TSRANK(VOLUME, 5), TSRANK(HIGH, 5), 5), 3))
    def GTJAalpha005(self):
        alpha = -1 * ts_max(correlation(ts_rank(self.volume,5), ts_rank(self.high,5), 5) ,3)
        return alpha[self.start_date_index:self.end_date_index]
    
    #Alpha6 (RANK(SIGN(DELTA((((OPEN * 0.85) + (HIGH * 0.15))), 4)))* -1)
    def GTJAalpha006(self):
        alpha = rolling_rank(sign(delta((((self.open * 0.85) + (self.high * 0.15))), 4)))* -1
        return alpha[self.start_date_index:self.end_date_index]
    
    #Alpha7 ((RANK(MAX((VWAP - CLOSE), 3)) + RANK(MIN((VWAP - CLOSE), 3))) * RANK(DELTA(VOLUME, 3)))
    def GTJAalpha007(self):
        alpha = (rolling_rank(np.maximum((self.vwap - self.close), 3)) + rolling_rank(np.minimum((self.vwap - self.close), 3))) * rolling_rank(delta(self.volume, 3))
        return alpha[self.start_date_index:self.end_date_index]
    
    #Alpha8 RANK(DELTA(((((HIGH + LOW) / 2) * 0.2) + (VWAP * 0.8)), 4) * -1)
    def GTJAalpha008(self):
        alpha = rolling_rank(delta(((((self.high + self.low) / 2) * 0.2) + (self.vwap * 0.8)), 4) * -1)
        return alpha[self.start_date_index:self.end_date_index]
    
    #Alpha9 SMA(((HIGH+LOW)/2-(DELAY(HIGH,1)+DELAY(LOW,1))/2)*(HIGH-LOW)/VOLUME,7,2)
    def GTJAalpha009(self):
        alpha = ema(((self.high+self.low)/2-(delay(self.high,1)+delay(self.low,1))/2)*(self.high-self.low)/self.volume,7,2)
        return alpha[self.start_date_index:self.end_date_index]        
    
    #Alpha10 (RANK(MAX(((RET < 0) ? STD(RET, 20) : CLOSE)^2),5))
    def GTJAalpha010(self):
        condition1 = (self.returns < 0)
        inner1_true = stddev(self.returns, 20)
        inner1_false = self.close
        inner1 = pd.Series(np.where(condition1, inner1_true, inner1_false))
        alpha = rolling_rank(np.maximum(inner1**2, 5))        
        return alpha[self.start_date_index:self.end_date_index]
    
    #Alpha11 SUM(((CLOSE-LOW)-(HIGH-CLOSE))./(HIGH-LOW).*VOLUME,6)
    def GTJAalpha011(self):
        alpha = ts_sum(((self.close-self.low)-(self.high-self.close))/(self.high-self.low)*self.volume,6)
        return alpha[self.start_date_index:self.end_date_index]
    
    #Alpha12 (RANK((OPEN - (SUM(VWAP, 10) / 10)))) * (-1 * (RANK(ABS((CLOSE - VWAP)))))
    def GTJAalpha012(self):
        alpha = rolling_rank((self.open - (ts_sum(self.vwap, 10) / 10))) * -1 * (rolling_rank(abs((self.close - self.vwap))))
        return alpha[self.start_date_index:self.end_date_index]
    
    #Alpha13 (((HIGH * LOW)^0.5) - VWAP)
    def GTJAalpha013(self):
        alpha = ((self.high * self.low)**0.5) - self.vwap
        return alpha[self.start_date_index:self.end_date_index]
    
    #Alpha14 CLOSE-DELAY(CLOSE,5)
    def GTJAalpha014(self):
        alpha = self.close - delay(self.close,5)
        return alpha[self.start_date_index:self.end_date_index]
    
    #Alpha15 OPEN/DELAY(CLOSE,1)-1
    def GTJAalpha015(self):
        alpha = (self.open/delay(self.close, 1)) -1
        return alpha[self.start_date_index:self.end_date_index]
    
    #Alpha16 (-1 * TSMAX(RANK(CORR(RANK(VOLUME), RANK(VWAP), 5)), 5))
    def GTJAalpha016(self):
        alpha = -1 * ts_max(rolling_rank(correlation(rolling_rank(self.volume), rolling_rank(self.vwap), 5)), 5)
        return alpha[self.start_date_index:self.end_date_index]
    
    #Alpha17 RANK((VWAP - MAX(VWAP, 15)))^DELTA(CLOSE, 5)
    def GTJAalpha017(self):
        alpha = rolling_rank((self.vwap - np.maximum(self.vwap, 15)), window=2)**delta(self.close, 5)
        return alpha[self.start_date_index:self.end_date_index]       
    
    #Alpha18 CLOSE/DELAY(CLOSE,5)
    def GTJAalpha018(self):
        alpha = self.close / delay(self.close, 5)
        return alpha[self.start_date_index:self.end_date_index]
    
    #Alpha19 (CLOSE<DELAY(CLOSE,5)?(CLOSE-DELAY(CLOSE,5))/DELAY(CLOSE,5):(CLOSE=DELAY(CLOSE,5)?0:(CLOSE-DELAY(CLOSE,5))/CLOSE))
    def GTJAalpha019(self):
        condition1 = (self.close == delay(self.close,5))
        inner1_true=pd.Series(np.zeros(len(condition1)))
        inner1_false=(self.close-delay(self.close,5))/self.close
        inner1 = pd.Series(np.where(condition1, inner1_true, inner1_false))        
        condition2 = (self.close<delay(self.close,5))
        inner2_true = (self.close-delay(self.close,5))/delay(self.close,5)
        inner2_false = inner1
        alpha = pd.Series(np.where(condition2, inner2_true, inner2_false))
        return alpha[self.start_date_index:self.end_date_index]
        
    #Alpha20 (CLOSE-DELAY(CLOSE,6))/DELAY(CLOSE,6)*100
    def GTJAalpha020(self):
        alpha = (self.close-delay(self.close,6)) / delay(self.close,6) *100
        return alpha[self.start_date_index:self.end_date_index]
    
    #Alpha21 REGBETA(MEAN(CLOSE,6),SEQUENCE(6))
    def GTJAalpha021(self):     #I'm not sure if I've understood the formula correctly.
        y = sma(self.close, 6)
        alpha = pd.Series(np.nan, index=self.close.index)
        for i in range(6-1,len(y)):
            alpha.iloc[i]=sp.stats.linregress(pd.Series(np.arange(1,7)), y[i-6+1:i+1])[0]        
        return alpha[self.start_date_index:self.end_date_index]
    
    #Alpha22 SMEAN(((CLOSE-MEAN(CLOSE,6))/MEAN(CLOSE,6)-DELAY((CLOSE-MEAN(CLOSE,6))/MEAN(CLOSE,6),3)),12,1)
    def GTJAalpha022(self):
        alpha = ema(((self.close-sma(self.close,6))/sma(self.close,6)-delay((self.close-sma(self.close,6))/sma(self.close,6),3)),12,1)
        return alpha[self.start_date_index:self.end_date_index]
    
    #Alpha23 SMA((CLOSE>DELAY(CLOSE,1)?STD(CLOSE,20):0),20,1) / (SMA((CLOSE>DELAY(CLOSE,1)?STD(CLOSE,20):0),20,1)+SMA((CLOSE<=DELAY(CLOSE,1)?STD(CLOSE,20):0),20,1))*100
    def GTJAalpha023(self):
        condition1 = (self.close > delay(self.close,1))
        inner1_true= stddev(self.close, 20)
        inner1_false = pd.Series(np.zeros(len(condition1)))
        inner1 = pd.Series(np.where(condition1, inner1_true, inner1_false))        
        condition2 = (self.close <= delay(self.close,1))
        inner2_true= stddev(self.close, 20)
        inner2_false = pd.Series(np.zeros(len(condition2)))
        inner2 = pd.Series(np.where(condition2, inner2_true, inner2_false))                
        alpha = ema(inner1,20,1) / (ema(inner1,20,1)+ema(inner2,20,1))*100
        return alpha[self.start_date_index:self.end_date_index]
    
    #Alpha24 SMA(CLOSE-DELAY(CLOSE,5),5,1)
    def GTJAalpha024(self):
        alpha = ema(self.close-delay(self.close,5),5,1)
        return alpha[self.start_date_index:self.end_date_index]
        
    #Alpha25 ((-1 * RANK((DELTA(CLOSE, 7) * (1 - RANK(DECAYLINEAR((VOLUME / MEAN(VOLUME,20)), 9)))))) * (1 + RANK(SUM(RET, 250))))
    def GTJAalpha025(self):
        alpha = (-1 * rolling_rank((delta(self.close, 7) * (1 - rolling_rank(decay_linear((self.volume / sma(self.volume,20)), 9)))))) * (1 + rolling_rank(ts_sum(self.returns, 250)))
        return alpha[self.start_date_index:self.end_date_index]
    
    #Alpha26 ((((SUM(CLOSE, 7) / 7) - CLOSE)) + ((CORR(VWAP, DELAY(CLOSE, 5), 230))))
    def GTJAalpha026(self):
        alpha = (((ts_sum(self.close, 7) / 7) - self.close)) + ((correlation(self.vwap, delay(self.close, 5), 230)))
        return alpha[self.start_date_index:self.end_date_index]
    
    #Alpha27 WMA((CLOSE-DELAY(CLOSE,3))/DELAY(CLOSE,3)*100+(CLOSE-DELAY(CLOSE,6))/DELAY(CLOSE,6)*100,12)
    def GTJAalpha027(self):
        alpha = wma(( self.close-delay(self.close,3))/delay(self.close,3)*100 + (self.close-delay(self.close,6))/delay(self.close,6)*100 ,12)
        return alpha[self.start_date_index:self.end_date_index]
    
    #Alpha28 3*SMA((CLOSE-TSMIN(LOW,9))/(TSMAX(HIGH,9)-TSMIN(LOW,9))*100,3,1)-2*SMA(SMA((CLOSE-TSMIN(LOW,9))/(MAX(HIGH,9)-TSMAX(LOW,9))*100,3,1),3,1)   
    def GTJAalpha028(self):
        alpha = 3*ema((self.close-ts_min(self.low,9))/(ts_max(self.high,9)-ts_min(self.low,9))*100,3,1)-2*ema(ema((self.close-ts_min(self.low,9))/(ts_max(self.high,9)-ts_min(self.low,9))*100,3,1),3,1) 
        return alpha[self.start_date_index:self.end_date_index]
    
    #Alpha29 (CLOSE-DELAY(CLOSE,6))/DELAY(CLOSE,6)*VOLUME
    def GTJAalpha029(self):
        alpha = (self.close-delay(self.close,6))/delay(self.close,6)*self.volume
        return alpha[self.start_date_index:self.end_date_index]
    
    #Alpha30 WMA((REGRESI(CLOSE/DELAY(CLOSE)-1,MKT,SMB,HML，60))^2,20)
    def GTJAalpha030(self):
        y = (self.close/delay(self.close)) -1
        y.rename("y",inplace=True)
        y = pd.concat([self.available_dates, y],axis=1)
        MKT = self.benchmarkindexclose.pct_change()
        MKT.rename("MKT", inplace=True)
        MKT = pd.concat([self.available_dates,MKT],axis=1)
        FFfactor_data=pd.read_csv("analysis/FFfactors_daily.csv")
        FFfactor_data_needed = FFfactor_data[["TRADE_DATE","SMB","HML"]]        
        dt = pd.merge(y, MKT, on=['TRADE_DATE'], how="left")
        dt = pd.merge(dt, FFfactor_data_needed, on=['TRADE_DATE'], how="left")
        dt["const"]=1
        result = pd.Series(np.nan, index=dt.index)
        for i in range(60-1,len(y)):
            dt_piece = dt[i-60+1:i+1]
            dt_piece= dt_piece.dropna()
            y = dt_piece["y"]
            x = dt_piece[["MKT","SMB","HML","const"]]
            if len(y)!=0:
                model = sm.OLS(y,x)
                result.iloc[i] = model.fit().params.loc["const"]    
        print((result)**2)
        alpha = wma((result)**2,20)    
        return alpha[self.start_date_index:self.end_date_index]
    
    #Alpha31 (CLOSE-MEAN(CLOSE,12))/MEAN(CLOSE,12)*100
    def GTJAalpha031(self):
        alpha = (self.close-sma(self.close,12))/sma(self.close,12)*100
        return alpha[self.start_date_index:self.end_date_index]
    
    #Alpha32 (-1 * SUM(RANK(CORR(RANK(HIGH), RANK(VOLUME), 3)), 3))
    def GTJAalpha032(self):
        alpha = -1 * ts_sum(rolling_rank(correlation(rolling_rank(self.high), rolling_rank(self.volume), 3)), 3)
        return alpha[self.start_date_index:self.end_date_index]
    
    #Alpha33 ((((-1 * TSMIN(LOW, 5)) + DELAY(TSMIN(LOW, 5), 5)) * RANK(((SUM(RET, 240) - SUM(RET, 20)) / 220))) * TSRANK(VOLUME, 5))
    def GTJAalpha033(self):
        alpha = (((-1 * ts_min(self.low, 5)) + delay(ts_min(self.low, 5), 5)) * rolling_rank(((ts_sum(self.returns, 240) - ts_sum(self.returns, 20)) / 220))) * ts_rank(self.volume, 5)
        return alpha[self.start_date_index:self.end_date_index]
    
    #Alpha34 MEAN(CLOSE,12)/CLOSE
    def GTJAalpha034(self):
        alpha = sma(self.close,12) / self.close
        return alpha[self.start_date_index:self.end_date_index]
    
    #Alpha35 (MIN(RANK(DECAYLINEAR(DELTA(OPEN, 1), 15)), RANK(DECAYLINEAR(CORR((VOLUME), ((OPEN * 0.65) + (OPEN *0.35)), 17),7))) * -1)
    def GTJAalpha035(self):
        alpha = np.minimum(rolling_rank(decay_linear(delta(self.open, 1), 15)), rolling_rank(decay_linear(correlation((self.volume), ((self.open * 0.65) + (self.open *0.35)), 17),7))) * -1
        return alpha[self.start_date_index:self.end_date_index]
    
    #Alpha36 RANK(SUM(CORR(RANK(VOLUME), RANK(VWAP)), 6), 2) 
    def GTJAalpha036(self):
        alpha = rolling_rank(ts_sum(correlation(rolling_rank(self.volume), rolling_rank(self.vwap)), 6), 2) 
        return alpha[self.start_date_index:self.end_date_index]
    
    #Alpha37 (-1 * RANK(((SUM(OPEN, 5) * SUM(RET, 5)) - DELAY((SUM(OPEN, 5) * SUM(RET, 5)), 10)))) 
    def GTJAalpha037(self):
        alpha = -1 * rolling_rank(((ts_sum(self.open, 5) * ts_sum(self.returns, 5)) - delay((ts_sum(self.open, 5) * ts_sum(self.returns, 5)), 10)))
        return alpha[self.start_date_index:self.end_date_index]
    
    #Alpha38 (((SUM(HIGH, 20) / 20) < HIGH) ? (-1 * DELTA(HIGH, 2)) : 0) 
    def GTJAalpha038(self):
        condition1 = ((ts_sum(self.high, 20) / 20) < self.high)
        inner1_true= -1 * delta(self.high, 2)
        inner1_false = pd.Series(np.zeros(len(condition1)))
        alpha = pd.Series(np.where(condition1, inner1_true, inner1_false))         
        return alpha[self.start_date_index:self.end_date_index]
        
    #Alpha39 ((RANK(DECAYLINEAR(DELTA((CLOSE), 2),8)) - RANK(DECAYLINEAR(CORR(((VWAP * 0.3) + (OPEN * 0.7)), SUM(MEAN(VOLUME,180), 37), 14), 12))) * -1) 
    def GTJAalpha039(self):
        alpha = ((rolling_rank(decay_linear(delta((self.close), 2),8)) - rolling_rank(decay_linear(correlation(((self.vwap * 0.3) + (self.open * 0.7)), ts_sum(sma(self.volume,180), 37), 14), 12))) * -1) 
        return alpha
    
    #Alpha40 SUM((CLOSE>DELAY(CLOSE,1)?VOLUME:0),26)/SUM((CLOSE<=DELAY(CLOSE,1)?VOLUME:0),26)*100 
    def GTJAalpha040(self):
        condition1 = (self.close > delay(self.close,1))
        inner1_true= self.volume
        inner1_false = pd.Series(np.zeros(len(condition1)))
        inner1 = pd.Series(np.where(condition1, inner1_true, inner1_false))        
        condition2 = (self.close <= delay(self.close,1))
        inner2_true= self.volume
        inner2_false = pd.Series(np.zeros(len(condition2)))
        inner2 = pd.Series(np.where(condition2, inner2_true, inner2_false))                    
        alpha = ts_sum(inner1,26) / ts_sum(inner2,26)*100 
        return alpha[self.start_date_index:self.end_date_index]
    
    #Alpha41 (RANK(MAX(DELTA((VWAP), 3), 5))* -1) 
    def GTJAalpha041(self):
        alpha = rolling_rank(np.maximum(delta((self.vwap), 3), 5))* -1
        return alpha[self.start_date_index:self.end_date_index]
    
    #Alpha42 ((-1 * RANK(STD(HIGH, 10))) * CORR(HIGH, VOLUME, 10)) 
    def GTJAalpha042(self):
        alpha = (-1 * rolling_rank(stddev(self.high, 10))) * correlation(self.high, self.volume, 10)
        return alpha[self.start_date_index:self.end_date_index]
    
    #Alpha43 SUM((CLOSE>DELAY(CLOSE,1)?VOLUME:(CLOSE<DELAY(CLOSE,1)?-VOLUME:0)),6)
    def GTJAalpha043(self):
        condition1 = (self.close < delay(self.close,1))
        inner1_true = -1* self.volume
        inner1_false = pd.Series(np.zeros(len(condition1)))           
        inner1 = pd.Series(np.where(condition1, inner1_true, inner1_false)) 
        condition2 = (self.close > delay(self.close,1))
        inner2_true = self.volume        
        inner2_false = inner1
        inner2 = pd.Series(np.where(condition2, inner2_true, inner2_false))         
        alpha = ts_sum(inner2,6)
        return alpha[self.start_date_index:self.end_date_index]
        
    #Alpha44 (TSRANK(DECAYLINEAR(CORR(((LOW )), MEAN(VOLUME,10), 7), 6),4) + TSRANK(DECAYLINEAR(DELTA((VWAP), 3), 10), 15))
    def GTJAalpha044(self):
        alpha = ts_rank(decay_linear(correlation(self.low, sma(self.volume,10), 7), 6),4) + ts_rank(decay_linear(delta((self.vwap), 3), 10), 15)
        return alpha[self.start_date_index:self.end_date_index]
    
    #Alpha45 (RANK(DELTA((((CLOSE * 0.6) + (OPEN *0.4))), 1)) * RANK(CORR(VWAP, MEAN(VOLUME,150), 15)))
    def GTJAalpha045(self):
        alpha = rolling_rank(delta((((self.close * 0.6) + (self.open *0.4))), 1)) * rolling_rank(correlation(self.vwap, sma(self.volume,150), 15))
        return alpha[self.start_date_index:self.end_date_index]
    
    #Alpha46 (MEAN(CLOSE,3)+MEAN(CLOSE,6)+MEAN(CLOSE,12)+MEAN(CLOSE,24))/(4*CLOSE)
    def GTJAalpha046(self):
        alpha = (sma(self.close,3)+sma(self.close,6)+sma(self.close,12)+sma(self.close,24))/(4*self.close)
        return alpha[self.start_date_index:self.end_date_index]
    
    #Alpha47 SMA((TSMAX(HIGH,6)-CLOSE)/(TSMAX(HIGH,6)-TSMIN(LOW,6))*100,9,1)
    def GTJAalpha047(self):
        alpha = ema((ts_max(self.high,6)-self.close)/(ts_max(self.high,6)-ts_min(self.low,6))*100,9,1)
        alpha = alpha * 1
        return alpha[self.start_date_index:self.end_date_index]
    
    #Alpha48 (-1*((RANK(((SIGN((CLOSE - DELAY(CLOSE, 1))) + SIGN((DELAY(CLOSE, 1) - DELAY(CLOSE, 2)))) + SIGN((DELAY(CLOSE, 2) - DELAY(CLOSE, 3)))))) * SUM(VOLUME, 5)) / SUM(VOLUME, 20))
    def GTJAalpha048(self):
        alpha = -1*((rolling_rank(((sign((self.close - delay(self.close, 1))) + sign((delay(self.close, 1) - delay(self.close, 2)))) + sign((delay(self.close, 2) - delay(self.close, 3)))))) * ts_sum(self.volume, 5)) / ts_sum(self.volume, 20)
        return alpha[self.start_date_index:self.end_date_index]
    
    #Alpha49 SUM(((HIGH+LOW)>=(DELAY(HIGH,1)+DELAY(LOW,1))?0:MAX(ABS(HIGH-DELAY(HIGH,1)),ABS(LOW-DELAY(LOW,1)))),12)/(SUM(((HIGH+LOW)>=(DELAY(HIGH,1)+DELAY(LOW,1))?0:MAX(ABS(HIGH-DELAY(HIGH,1)),ABS(LOW-DELAY(LOW,1)))),12)+SUM(((HIGH+LOW)<=(DELAY(HIGH,1)+DELAY(LOW,1))?0:MAX(ABS(HIGH-DELAY(HIGH,1)),ABS(LOW-DELAY(LOW,1)))),12))    
    def GTJAalpha049(self):
        condition1 = ((self.high+self.low)>=(delay(self.high,1)+delay(self.low,1)))
        inner1_true = pd.Series(np.zeros(len(condition1)))
        inner1_false = np.maximum(abs(self.high-delay(self.high,1)),abs(self.low-delay(self.low,1)))
        inner1 = pd.Series(np.where(condition1, inner1_true, inner1_false)) 
        condition2 = ((self.high+self.low)<=(delay(self.high,1)+delay(self.low,1)))
        inner2 = pd.Series(np.where(condition2, inner1_true, inner1_false))        
        alpha = ts_sum(inner1,12) / (ts_sum(inner1,12)+ts_sum(inner2,12))
        return alpha[self.start_date_index:self.end_date_index]
        
    #Alpha50 SUM(((HIGH+LOW)<=(DELAY(HIGH,1)+DELAY(LOW,1))?0:MAX(ABS(HIGH-DELAY(HIGH,1)),ABS(LOW-DELAY(LOW,1)))),12)/(SUM(((HIGH+LOW)<=(DELAY(HIGH,1)+DELAY(LOW,1))?0:MAX(ABS(HIGH-DELAY(HIGH,1)),ABS(LOW-DELAY(LOW,1)))),12)+SUM(((HIGH+LOW)>=(DELAY(HIGH,1)+DELAY(LOW,1))?0:MAX(ABS(HIGH-DELAY(HI GH,1)),ABS(LOW-DELAY(LOW,1)))),12))-SUM(((HIGH+LOW)>=(DELAY(HIGH,1)+DELAY(LOW,1))?0:MAX(ABS(HI GH-DELAY(HIGH,1)),ABS(LOW-DELAY(LOW,1)))),12)/(SUM(((HIGH+LOW)>=(DELAY(HIGH,1)+DELAY(LOW,1))?0: MAX(ABS(HIGH-DELAY(HIGH,1)),ABS(LOW-DELAY(LOW,1)))),12)+SUM(((HIGH+LOW)<=(DELAY(HIGH,1)+DELA Y(LOW,1))?0:MAX(ABS(HIGH-DELAY(HIGH,1)),ABS(LOW-DELAY(LOW,1)))),12)) 
    def GTJAalpha050(self):
        condition1 = ((self.high+self.low) >= (delay(self.high,1)+delay(self.low,1)))
        inner1_true = pd.Series(np.zeros(len(condition1)))
        inner1_false = np.maximum(abs(self.high-delay(self.high,1)),abs(self.low-delay(self.low,1)))
        inner1 = pd.Series(np.where(condition1, inner1_true, inner1_false))        
        condition2 = ((self.high+self.low) <= (delay(self.high,1)+delay(self.low,1)))
        inner2 = pd.Series(np.where(condition2, inner1_true, inner1_false))  
        alpha = (ts_sum(inner2,12)-ts_sum(inner1,12))/(ts_sum(inner2,12)+ts_sum(inner1,12)) 
        return alpha[self.start_date_index:self.end_date_index]
    
    #Alpha51 SUM(((HIGH+LOW)<=(DELAY(HIGH,1)+DELAY(LOW,1))?0:MAX(ABS(HIGH-DELAY(HIGH,1)),ABS(LOW-DELAY(LOW,1)))),12)/(SUM(((HIGH+LOW)<=(DELAY(HIGH,1)+DELAY(LOW,1))?0:MAX(ABS(HIGH-DELAY(HIGH,1)),ABS(LOW-DELAY(LOW,1)))),12)+SUM(((HIGH+LOW)>=(DELAY(HIGH,1)+DELAY(LOW,1))?0:MAX(ABS(HIGH-DELAY(HI GH,1)),ABS(LOW-DELAY(LOW,1)))),12)) 
    def GTJAalpha051(self):
        condition1 = ((self.high+self.low) >= (delay(self.high,1)+delay(self.low,1)))
        inner1_true = pd.Series(np.zeros(len(condition1)))
        inner1_false = np.maximum(abs(self.high-delay(self.high,1)),abs(self.low-delay(self.low,1)))
        inner1 = pd.Series(np.where(condition1, inner1_true, inner1_false))        
        condition2 = ((self.high+self.low) <= (delay(self.high,1)+delay(self.low,1)))
        inner2 = pd.Series(np.where(condition2, inner1_true, inner1_false))  
        alpha = ts_sum(inner2,12) / (ts_sum(inner1,12)+ts_sum(inner2,12)) 
        return alpha[self.start_date_index:self.end_date_index]
    
    #Alpha52 SUM(MAX(0,HIGH-DELAY((HIGH+LOW+CLOSE)/3,1)),26)/SUM(MAX(0,DELAY((HIGH+LOW+CLOSE)/3,1)-LOW),26)* 100 
    def GTJAalpha052(self):
        alpha = ts_sum(np.maximum(0,self.high-delay((self.high+self.low+self.close)/3,1)),26)/ts_sum(np.maximum(0,delay((self.high+self.low+self.close)/3,1)-self.low),26)* 100
        return alpha[self.start_date_index:self.end_date_index]
    
    #Alpha53 COUNT(CLOSE>DELAY(CLOSE,1),12)/12*100 
    def GTJAalpha053(self):
        condition = (self.close>delay(self.close,1))
        count = pd.Series(np.nan, index=self.close.index)
        for i in range(12-1,len(condition)):
            count.iloc[i]=condition[i-12+1:i+1].sum()
        alpha = count / 12 * 100
        return alpha[self.start_date_index:self.end_date_index]
    
    #Alpha54 (-1 * RANK((STD(ABS(CLOSE - OPEN)) + (CLOSE - OPEN)) + CORR(CLOSE, OPEN,10))) 
    def GTJAalpha054(self):
        alpha = -1 * rolling_rank((stddev(abs(self.close - self.open)) + (self.close - self.open)) + correlation(self.close,self.open,10))
        return alpha[self.start_date_index:self.end_date_index]
    
    #Alpha55 SUM(16*(CLOSE-DELAY(CLOSE,1)+(CLOSE-OPEN)/2+DELAY(CLOSE,1)-DELAY(OPEN,1))/((ABS(HIGH-DELAY(CLOSE,1))>ABS(LOW-DELAY(CLOSE,1)) & ABS(HIGH-DELAY(CLOSE,1))>ABS(HIGH-DELAY(LOW,1))?ABS(HIGH-DELAY(CLOSE,1))+ABS(LOW-DELAY(CLOSE,1))/2+ABS(DELAY(CLOSE,1)-DELAY(OPEN,1))/4:(ABS(LOW-DELAY(CLOSE,1))>ABS(HIGH-DELAY(LOW,1)) & ABS(LOW-DELAY(CLOSE,1))>ABS(HIGH-DELAY(CLOSE,1))?ABS(LOW-DELAY(CLOSE,1))+ABS(HIGH-DELAY(CLO SE,1))/2+ABS(DELAY(CLOSE,1)-DELAY(OPEN,1))/4:ABS(HIGH-DELAY(LOW,1))+ABS(DELAY(CLOSE,1)-DELAY(OPEN,1))/4)))*MAX(ABS(HIGH-DELAY(CLOSE,1)),ABS(LOW-DELAY(CLOSE,1))),20) 
    def GTJAalpha055(self):
        condition1 = (abs(self.low-delay(self.close,1))>abs(self.high-delay(self.low,1))) & (abs(self.low-delay(self.close,1))>abs(self.high-delay(self.close,1)))
        inner1_true = abs(self.low-delay(self.close,1)) + abs(self.high-delay(self.close,1))/2 + abs(delay(self.close,1)-delay(self.open,1))/4
        inner1_false = abs(self.high-delay(self.low,1)) + abs(delay(self.close,1)-delay(self.open,1))/4
        inner1 = pd.Series(np.where(condition1, inner1_true, inner1_false))         
        condition2 = (abs(self.high-delay(self.close,1))>abs(self.low-delay(self.close,1))) & (abs(self.high-delay(self.close,1))>abs(self.high-delay(self.low,1)))
        inner2_true = abs(self.high-delay(self.close,1))+abs(self.low-delay(self.close,1))/2+abs(delay(self.close,1)-delay(self.open,1))/4
        inner2_false = inner1
        inner2 = pd.Series(np.where(condition2, inner2_true, inner2_false))  
        alpha = ts_sum(16*(self.close-delay(self.close,1)+(self.close-self.open)/2+delay(self.close,1)-delay(self.open,1))/(inner2)*np.maximum(abs(self.high-delay(self.close,1)),abs(self.low-delay(self.close,1))),20) 
        return alpha[self.start_date_index:self.end_date_index]
        
    #Alpha56 (RANK((OPEN - TSMIN(OPEN, 12))) < RANK((RANK(CORR(SUM(((HIGH + LOW) / 2), 19), SUM(MEAN(VOLUME,40), 19), 13))^5))) 
    def GTJAalpha056(self):
        alpha = rolling_rank((self.open - ts_min(self.open, 12))) < rolling_rank((rolling_rank(correlation(ts_sum(((self.high + self.low) / 2), 19), ts_sum(sma(self.volume,40), 19), 13))**5))
        return alpha[self.start_date_index:self.end_date_index]
    
    #Alpha57 SMA((CLOSE-TSMIN(LOW,9))/(TSMAX(HIGH,9)-TSMIN(LOW,9))*100,3,1) 
    def GTJAalpha057(self):
        alpha = ema((self.close-ts_min(self.low,9))/(ts_max(self.high,9)-ts_min(self.low,9))*100,3,1) 
        return alpha[self.start_date_index:self.end_date_index]
    
    #Alpha58 COUNT(CLOSE>DELAY(CLOSE,1),20)/20*10
    def GTJAalpha058(self):
        condition = (self.close>delay(self.close,1))
        count = pd.Series(np.nan, index=self.close.index)
        for i in range(20-1,len(condition)):
            count.iloc[i]=condition[i-20+1:i+1].sum()
        alpha = count / 20 * 10
        return alpha[self.start_date_index:self.end_date_index]
        
    #Alpha59 SUM((CLOSE=DELAY(CLOSE,1)?0:CLOSE-(CLOSE>DELAY(CLOSE,1)?MIN(LOW,DELAY(CLOSE,1)):MAX(HIGH,DELAY(CLOSE,1)))),20) 
    def GTJAalpha059(self):
        condition1 = self.close > delay(self.close,1)
        inner1_true = np.minimum(self.low,delay(self.close,1))
        inner1_false = np.maximum(self.high,delay(self.close,1))
        inner1 = pd.Series(np.where(condition1, inner1_true, inner1_false))     
        condition2 = (self.close == delay(self.close,1))
        inner2_true = pd.Series(np.zeros(len(condition2)))      
        inner2_false = self.close-inner1
        inner2 = pd.Series(np.where(condition2, inner2_true, inner2_false))            
        alpha = ts_sum(inner2, 20)
        return alpha[self.start_date_index:self.end_date_index]
    
    #Alpha60 SUM(((CLOSE-LOW)-(HIGH-CLOSE))./(HIGH-LOW).*VOLUME,20) 
    def GTJAalpha060(self):
        alpha = ts_sum(((self.close-self.low)-(self.high-self.close))/(self.high-self.low)*self.volume,20)
        return alpha[self.start_date_index:self.end_date_index]    
    
    #Alpha61 (MAX(RANK(DECAYLINEAR(DELTA(VWAP, 1), 12)), RANK(DECAYLINEAR(RANK(CORR((LOW),MEAN(VOLUME,80), 8)), 17))) * -1) 
    def GTJAalpha061(self):
        alpha = np.maximum(rolling_rank(decay_linear(delta(self.vwap, 1), 12)), rolling_rank(decay_linear(rolling_rank(correlation(self.low,sma(self.volume,80), 8)), 17))) * -1
        return alpha[self.start_date_index:self.end_date_index]
    
    #Alpha62 (-1 * CORR(HIGH, RANK(VOLUME), 5)) 
    def GTJAalpha062(self):
        alpha = -1 * correlation(self.high, rolling_rank(self.volume), 5)
        return alpha[self.start_date_index:self.end_date_index]
    
    #Alpha63 SMA(MAX(CLOSE-DELAY(CLOSE,1),0),6,1)/SMA(ABS(CLOSE-DELAY(CLOSE,1)),6,1)*100 
    def GTJAalpha063(self):
        alpha = ema(np.maximum(self.close-delay(self.close,1),0),6,1) / ema(abs(self.close-delay(self.close,1)),6,1)*100 
        return alpha[self.start_date_index:self.end_date_index]
    
    #Alpha64 (MAX(RANK(DECAYLINEAR(CORR(RANK(VWAP), RANK(VOLUME), 4), 4)), RANK(DECAYLINEAR(MAX(CORR(RANK(CLOSE), RANK(MEAN(VOLUME,60)), 4), 13), 14))) * -1) 
    def GTJAalpha064(self):
        alpha = np.maximum(rolling_rank(decay_linear(correlation(rolling_rank(self.vwap), rolling_rank(self.volume), 4), 4)), rolling_rank(decay_linear(np.maximum(correlation(rolling_rank(self.close), rolling_rank(sma(self.volume,60)), 4), 13), 14))) * -1
        return alpha[self.start_date_index:self.end_date_index]
    
    #Alpha65 MEAN(CLOSE,6)/CLOSE 
    def GTJAalpha065(self):
        alpha = sma(self.close,6)/self.close
        return alpha[self.start_date_index:self.end_date_index]
    
    #Alpha66 (CLOSE-MEAN(CLOSE,6))/MEAN(CLOSE,6)*100 
    def GTJAalpha066(self):
        alpha = (self.close-sma(self.close,6)) / sma(self.close,6) *100 
        return alpha[self.start_date_index:self.end_date_index]
    
    #Alpha67 SMA(MAX(CLOSE-DELAY(CLOSE,1),0),24,1)/SMA(ABS(CLOSE-DELAY(CLOSE,1)),24,1)*100 
    def GTJAalpha067(self):
        alpha = ema(np.maximum(self.close-delay(self.close,1),0),24,1) / ema(abs(self.close-delay(self.close,1)),24,1) *100 
        return alpha[self.start_date_index:self.end_date_index]
    
    #Alpha68 SMA(((HIGH+LOW)/2-(DELAY(HIGH,1)+DELAY(LOW,1))/2)*(HIGH-LOW)/VOLUME,15,2) 
    def GTJAalpha068(self):
        alpha = ema(((self.high+self.low)/2-(delay(self.high,1)+delay(self.low,1))/2)*(self.high-self.low)/self.volume, 15, 2)
        return alpha[self.start_date_index:self.end_date_index]
    
    #Alpha69 (SUM(DTM,20)>SUM(DBM,20)?(SUM(DTM,20)-SUM(DBM,20))/SUM(DTM,20)：(SUM(DTM,20)=SUM(DBM,20)?0：(SUM(DTM,20)-SUM(DBM,20))/SUM(DBM,20))) 
    def GTJAalpha069(self):
        condition_dtm = (self.open<=delay(self.open,1))
        dtm_true = pd.Series(np.zeros(len(condition_dtm)))  
        dtm_false = np.maximum(self.high-self.open, self.open-delay(self.open,1))
        dtm = pd.Series(np.where(condition_dtm, dtm_true, dtm_false))  
        condition_dbm = (self.open>=delay(self.open,1))
        dbm_true = pd.Series(np.zeros(len(condition_dbm)))  
        dbm_false = np.maximum(self.open-self.low, self.open-delay(self.open,1))
        dbm = pd.Series(np.where(condition_dbm, dbm_true, dbm_false))         
        condition1 = (ts_sum(dtm,20) == ts_sum(dbm,20))
        inner1_true = pd.Series(np.zeros(len(condition1)))
        inner1_false = (ts_sum(dtm,20)-ts_sum(dbm,20)) / ts_sum(dbm,20)
        inner1 = pd.Series(np.where(condition1, inner1_true, inner1_false))     
        condition2 = (ts_sum(dtm,20) > ts_sum(dbm,20))
        inner2_true = (ts_sum(dtm,20)-ts_sum(dbm,20)) / ts_sum(dtm,20)      
        inner2_false = inner1
        alpha = pd.Series(np.where(condition2, inner2_true, inner2_false))            
        return alpha[self.start_date_index:self.end_date_index]
    
    #Alpha70 STD(AMOUNT,6) 
    def GTJAalpha070(self):
        alpha = stddev(self.amount, 6)
        return alpha[self.start_date_index:self.end_date_index]
    
    #Alpha71 (CLOSE-MEAN(CLOSE,24))/MEAN(CLOSE,24)*100 
    def GTJAalpha071(self):
        alpha = (self.close-sma(self.close,24))/sma(self.close,24)*100
        return alpha[self.start_date_index:self.end_date_index]
    
    #Alpha72 SMA((TSMAX(HIGH,6)-CLOSE)/(TSMAX(HIGH,6)-TSMIN(LOW,6))*100,15,1) 
    def GTJAalpha072(self):
        alpha = ema((ts_max(self.high,6)-self.close)/(ts_max(self.high,6)-ts_min(self.low,6))*100, 15, 1)
        return alpha[self.start_date_index:self.end_date_index] 
    
    #Alpha73 ((TSRANK(DECAYLINEAR(DECAYLINEAR(CORR((CLOSE), VOLUME, 10), 16), 4), 5) - RANK(DECAYLINEAR(CORR(VWAP, MEAN(VOLUME,30), 4),3))) * -1) 
    def GTJAalpha073(self):
        alpha = (ts_rank(decay_linear(decay_linear(correlation(self.close, self.volume, 10), 16), 4), 5) - rolling_rank(decay_linear(correlation(self.vwap, sma(self.volume,30), 4),3))) * -1
        return alpha[self.start_date_index:self.end_date_index]
    
    #Alpha74 (RANK(CORR(SUM(((LOW * 0.35) + (VWAP * 0.65)), 20), SUM(MEAN(VOLUME,40), 20), 7)) + RANK(CORR(RANK(VWAP), RANK(VOLUME), 6))) 
    def GTJAalpha074(self):
        alpha = rolling_rank(correlation(ts_sum(((self.low * 0.35) + (self.vwap * 0.65)), 20), ts_sum(sma(self.volume,40), 20), 7)) + rolling_rank(correlation(rolling_rank(self.vwap), rolling_rank(self.volume), 6))
        return alpha[self.start_date_index:self.end_date_index]
    
    #Alpha75 COUNT(CLOSE>OPEN & BANCHMARKINDEXCLOSE<BANCHMARKINDEXOPEN,50)/COUNT(BANCHMARKINDEXCLOSE<BANCHMARKINDEXOPEN,50) 
    def GTJAalpha075(self):
        condition_count1 = ((self.close>self.open) & (self.benchmarkindexclose<self.benchmarkindexopen))
        count1 = pd.Series(np.nan, index=condition_count1.index)
        for i in range(50-1,len(condition_count1)):
            count1.iloc[i]=condition_count1[i-50+1:i+1].sum()        
        condition_count2 = (self.benchmarkindexclose < self.benchmarkindexopen)
        count2 = pd.Series(np.nan, index=condition_count2.index)
        for i in range(50-1,len(condition_count2)):
            count2.iloc[i]=condition_count1[i-50+1:i+1].sum()          
        alpha = count1 / count2
        return alpha[self.start_date_index:self.end_date_index]
        
    #Alpha76 STD(ABS((CLOSE/DELAY(CLOSE,1)-1))/VOLUME,20)/MEAN(ABS((CLOSE/DELAY(CLOSE,1)-1))/VOLUME,20)
    def GTJAalpha076(self):
        alpha = stddev(abs((self.close/delay(self.close,1)-1))/self.volume,20)/sma(abs((self.close/delay(self.close,1)-1))/self.volume, 20)
        return alpha[self.start_date_index:self.end_date_index]
        
    #Alpha77 MIN(RANK(DECAYLINEAR(((((HIGH + LOW) / 2) + HIGH) - (VWAP + HIGH)), 20)), RANK(DECAYLINEAR(CORR(((HIGH + LOW) / 2), MEAN(VOLUME,40), 3), 6)))
    def GTJAalpha077(self):
        alpha = np.minimum(rolling_rank(decay_linear(((((self.high + self.low) / 2) + self.high) - (self.vwap + self.high)), 20)), rolling_rank(decay_linear(correlation(((self.high + self.low) / 2), sma(self.volume,40), 3), 6)))
        return alpha[self.start_date_index:self.end_date_index]
    
    #Alpha78 ((HIGH+LOW+CLOSE)/3-MA((HIGH+LOW+CLOSE)/3,12))/(0.015*MEAN(ABS(CLOSE-MEAN((HIGH+LOW+CLOSE)/3,12)),12))
    def GTJAalpha078(self):
        alpha = ((self.high+self.low+self.close)/3-sma((self.high+self.low+self.close)/3,12)) / (0.015*sma(abs(self.close-sma((self.high+self.low+self.close)/3,12)),12))
        return alpha[self.start_date_index:self.end_date_index]
    
    #Alpha79 SMA(MAX(CLOSE-DELAY(CLOSE,1),0),12,1)/SMA(ABS(CLOSE-DELAY(CLOSE,1)),12,1)*100
    def GTJAalpha079(self):
        alpha = ema(np.maximum(self.close-delay(self.close,1),0),12,1) / ema(abs(self.close-delay(self.close,1)),12,1)*100
        return alpha[self.start_date_index:self.end_date_index]
    
    #Alpha80 (VOLUME-DELAY(VOLUME,5))/DELAY(VOLUME,5)*100
    def GTJAalpha080(self):
        alpha = (self.volume-delay(self.volume,5)) / delay(self.volume,5) * 100
        return alpha[self.start_date_index:self.end_date_index]
    
    #Alpha81 SMA(VOLUME,21,2)
    def GTJAalpha081(self):
        alpha = ema(self.volume, 21, 2)
        return alpha[self.start_date_index:self.end_date_index]
    
    #Alpha82 SMA((TSMAX(HIGH,6)-CLOSE)/(TSMAX(HIGH,6)-TSMIN(LOW,6))*100,20,1)
    def GTJAalpha082(self):
        alpha = ema((ts_max(self.high,6)-self.close)/(ts_max(self.high,6)-ts_min(self.low,6))*100, 20, 1)
        return alpha[self.start_date_index:self.end_date_index]
    
    #Alpha83 (-1 * RANK(COVIANCE(RANK(HIGH), RANK(VOLUME), 5)))
    def GTJAalpha083(self):
        alpha = -1 * rolling_rank(covariance(rolling_rank(self.high), rolling_rank(self.volume), 5))
        return alpha[self.start_date_index:self.end_date_index]
    
    #Alpha84 SUM((CLOSE>DELAY(CLOSE,1)?VOLUME:(CLOSE<DELAY(CLOSE,1)?-VOLUME:0)),20)
    def GTJAalpha084(self):
        condition1 = (self.close < delay(self.close,1))
        inner1_true = -1 * self.volume
        inner1_false = pd.Series(np.zeros(len(condition1)))
        inner1 = pd.Series(np.where(condition1, inner1_true, inner1_false))     
        condition2 = (self.close > delay(self.close,1))
        inner2_true =  self.volume     
        inner2_false = inner1    
        inner2 = pd.Series(np.where(condition2, inner2_true, inner2_false))   
        alpha = ts_sum(inner2, 20)
        return alpha[self.start_date_index:self.end_date_index]
    
    #Alpha85 (TSRANK((VOLUME / MEAN(VOLUME,20)), 20) * TSRANK((-1 * DELTA(CLOSE, 7)), 8))
    def GTJAalpha085(self):
        alpha = ts_rank((self.volume / sma(self.volume,20)), 20) * ts_rank((-1 * delta(self.close, 7)), 8)
        return alpha[self.start_date_index:self.end_date_index]
    
    #Alpha86 ((0.25 < (((DELAY(CLOSE, 20) - DELAY(CLOSE, 10)) / 10) - ((DELAY(CLOSE, 10) - CLOSE) / 10))) ? (-1 * 1) : (((((DELAY(CLOSE, 20) - DELAY(CLOSE, 10)) / 10) - ((DELAY(CLOSE, 10) - CLOSE) / 10)) < 0) ? 1 : ((-1 * 1) * (CLOSE - DELAY(CLOSE, 1)))))
    def GTJAalpha086(self):
        condition1 = ((((delay(self.close, 20) - delay(self.close, 10)) / 10) - ((delay(self.close, 10) - self.close) / 10)) < 0)
        inner1_true = pd.Series(np.ones(len(condition1)))
        inner1_false = -1 * (self.close - delay(self.close, 1))
        inner1 = pd.Series(np.where(condition1, inner1_true, inner1_false))  
        condition2 = (0.25 < (((delay(self.close, 20) - delay(self.close, 10)) / 10) - ((delay(self.close, 10) - self.close) / 10)))
        inner2_true = -1 * pd.Series(np.ones(len(condition2)))
        inner2_false = inner1
        alpha = pd.Series(np.where(condition2, inner2_true, inner2_false))   
        return alpha[self.start_date_index:self.end_date_index]
        
    #Alpha87 ((RANK(DECAYLINEAR(DELTA(VWAP, 4), 7)) + TSRANK(DECAYLINEAR(((((LOW * 0.9) + (LOW * 0.1)) - VWAP) / (OPEN - ((HIGH + LOW) / 2))), 11), 7)) * -1) 
    def GTJAalpha087(self):
        alpha = (ts_rank(decay_linear(delta(self.vwap, 4), 7)) + ts_rank(decay_linear(((((self.low * 0.9) + (self.low * 0.1)) - self.vwap) / (self.open - ((self.high + self.low) / 2))), 11), 7)) * -1
        return alpha[self.start_date_index:self.end_date_index]       
    
    #Alpha88 (CLOSE-DELAY(CLOSE,20))/DELAY(CLOSE,20)*100 
    def GTJAalpha088(self):
        alpha = (self.close-delay(self.close,20))/delay(self.close,20) * 100 
        return alpha[self.start_date_index:self.end_date_index]
    
    #Alpha89 2*(SMA(CLOSE,13,2)-SMA(CLOSE,27,2)-SMA(SMA(CLOSE,13,2)-SMA(CLOSE,27,2),10,2)) 
    def GTJAalpha089(self):
        alpha = 2*(ema(self.close,13,2)-ema(self.close,27,2)-ema(ema(self.close,13,2)-ema(self.close,27,2),10,2))
        return alpha[self.start_date_index:self.end_date_index]
    
    #Alpha90 (RANK(CORR(RANK(VWAP), RANK(VOLUME), 5)) * -1) 
    def GTJAalpha090(self):
        alpha = rolling_rank(correlation(rolling_rank(self.vwap), rolling_rank(self.volume), 5)) * -1
        return alpha[self.start_date_index:self.end_date_index]
    
    #Alpha91 ((RANK((CLOSE - MAX(CLOSE, 5)))*RANK(CORR((MEAN(VOLUME,40)), LOW, 5))) * -1)
    def GTJAalpha091(self):
        alpha = (rolling_rank((self.close - np.maximum(self.close, 5)))*rolling_rank(correlation((sma(self.volume,40)), self.low, 5))) * -1
        return alpha[self.start_date_index:self.end_date_index]
    
    #Alpha92 (MAX(RANK(DECAYLINEAR(DELTA(((CLOSE * 0.35) + (VWAP *0.65)), 2), 3)), TSRANK(DECAYLINEAR(ABS(CORR((MEAN(VOLUME,180)), CLOSE, 13)), 5), 15)) * -1) 
    def GTJAalpha092(self):
        alpha = np.maximum(rolling_rank(decay_linear(delta(((self.close * 0.35) + (self.vwap *0.65)), 2), 3)), ts_rank(decay_linear(abs(correlation((sma(self.volume,180)), self.close, 13)), 5), 15)) * -1
        return alpha[self.start_date_index:self.end_date_index]
    
    #Alpha93 SUM((OPEN>=DELAY(OPEN,1)?0:MAX((OPEN-LOW),(OPEN-DELAY(OPEN,1)))),20) 
    def GTJAalpha093(self):
        condition1 = (self.open >= delay(self.open,1))
        inner1_true = pd.Series(np.zeros(len(condition1)))
        inner1_false = np.maximum((self.open-self.low),(self.open-delay(self.open,1)))
        inner1 = pd.Series(np.where(condition1, inner1_true, inner1_false))          
        alpha = ts_sum(inner1, 20)
        return alpha[self.start_date_index:self.end_date_index]
    
    #Alpha94 SUM((CLOSE>DELAY(CLOSE,1)?VOLUME:(CLOSE<DELAY(CLOSE,1)?-VOLUME:0)),30) 
    def GTJAalpha094(self):
        condition1 = (self.close < delay(self.close,1))
        inner1_true = -1 * self.volume
        inner1_false = pd.Series(np.zeros(len(condition1)))
        inner1 = pd.Series(np.where(condition1, inner1_true, inner1_false))          
        condition2 = (self.close > delay(self.close,1))
        inner2_true =  self.volume     
        inner2_false = inner1    
        inner2 = pd.Series(np.where(condition2, inner2_true, inner2_false))           
        alpha = ts_sum(inner2, 30)        
        return alpha[self.start_date_index:self.end_date_index]
    
    #Alpha95 STD(AMOUNT,20)
    def GTJAalpha095(self):
        alpha = stddev(self.amount, 20)
        return alpha[self.start_date_index:self.end_date_index]
    
    #Alpha96 SMA(SMA((CLOSE-TSMIN(LOW,9))/(TSMAX(HIGH,9)-TSMIN(LOW,9))*100,3,1),3,1)
    def GTJAalpha096(self):
        alpha = ema(ema((self.close-ts_min(self.low,9))/(ts_max(self.high,9)-ts_min(self.low,9))*100,3,1),3,1)
        return alpha[self.start_date_index:self.end_date_index]

    #Alpha97 STD(VOLUME,10)
    def GTJAalpha097(self):
        alpha = stddev(self.volume, 10)
        return alpha[self.start_date_index:self.end_date_index]

    #Alpha98 ((((DELTA((SUM(CLOSE, 100) / 100), 100) / DELAY(CLOSE, 100)) < 0.05) || ((DELTA((SUM(CLOSE, 100) / 100), 100) / DELAY(CLOSE, 100)) == 0.05)) ? (-1 * (CLOSE - TSMIN(CLOSE, 100))) : (-1 * DELTA(CLOSE, 3)))
    def GTJAalpha098(self):
        condition1 = ((delta((ts_sum(self.close, 100) / 100), 100) / delay(self.close, 100)) < 0.05) | ((delta((ts_sum(self.close, 100) / 100), 100) / delay(self.close, 100)) == 0.05)
        inner1_true = -1 * (self.close - ts_min(self.close, 100))
        inner1_false = -1 * delta(self.close, 3)
        alpha = pd.Series(np.where(condition1, inner1_true, inner1_false))  
        return alpha[self.start_date_index:self.end_date_index]
    
    #Alpha99 (-1 * RANK(COVIANCE(RANK(CLOSE), RANK(VOLUME), 5))) 
    def GTJAalpha099(self):
        alpha = -1 * rolling_rank(covariance(rolling_rank(self.close), rolling_rank(self.volume), 5))
        return alpha[self.start_date_index:self.end_date_index]
    
    #Alpha100 STD(VOLUME,20) 
    def GTJAalpha100(self):
        alpha = stddev(self.volume, 20)
        return alpha[self.start_date_index:self.end_date_index]
    
    #Alpha101 ((RANK(CORR(CLOSE, SUM(MEAN(VOLUME,30), 37), 15)) < RANK(CORR(RANK(((HIGH * 0.1) + (VWAP * 0.9))), RANK(VOLUME), 11))) * -1) 
    def GTJAalpha101(self):
        alpha = (ts_rank(correlation(self.close, ts_sum(sma(self.volume,30), 37), 15)) < ts_rank(correlation(ts_rank(((self.high * 0.1) + (self.vwap * 0.9))), ts_rank(self.volume), 11))) * -1
        return alpha[self.start_date_index:self.end_date_index]
    
    #Alpha102 SMA(MAX(VOLUME-DELAY(VOLUME,1),0),6,1)/SMA(ABS(VOLUME-DELAY(VOLUME,1)),6,1)*100 
    def GTJAalpha102(self):
        alpha = ema(np.maximum(self.volume-delay(self.volume,1),0),6,1)/ema(abs(self.volume-delay(self.volume,1)),6,1)*100
        return alpha[self.start_date_index:self.end_date_index]
    
    #Alpha103 ((20-LOWDAY(LOW,20))/20)*100 
    def GTJAalpha103(self):
        alpha = ((20-lowday(self.low,20))/20)*100
        return alpha[self.start_date_index:self.end_date_index]
    
    #Alpha104 (-1 * (DELTA(CORR(HIGH, VOLUME, 5), 5) * RANK(STD(CLOSE, 20)))) 
    def GTJAalpha104(self):
        alpha = -1 * (delta(correlation(self.high, self.volume, 5), 5) * rolling_rank(stddev(self.close, 20)))
        return alpha[self.start_date_index:self.end_date_index]
    
    #Alpha105 (-1 * CORR(RANK(OPEN), RANK(VOLUME), 10)) 
    def GTJAalpha105(self):
        alpha = -1 * correlation(rolling_rank(self.open), rolling_rank(self.volume), 10)
        return alpha[self.start_date_index:self.end_date_index]
    
    #Alpha106 CLOSE-DELAY(CLOSE,20) 
    def GTJAalpha106(self):
        alpha = self.close - delay(self.close, 20)
        return alpha[self.start_date_index:self.end_date_index]
    
    #Alpha107 (((-1 * RANK((OPEN - DELAY(HIGH, 1)))) * RANK((OPEN - DELAY(CLOSE, 1)))) * RANK((OPEN - DELAY(LOW, 1)))) 
    def GTJAalpha107(self):
        alpha = ((-1 * rolling_rank((self.open - delay(self.high, 1)))) * rolling_rank((self.open - delay(self.close, 1)))) * rolling_rank((self.open - delay(self.low, 1)))
        return alpha[self.start_date_index:self.end_date_index] 
    
    #Alpha108 ((RANK((HIGH - MIN(HIGH, 2)))^RANK(CORR((VWAP), (MEAN(VOLUME,120)), 6))) * -1) 
    def GTJAalpha108(self):
        alpha = (rolling_rank((self.high - np.minimum(self.high, 2)))**rolling_rank(correlation((self.vwap), (sma(self.volume,120)), 6))) * -1
        return alpha[self.start_date_index:self.end_date_index]
    
    #Alpha109 SMA(HIGH-LOW,10,2)/SMA(SMA(HIGH-LOW,10,2),10,2) 
    def GTJAalpha109(self):
        alpha = ema(self.high-self.low,10,2) / ema(ema(self.high-self.low,10,2), 10, 2)
        return alpha[self.start_date_index:self.end_date_index]
    
    #Alpha110 SUM(MAX(0,HIGH-DELAY(CLOSE,1)),20)/SUM(MAX(0,DELAY(CLOSE,1)-LOW),20)*100 
    def GTJAalpha110(self):
        alpha = ts_sum(np.maximum(0,self.high-delay(self.close,1)),20) / ts_sum(np.maximum(0,delay(self.close,1)-self.low),20) * 100 
        return alpha[self.start_date_index:self.end_date_index]
    
    #Alpha111 SMA(VOL*((CLOSE-LOW)-(HIGH-CLOSE))/(HIGH-LOW),11,2)-SMA(VOL*((CLOSE-LOW)-(HIGH-CLOSE))/(HIGH-LOW),4,2) 
    def GTJAalpha111(self):
        alpha= ema(self.volume*((self.close-self.low)-(self.high-self.close))/(self.high-self.low),11,2) - ema(self.volume*((self.close-self.low)-(self.high-self.close))/(self.high-self.low),4,2)
        return alpha[self.start_date_index:self.end_date_index]
    
    #Alpha112 (SUM((CLOSE-DELAY(CLOSE,1)>0?CLOSE-DELAY(CLOSE,1):0),12) - SUM((CLOSE-DELAY(CLOSE,1)<0?ABS(CLOSE-DELAY(CLOSE,1)):0),12)) / (SUM((CLOSE-DELAY(CLOSE,1)>0?CLOSE-DELAY(CLOSE,1):0),12) + SUM((CLOSE-DELAY(CLOSE,1)<0?ABS(CLOSE-DELAY(CLOSE,1)):0),12))*100
    def GTJAalpha112(self):
        condition1 = (self.close-delay(self.close,1) > 0)
        condition2 = (self.close-delay(self.close,1) < 0)   
        inner1_true = self.close-delay(self.close,1)
        inner2_true = abs(self.close-delay(self.close,1))
        inner_false = pd.Series(np.zeros(len(condition1))) 
        inner1 = pd.Series(np.where(condition1, inner1_true, inner_false)) 
        inner2 = pd.Series(np.where(condition2, inner2_true, inner_false))     
        alpha = (ts_sum(inner1,12)-ts_sum(inner2,12)) / (ts_sum(inner1,12)+ts_sum(inner2,12)) * 100
        return alpha[self.start_date_index:self.end_date_index]
  
    #Alpha113 (-1 * ((RANK((SUM(DELAY(CLOSE, 5), 20) / 20)) * CORR(CLOSE, VOLUME, 2)) * RANK(CORR(SUM(CLOSE, 5), SUM(CLOSE, 20), 2))))
    def GTJAalpha113(self):
        alpha = -1 * ((rolling_rank((ts_sum(delay(self.close, 5), 20) / 20)) * correlation(self.close, self.volume, 2)) * rolling_rank(correlation(ts_sum(self.close, 5), ts_sum(self.close, 20), 2)))
        return alpha[self.start_date_index:self.end_date_index]
    
    #Alpha114 ((RANK(DELAY(((HIGH - LOW) / (SUM(CLOSE, 5) / 5)), 2)) * RANK(RANK(VOLUME))) / (((HIGH - LOW) / (SUM(CLOSE, 5) / 5)) / (VWAP - CLOSE)))
    def GTJAalpha114(self):
        alpha = (rolling_rank(delay(((self.high - self.low) / (ts_sum(self.close, 5) / 5)), 2)) * rolling_rank(rolling_rank(self.volume))) / (((self.high - self.low) / (ts_sum(self.close, 5) / 5)) / (self.vwap - self.close))
        return alpha[self.start_date_index:self.end_date_index]
    
    #Alpha115 (RANK(CORR(((HIGH * 0.9) + (CLOSE * 0.1)), MEAN(VOLUME,30), 10))^RANK(CORR(TSRANK(((HIGH + LOW) / 2), 4), TSRANK(VOLUME, 10), 7)))
    def GTJAalpha115(self):
        alpha = rolling_rank(correlation(((self.high * 0.9) + (self.close * 0.1)), sma(self.volume,30), 10)) ** rolling_rank(correlation(ts_rank(((self.high + self.low) / 2), 4), ts_rank(self.volume, 10), 7))
        return alpha[self.start_date_index:self.end_date_index]

    #Alpha116 REGBETA(CLOSE,SEQUENCE,20)
    def GTJAalpha116(self):
        y = self.close
        alpha = pd.Series(np.nan, index=self.close.index)
        for i in range(20-1,len(y)):
            alpha.iloc[i]=sp.stats.linregress(pd.Series(np.arange(1,21)), y[i-20+1:i+1])[0]        
        return alpha[self.start_date_index:self.end_date_index]
    
    #Alpha117 ((TSRANK(VOLUME, 32) * (1 - TSRANK(((CLOSE + HIGH) - LOW), 16))) * (1 - TSRANK(RET, 32)))
    def GTJAalpha117(self):
        alpha = (ts_rank(self.volume, 32) * (1 - ts_rank(((self.close + self.high) - self.low), 16))) * (1 - ts_rank(self.returns, 32))
        return alpha[self.start_date_index:self.end_date_index]
    
    #Alpha118 SUM(HIGH-OPEN,20)/SUM(OPEN-LOW,20)*100
    def GTJAalpha118(self):
        alpha = ts_sum(self.high-self.open,20) / ts_sum(self.open-self.low,20) * 100
        return alpha[self.start_date_index:self.end_date_index]
    
    #Alpha119 (RANK(DECAYLINEAR(CORR(VWAP, SUM(MEAN(VOLUME,5), 26), 5), 7)) - RANK(DECAYLINEAR(TSRANK(MIN(CORR(RANK(OPEN), RANK(MEAN(VOLUME,15)), 21), 9), 7), 8)))
    def GTJAalpha119(self):
        alpha = rolling_rank(decay_linear(correlation(self.vwap, ts_sum(sma(self.volume,5), 26), 5), 7)) - rolling_rank(decay_linear(ts_rank(np.minimum(correlation(rolling_rank(self.open), rolling_rank(sma(self.volume,15)), 21), 9), 7), 8))
        return alpha[self.start_date_index:self.end_date_index]

    #Alpha120 (RANK((VWAP - CLOSE)) / RANK((VWAP + CLOSE)))
    def GTJAalpha120(self):
        alpha = rolling_rank((self.vwap - self.close)) / rolling_rank((self.vwap + self.close))
        return alpha[self.start_date_index:self.end_date_index]
    
    #Alpha121 ((RANK((VWAP - MIN(VWAP, 12)))^TSRANK(CORR(TSRANK(VWAP, 20), TSRANK(MEAN(VOLUME,60), 2), 18), 3)) * -1)
    def GTJAalpha121(self):
        alpha = (rolling_rank((self.vwap - np.minimum(self.vwap, 12))) ** ts_rank(correlation(ts_rank(self.vwap, 20), ts_rank(sma(self.volume,60), 2), 18), 3)) * -1
        return alpha[self.start_date_index:self.end_date_index]
    
    #Alpha122 (SMA(SMA(SMA(LOG(CLOSE),13,2),13,2),13,2)-DELAY(SMA(SMA(SMA(LOG(CLOSE),13,2),13,2),13,2),1))/DELAY(SMA(SMA(SMA(LOG(CLOSE),13,2),13,2),13,2),1)
    def GTJAalpha122(self):
        alpha = (ema(ema(ema(np.log(self.close),13,2),13,2),13,2)-delay(ema(ema(ema(np.log(self.close),13,2),13,2),13,2),1)) / delay(ema(ema(ema(np.log(self.close),13,2),13,2),13,2),1)
        return alpha[self.start_date_index:self.end_date_index]
    
    #Alpha123 (RANK(CORR(SUM(((HIGH + LOW) / 2), 20), SUM(MEAN(VOLUME,60), 20), 9)) < RANK(CORR(LOW, VOLUME, 6))) * -1
    def GTJAalpha123(self):
        alpha = ( rolling_rank(correlation(ts_sum(((self.high + self.low) / 2), 20), ts_sum(sma(self.volume, 60), 20), 9)) < rolling_rank(correlation(self.low, self.volume, 6)) ) * -1
        return alpha[self.start_date_index:self.end_date_index]
    
    #Alpha124 (CLOSE - VWAP) / DECAYLINEAR(RANK(TSMAX(CLOSE, 30)),2)
    def GTJAalpha124(self):
        alpha = (self.close - self.vwap) / decay_linear(rolling_rank(ts_max(self.close, 30)),2)
        return alpha[self.start_date_index:self.end_date_index]
    
    #Alpha125 (RANK(DECAYLINEAR(CORR((VWAP), MEAN(VOLUME,80),17), 20)) / RANK(DECAYLINEAR(DELTA(((CLOSE * 0.5) + (VWAP * 0.5)), 3), 16))) 
    def GTJAalpha125(self):
        alpha = rolling_rank(decay_linear(correlation((self.vwap), sma(self.volume,80),17), 20)) / rolling_rank(decay_linear(delta(((self.close * 0.5) + (self.vwap * 0.5)), 3), 16))
        return alpha[self.start_date_index:self.end_date_index]
    
    #Alpha126 (CLOSE+HIGH+LOW)/3 
    def GTJAalpha126(self):
        alpha = (self.close+self.high+self.low)/3 
        return alpha[self.start_date_index:self.end_date_index]
    
    #Alpha127 (MEAN((100*(CLOSE-MAX(CLOSE,12))/(MAX(CLOSE,12)))^2))^(1/2) 
    def GTJAalpha127(self):
        alpha = (sma((100*(self.close-np.maximum(self.close,12))/(np.maximum(self.close,12)))**2)) ** 0.5
        return alpha[self.start_date_index:self.end_date_index]
    
    #Alpha128 100-(100/(1+SUM(((HIGH+LOW+CLOSE)/3>DELAY((HIGH+LOW+CLOSE)/3,1)?(HIGH+LOW+CLOSE)/3*VOLUME:0),14)/SUM(((HIGH+LOW+CLOSE)/3<DELAY((HIGH+LOW+CLOSE)/3,1)?(HIGH+LOW+CLOSE)/3*VOLUME:0),14)))
    def GTJAalpha128(self):
        condition1 = ((self.high+self.low+self.close)/3 > delay((self.high+self.low+self.close)/3,1))
        condition2 = ((self.high+self.low+self.close)/3 < delay((self.high+self.low+self.close)/3,1))   
        inner_true = (self.high+self.low+self.close)/3 * self.volume
        inner_false = pd.Series(np.zeros(len(condition1))) 
        inner1 = pd.Series(np.where(condition1, inner_true, inner_false)) 
        inner2 = pd.Series(np.where(condition2, inner_true, inner_false))          
        alpha = 100-(100/(1+ts_sum(inner1,14)/ts_sum(inner2,14)))
        return alpha[self.start_date_index:self.end_date_index]     
    
    #Alpha129 SUM((CLOSE-DELAY(CLOSE,1)<0?ABS(CLOSE-DELAY(CLOSE,1)):0),12)
    def GTJAalpha129(self):
        condition1 = (self.close-delay(self.close,1) < 0)
        inner1_true = abs(self.close-delay(self.close,1))
        inner1_false = pd.Series(np.zeros(len(condition1)))
        inner1 = pd.Series(np.where(condition1, inner1_true, inner1_false)) 
        alpha = ts_sum(inner1,12)
        return alpha[self.start_date_index:self.end_date_index]
    
    #Alpha130 (RANK(DECAYLINEAR(CORR(((HIGH + LOW) / 2), MEAN(VOLUME,40), 9), 10)) / RANK(DECAYLINEAR(CORR(RANK(VWAP), RANK(VOLUME), 7),3)))
    def GTJAalpha130(self):
        alpha = rolling_rank(decay_linear(correlation(((self.high + self.low) / 2), sma(self.volume,40), 9), 10)) / rolling_rank(decay_linear(correlation(rolling_rank(self.vwap), rolling_rank(self.volume), 7),3))
        return alpha[self.start_date_index:self.end_date_index]
    
    #Alpha131 (RANK(DELAT(VWAP, 1))^TSRANK(CORR(CLOSE,MEAN(VOLUME,50), 18), 18))  
    def GTJAalpha131(self):    #"DELAT" for delay or delta?
        alpha = rolling_rank(delay(self.vwap, 1)) ** ts_rank(correlation(self.close,sma(self.volume,50), 18), 18)
        return alpha[self.start_date_index:self.end_date_index]
    
    #Alpha132 MEAN(AMOUNT,20)
    def GTJAalpha132(self):
        alpha = sma(self.amount, 20)
        return alpha[self.start_date_index:self.end_date_index]
    
    #Alpha133 ((20-HIGHDAY(HIGH,20))/20)*100-((20-LOWDAY(LOW,20))/20)*100
    def GTJAalpha133(self):
        alpha = ((20-highday(self.high,20))/20)*100 - ((20-lowday(self.low,20))/20)*100
        return alpha[self.start_date_index:self.end_date_index]
    
    #Alpha134 (CLOSE-DELAY(CLOSE,12))/DELAY(CLOSE,12)*VOLUME
    def GTJAalpha134(self):
        alpha = (self.close-delay(self.close,12)) / delay(self.close,12) * self.volume
        return alpha[self.start_date_index:self.end_date_index]
    
    #Alpha135 SMA(DELAY(CLOSE/DELAY(CLOSE,20),1),20,1)
    def GTJAalpha135(self):
        alpha = ema(delay(self.close/delay(self.close,20),1),20,1)
        return alpha[self.start_date_index:self.end_date_index]
    
    #Alpha136 ((-1 * RANK(DELTA(RET, 3))) * CORR(OPEN, VOLUME, 10))
    def GTJAalpha136(self):
        alpha = (-1 * rolling_rank(delta(self.returns, 3))) * correlation(self.open, self.volume, 10)
        return alpha[self.start_date_index:self.end_date_index]
    
    #Alpha137 16*(CLOSE-DELAY(CLOSE,1)+(CLOSE-OPEN)/2+DELAY(CLOSE,1)-DELAY(OPEN,1)) / ((ABS(HIGH-DELAY(CLOSE,1))>ABS(LOW-DELAY(CLOSE,1)) & ABS(HIGH-DELAY(CLOSE,1))>ABS(HIGH-DELAY(LOW,1))?ABS(HIGH-DELAY(CLOSE,1))+ABS(LOW-DELAY(CLOSE,1))/2+ABS(DELAY(CLOSE,1)-DELAY(OPEN,1))/4:(ABS(LOW-DELAY(CLOSE,1))>ABS(HIGH-DELAY(LOW,1)) & ABS(LOW-DELAY(CLOSE,1))>ABS(HIGH-DELAY(CLOSE,1))?ABS(LOW-DELAY(CLOSE,1))+ABS(HIGH-DELAY(CLOSE,1))/2+ABS(DELAY(CLOSE,1)-DELAY(OPEN,1))/4:ABS(HIGH-DELAY(LOW,1))+ABS(DELAY(CLOSE,1)-DELAY(OPEN,1))/4))) * MAX(ABS(HIGH-DELAY(CLOSE,1)),ABS(LOW-DELAY(CLOSE,1)))    
    def GTJAalpha137(self):
        condition1 = (abs(self.low-delay(self.close,1))>abs(self.high-delay(self.low,1))) & (abs(self.low-delay(self.close,1))>abs(self.high-delay(self.close,1)))
        inner1_true = abs(self.low-delay(self.close,1)) + abs(self.high-delay(self.close,1))/2 + abs(delay(self.close,1)-delay(self.open,1))/4
        inner1_false = abs(self.high-delay(self.low,1)) + abs(delay(self.close,1)-delay(self.open,1))/4
        inner1 = pd.Series(np.where(condition1, inner1_true, inner1_false))         
        condition2 = (abs(self.high-delay(self.close,1))>abs(self.low-delay(self.close,1))) & (abs(self.high-delay(self.close,1))>abs(self.high-delay(self.low,1)))
        inner2_true = abs(self.high-delay(self.close,1)) + abs(self.low-delay(self.close,1))/2 + abs(delay(self.close,1)-delay(self.open,1))/4
        inner2_false = inner1
        inner2 = pd.Series(np.where(condition2, inner2_true, inner2_false))         
        alpha = 16*(self.close-delay(self.close,1)+(self.close-self.open)/2+delay(self.close,1)-delay(self.open,1)) / inner2 * np.maximum(abs(self.high-delay(self.close,1)),abs(self.low-delay(self.close,1)))
        return alpha[self.start_date_index:self.end_date_index]

    #Alpha138 ((RANK(DECAYLINEAR(DELTA((((LOW * 0.7) + (VWAP *0.3))), 3), 20)) - TSRANK(DECAYLINEAR(TSRANK(CORR(TSRANK(LOW, 8), TSRANK(MEAN(VOLUME,60), 17), 5), 19), 16), 7)) * -1) 
    def GTJAalpha138(self):
        alpha = (rolling_rank(decay_linear(delta((((self.low * 0.7) + (self.vwap *0.3))), 3), 20)) - ts_rank(decay_linear(ts_rank(correlation(ts_rank(self.low, 8), ts_rank(sma(self.volume,60), 17), 5), 19), 16), 7)) * -1
        return alpha[self.start_date_index:self.end_date_index]
    
    #Alpha139 (-1 * CORR(OPEN, VOLUME, 10)) 
    def GTJAalpha139(self):
        alpha = -1 * correlation(self.open,self.volume,10)
        return alpha[self.start_date_index:self.end_date_index]
    
    #Alpha140 MIN(RANK(DECAYLINEAR(((RANK(OPEN) + RANK(LOW)) - (RANK(HIGH) + RANK(CLOSE))), 8)), TSRANK(DECAYLINEAR(CORR(TSRANK(CLOSE, 8), TSRANK(MEAN(VOLUME,60), 20), 8), 7), 3)) 
    def GTJAalpha140(self):
        alpha = np.minimum(rolling_rank(decay_linear(((rolling_rank(self.open) + rolling_rank(self.low)) - (rolling_rank(self.high) + rolling_rank(self.close))), 8)), ts_rank(decay_linear(correlation(ts_rank(self.close,8), ts_rank(sma(self.volume,60), 20), 8), 7), 3)) 
        return alpha[self.start_date_index:self.end_date_index]
    
    #Alpha141 (RANK(CORR(RANK(HIGH), RANK(MEAN(VOLUME,15)), 9))* -1) 
    def GTJAalpha141(self):
        alpha = rolling_rank(correlation(rolling_rank(self.high), rolling_rank(sma(self.volume,15)), 9))* -1
        return alpha[self.start_date_index:self.end_date_index]
    
    #Alpha142 (((-1 * RANK(TSRANK(CLOSE, 10))) * RANK(DELTA(DELTA(CLOSE, 1), 1))) * RANK(TSRANK((VOLUME /MEAN(VOLUME,20)), 5))) 
    def GTJAalpha142(self):
        alpha = ((-1 * rolling_rank(ts_rank(self.close, 10))) * ts_rank(delta(delta(self.close, 1), 1))) * rolling_rank(ts_rank((self.volume/sma(self.volume,20)), 5))
        return alpha[self.start_date_index:self.end_date_index]
    
    #Alpha143 CLOSE>DELAY(CLOSE,1)?(CLOSE-DELAY(CLOSE,1))/DELAY(CLOSE,1)*SELF:SELF 
    def GTJAalpha143(self):
        alpha = pd.Series(1, index=self.close.index)
        for i in range(1,len(alpha)):
            if self.close.iloc[i]>self.close.iloc[i-1]:
                alpha.iloc[i] = ((self.close.iloc[i]-self.close.iloc[i-1])/self.close.iloc[i-1]) * alpha.iloc[i-1]
            else:
                alpha.iloc[i] = alpha.iloc[i-1]
        return alpha[self.start_date_index:self.end_date_index]
    
    #Alpha144 SUMIF(ABS(CLOSE/DELAY(CLOSE,1)-1)/AMOUNT,20,CLOSE<DELAY(CLOSE,1)) / COUNT(CLOSE<DELAY(CLOSE,1),20)
    def GTJAalpha144(self):
        condition = self.close < delay(self.close,1)
        inner_sumif = abs(self.close/delay(self.close,1)-1) / self.amount
        value_sumif = ts_sum(inner_sumif*condition, window=20)
        count = pd.Series(np.nan, index=condition.index)
        for i in range(20-1,len(condition)):
            count.iloc[i]=condition[i-20+1:i+1].sum()        
        alpha = value_sumif / count
        return alpha[self.start_date_index:self.end_date_index]
    
    #Alpha145 (MEAN(VOLUME,9)-MEAN(VOLUME,26))/MEAN(VOLUME,12)*100
    def GTJAalpha145(self):
        alpha = (sma(self.volume,9)-sma(self.volume,26))/sma(self.volume,12)*100
        return alpha[self.start_date_index:self.end_date_index]
    
    #Alpha146 MEAN((CLOSE-DELAY(CLOSE,1))/DELAY(CLOSE,1)-SMA((CLOSE-DELAY(CLOSE,1))/DELAY(CLOSE,1),61,2),20)*((CLOSE-DELAY(CLOSE,1))/DELAY(CLOSE,1)-SMA((CLOSE-DELAY(CLOSE,1))/DELAY(CLOSE,1),61,2))/SMA(((CLOSE-DELAY(CLOSE,1))/DELAY(CLOSE,1)-((CLOSE-DELAY(CLOSE,1))/DELAY(CLOSE,1)-SMA((CLOSE-DELAY(CLOSE,1))/DELAY(CLOSE,1),61,2)))^2,60)
    def GTJAalpha146(self):
        alpha = sma((self.close-delay(self.close,1))/delay(self.close,1)-ema((self.close-delay(self.close,1))/delay(self.close,1),61,2),20) * ((self.close-delay(self.close,1))/delay(self.close,1)-ema((self.close-delay(self.close,1))/delay(self.close,1),61,2)) / ema(((self.close-delay(self.close,1))/delay(self.close,1)-((self.close-delay(self.close,1))/delay(self.close,1)-ema((self.close-delay(self.close,1))/delay(self.close,1),61,2)))**2,60,2)
        return alpha[self.start_date_index:self.end_date_index]
    
    #Alpha147 REGBETA(MEAN(CLOSE,12),SEQUENCE(12))    
    def GTJAalpha147(self):
        y = sma(self.close, 12)
        alpha = pd.Series(np.nan, index=self.close.index)
        for i in range(12-1,len(y)):
            alpha.iloc[i]=sp.stats.linregress(pd.Series(np.arange(1,13)), y[i-12+1:i+1])[0]        
        return alpha[self.start_date_index:self.end_date_index]

    #Alpha148 ((RANK(CORR((OPEN), SUM(MEAN(VOLUME,60), 9), 6)) < RANK((OPEN - TSMIN(OPEN, 14)))) * -1)
    def GTJAalpha148(self):
        alpha = (rolling_rank(correlation(self.open, ts_sum(sma(self.volume,60), 9), 6)) < rolling_rank((self.open - ts_min(self.open, 14)))) * -1
        return alpha[self.start_date_index:self.end_date_index]
    
    #Alpha149 REGBETA(FILTER(CLOSE/DELAY(CLOSE,1)-1,BANCHMARKINDEXCLOSE<DELAY(BANCHMARKINDEXCLOSE,1)),FILTER(BANCHMARKINDEXCLOSE/DELAY(BANCHMARKINDEXCLOSE,1)-1,BANCHMARKINDEXCLOSE<DELAY(BANCHMARKINDEXCLOSE,1)),252)
    def GTJAalpha149(self):
        y = self.close/delay(self.close,1)-1
        y_satisfied = y[self.benchmarkindexclose < delay(self.benchmarkindexclose,1)]
        x = self.benchmarkindexclose/delay(self.benchmarkindexclose,1) -1
        x_satisfied = x[self.benchmarkindexclose < delay(self.benchmarkindexclose,1)]
        alpha = pd.Series(np.nan, index=y.index)
        for i in range(252-1,len(alpha)):
            try: #防止区间内没有符合条件的结果
                alpha.iloc[i] = sp.stats.linregress(x_satisfied.loc[i-252+1:i+1], y_satisfied.loc[i-252+1:i+1])[0]        
            except:
                alpha.iloc[i] = 0
        return alpha[self.start_date_index:self.end_date_index]        
    
    #Alpha150 (CLOSE+HIGH+LOW)/3*VOLUME
    def GTJAalpha150(self):
        alpha = (self.close+self.high+self.low) / 3 * self.volume
        return alpha[self.start_date_index:self.end_date_index]

    #Alpha151 SMA(CLOSE-DELAY(CLOSE,20),20,1)
    def GTJAalpha151(self):
        alpha = ema(self.close-delay(self.close,20),20,1)
        return alpha[self.start_date_index:self.end_date_index]
    
    #Alpha152 SMA(MEAN(DELAY(SMA(DELAY(CLOSE/DELAY(CLOSE,9),1),9,1),1),12)-MEAN(DELAY(SMA(DELAY(CLOSE/DELAY (CLOSE,9),1),9,1),1),26),9,1) 
    def GTJAalpha152(self):
        alpha = ema(sma(delay(ema(delay(self.close/delay(self.close,9),1),9,1),1),12) - sma(delay(ema(delay(self.close/delay(self.close,9),1),9,1),1),26),9,1) 
        return alpha[self.start_date_index:self.end_date_index]
    
    #Alpha153 (MEAN(CLOSE,3)+MEAN(CLOSE,6)+MEAN(CLOSE,12)+MEAN(CLOSE,24))/4 
    def GTJAalpha153(self):
        alpha = (sma(self.close,3)+sma(self.close,6)+sma(self.close,12)+sma(self.close,24))/4 
        return alpha[self.start_date_index:self.end_date_index]
    
    #Alpha154 (((VWAP - MIN(VWAP, 16))) < (CORR(VWAP, MEAN(VOLUME,180), 18))) 
    def GTJAalpha154(self):
        alpha = (((self.vwap - np.minimum(self.vwap, 16))) < (correlation(self.vwap, sma(self.volume,180), 18)))
        alpha = alpha * 1
        return alpha[self.start_date_index:self.end_date_index]
    
    #Alpha155 SMA(VOLUME,13,2)-SMA(VOLUME,27,2)-SMA(SMA(VOLUME,13,2)-SMA(VOLUME,27,2),10,2) 
    def GTJAalpha155(self):
        alpha = ema(self.volume,13,2)-ema(self.volume,27,2)-ema(ema(self.volume,13,2)-ema(self.volume,27,2),10,2) 
        return alpha[self.start_date_index:self.end_date_index]
    
    #Alpha156 (MAX(RANK(DECAYLINEAR(DELTA(VWAP, 5), 3)), RANK(DECAYLINEAR(((DELTA(((OPEN * 0.15) + (LOW *0.85)), 2) / ((OPEN * 0.15) + (LOW * 0.85))) * -1), 3))) * -1) 
    def GTJAalpha156(self):
        alpha = np.maximum(rolling_rank(decay_linear(delta(self.vwap, 5), 3)), rolling_rank(decay_linear(((delta(((self.open * 0.15) + (self.low *0.85)), 2) / ((self.open * 0.15) + (self.low * 0.85))) * -1), 3))) * -1
        return alpha[self.start_date_index:self.end_date_index]
    
    #Alpha157 (MIN(PROD(RANK(RANK(LOG(SUM(TSMIN(RANK(RANK((-1 * RANK(DELTA((CLOSE - 1), 5))))), 2), 1)))), 1), 5) + TSRANK(DELAY((-1 * RET), 6), 5)) 
    def GTJAalpha157(self):
        alpha = np.minimum(rolling_product(rolling_rank(rolling_rank(np.log(ts_sum(ts_min(rolling_rank(rolling_rank((-1 * rolling_rank(delta((self.close - 1), 5))))), 2), 1)))), 1), 5) + ts_rank(delay(-1*self.returns, 6), 5)
        return alpha[self.start_date_index:self.end_date_index]
    
    #Alpha158 ((HIGH-SMA(CLOSE,15,2))-(LOW-SMA(CLOSE,15,2)))/CLOSE 
    def GTJAalpha158(self):
        alpha = ((self.high-ema(self.close,15,2))-(self.low-ema(self.close,15,2)))/self.close
        return alpha[self.start_date_index:self.end_date_index]
    
    #Alpha159 ((CLOSE-SUM(MIN(LOW,DELAY(CLOSE,1)),6))/SUM(MAX(HGIH,DELAY(CLOSE,1))-MIN(LOW,DELAY(CLOSE,1)),6) *12*24 + (CLOSE-SUM(MIN(LOW,DELAY(CLOSE,1)),12))/SUM(MAX(HGIH,DELAY(CLOSE,1))-MIN(LOW,DELAY(CL OSE,1)),12)*6*24 + (CLOSE-SUM(MIN(LOW,DELAY(CLOSE,1)),24))/SUM(MAX(HGIH,DELAY(CLOSE,1))-MIN(LOW,DELAY(CLOSE,1)),24)*6*24)*100 /(6*12+6*24+12*24) 
    def GTJAalpha159(self):
        alpha = ((self.close-ts_sum(np.minimum(self.low,delay(self.close,1)),6))/ts_sum(np.maximum(self.high,delay(self.close,1))-np.minimum(self.low,delay(self.close,1)),6) *12*24 + (self.close-ts_sum(np.minimum(self.low,delay(self.close,1)),12))/ts_sum(np.maximum(self.high,delay(self.close,1))-np.minimum(self.low,delay(self.close,1)),12)*6*24 + (self.close-ts_sum(np.minimum(self.low,delay(self.close,1)),24))/ts_sum(np.maximum(self.high,delay(self.close,1))-np.minimum(self.low,delay(self.close,1)),24)*6*24)*100 /(6*12+6*24+12*24)
        return alpha
    
    #Alpha160 SMA((CLOSE<=DELAY(CLOSE,1)?STD(CLOSE,20):0),20,1) 
    def GTJAalpha160(self):
        condition1 = (self.close <= delay(self.close,1))
        inner1_true = stddev(self.close,20)
        inner1_false = pd.Series(np.zeros(len(condition1))) 
        inner1 = pd.Series(np.where(condition1, inner1_true, inner1_false))        
        alpha = ema(inner1,20,1)
        return alpha[self.start_date_index:self.end_date_index]
    
    #Alpha161 MEAN(MAX(MAX((HIGH-LOW),ABS(DELAY(CLOSE,1)-HIGH)),ABS(DELAY(CLOSE,1)-LOW)),12) 
    def GTJAalpha161(self):
        alpha = sma(np.maximum(np.maximum((self.high-self.low),abs(delay(self.close,1)-self.high)),abs(delay(self.close,1)-self.low)),12) 
        return alpha[self.start_date_index:self.end_date_index]
    
    #Alpha162 (SMA(MAX(CLOSE-DELAY(CLOSE,1),0),12,1)/SMA(ABS(CLOSE-DELAY(CLOSE,1)),12,1)*100-MIN(SMA(MAX(CLOS E-DELAY(CLOSE,1),0),12,1)/SMA(ABS(CLOSE-DELAY(CLOSE,1)),12,1)*100,12)) / (MAX(SMA(MAX(CLOSE-DELAY(CLOSE,1),0),12,1)/SMA(ABS(CLOSE-DELAY(CLOSE,1)),12,1)*100,12)-MIN(SMA(MAX(CLOSE-DELAY(CLOSE,1),0),12, 1)/SMA(ABS(CLOSE-DELAY(CLOSE,1)),12,1)*100,12)) 
    def GTJAalpha162(self):
        alpha = (ema(np.maximum(self.close-delay(self.close,1),0),12,1)/ema(abs(self.close-delay(self.close,1)),12,1)*100-np.minimum(ema(np.maximum(self.close-delay(self.close,1),0),12,1)/ema(abs(self.close-delay(self.close,1)),12,1)*100,12)) / (np.maximum(ema(np.maximum(self.close-delay(self.close,1),0),12,1)/ema(abs(self.close-delay(self.close,1)),12,1)*100,12)-np.minimum(ema(np.maximum(self.close-delay(self.close,1),0),12,1)/ema(abs(self.close-delay(self.close,1)),12,1)*100,12)) 
        return alpha
    
    #Alpha163 RANK(((((-1 * RET) * MEAN(VOLUME,20)) * VWAP) * (HIGH - CLOSE))) 
    def GTJAalpha163(self):
        alpha = rolling_rank(((((-1 * self.returns) * sma(self.volume,20)) * self.vwap) * (self.high - self.low))) 
        return alpha[self.start_date_index:self.end_date_index]
    
    #Alpha164 SMA((((CLOSE>DELAY(CLOSE,1))?1/(CLOSE-DELAY(CLOSE,1)):1) - MIN(((CLOSE>DELAY(CLOSE,1))?1/(CLOSE-DELAY(CLOSE,1)):1),12)) / (HIGH-LOW)*100,13,2) 
    def GTJAalpha164(self):    
        condition1 = (self.close > delay(self.close,1))
        inner1_true = 1/(self.close-delay(self.close,1))
        inner1_false = pd.Series(np.ones(len(condition1))) 
        inner1 = pd.Series(np.where(condition1, inner1_true, inner1_false))  
        alpha = ema((inner1 - np.minimum(inner1,12)) / (self.high-self.low)*100,13,2) 
        return alpha[self.start_date_index:self.end_date_index]
        
    #Alpha165 MAX(SUMAC(CLOSE-MEAN(CLOSE,48)))-MIN(SUMAC(CLOSE-MEAN(CLOSE,48)))/STD(CLOSE,48) 
    def GTJAalpha165(self):   #I'm not sure if I've understood the formula correctly.
        alpha = np.nanmax(ts_sum(self.close-sma(self.close,48),48))-np.nanmin(ts_sum(self.close-sma(self.close,48),48))/stddev(self.close,48)
        return alpha[self.start_date_index:self.end_date_index]
    
    #Alpha166 -20* (20-1)^1.5*SUM(CLOSE/DELAY(CLOSE,1)-1-MEAN(CLOSE/DELAY(CLOSE,1)-1,20),20) / ((20-1)*(20-2)(SUM((CLOSE/DELAY(CLOSE,1),20)^2,20))^1.5) 
    def GTJAalpha166(self):   #I'm not sure if I've understood the formula correctly.
        alpha = -20*((20-1)**1.5) * ts_sum(self.close/delay(self.close,1) -1 -sma(self.close/delay(self.close,1)-1,20), 20) / ((20-1)*(20-2)*(ts_sum((self.close/delay(self.close,1))**2,20))**1.5) 
        return alpha[self.start_date_index:self.end_date_index]
    
    #Alpha167 SUM((CLOSE-DELAY(CLOSE,1)>0?CLOSE-DELAY(CLOSE,1):0),12) 
    def GTJAalpha167(self):
        condition1 = (self.close-delay(self.close,1) > 0)
        inner1_true = self.close-delay(self.close,1)
        inner1_false = pd.Series(np.zeros(len(condition1))) 
        inner1 = pd.Series(np.where(condition1, inner1_true, inner1_false))  
        alpha = ts_sum(inner1, 12)
        return alpha[self.start_date_index:self.end_date_index]
    
    #Alpha168 (-1*VOLUME/MEAN(VOLUME,20)) 
    def GTJAalpha168(self):
        alpha = -1*self.volume/sma(self.volume,20)
        return alpha[self.start_date_index:self.end_date_index]
    
    #Alpha169 SMA(MEAN(DELAY(SMA(CLOSE-DELAY(CLOSE,1),9,1),1),12)-MEAN(DELAY(SMA(CLOSE-DELAY(CLOSE,1),9,1),1), 26),10,1) 
    def GTJAalpha169(self):
        alpha = ema(sma(delay(ema(self.close-delay(self.close,1),9,1),1),12)-sma(delay(ema(self.close-delay(self.close,1),9,1),1), 26),10,1) 
        return alpha[self.start_date_index:self.end_date_index]
    
    #Alpha170 ((((RANK((1 / CLOSE)) * VOLUME) / MEAN(VOLUME,20)) * ((HIGH * RANK((HIGH - CLOSE))) / (SUM(HIGH, 5) / 5))) - RANK((VWAP - DELAY(VWAP, 5)))) 
    def GTJAalpha170(self):
        alpha = (((rolling_rank((1 / self.close)) * self.volume) / sma(self.volume,20)) * ((self.high * rolling_rank((self.high - self.close))) / (ts_sum(self.high,5)/5))) - rolling_rank((self.vwap - delay(self.vwap, 5)))
        return alpha[self.start_date_index:self.end_date_index]
    
    #Alpha171 ((-1 * ((LOW - CLOSE) * (OPEN^5))) / ((CLOSE - HIGH) * (CLOSE^5))) 
    def GTJAalpha171(self):
        alpha = (-1 * ((self.low - self.close) * (self.open**5))) / ((self.close - self.high) * (self.close**5))
        return alpha[self.start_date_index:self.end_date_index]
    
    #Alpha172 MEAN(ABS(SUM((LD>0 & LD>HD)?LD:0,14)*100/SUM(TR,14)-SUM((HD>0 & HD>LD)?HD:0,14)*100/SUM(TR,14))/(SUM((LD>0 & LD>HD)?LD:0,14)*100/SUM(TR,14)+SUM((HD>0 & HD>LD)?HD:0,14)*100/SUM(TR,14))*100,6) 
    def GTJAalpha172(self):
        tr = np.maximum( np.maximum(self.high-self.low,abs(self.high-delay(self.close,1))), abs(self.low-delay(self.close,1)))
        hd = self.high - delay(self.high,1)
        ld = delay(self.low,1) - self.low
        condition1 = (ld>0) & (ld>hd)
        condition2 = (hd>0) & (hd>ld)
        inner_false = pd.Series(np.zeros(len(tr)))
        inner1 = pd.Series(np.where(condition1, ld, inner_false))
        inner2 = pd.Series(np.where(condition2, hd, inner_false))
        alpha = sma(abs(ts_sum(inner1,14)*100/ts_sum(tr,14)-ts_sum(inner2,14)*100/ts_sum(tr,14)) / (ts_sum(inner1,14)*100/ts_sum(tr,14)+ts_sum(inner2,14)*100/ts_sum(tr,14)) *100, 6)
        return alpha[self.start_date_index:self.end_date_index]
    
    #Alpha173 3*SMA(CLOSE,13,2)-2*SMA(SMA(CLOSE,13,2),13,2)+SMA(SMA(SMA(LOG(CLOSE),13,2),13,2),13,2) 
    def GTJAalpha173(self):
        alpha = 3*ema(self.close,13,2) - 2*ema(ema(self.close,13,2),13,2) + ema(ema(ema(np.log(self.close),13,2),13,2),13,2)
        return alpha[self.start_date_index:self.end_date_index]
    
    #Alpha174 SMA((CLOSE>DELAY(CLOSE,1)?STD(CLOSE,20):0),20,1) 
    def GTJAalpha174(self):
        condition1 = (self.close > delay(self.close,1))
        inner1_true = stddev(self.close,20)
        inner1_false = pd.Series(np.zeros(len(condition1)))
        inner = pd.Series(np.where(condition1, inner1_true, inner1_false))
        alpha = ema(inner, 20, 1)
        return alpha[self.start_date_index:self.end_date_index]
    
    #Alpha175 MEAN(MAX(MAX((HIGH-LOW),ABS(DELAY(CLOSE,1)-HIGH)),ABS(DELAY(CLOSE,1)-LOW)),6) 
    def GTJAalpha175(self):
        alpha = sma(np.maximum(np.maximum((self.high-self.low),abs(delay(self.close,1)-self.high)),abs(delay(self.close,1)-self.low)),6) 
        return alpha[self.start_date_index:self.end_date_index]
    
    #Alpha176 CORR(RANK(((CLOSE - TSMIN(LOW, 12)) / (TSMAX(HIGH, 12) - TSMIN(LOW,12)))), RANK(VOLUME), 6) 
    def GTJAalpha176(self):
        alpha = correlation(rolling_rank(((self.close-ts_min(self.low, 12)) / (ts_max(self.high, 12)-ts_min(self.low,12)))), rolling_rank(self.volume), 6) 
        return alpha[self.start_date_index:self.end_date_index]
    
    #Alpha177 ((20-HIGHDAY(HIGH,20))/20)*100 
    def GTJAalpha177(self):
        alpha = ((20-highday(self.high,20))/20) * 100 
        return alpha[self.start_date_index:self.end_date_index]
    
    #Alpha178 (CLOSE-DELAY(CLOSE,1))/DELAY(CLOSE,1)*VOLUME 
    def GTJAalpha178(self):
        alpha = (self.close-delay(self.close,1)) / delay(self.close,1) * self.volume 
        return alpha[self.start_date_index:self.end_date_index]
    
    #Alpha179 (RANK(CORR(VWAP, VOLUME, 4)) *RANK(CORR(RANK(LOW), RANK(MEAN(VOLUME,50)), 12))) 
    def GTJAalpha179(self):
        alpha = rolling_rank(correlation(self.vwap, self.volume, 4)) * rolling_rank(correlation(rolling_rank(self.low), rolling_rank(sma(self.volume,50)), 12))
        return alpha[self.start_date_index:self.end_date_index]
    
    #Alpha180 ((MEAN(VOLUME,20) < VOLUME) ? ((-1 * TSRANK(ABS(DELTA(CLOSE, 7)), 60)) * SIGN(DELTA(CLOSE, 7)) : (-1 * VOLUME)))

    def GTJAalpha180(self):
        condition1 = (sma(self.volume,20) < self.volume)
        inner1_true = -1 * ts_rank(abs(delta(self.close, 7)), 60) * sign(delta(self.close, 7))
        inner1_false = -1 * self.volume
        alpha = pd.Series(np.where(condition1, inner1_true, inner1_false)) 
        return alpha[self.start_date_index:self.end_date_index]
    
    #Alpha181 SUM(((CLOSE/DELAY(CLOSE,1)-1)-MEAN((CLOSE/DELAY(CLOSE,1)-1),20))-(BANCHMARKINDEXCLOSE-MEAN(BANCHMARKINDEXCLOSE,20))^2,20)/SUM((BANCHMARKINDEXCLOSE-MEAN(BANCHMARKINDEXCLOSE,20))^3) 
    def GTJAalpha181(self):
        alpha = ts_sum(((self.close/delay(self.close,1)-1)-sma((self.close/delay(self.close,1)-1),20))-(self.benchmarkindexclose-sma(self.benchmarkindexclose,20))**2, 20) / ts_sum((self.benchmarkindexclose-sma(self.benchmarkindexclose,20))**3) 
        return alpha[self.start_date_index:self.end_date_index]
    
    #Alpha182 COUNT((CLOSE>OPEN & BANCHMARKINDEXCLOSE>BANCHMARKINDEXOPEN) OR (CLOSE<OPEN & BANCHMARKINDEXCLOSE<BANCHMARKINDEXOPEN),20) /20
    def GTJAalpha182(self):
        condition_count1 = ((self.close>self.open) & (self.benchmarkindexclose>self.benchmarkindexopen)) | ((self.close<self.open) & (self.benchmarkindexclose<self.benchmarkindexopen))
        count1 = pd.Series(np.nan, index=condition_count1.index)
        for i in range(20-1,len(condition_count1)):
            count1.iloc[i]=condition_count1[i-20+1:i+1].sum()         
        alpha = count1 / 20
        return alpha[self.start_date_index:self.end_date_index]
    
    #Alpha183 MAX(SUMAC(CLOSE-MEAN(CLOSE,24)))-MIN(SUMAC(CLOSE-MEAN(CLOSE,24)))/STD(CLOSE,24)
    def GTJAalpha183(self):
        alpha = np.nanmax(ts_sum(self.close-sma(self.close,24),24)) - np.nanmin(ts_sum(self.close-sma(self.close,24)))/stddev(self.close,24)
        return alpha[self.start_date_index:self.end_date_index]
    
    #Alpha184 (RANK(CORR(DELAY((OPEN - CLOSE), 1), CLOSE, 200)) + RANK((OPEN - CLOSE)))
    def GTJAalpha184(self):
        alpha = rolling_rank(correlation(delay((self.open-self.close),1),self.close,200)) + rolling_rank((self.open-self.close))
        return alpha[self.start_date_index:self.end_date_index]
    
    #Alpha185 RANK((-1 * ((1 - (OPEN / CLOSE))^2)))
    def GTJAalpha185(self): #missing parameter for rolling_rank
        alpha = rolling_rank(-1 * (1 - (self.open/self.close))**2)
        return alpha[self.start_date_index:self.end_date_index]
 
    #Alpha186 (MEAN(ABS(SUM((LD>0 & LD>HD)?LD:0,14)*100/SUM(TR,14)-SUM((HD>0 &HD>LD)?HD:0,14)*100/SUM(TR,14))/(SUM((LD>0 & LD>HD)?LD:0,14)*100/SUM(TR,14)+SUM((HD>0 & HD>LD)?HD:0,14)*100/SUM(TR,14))*100,6) + DELAY(MEAN(ABS(SUM((LD>0 & LD>HD)?LD:0,14)*100/SUM(TR,14)-SUM((HD>0 & HD>LD)?HD:0,14)*100/SUM(TR,14))/(SUM((LD>0 & LD>HD)?LD:0,14)*100/SUM(TR,14)+SUM((HD>0 & HD>LD)?HD:0,14)*100/SUM(TR,14))*100,6),6))/2
    def GTJAalpha186(self):
        tr = np.maximum( np.maximum(self.high-self.low,abs(self.high-delay(self.close,1))), abs(self.low-delay(self.close,1)))
        hd = self.high - delay(self.high,1)
        ld = delay(self.low,1) - self.low
        condition1 = (ld>0) & (ld>hd)
        condition2 = (hd>0) & (hd>ld)
        inner_false = pd.Series(np.zeros(len(tr)))
        inner1 = pd.Series(np.where(condition1, ld, inner_false))
        inner2 = pd.Series(np.where(condition2, hd, inner_false))
        alpha_part1 = sma(abs(ts_sum(inner1,14)*100/ts_sum(tr,14)-ts_sum(inner2,14)*100/ts_sum(tr,14)) / (ts_sum(inner1,14)*100/ts_sum(tr,14)+ts_sum(inner2,14)*100/ts_sum(tr,14))*100, 6) 
        alpha_part2 = delay(sma(abs(ts_sum(inner1,14)*100/ts_sum(tr,14)-ts_sum(condition2,14)*100/ts_sum(tr,14)) / (ts_sum(inner1,14)*100/ts_sum(tr,14)+ts_sum(inner1,14)*100/ts_sum(tr,14)) * 100, 6), 6)        
        alpha = (alpha_part1+alpha_part2)/2
        return alpha[self.start_date_index:self.end_date_index]
        
    #Alpha187 SUM((OPEN<=DELAY(OPEN,1)?0:MAX((HIGH-OPEN),(OPEN-DELAY(OPEN,1)))),20) 
    def GTJAalpha187(self):
        condition1 = (self.open <= delay(self.open,1))
        inner1_true = pd.Series(np.zeros(len(condition1)))
        inner1_false = np.maximum((self.high-self.open),(self.open-delay(self.open,1)))
        inner1 = pd.Series(np.where(condition1, inner1_true, inner1_false))
        alpha = ts_sum(inner1,20)  
        return alpha[self.start_date_index:self.end_date_index]
    
    #Alpha188 ((HIGH-LOW–SMA(HIGH-LOW,11,2))/SMA(HIGH-LOW,11,2))*100 
    def GTJAalpha188(self):
        alpha = ((self.high-self.low-ema(self.high-self.low,11,2))/ema(self.high-self.low,11,2))*100 
        return alpha[self.start_date_index:self.end_date_index]
    
    #Alpha189 MEAN(ABS(CLOSE-MEAN(CLOSE,6)),6) 
    def GTJAalpha189(self):
        alpha = sma(abs(self.close-sma(self.close,6)),6)
        return alpha[self.start_date_index:self.end_date_index]
    
    #Alpha190 LOG((COUNT(CLOSE/DELAY(CLOSE)-1>((CLOSE/DELAY(CLOSE,19))^(1/20)-1),20)-1)*(SUMIF(((CLOSE/DELAY(CLOSE)-1-(CLOSE/DELAY(CLOSE,19))^(1/20)-1))^2,20,CLOSE/DELAY(CLOSE)-1<(CLOSE/DELAY(CLOSE,19))^(1/20)-1))/((COUNT((CLOSE/DELAY(CLOSE)-1<(CLOSE/DELAY(CLOSE,19))^(1/20)-1),20))*(SUMIF((CLOSE/DELAY(CLOSE)-1-((CLOSE/DELAY(CLOSE,19))^(1/20)-1))^2,20,CLOSE/DELAY(CLOSE)-1>(CLOSE/DELAY(CLOSE,19))^(1/20)-1))))
    def GTJAalpha190(self):
        condition_count1 = self.close/delay(self.close)-1 > ((self.close/delay(self.close,19))**(1/20) -1)
        count1 = pd.Series(np.nan, index=condition_count1.index)
        for i in range(20-1,len(condition_count1)):
            count1.iloc[i]=condition_count1[i-20+1:i+1].sum()
        condition_count2 = self.close/delay(self.close)-1 < ((self.close/delay(self.close,19))**(1/20) -1)
        count2 = pd.Series(np.nan, index=condition_count2.index)
        for i in range(20-1,len(condition_count2)):
            count2.iloc[i]=condition_count2[i-20+1:i+1].sum()        
        condition_sumif1 = self.close/delay(self.close)-1 < (self.close/delay(self.close,19))**(1/20)-1
        condition_sumif2 = self.close/delay(self.close)-1 > (self.close/delay(self.close,19))**(1/20)-1
        inner_sumif = (self.close/delay(self.close)-1-(self.close/delay(self.close,19))**(1/20)-1)**2
        value1 = ts_sum(inner_sumif * condition_sumif1, window=20)
        value2 = ts_sum(inner_sumif * condition_sumif2, window=20)
        alpha = np.log( (count1-1)*(value1)/((count2)*(value2)) )        
        return alpha[self.start_date_index:self.end_date_index]
    
    #Alpha191 ((CORR(MEAN(VOLUME,20), LOW, 5) + ((HIGH + LOW) / 2)) - CLOSE)
    def GTJAalpha191(self):
        alpha = (correlation(sma(self.volume,20),self.low,5) + ((self.high+self.low)/2)) - self.close
        return alpha[self.start_date_index:self.end_date_index]



def get_Alpha101_allalphas(ts_code="000001.SZ",start_date=20210101,end_date=20211231): #取出一个股票的所有alpha的接口  
    Alpha101_results = Alphas(ts_code,start_date=start_date,end_date=end_date)
    Alpha101_results.initializer()
    df = pd.DataFrame(Alpha101_results.alpha001().rename("Alpha001"))
    df['Alpha002']=Alpha101_results.alpha002()
    df['Alpha003']=Alpha101_results.alpha003()
    df['Alpha004']=Alpha101_results.alpha004()
    df['Alpha005']=Alpha101_results.alpha005()
    df['Alpha006']=Alpha101_results.alpha006()
    df['Alpha007']=Alpha101_results.alpha007()
    df['Alpha008']=Alpha101_results.alpha008()
    df['Alpha009']=Alpha101_results.alpha009()
    df['Alpha010']=Alpha101_results.alpha010()
    df['Alpha011']=Alpha101_results.alpha011()
    df['Alpha012']=Alpha101_results.alpha012()
    df['Alpha013']=Alpha101_results.alpha013()
    df['Alpha014']=Alpha101_results.alpha014()
    df['Alpha015']=Alpha101_results.alpha015()
    df['Alpha016']=Alpha101_results.alpha016()
    df['Alpha017']=Alpha101_results.alpha017()
    df['Alpha018']=Alpha101_results.alpha018()
    df['Alpha019']=Alpha101_results.alpha019()
    df['Alpha020']=Alpha101_results.alpha020()
    df['Alpha021']=Alpha101_results.alpha021()
    df['Alpha022']=Alpha101_results.alpha022()
    df['Alpha023']=Alpha101_results.alpha023()
    df['Alpha024']=Alpha101_results.alpha024()
    df['Alpha025']=Alpha101_results.alpha025()
    df['Alpha026']=Alpha101_results.alpha026()
    df['Alpha027']=Alpha101_results.alpha027()
    df['Alpha028']=Alpha101_results.alpha028()
    df['Alpha029']=Alpha101_results.alpha029()
    df['Alpha030']=Alpha101_results.alpha030()
    df['Alpha031']=Alpha101_results.alpha031()
    df['Alpha032']=Alpha101_results.alpha032()
    df['Alpha033']=Alpha101_results.alpha033()
    df['Alpha034']=Alpha101_results.alpha034()
    df['Alpha035']=Alpha101_results.alpha035()
    df['Alpha036']=Alpha101_results.alpha036()
    df['Alpha037']=Alpha101_results.alpha037()
    df['Alpha038']=Alpha101_results.alpha038()
    df['Alpha039']=Alpha101_results.alpha039()
    df['Alpha040']=Alpha101_results.alpha040()
    df['Alpha041']=Alpha101_results.alpha041()
    df['Alpha042']=Alpha101_results.alpha042()
    df['Alpha043']=Alpha101_results.alpha043()
    df['Alpha044']=Alpha101_results.alpha044()
    df['Alpha045']=Alpha101_results.alpha045()
    df['Alpha046']=Alpha101_results.alpha046()
    df['Alpha047']=Alpha101_results.alpha047()
    df['Alpha048']=Alpha101_results.alpha048()
    df['Alpha049']=Alpha101_results.alpha049()
    df['Alpha050']=Alpha101_results.alpha050()
    df['Alpha051']=Alpha101_results.alpha051()
    df['Alpha052']=Alpha101_results.alpha052()
    df['Alpha053']=Alpha101_results.alpha053()
    df['Alpha054']=Alpha101_results.alpha054()
    df['Alpha055']=Alpha101_results.alpha055()
    df['Alpha047']=Alpha101_results.alpha056()
    df['Alpha057']=Alpha101_results.alpha057()
    df['Alpha058']=Alpha101_results.alpha058()        
    df['Alpha059']=Alpha101_results.alpha059()        
    df['Alpha060']=Alpha101_results.alpha060()
    df['Alpha061']=Alpha101_results.alpha061()
    df['Alpha062']=Alpha101_results.alpha062()
    df['Alpha063']=Alpha101_results.alpha063()
    df['Alpha064']=Alpha101_results.alpha064()
    df['Alpha065']=Alpha101_results.alpha065()
    df['Alpha066']=Alpha101_results.alpha066()
    df['Alpha067']=Alpha101_results.alpha067()
    df['Alpha068']=Alpha101_results.alpha068()
    df['Alpha069']=Alpha101_results.alpha069()
    df['Alpha070']=Alpha101_results.alpha070()
    df['Alpha071']=Alpha101_results.alpha071()
    df['Alpha072']=Alpha101_results.alpha072()
    df['Alpha073']=Alpha101_results.alpha073()
    df['Alpha074']=Alpha101_results.alpha074()
    df['Alpha075']=Alpha101_results.alpha075()
    df['Alpha076']=Alpha101_results.alpha076()
    df['Alpha077']=Alpha101_results.alpha077()
    df['Alpha078']=Alpha101_results.alpha078()
    df['Alpha079']=Alpha101_results.alpha079()
    df['Alpha080']=Alpha101_results.alpha080()
    df['Alpha081']=Alpha101_results.alpha081()
    df['Alpha082']=Alpha101_results.alpha082()
    df['Alpha083']=Alpha101_results.alpha083()
    df['Alpha084']=Alpha101_results.alpha084()
    df['Alpha085']=Alpha101_results.alpha085()
    df['Alpha086']=Alpha101_results.alpha086()
    df['Alpha087']=Alpha101_results.alpha087()
    df['Alpha088']=Alpha101_results.alpha088()
    df['Alpha089']=Alpha101_results.alpha089()
    df['Alpha090']=Alpha101_results.alpha090()
    df['Alpha091']=Alpha101_results.alpha091()
    df['Alpha092']=Alpha101_results.alpha092()
    df['Alpha093']=Alpha101_results.alpha093()
    df['Alpha094']=Alpha101_results.alpha094()
    df['Alpha095']=Alpha101_results.alpha095()
    df['Alpha096']=Alpha101_results.alpha096()
    df['Alpha097']=Alpha101_results.alpha097()
    df['Alpha098']=Alpha101_results.alpha098()
    df['Alpha099']=Alpha101_results.alpha099()
    df['Alpha100']=Alpha101_results.alpha100()
    df['Alpha101']=Alpha101_results.alpha101()  
    df.index = Alpha101_results.output_dates
    return df



def get_GTJAalphas_allalphas(ts_code="000001.SZ",start_date=20210101,end_date=20211231):  
    GTJAalphas_results = GTJAalphas(ts_code,start_date=start_date,end_date=end_date)
    GTJAalphas_results.initializer()
    df = pd.DataFrame(GTJAalphas_results.GTJAalpha001().rename('GTJAalpha001'))
    df['GTJAalpha002']=GTJAalphas_results.GTJAalpha002()
    df['GTJAalpha003']=GTJAalphas_results.GTJAalpha003()
    df['GTJAalpha004']=GTJAalphas_results.GTJAalpha004()
    df['GTJAalpha005']=GTJAalphas_results.GTJAalpha005()
    df['GTJAalpha006']=GTJAalphas_results.GTJAalpha006()
    df['GTJAalpha007']=GTJAalphas_results.GTJAalpha007()
    df['GTJAalpha008']=GTJAalphas_results.GTJAalpha008()
    df['GTJAalpha009']=GTJAalphas_results.GTJAalpha009()
    df['GTJAalpha010']=GTJAalphas_results.GTJAalpha010()
    df['GTJAalpha011']=GTJAalphas_results.GTJAalpha011()
    df['GTJAalpha012']=GTJAalphas_results.GTJAalpha012()
    df['GTJAalpha013']=GTJAalphas_results.GTJAalpha013()
    df['GTJAalpha014']=GTJAalphas_results.GTJAalpha014()
    df['GTJAalpha015']=GTJAalphas_results.GTJAalpha015()
    df['GTJAalpha016']=GTJAalphas_results.GTJAalpha016()
    df['GTJAalpha017']=GTJAalphas_results.GTJAalpha017()
    df['GTJAalpha018']=GTJAalphas_results.GTJAalpha018()
    df['GTJAalpha019']=GTJAalphas_results.GTJAalpha019()
    df['GTJAalpha020']=GTJAalphas_results.GTJAalpha020()
    df['GTJAalpha021']=GTJAalphas_results.GTJAalpha021()
    df['GTJAalpha022']=GTJAalphas_results.GTJAalpha022()
    df['GTJAalpha023']=GTJAalphas_results.GTJAalpha023()
    df['GTJAalpha024']=GTJAalphas_results.GTJAalpha024()
    df['GTJAalpha025']=GTJAalphas_results.GTJAalpha025()
    df['GTJAalpha026']=GTJAalphas_results.GTJAalpha026()
    df['GTJAalpha027']=GTJAalphas_results.GTJAalpha027()
    df['GTJAalpha028']=GTJAalphas_results.GTJAalpha028()
    df['GTJAalpha029']=GTJAalphas_results.GTJAalpha029()
    df['GTJAalpha030']=GTJAalphas_results.GTJAalpha030()
    df['GTJAalpha031']=GTJAalphas_results.GTJAalpha031()
    df['GTJAalpha032']=GTJAalphas_results.GTJAalpha032()
    df['GTJAalpha033']=GTJAalphas_results.GTJAalpha033()
    df['GTJAalpha034']=GTJAalphas_results.GTJAalpha034()
    df['GTJAalpha035']=GTJAalphas_results.GTJAalpha035()
    df['GTJAalpha036']=GTJAalphas_results.GTJAalpha036()
    df['GTJAalpha037']=GTJAalphas_results.GTJAalpha037()
    df['GTJAalpha038']=GTJAalphas_results.GTJAalpha038()
    df['GTJAalpha039']=GTJAalphas_results.GTJAalpha039()
    df['GTJAalpha040']=GTJAalphas_results.GTJAalpha040()
    df['GTJAalpha041']=GTJAalphas_results.GTJAalpha041()
    df['GTJAalpha042']=GTJAalphas_results.GTJAalpha042()
    df['GTJAalpha043']=GTJAalphas_results.GTJAalpha043()
    df['GTJAalpha044']=GTJAalphas_results.GTJAalpha044()
    df['GTJAalpha045']=GTJAalphas_results.GTJAalpha045()
    df['GTJAalpha046']=GTJAalphas_results.GTJAalpha046()
    df['GTJAalpha047']=GTJAalphas_results.GTJAalpha047()
    df['GTJAalpha048']=GTJAalphas_results.GTJAalpha048()
    df['GTJAalpha049']=GTJAalphas_results.GTJAalpha049()
    df['GTJAalpha050']=GTJAalphas_results.GTJAalpha050()
    df['GTJAalpha051']=GTJAalphas_results.GTJAalpha051()
    df['GTJAalpha052']=GTJAalphas_results.GTJAalpha052()
    df['GTJAalpha053']=GTJAalphas_results.GTJAalpha053()
    df['GTJAalpha054']=GTJAalphas_results.GTJAalpha054()
    df['GTJAalpha055']=GTJAalphas_results.GTJAalpha055()
    df['GTJAalpha047']=GTJAalphas_results.GTJAalpha056()
    df['GTJAalpha057']=GTJAalphas_results.GTJAalpha057()
    df['GTJAalpha058']=GTJAalphas_results.GTJAalpha058()        
    df['GTJAalpha059']=GTJAalphas_results.GTJAalpha059()        
    df['GTJAalpha060']=GTJAalphas_results.GTJAalpha060()
    df['GTJAalpha061']=GTJAalphas_results.GTJAalpha061()
    df['GTJAalpha062']=GTJAalphas_results.GTJAalpha062()
    df['GTJAalpha063']=GTJAalphas_results.GTJAalpha063()
    df['GTJAalpha064']=GTJAalphas_results.GTJAalpha064()
    df['GTJAalpha065']=GTJAalphas_results.GTJAalpha065()
    df['GTJAalpha066']=GTJAalphas_results.GTJAalpha066()
    df['GTJAalpha067']=GTJAalphas_results.GTJAalpha067()
    df['GTJAalpha068']=GTJAalphas_results.GTJAalpha068()
    df['GTJAalpha069']=GTJAalphas_results.GTJAalpha069()
    df['GTJAalpha070']=GTJAalphas_results.GTJAalpha070()
    df['GTJAalpha071']=GTJAalphas_results.GTJAalpha071()
    df['GTJAalpha072']=GTJAalphas_results.GTJAalpha072()
    df['GTJAalpha073']=GTJAalphas_results.GTJAalpha073()
    df['GTJAalpha074']=GTJAalphas_results.GTJAalpha074()
    df['GTJAalpha075']=GTJAalphas_results.GTJAalpha075()
    df['GTJAalpha076']=GTJAalphas_results.GTJAalpha076()
    df['GTJAalpha077']=GTJAalphas_results.GTJAalpha077()
    df['GTJAalpha078']=GTJAalphas_results.GTJAalpha078()
    df['GTJAalpha079']=GTJAalphas_results.GTJAalpha079()
    df['GTJAalpha080']=GTJAalphas_results.GTJAalpha080()
    df['GTJAalpha081']=GTJAalphas_results.GTJAalpha081()
    df['GTJAalpha082']=GTJAalphas_results.GTJAalpha082()
    df['GTJAalpha083']=GTJAalphas_results.GTJAalpha083()
    df['GTJAalpha084']=GTJAalphas_results.GTJAalpha084()
    df['GTJAalpha085']=GTJAalphas_results.GTJAalpha085()
    df['GTJAalpha086']=GTJAalphas_results.GTJAalpha086()
    df['GTJAalpha087']=GTJAalphas_results.GTJAalpha087()
    df['GTJAalpha088']=GTJAalphas_results.GTJAalpha088()
    df['GTJAalpha089']=GTJAalphas_results.GTJAalpha089()
    df['GTJAalpha090']=GTJAalphas_results.GTJAalpha090()
    df['GTJAalpha091']=GTJAalphas_results.GTJAalpha091()
    df['GTJAalpha092']=GTJAalphas_results.GTJAalpha092()
    df['GTJAalpha093']=GTJAalphas_results.GTJAalpha093()
    df['GTJAalpha094']=GTJAalphas_results.GTJAalpha094()
    df['GTJAalpha095']=GTJAalphas_results.GTJAalpha095()
    df['GTJAalpha096']=GTJAalphas_results.GTJAalpha096()
    df['GTJAalpha097']=GTJAalphas_results.GTJAalpha097()
    df['GTJAalpha098']=GTJAalphas_results.GTJAalpha098()
    df['GTJAalpha099']=GTJAalphas_results.GTJAalpha099()
    df['GTJAalpha100']=GTJAalphas_results.GTJAalpha100()
    df['GTJAalpha101']=GTJAalphas_results.GTJAalpha101() 
    df['GTJAalpha102']=GTJAalphas_results.GTJAalpha102()
    df['GTJAalpha103']=GTJAalphas_results.GTJAalpha103()
    df['GTJAalpha104']=GTJAalphas_results.GTJAalpha104()
    df['GTJAalpha105']=GTJAalphas_results.GTJAalpha105()
    df['GTJAalpha106']=GTJAalphas_results.GTJAalpha106()
    df['GTJAalpha107']=GTJAalphas_results.GTJAalpha107()
    df['GTJAalpha108']=GTJAalphas_results.GTJAalpha108()
    df['GTJAalpha109']=GTJAalphas_results.GTJAalpha109()
    df['GTJAalpha110']=GTJAalphas_results.GTJAalpha110()
    df['GTJAalpha111']=GTJAalphas_results.GTJAalpha111()
    df['GTJAalpha112']=GTJAalphas_results.GTJAalpha112()
    df['GTJAalpha113']=GTJAalphas_results.GTJAalpha113()
    df['GTJAalpha114']=GTJAalphas_results.GTJAalpha114()
    df['GTJAalpha115']=GTJAalphas_results.GTJAalpha115()
    df['GTJAalpha116']=GTJAalphas_results.GTJAalpha116()
    df['GTJAalpha117']=GTJAalphas_results.GTJAalpha117()
    df['GTJAalpha118']=GTJAalphas_results.GTJAalpha118()
    df['GTJAalpha119']=GTJAalphas_results.GTJAalpha119()
    df['GTJAalpha120']=GTJAalphas_results.GTJAalpha120()
    df['GTJAalpha121']=GTJAalphas_results.GTJAalpha121()
    df['GTJAalpha122']=GTJAalphas_results.GTJAalpha122()
    df['GTJAalpha123']=GTJAalphas_results.GTJAalpha123()
    df['GTJAalpha124']=GTJAalphas_results.GTJAalpha124()
    df['GTJAalpha125']=GTJAalphas_results.GTJAalpha125()
    df['GTJAalpha126']=GTJAalphas_results.GTJAalpha126()
    df['GTJAalpha127']=GTJAalphas_results.GTJAalpha127()
    df['GTJAalpha128']=GTJAalphas_results.GTJAalpha128()
    df['GTJAalpha129']=GTJAalphas_results.GTJAalpha129()
    df['GTJAalpha130']=GTJAalphas_results.GTJAalpha130()
    df['GTJAalpha131']=GTJAalphas_results.GTJAalpha131()
    df['GTJAalpha132']=GTJAalphas_results.GTJAalpha132()
    df['GTJAalpha133']=GTJAalphas_results.GTJAalpha133()
    df['GTJAalpha134']=GTJAalphas_results.GTJAalpha134()
    df['GTJAalpha135']=GTJAalphas_results.GTJAalpha135()
    df['GTJAalpha136']=GTJAalphas_results.GTJAalpha136()
    df['GTJAalpha137']=GTJAalphas_results.GTJAalpha137()
    df['GTJAalpha138']=GTJAalphas_results.GTJAalpha138()
    df['GTJAalpha139']=GTJAalphas_results.GTJAalpha139()
    df['GTJAalpha140']=GTJAalphas_results.GTJAalpha140()
    df['GTJAalpha141']=GTJAalphas_results.GTJAalpha141()
    df['GTJAalpha142']=GTJAalphas_results.GTJAalpha142()
    df['GTJAalpha143']=GTJAalphas_results.GTJAalpha143()
    df['GTJAalpha144']=GTJAalphas_results.GTJAalpha144()
    df['GTJAalpha145']=GTJAalphas_results.GTJAalpha145()
    df['GTJAalpha146']=GTJAalphas_results.GTJAalpha146()
    df['GTJAalpha147']=GTJAalphas_results.GTJAalpha147()
    df['GTJAalpha148']=GTJAalphas_results.GTJAalpha148()
    df['GTJAalpha149']=GTJAalphas_results.GTJAalpha149()
    df['GTJAalpha150']=GTJAalphas_results.GTJAalpha150()
    df['GTJAalpha151']=GTJAalphas_results.GTJAalpha151()
    df['GTJAalpha152']=GTJAalphas_results.GTJAalpha152()
    df['GTJAalpha153']=GTJAalphas_results.GTJAalpha153()
    df['GTJAalpha154']=GTJAalphas_results.GTJAalpha154()
    df['GTJAalpha155']=GTJAalphas_results.GTJAalpha155()
    df['GTJAalpha147']=GTJAalphas_results.GTJAalpha156()
    df['GTJAalpha157']=GTJAalphas_results.GTJAalpha157()
    df['GTJAalpha158']=GTJAalphas_results.GTJAalpha158()        
    df['GTJAalpha159']=GTJAalphas_results.GTJAalpha159()        
    df['GTJAalpha160']=GTJAalphas_results.GTJAalpha160()
    df['GTJAalpha161']=GTJAalphas_results.GTJAalpha161()
    df['GTJAalpha162']=GTJAalphas_results.GTJAalpha162()
    df['GTJAalpha163']=GTJAalphas_results.GTJAalpha163()
    df['GTJAalpha164']=GTJAalphas_results.GTJAalpha164()
    df['GTJAalpha165']=GTJAalphas_results.GTJAalpha165()
    df['GTJAalpha166']=GTJAalphas_results.GTJAalpha166()
    df['GTJAalpha167']=GTJAalphas_results.GTJAalpha167()
    df['GTJAalpha168']=GTJAalphas_results.GTJAalpha168()
    df['GTJAalpha169']=GTJAalphas_results.GTJAalpha169()
    df['GTJAalpha170']=GTJAalphas_results.GTJAalpha170()
    df['GTJAalpha171']=GTJAalphas_results.GTJAalpha171()
    df['GTJAalpha172']=GTJAalphas_results.GTJAalpha172()
    df['GTJAalpha173']=GTJAalphas_results.GTJAalpha173()
    df['GTJAalpha174']=GTJAalphas_results.GTJAalpha174()
    df['GTJAalpha175']=GTJAalphas_results.GTJAalpha175()
    df['GTJAalpha176']=GTJAalphas_results.GTJAalpha176()
    df['GTJAalpha177']=GTJAalphas_results.GTJAalpha177()
    df['GTJAalpha178']=GTJAalphas_results.GTJAalpha178()
    df['GTJAalpha179']=GTJAalphas_results.GTJAalpha179()
    df['GTJAalpha180']=GTJAalphas_results.GTJAalpha180()
    df['GTJAalpha181']=GTJAalphas_results.GTJAalpha181()
    df['GTJAalpha182']=GTJAalphas_results.GTJAalpha182()
    df['GTJAalpha183']=GTJAalphas_results.GTJAalpha183()
    df['GTJAalpha184']=GTJAalphas_results.GTJAalpha184()
    df['GTJAalpha185']=GTJAalphas_results.GTJAalpha185()
    df['GTJAalpha186']=GTJAalphas_results.GTJAalpha186()
    df['GTJAalpha187']=GTJAalphas_results.GTJAalpha187()
    df['GTJAalpha188']=GTJAalphas_results.GTJAalpha188()
    df['GTJAalpha189']=GTJAalphas_results.GTJAalpha189()
    df['GTJAalpha190']=GTJAalphas_results.GTJAalpha190()
    df['GTJAalpha191']=GTJAalphas_results.GTJAalpha191()
    df.index = GTJAalphas_results.output_dates
    return df    



def get_Alpha101_allstocks(alpha_name="Alpha001", start_date=20210101, end_date=20211231):  #取出所有股票的单个alpha的接口
    Alpha101_results = Alphas(ts_code="All",start_date=start_date,end_date=end_date)
    stock_list=local_source.get_stock_list(cols='TS_CODE,INDUSTRY')["TS_CODE"]
    df_all = 0
    for ts_code in pb(stock_list, desc='Please wait', colour='#ffffff'):
        try: #防止所给日期区间内没有交易日
            Alpha101_results.initializer(ts_code_chosen=ts_code)
            if alpha_name == 'Alpha001': df = pd.DataFrame(Alpha101_results.alpha001().rename("Alpha001"))
            elif alpha_name == 'Alpha002': df = pd.DataFrame(Alpha101_results.alpha002().rename("Alpha002"))
            elif alpha_name == 'Alpha003': df = pd.DataFrame(Alpha101_results.alpha003().rename("Alpha003"))
            elif alpha_name == 'Alpha004': df = pd.DataFrame(Alpha101_results.alpha004().rename("Alpha004"))
            elif alpha_name == 'Alpha005': df = pd.DataFrame(Alpha101_results.alpha005().rename("Alpha005"))
            elif alpha_name == 'Alpha006': df = pd.DataFrame(Alpha101_results.alpha006().rename("Alpha006"))
            elif alpha_name == 'Alpha007': df = pd.DataFrame(Alpha101_results.alpha007().rename("Alpha007"))
            elif alpha_name == 'Alpha008': df = pd.DataFrame(Alpha101_results.alpha008().rename("Alpha008"))
            elif alpha_name == 'Alpha009': df = pd.DataFrame(Alpha101_results.alpha009().rename("Alpha009"))
            elif alpha_name == 'Alpha011': df = pd.DataFrame(Alpha101_results.alpha011().rename("Alpha011"))
            elif alpha_name == 'Alpha012': df = pd.DataFrame(Alpha101_results.alpha012().rename("Alpha012"))
            elif alpha_name == 'Alpha013': df = pd.DataFrame(Alpha101_results.alpha013().rename("Alpha013"))
            elif alpha_name == 'Alpha014': df = pd.DataFrame(Alpha101_results.alpha014().rename("Alpha014"))
            elif alpha_name == 'Alpha015': df = pd.DataFrame(Alpha101_results.alpha015().rename("Alpha015"))
            elif alpha_name == 'Alpha016': df = pd.DataFrame(Alpha101_results.alpha016().rename("Alpha016"))
            elif alpha_name == 'Alpha017': df = pd.DataFrame(Alpha101_results.alpha017().rename("Alpha017"))
            elif alpha_name == 'Alpha018': df = pd.DataFrame(Alpha101_results.alpha018().rename("Alpha018"))
            elif alpha_name == 'Alpha019': df = pd.DataFrame(Alpha101_results.alpha019().rename("Alpha019"))        
            elif alpha_name == 'Alpha021': df = pd.DataFrame(Alpha101_results.alpha021().rename("Alpha021"))
            elif alpha_name == 'Alpha022': df = pd.DataFrame(Alpha101_results.alpha022().rename("Alpha022"))
            elif alpha_name == 'Alpha023': df = pd.DataFrame(Alpha101_results.alpha023().rename("Alpha023"))
            elif alpha_name == 'Alpha024': df = pd.DataFrame(Alpha101_results.alpha024().rename("Alpha024"))
            elif alpha_name == 'Alpha025': df = pd.DataFrame(Alpha101_results.alpha025().rename("Alpha025"))
            elif alpha_name == 'Alpha026': df = pd.DataFrame(Alpha101_results.alpha026().rename("Alpha026"))
            elif alpha_name == 'Alpha027': df = pd.DataFrame(Alpha101_results.alpha027().rename("Alpha027"))
            elif alpha_name == 'Alpha028': df = pd.DataFrame(Alpha101_results.alpha028().rename("Alpha028"))
            elif alpha_name == 'Alpha029': df = pd.DataFrame(Alpha101_results.alpha029().rename("Alpha029"))
            elif alpha_name == 'Alpha031': df = pd.DataFrame(Alpha101_results.alpha031().rename("Alpha031"))
            elif alpha_name == 'Alpha032': df = pd.DataFrame(Alpha101_results.alpha032().rename("Alpha032"))
            elif alpha_name == 'Alpha033': df = pd.DataFrame(Alpha101_results.alpha033().rename("Alpha033"))
            elif alpha_name == 'Alpha034': df = pd.DataFrame(Alpha101_results.alpha034().rename("Alpha034"))
            elif alpha_name == 'Alpha035': df = pd.DataFrame(Alpha101_results.alpha035().rename("Alpha035"))
            elif alpha_name == 'Alpha036': df = pd.DataFrame(Alpha101_results.alpha036().rename("Alpha036"))
            elif alpha_name == 'Alpha037': df = pd.DataFrame(Alpha101_results.alpha037().rename("Alpha037"))
            elif alpha_name == 'Alpha038': df = pd.DataFrame(Alpha101_results.alpha038().rename("Alpha038"))
            elif alpha_name == 'Alpha039': df = pd.DataFrame(Alpha101_results.alpha039().rename("Alpha039"))
            elif alpha_name == 'Alpha041': df = pd.DataFrame(Alpha101_results.alpha041().rename("Alpha041"))
            elif alpha_name == 'Alpha042': df = pd.DataFrame(Alpha101_results.alpha042().rename("Alpha042"))
            elif alpha_name == 'Alpha043': df = pd.DataFrame(Alpha101_results.alpha043().rename("Alpha043"))
            elif alpha_name == 'Alpha044': df = pd.DataFrame(Alpha101_results.alpha044().rename("Alpha044"))
            elif alpha_name == 'Alpha045': df = pd.DataFrame(Alpha101_results.alpha045().rename("Alpha045"))
            elif alpha_name == 'Alpha046': df = pd.DataFrame(Alpha101_results.alpha046().rename("Alpha046"))
            elif alpha_name == 'Alpha047': df = pd.DataFrame(Alpha101_results.alpha047().rename("Alpha047"))
            elif alpha_name == 'Alpha048': df = pd.DataFrame(Alpha101_results.alpha048().rename("Alpha048"))
            elif alpha_name == 'Alpha049': df = pd.DataFrame(Alpha101_results.alpha049().rename("Alpha049"))
            elif alpha_name == 'Alpha051': df = pd.DataFrame(Alpha101_results.alpha051().rename("Alpha051"))
            elif alpha_name == 'Alpha052': df = pd.DataFrame(Alpha101_results.alpha052().rename("Alpha052"))
            elif alpha_name == 'Alpha053': df = pd.DataFrame(Alpha101_results.alpha053().rename("Alpha053"))
            elif alpha_name == 'Alpha054': df = pd.DataFrame(Alpha101_results.alpha054().rename("Alpha054"))
            elif alpha_name == 'Alpha055': df = pd.DataFrame(Alpha101_results.alpha055().rename("Alpha055"))
            elif alpha_name == 'Alpha056': df = pd.DataFrame(Alpha101_results.alpha056().rename("Alpha056"))
            elif alpha_name == 'Alpha057': df = pd.DataFrame(Alpha101_results.alpha057().rename("Alpha057"))
            elif alpha_name == 'Alpha058': df = pd.DataFrame(Alpha101_results.alpha058().rename("Alpha058"))
            elif alpha_name == 'Alpha059': df = pd.DataFrame(Alpha101_results.alpha059().rename("Alpha059"))
            elif alpha_name == 'Alpha061': df = pd.DataFrame(Alpha101_results.alpha061().rename("Alpha061"))
            elif alpha_name == 'Alpha062': df = pd.DataFrame(Alpha101_results.alpha062().rename("Alpha062"))
            elif alpha_name == 'Alpha063': df = pd.DataFrame(Alpha101_results.alpha063().rename("Alpha063"))
            elif alpha_name == 'Alpha064': df = pd.DataFrame(Alpha101_results.alpha064().rename("Alpha064"))
            elif alpha_name == 'Alpha065': df = pd.DataFrame(Alpha101_results.alpha065().rename("Alpha065"))
            elif alpha_name == 'Alpha066': df = pd.DataFrame(Alpha101_results.alpha066().rename("Alpha066"))
            elif alpha_name == 'Alpha067': df = pd.DataFrame(Alpha101_results.alpha067().rename("Alpha067"))
            elif alpha_name == 'Alpha068': df = pd.DataFrame(Alpha101_results.alpha068().rename("Alpha068"))
            elif alpha_name == 'Alpha069': df = pd.DataFrame(Alpha101_results.alpha069().rename("Alpha069"))
            elif alpha_name == 'Alpha071': df = pd.DataFrame(Alpha101_results.alpha071().rename("Alpha071"))
            elif alpha_name == 'Alpha072': df = pd.DataFrame(Alpha101_results.alpha072().rename("Alpha072"))
            elif alpha_name == 'Alpha073': df = pd.DataFrame(Alpha101_results.alpha073().rename("Alpha073"))
            elif alpha_name == 'Alpha074': df = pd.DataFrame(Alpha101_results.alpha074().rename("Alpha074"))
            elif alpha_name == 'Alpha075': df = pd.DataFrame(Alpha101_results.alpha075().rename("Alpha075"))
            elif alpha_name == 'Alpha076': df = pd.DataFrame(Alpha101_results.alpha076().rename("Alpha076"))
            elif alpha_name == 'Alpha077': df = pd.DataFrame(Alpha101_results.alpha077().rename("Alpha077"))
            elif alpha_name == 'Alpha078': df = pd.DataFrame(Alpha101_results.alpha078().rename("Alpha078"))
            elif alpha_name == 'Alpha079': df = pd.DataFrame(Alpha101_results.alpha079().rename("Alpha079"))
            elif alpha_name == 'Alpha081': df = pd.DataFrame(Alpha101_results.alpha081().rename("Alpha081"))
            elif alpha_name == 'Alpha082': df = pd.DataFrame(Alpha101_results.alpha082().rename("Alpha082"))
            elif alpha_name == 'Alpha083': df = pd.DataFrame(Alpha101_results.alpha083().rename("Alpha083"))
            elif alpha_name == 'Alpha084': df = pd.DataFrame(Alpha101_results.alpha084().rename("Alpha084"))
            elif alpha_name == 'Alpha085': df = pd.DataFrame(Alpha101_results.alpha085().rename("Alpha085"))
            elif alpha_name == 'Alpha086': df = pd.DataFrame(Alpha101_results.alpha086().rename("Alpha086"))
            elif alpha_name == 'Alpha087': df = pd.DataFrame(Alpha101_results.alpha087().rename("Alpha087"))
            elif alpha_name == 'Alpha088': df = pd.DataFrame(Alpha101_results.alpha088().rename("Alpha088"))
            elif alpha_name == 'Alpha089': df = pd.DataFrame(Alpha101_results.alpha089().rename("Alpha089"))
            elif alpha_name == 'Alpha091': df = pd.DataFrame(Alpha101_results.alpha091().rename("Alpha091"))
            elif alpha_name == 'Alpha092': df = pd.DataFrame(Alpha101_results.alpha092().rename("Alpha092"))
            elif alpha_name == 'Alpha093': df = pd.DataFrame(Alpha101_results.alpha093().rename("Alpha093"))
            elif alpha_name == 'Alpha094': df = pd.DataFrame(Alpha101_results.alpha094().rename("Alpha094"))
            elif alpha_name == 'Alpha095': df = pd.DataFrame(Alpha101_results.alpha095().rename("Alpha095"))
            elif alpha_name == 'Alpha096': df = pd.DataFrame(Alpha101_results.alpha096().rename("Alpha096"))
            elif alpha_name == 'Alpha097': df = pd.DataFrame(Alpha101_results.alpha097().rename("Alpha097"))
            elif alpha_name == 'Alpha098': df = pd.DataFrame(Alpha101_results.alpha098().rename("Alpha098"))
            elif alpha_name == 'Alpha099': df = pd.DataFrame(Alpha101_results.alpha099().rename("Alpha099"))
            elif alpha_name == 'Alpha100': df = pd.DataFrame(Alpha101_results.alpha100().rename("Alpha100"))
            elif alpha_name == 'Alpha101': df = pd.DataFrame(Alpha101_results.alpha101().rename("Alpha101"))
            df = pd.concat([Alpha101_results.output_dates, df],axis=1)
            df.insert(loc=0,column='TS_CODE',value=ts_code)
            if type(df_all) == int:
                df_all = df
            else:
                df_all = pd.concat([df_all, df],axis=0)    
        except:
            pass
    return df_all



def get_GTJAalphas_allstocks(alpha_name="GTJAalpha001", start_date=20210101, end_date=20211231):
    GTJAalphas_results = GTJAalphas(ts_code="All",start_date=start_date,end_date=end_date)
    stock_list=local_source.get_stock_list(cols='TS_CODE,INDUSTRY')["TS_CODE"]
    df_all = 0
    for ts_code in pb(stock_list, desc='Please wait', colour='#ffffff'):
        try: #防止所给日期区间内没有交易日
            GTJAalphas_results.initializer(ts_code_chosen=ts_code)
            if alpha_name == 'GTJAalpha001': 
                df = pd.DataFrame(GTJAalphas_results.GTJAalpha001().rename('GTJAalpha001'))            
            elif alpha_name == 'GTJAalpha002': df = pd.DataFrame(GTJAalphas_results.GTJAalpha002().rename('GTJAalpha002'))
            elif alpha_name == 'GTJAalpha003': df = pd.DataFrame(GTJAalphas_results.GTJAalpha003().rename('GTJAalpha003'))
            elif alpha_name == 'GTJAalpha004': df = pd.DataFrame(GTJAalphas_results.GTJAalpha004().rename('GTJAalpha004'))
            elif alpha_name == 'GTJAalpha005': df = pd.DataFrame(GTJAalphas_results.GTJAalpha005().rename('GTJAalpha005'))
            elif alpha_name == 'GTJAalpha006': df = pd.DataFrame(GTJAalphas_results.GTJAalpha006().rename('GTJAalpha006'))
            elif alpha_name == 'GTJAalpha007': df = pd.DataFrame(GTJAalphas_results.GTJAalpha007().rename('GTJAalpha007'))
            elif alpha_name == 'GTJAalpha008': df = pd.DataFrame(GTJAalphas_results.GTJAalpha008().rename('GTJAalpha008'))
            elif alpha_name == 'GTJAalpha009': df = pd.DataFrame(GTJAalphas_results.GTJAalpha009().rename('GTJAalpha009'))
            elif alpha_name == 'GTJAalpha011': df = pd.DataFrame(GTJAalphas_results.GTJAalpha011().rename('GTJAalpha011'))            
            elif alpha_name == 'GTJAalpha012': df = pd.DataFrame(GTJAalphas_results.GTJAalpha012().rename('GTJAalpha012'))
            elif alpha_name == 'GTJAalpha013': df = pd.DataFrame(GTJAalphas_results.GTJAalpha013().rename('GTJAalpha013'))
            elif alpha_name == 'GTJAalpha014': df = pd.DataFrame(GTJAalphas_results.GTJAalpha014().rename('GTJAalpha014'))
            elif alpha_name == 'GTJAalpha015': df = pd.DataFrame(GTJAalphas_results.GTJAalpha015().rename('GTJAalpha015'))
            elif alpha_name == 'GTJAalpha016': df = pd.DataFrame(GTJAalphas_results.GTJAalpha016().rename('GTJAalpha016'))
            elif alpha_name == 'GTJAalpha017': df = pd.DataFrame(GTJAalphas_results.GTJAalpha017().rename('GTJAalpha017'))
            elif alpha_name == 'GTJAalpha018': df = pd.DataFrame(GTJAalphas_results.GTJAalpha018().rename('GTJAalpha018'))
            elif alpha_name == 'GTJAalpha019': df = pd.DataFrame(GTJAalphas_results.GTJAalpha019().rename('GTJAalpha019'))            
            elif alpha_name == 'GTJAalpha021': df = pd.DataFrame(GTJAalphas_results.GTJAalpha021().rename('GTJAalpha021'))            
            elif alpha_name == 'GTJAalpha022': df = pd.DataFrame(GTJAalphas_results.GTJAalpha022().rename('GTJAalpha022'))
            elif alpha_name == 'GTJAalpha023': df = pd.DataFrame(GTJAalphas_results.GTJAalpha023().rename('GTJAalpha023'))
            elif alpha_name == 'GTJAalpha024': df = pd.DataFrame(GTJAalphas_results.GTJAalpha024().rename('GTJAalpha024'))
            elif alpha_name == 'GTJAalpha025': df = pd.DataFrame(GTJAalphas_results.GTJAalpha025().rename('GTJAalpha025'))
            elif alpha_name == 'GTJAalpha026': df = pd.DataFrame(GTJAalphas_results.GTJAalpha026().rename('GTJAalpha026'))
            elif alpha_name == 'GTJAalpha027': df = pd.DataFrame(GTJAalphas_results.GTJAalpha027().rename('GTJAalpha027'))
            elif alpha_name == 'GTJAalpha028': df = pd.DataFrame(GTJAalphas_results.GTJAalpha028().rename('GTJAalpha028'))
            elif alpha_name == 'GTJAalpha029': df = pd.DataFrame(GTJAalphas_results.GTJAalpha029().rename('GTJAalpha029'))
            elif alpha_name == 'GTJAalpha031': df = pd.DataFrame(GTJAalphas_results.GTJAalpha031().rename('GTJAalpha031'))            
            elif alpha_name == 'GTJAalpha032': df = pd.DataFrame(GTJAalphas_results.GTJAalpha032().rename('GTJAalpha032'))
            elif alpha_name == 'GTJAalpha033': df = pd.DataFrame(GTJAalphas_results.GTJAalpha033().rename('GTJAalpha033'))
            elif alpha_name == 'GTJAalpha034': df = pd.DataFrame(GTJAalphas_results.GTJAalpha034().rename('GTJAalpha034'))
            elif alpha_name == 'GTJAalpha035': df = pd.DataFrame(GTJAalphas_results.GTJAalpha035().rename('GTJAalpha035'))
            elif alpha_name == 'GTJAalpha036': df = pd.DataFrame(GTJAalphas_results.GTJAalpha036().rename('GTJAalpha036'))
            elif alpha_name == 'GTJAalpha037': df = pd.DataFrame(GTJAalphas_results.GTJAalpha037().rename('GTJAalpha037'))
            elif alpha_name == 'GTJAalpha038': df = pd.DataFrame(GTJAalphas_results.GTJAalpha038().rename('GTJAalpha038'))
            elif alpha_name == 'GTJAalpha039': df = pd.DataFrame(GTJAalphas_results.GTJAalpha039().rename('GTJAalpha039'))
            elif alpha_name == 'GTJAalpha041': df = pd.DataFrame(GTJAalphas_results.GTJAalpha041().rename('GTJAalpha041'))            
            elif alpha_name == 'GTJAalpha042': df = pd.DataFrame(GTJAalphas_results.GTJAalpha042().rename('GTJAalpha042'))
            elif alpha_name == 'GTJAalpha043': df = pd.DataFrame(GTJAalphas_results.GTJAalpha043().rename('GTJAalpha043'))
            elif alpha_name == 'GTJAalpha044': df = pd.DataFrame(GTJAalphas_results.GTJAalpha044().rename('GTJAalpha044'))
            elif alpha_name == 'GTJAalpha045': df = pd.DataFrame(GTJAalphas_results.GTJAalpha045().rename('GTJAalpha045'))
            elif alpha_name == 'GTJAalpha046': df = pd.DataFrame(GTJAalphas_results.GTJAalpha046().rename('GTJAalpha046'))
            elif alpha_name == 'GTJAalpha047': df = pd.DataFrame(GTJAalphas_results.GTJAalpha047().rename('GTJAalpha047'))
            elif alpha_name == 'GTJAalpha048': df = pd.DataFrame(GTJAalphas_results.GTJAalpha048().rename('GTJAalpha048'))
            elif alpha_name == 'GTJAalpha049': df = pd.DataFrame(GTJAalphas_results.GTJAalpha049().rename('GTJAalpha049'))
            elif alpha_name == 'GTJAalpha051': df = pd.DataFrame(GTJAalphas_results.GTJAalpha051().rename('GTJAalpha051'))            
            elif alpha_name == 'GTJAalpha052': df = pd.DataFrame(GTJAalphas_results.GTJAalpha052().rename('GTJAalpha052'))
            elif alpha_name == 'GTJAalpha053': df = pd.DataFrame(GTJAalphas_results.GTJAalpha053().rename('GTJAalpha053'))
            elif alpha_name == 'GTJAalpha054': df = pd.DataFrame(GTJAalphas_results.GTJAalpha054().rename('GTJAalpha054'))
            elif alpha_name == 'GTJAalpha055': df = pd.DataFrame(GTJAalphas_results.GTJAalpha055().rename('GTJAalpha055'))
            elif alpha_name == 'GTJAalpha056': df = pd.DataFrame(GTJAalphas_results.GTJAalpha056().rename('GTJAalpha056'))
            elif alpha_name == 'GTJAalpha057': df = pd.DataFrame(GTJAalphas_results.GTJAalpha057().rename('GTJAalpha057'))
            elif alpha_name == 'GTJAalpha058': df = pd.DataFrame(GTJAalphas_results.GTJAalpha058().rename('GTJAalpha058'))
            elif alpha_name == 'GTJAalpha059': df = pd.DataFrame(GTJAalphas_results.GTJAalpha059().rename('GTJAalpha059'))
            elif alpha_name == 'GTJAalpha061': df = pd.DataFrame(GTJAalphas_results.GTJAalpha061().rename('GTJAalpha061'))            
            elif alpha_name == 'GTJAalpha062': df = pd.DataFrame(GTJAalphas_results.GTJAalpha062().rename('GTJAalpha062'))
            elif alpha_name == 'GTJAalpha063': df = pd.DataFrame(GTJAalphas_results.GTJAalpha063().rename('GTJAalpha063'))
            elif alpha_name == 'GTJAalpha064': df = pd.DataFrame(GTJAalphas_results.GTJAalpha064().rename('GTJAalpha064'))
            elif alpha_name == 'GTJAalpha065': df = pd.DataFrame(GTJAalphas_results.GTJAalpha065().rename('GTJAalpha065'))
            elif alpha_name == 'GTJAalpha066': df = pd.DataFrame(GTJAalphas_results.GTJAalpha066().rename('GTJAalpha066'))
            elif alpha_name == 'GTJAalpha067': df = pd.DataFrame(GTJAalphas_results.GTJAalpha067().rename('GTJAalpha067'))
            elif alpha_name == 'GTJAalpha068': df = pd.DataFrame(GTJAalphas_results.GTJAalpha068().rename('GTJAalpha068'))
            elif alpha_name == 'GTJAalpha069': df = pd.DataFrame(GTJAalphas_results.GTJAalpha069().rename('GTJAalpha069'))
            elif alpha_name == 'GTJAalpha071': df = pd.DataFrame(GTJAalphas_results.GTJAalpha071().rename('GTJAalpha071'))            
            elif alpha_name == 'GTJAalpha072': df = pd.DataFrame(GTJAalphas_results.GTJAalpha072().rename('GTJAalpha072'))
            elif alpha_name == 'GTJAalpha073': df = pd.DataFrame(GTJAalphas_results.GTJAalpha073().rename('GTJAalpha073'))
            elif alpha_name == 'GTJAalpha074': df = pd.DataFrame(GTJAalphas_results.GTJAalpha074().rename('GTJAalpha074'))
            elif alpha_name == 'GTJAalpha075': df = pd.DataFrame(GTJAalphas_results.GTJAalpha075().rename('GTJAalpha075'))
            elif alpha_name == 'GTJAalpha076': df = pd.DataFrame(GTJAalphas_results.GTJAalpha076().rename('GTJAalpha076'))
            elif alpha_name == 'GTJAalpha077': df = pd.DataFrame(GTJAalphas_results.GTJAalpha077().rename('GTJAalpha077'))
            elif alpha_name == 'GTJAalpha078': df = pd.DataFrame(GTJAalphas_results.GTJAalpha078().rename('GTJAalpha078'))
            elif alpha_name == 'GTJAalpha079': df = pd.DataFrame(GTJAalphas_results.GTJAalpha079().rename('GTJAalpha079'))
            elif alpha_name == 'GTJAalpha081': df = pd.DataFrame(GTJAalphas_results.GTJAalpha081().rename('GTJAalpha081'))            
            elif alpha_name == 'GTJAalpha082': df = pd.DataFrame(GTJAalphas_results.GTJAalpha082().rename('GTJAalpha082'))
            elif alpha_name == 'GTJAalpha083': df = pd.DataFrame(GTJAalphas_results.GTJAalpha083().rename('GTJAalpha083'))
            elif alpha_name == 'GTJAalpha084': df = pd.DataFrame(GTJAalphas_results.GTJAalpha084().rename('GTJAalpha084'))
            elif alpha_name == 'GTJAalpha085': df = pd.DataFrame(GTJAalphas_results.GTJAalpha085().rename('GTJAalpha085'))
            elif alpha_name == 'GTJAalpha086': df = pd.DataFrame(GTJAalphas_results.GTJAalpha086().rename('GTJAalpha086'))
            elif alpha_name == 'GTJAalpha087': df = pd.DataFrame(GTJAalphas_results.GTJAalpha087().rename('GTJAalpha087'))
            elif alpha_name == 'GTJAalpha088': df = pd.DataFrame(GTJAalphas_results.GTJAalpha088().rename('GTJAalpha088'))
            elif alpha_name == 'GTJAalpha089': df = pd.DataFrame(GTJAalphas_results.GTJAalpha089().rename('GTJAalpha089'))
            elif alpha_name == 'GTJAalpha091': df = pd.DataFrame(GTJAalphas_results.GTJAalpha091().rename('GTJAalpha091'))            
            elif alpha_name == 'GTJAalpha092': df = pd.DataFrame(GTJAalphas_results.GTJAalpha092().rename('GTJAalpha092'))
            elif alpha_name == 'GTJAalpha093': df = pd.DataFrame(GTJAalphas_results.GTJAalpha093().rename('GTJAalpha093'))
            elif alpha_name == 'GTJAalpha094': df = pd.DataFrame(GTJAalphas_results.GTJAalpha094().rename('GTJAalpha094'))
            elif alpha_name == 'GTJAalpha095': df = pd.DataFrame(GTJAalphas_results.GTJAalpha095().rename('GTJAalpha095'))
            elif alpha_name == 'GTJAalpha096': df = pd.DataFrame(GTJAalphas_results.GTJAalpha096().rename('GTJAalpha096'))
            elif alpha_name == 'GTJAalpha097': df = pd.DataFrame(GTJAalphas_results.GTJAalpha097().rename('GTJAalpha097'))
            elif alpha_name == 'GTJAalpha098': df = pd.DataFrame(GTJAalphas_results.GTJAalpha098().rename('GTJAalpha098'))
            elif alpha_name == 'GTJAalpha099': df = pd.DataFrame(GTJAalphas_results.GTJAalpha099().rename('GTJAalpha099'))
            elif alpha_name == 'GTJAalpha100': df = pd.DataFrame(GTJAalphas_results.GTJAalpha100().rename('GTJAalpha100'))
            elif alpha_name == 'GTJAalpha101': df = pd.DataFrame(GTJAalphas_results.GTJAalpha101().rename('GTJAalpha101'))            
            elif alpha_name == 'GTJAalpha102': df = pd.DataFrame(GTJAalphas_results.GTJAalpha102().rename('GTJAalpha102'))
            elif alpha_name == 'GTJAalpha103': df = pd.DataFrame(GTJAalphas_results.GTJAalpha103().rename('GTJAalpha103'))
            elif alpha_name == 'GTJAalpha104': df = pd.DataFrame(GTJAalphas_results.GTJAalpha104().rename('GTJAalpha104'))
            elif alpha_name == 'GTJAalpha105': df = pd.DataFrame(GTJAalphas_results.GTJAalpha105().rename('GTJAalpha105'))
            elif alpha_name == 'GTJAalpha106': df = pd.DataFrame(GTJAalphas_results.GTJAalpha106().rename('GTJAalpha106'))
            elif alpha_name == 'GTJAalpha107': df = pd.DataFrame(GTJAalphas_results.GTJAalpha107().rename('GTJAalpha107'))
            elif alpha_name == 'GTJAalpha108': df = pd.DataFrame(GTJAalphas_results.GTJAalpha108().rename('GTJAalpha108'))
            elif alpha_name == 'GTJAalpha109': df = pd.DataFrame(GTJAalphas_results.GTJAalpha109().rename('GTJAalpha109'))
            elif alpha_name == 'GTJAalpha111': df = pd.DataFrame(GTJAalphas_results.GTJAalpha111().rename('GTJAalpha111'))            
            elif alpha_name == 'GTJAalpha112': df = pd.DataFrame(GTJAalphas_results.GTJAalpha112().rename('GTJAalpha112'))
            elif alpha_name == 'GTJAalpha113': df = pd.DataFrame(GTJAalphas_results.GTJAalpha113().rename('GTJAalpha113'))
            elif alpha_name == 'GTJAalpha114': df = pd.DataFrame(GTJAalphas_results.GTJAalpha114().rename('GTJAalpha114'))
            elif alpha_name == 'GTJAalpha115': df = pd.DataFrame(GTJAalphas_results.GTJAalpha115().rename('GTJAalpha115'))
            elif alpha_name == 'GTJAalpha116': df = pd.DataFrame(GTJAalphas_results.GTJAalpha116().rename('GTJAalpha116'))
            elif alpha_name == 'GTJAalpha117': df = pd.DataFrame(GTJAalphas_results.GTJAalpha117().rename('GTJAalpha117'))
            elif alpha_name == 'GTJAalpha118': df = pd.DataFrame(GTJAalphas_results.GTJAalpha118().rename('GTJAalpha118'))
            elif alpha_name == 'GTJAalpha119': df = pd.DataFrame(GTJAalphas_results.GTJAalpha119().rename('GTJAalpha119'))            
            elif alpha_name == 'GTJAalpha121': df = pd.DataFrame(GTJAalphas_results.GTJAalpha121().rename('GTJAalpha121'))            
            elif alpha_name == 'GTJAalpha122': df = pd.DataFrame(GTJAalphas_results.GTJAalpha122().rename('GTJAalpha122'))
            elif alpha_name == 'GTJAalpha123': df = pd.DataFrame(GTJAalphas_results.GTJAalpha123().rename('GTJAalpha123'))
            elif alpha_name == 'GTJAalpha124': df = pd.DataFrame(GTJAalphas_results.GTJAalpha124().rename('GTJAalpha124'))
            elif alpha_name == 'GTJAalpha125': df = pd.DataFrame(GTJAalphas_results.GTJAalpha125().rename('GTJAalpha125'))
            elif alpha_name == 'GTJAalpha126': df = pd.DataFrame(GTJAalphas_results.GTJAalpha126().rename('GTJAalpha126'))
            elif alpha_name == 'GTJAalpha127': df = pd.DataFrame(GTJAalphas_results.GTJAalpha127().rename('GTJAalpha127'))
            elif alpha_name == 'GTJAalpha128': df = pd.DataFrame(GTJAalphas_results.GTJAalpha128().rename('GTJAalpha128'))
            elif alpha_name == 'GTJAalpha129': df = pd.DataFrame(GTJAalphas_results.GTJAalpha129().rename('GTJAalpha129'))
            elif alpha_name == 'GTJAalpha131': df = pd.DataFrame(GTJAalphas_results.GTJAalpha131().rename('GTJAalpha131'))            
            elif alpha_name == 'GTJAalpha132': df = pd.DataFrame(GTJAalphas_results.GTJAalpha132().rename('GTJAalpha132'))
            elif alpha_name == 'GTJAalpha133': df = pd.DataFrame(GTJAalphas_results.GTJAalpha133().rename('GTJAalpha133'))
            elif alpha_name == 'GTJAalpha134': df = pd.DataFrame(GTJAalphas_results.GTJAalpha134().rename('GTJAalpha134'))
            elif alpha_name == 'GTJAalpha135': df = pd.DataFrame(GTJAalphas_results.GTJAalpha135().rename('GTJAalpha135'))
            elif alpha_name == 'GTJAalpha136': df = pd.DataFrame(GTJAalphas_results.GTJAalpha136().rename('GTJAalpha136'))
            elif alpha_name == 'GTJAalpha137': df = pd.DataFrame(GTJAalphas_results.GTJAalpha137().rename('GTJAalpha137'))
            elif alpha_name == 'GTJAalpha138': df = pd.DataFrame(GTJAalphas_results.GTJAalpha138().rename('GTJAalpha138'))
            elif alpha_name == 'GTJAalpha139': df = pd.DataFrame(GTJAalphas_results.GTJAalpha139().rename('GTJAalpha139'))
            elif alpha_name == 'GTJAalpha141': df = pd.DataFrame(GTJAalphas_results.GTJAalpha141().rename('GTJAalpha141'))            
            elif alpha_name == 'GTJAalpha142': df = pd.DataFrame(GTJAalphas_results.GTJAalpha142().rename('GTJAalpha142'))
            elif alpha_name == 'GTJAalpha143': df = pd.DataFrame(GTJAalphas_results.GTJAalpha143().rename('GTJAalpha143'))
            elif alpha_name == 'GTJAalpha144': df = pd.DataFrame(GTJAalphas_results.GTJAalpha144().rename('GTJAalpha144'))
            elif alpha_name == 'GTJAalpha145': df = pd.DataFrame(GTJAalphas_results.GTJAalpha145().rename('GTJAalpha145'))
            elif alpha_name == 'GTJAalpha146': df = pd.DataFrame(GTJAalphas_results.GTJAalpha146().rename('GTJAalpha146'))
            elif alpha_name == 'GTJAalpha147': df = pd.DataFrame(GTJAalphas_results.GTJAalpha147().rename('GTJAalpha147'))
            elif alpha_name == 'GTJAalpha148': df = pd.DataFrame(GTJAalphas_results.GTJAalpha148().rename('GTJAalpha148'))
            elif alpha_name == 'GTJAalpha149': df = pd.DataFrame(GTJAalphas_results.GTJAalpha149().rename('GTJAalpha149'))
            elif alpha_name == 'GTJAalpha151': df = pd.DataFrame(GTJAalphas_results.GTJAalpha151().rename('GTJAalpha151'))            
            elif alpha_name == 'GTJAalpha152': df = pd.DataFrame(GTJAalphas_results.GTJAalpha152().rename('GTJAalpha152'))
            elif alpha_name == 'GTJAalpha153': df = pd.DataFrame(GTJAalphas_results.GTJAalpha153().rename('GTJAalpha153'))
            elif alpha_name == 'GTJAalpha154': df = pd.DataFrame(GTJAalphas_results.GTJAalpha154().rename('GTJAalpha154'))
            elif alpha_name == 'GTJAalpha155': df = pd.DataFrame(GTJAalphas_results.GTJAalpha155().rename('GTJAalpha155'))
            elif alpha_name == 'GTJAalpha156': df = pd.DataFrame(GTJAalphas_results.GTJAalpha156().rename('GTJAalpha156'))
            elif alpha_name == 'GTJAalpha157': df = pd.DataFrame(GTJAalphas_results.GTJAalpha157().rename('GTJAalpha157'))
            elif alpha_name == 'GTJAalpha158': df = pd.DataFrame(GTJAalphas_results.GTJAalpha158().rename('GTJAalpha158'))
            elif alpha_name == 'GTJAalpha159': df = pd.DataFrame(GTJAalphas_results.GTJAalpha159().rename('GTJAalpha159'))
            elif alpha_name == 'GTJAalpha161': df = pd.DataFrame(GTJAalphas_results.GTJAalpha161().rename('GTJAalpha161'))            
            elif alpha_name == 'GTJAalpha162': df = pd.DataFrame(GTJAalphas_results.GTJAalpha162().rename('GTJAalpha162'))
            elif alpha_name == 'GTJAalpha163': df = pd.DataFrame(GTJAalphas_results.GTJAalpha163().rename('GTJAalpha163'))
            elif alpha_name == 'GTJAalpha164': df = pd.DataFrame(GTJAalphas_results.GTJAalpha164().rename('GTJAalpha164'))
            elif alpha_name == 'GTJAalpha165': df = pd.DataFrame(GTJAalphas_results.GTJAalpha165().rename('GTJAalpha165'))
            elif alpha_name == 'GTJAalpha166': df = pd.DataFrame(GTJAalphas_results.GTJAalpha166().rename('GTJAalpha166'))
            elif alpha_name == 'GTJAalpha167': df = pd.DataFrame(GTJAalphas_results.GTJAalpha167().rename('GTJAalpha167'))
            elif alpha_name == 'GTJAalpha168': df = pd.DataFrame(GTJAalphas_results.GTJAalpha168().rename('GTJAalpha168'))
            elif alpha_name == 'GTJAalpha169': df = pd.DataFrame(GTJAalphas_results.GTJAalpha169().rename('GTJAalpha169'))
            elif alpha_name == 'GTJAalpha171': df = pd.DataFrame(GTJAalphas_results.GTJAalpha171().rename('GTJAalpha171'))            
            elif alpha_name == 'GTJAalpha172': df = pd.DataFrame(GTJAalphas_results.GTJAalpha172().rename('GTJAalpha172'))
            elif alpha_name == 'GTJAalpha173': df = pd.DataFrame(GTJAalphas_results.GTJAalpha173().rename('GTJAalpha173'))
            elif alpha_name == 'GTJAalpha174': df = pd.DataFrame(GTJAalphas_results.GTJAalpha174().rename('GTJAalpha174'))
            elif alpha_name == 'GTJAalpha175': df = pd.DataFrame(GTJAalphas_results.GTJAalpha175().rename('GTJAalpha175'))
            elif alpha_name == 'GTJAalpha176': df = pd.DataFrame(GTJAalphas_results.GTJAalpha176().rename('GTJAalpha176'))
            elif alpha_name == 'GTJAalpha177': df = pd.DataFrame(GTJAalphas_results.GTJAalpha177().rename('GTJAalpha177'))
            elif alpha_name == 'GTJAalpha178': df = pd.DataFrame(GTJAalphas_results.GTJAalpha178().rename('GTJAalpha178'))
            elif alpha_name == 'GTJAalpha179': df = pd.DataFrame(GTJAalphas_results.GTJAalpha179().rename('GTJAalpha179'))
            elif alpha_name == 'GTJAalpha181': df = pd.DataFrame(GTJAalphas_results.GTJAalpha181().rename('GTJAalpha181'))            
            elif alpha_name == 'GTJAalpha182': df = pd.DataFrame(GTJAalphas_results.GTJAalpha182().rename('GTJAalpha182'))
            elif alpha_name == 'GTJAalpha183': df = pd.DataFrame(GTJAalphas_results.GTJAalpha183().rename('GTJAalpha183'))
            elif alpha_name == 'GTJAalpha184': df = pd.DataFrame(GTJAalphas_results.GTJAalpha184().rename('GTJAalpha184'))
            elif alpha_name == 'GTJAalpha185': df = pd.DataFrame(GTJAalphas_results.GTJAalpha185().rename('GTJAalpha185'))
            elif alpha_name == 'GTJAalpha186': df = pd.DataFrame(GTJAalphas_results.GTJAalpha186().rename('GTJAalpha186'))
            elif alpha_name == 'GTJAalpha187': df = pd.DataFrame(GTJAalphas_results.GTJAalpha187().rename('GTJAalpha187'))
            elif alpha_name == 'GTJAalpha188': df = pd.DataFrame(GTJAalphas_results.GTJAalpha188().rename('GTJAalpha188'))
            elif alpha_name == 'GTJAalpha189': df = pd.DataFrame(GTJAalphas_results.GTJAalpha189().rename('GTJAalpha189'))
            elif alpha_name == 'GTJAalpha191': df = pd.DataFrame(GTJAalphas_results.GTJAalpha191().rename('GTJAalpha191'))  
            df = pd.concat([GTJAalphas_results.output_dates, df],axis=1)
            df.insert(loc=0,column='TS_CODE',value=ts_code)
            if type(df_all) == int:
                df_all = df
            else:
                df_all = pd.concat([df_all, df],axis=0)    
        except:
            pass
    return df_all






























