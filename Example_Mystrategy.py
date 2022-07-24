# -*- coding: utf-8 -*-


import numpy as np
import pandas as pd
from data_source import local_source
from analysis import factor_loader
from analysis import BackTest
from analysis import value_strategy_funcs
from analysis import alphas_base
from tqdm import tqdm as pb

#量化系统单股模式示例  
#1.使用alpha策略日频操作单个股票样例
class BackTest_UnivariateStock_with_Strategy(BackTest.BackTest_UnivariateStock):
    def __init__(self, df, start_date, end_date, freq, startcash, pct_commission_buy, pct_commission_sale, minimum_commission):
        BackTest.BackTest_UnivariateStock.__init__(self, df, start_date, end_date, freq, startcash, pct_commission_buy, pct_commission_sale, minimum_commission)
        self.factor = df['alpha']  #在这里定义策略要用的参数(以df中保存的因子为例)
        
    def MyStrategy(self):
        #测试例: alpha>0.8就下一天开盘价全部买入, alpha<0.2就下一天开盘价全部卖出
        try: 
            next_date = self.date_list.iloc[self.current_date_index + 1]
            if self.factor.loc[self.current_date]>0.8:
                executed_price = self.open.loc[next_date]
                amount = BackTest.BackTest_UnivariateStock.maximum_amount_to_buy(self, executed_price=executed_price)
                BackTest.BackTest_UnivariateStock.MarketOrder_Buy(self, executed_price=executed_price, amount=amount)
            if self.factor.loc[self.current_date]<0.2:
                BackTest.BackTest_UnivariateStock.MarketOrder_Sale(self, executed_price=self.open.loc[next_date], amount=self.shares_holding)
        except:
            pass  #用于处理最后一天无法获取下一天开盘价进行交易的情况


ts_code = '000001.SZ'
start_date=20210101
end_date=20211231
date_list = BackTest.get_all_tradedates(start_date=start_date, end_date=end_date)
data0 = local_source.get_quotations_daily(cols='TRADE_DATE,TS_CODE,OPEN,HIGH,LOW,CLOSE,VOL',condition="TS_CODE = " + "'" + ts_code + "' and TRADE_DATE>=" + str(start_date) + " and TRADE_DATE<=" + str(end_date))
Alphas = alphas_base.Alphas(ts_code=ts_code,start_date=start_date,end_date=end_date)
Alphas.initializer()
data_alpha = Alphas.alpha001().rename("alpha001")
data0["alpha"] = data_alpha.values
data0["TRADE_DATE"] = data0["TRADE_DATE"].astype(int)
data0 = data0.set_index("TRADE_DATE")
test = BackTest_UnivariateStock_with_Strategy(data0, start_date=start_date, end_date=end_date, freq='daily', startcash=1000000, pct_commission_buy=0.00012, pct_commission_sale=0.00112, minimum_commission=5)

transaction_history, cash_history = test.BackTest()

#-----------------------------------------------------------------------------------
#量化系统多股模式示例
#用内置因子数据寻找有显著收益的价值投资策略； 那之后, 应用其结果进行月频换仓的买卖

start_date = 20210101
end_date = 20211231

#(1)调用需要使用的内置因子数据
df=0
for i in ["#CURRENT_RATIO","#QUICK_RATIO","#CASH_RATIO", "#ROA"]:
    df_new =  factor_loader.EmbeddedFactors_allstocks(factor_name=i, start_date=start_date, end_date=end_date)
    df = factor_loader.add_factor(df_new, df)

#(2)构建Solvency和Profitability两个factor, 测试盈利能力并输出选股矩阵
factor_name_list = ["Solvency", "Profitability"]    
input_list = [["CURRENT_RATIO","QUICK_RATIO", "CASH_RATIO"], ["ROA"]]
input_ascending_list = [[True, True, True],[True]]
choice_matrix = value_strategy_funcs.value_strategy_tests_monthly(df, factor_name_list, input_list, input_ascending_list)


#(3)编写对应的交易策略
class BackTest_MultivariateStock_with_Strategy(BackTest.BackTest_MultivariateStock):
    def __init__(self, stock_df_dict, start_date, end_date, freq, startcash, pct_commission_buy, pct_commission_sale, minimum_commission):
        BackTest.BackTest_MultivariateStock.__init__(self, stock_df_dict, start_date, end_date, freq, startcash, pct_commission_buy, pct_commission_sale, minimum_commission)
        self.choice_matrix = choice_matrix
   
    def Multivariate_MyStrategy(self):
        choice_piece = self.choice_matrix.loc[self.current_date, :]
        hold_list = choice_piece[choice_piece==True].index
        not_hold_list = choice_piece[choice_piece==False].index
        total_cash = self.cash_history["CASH_TOTAL"].iloc[-1]
        cash_allocated = total_cash / len(hold_list)
        for key in not_hold_list:  #不再处于持有名单的股票全卖了, 钱转出来
            try:
                self.stock_set[key].MarketOrder_Sale(executed_price=self.stock_set[key].open.loc[self.next_date], amount=self.stock_set[key].shares_holding)
                self.Cash_Transfer(from_stock=key, to_stock='pool', amount=self.stock_set[key].cash_holding)
            except:
                pass #捕捉choice_matrix中存在但时间区间内并不能交易的股票报错
        for key in hold_list:
            try:
                if self.stock_set[key].cash_history["CASH_TOTAL"].iloc[-1] < cash_allocated:  #若目前往这个股投入的资金小于应投入的资金
                    cash_diff = cash_allocated - self.stock_set[key].cash_history["CASH_TOTAL"].iloc[-1]
                    self.Cash_Transfer(from_stock='pool', to_stock=key, amount=cash_diff)     #就先把钱转到这个股票的账户里
                    amount = self.stock_set[key].maximum_amount_to_buy(executed_price=self.stock_set[key].open.loc[self.next_date]) #然后得到可以买股票的数量
                    self.stock_set[key].MarketOrder_Buy(executed_price=self.stock_set[key].open.loc[self.next_date], amount=amount) #买
                if self.stock_set[key].cash_history["CASH_TOTAL"].iloc[-1] > cash_allocated: #若目前往这个股投入的资金大于应投入的资金  
                    cash_diff = self.stock_set[key].cash_history["CASH_TOTAL"].iloc[-1] - cash_allocated
                    amount = self.stock_set[key].minimum_amount_to_sale(executed_price=self.stock_set[key].close.loc[self.current_date], cash_needed = cash_diff) #需卖股票的数量
                    self.stock_set[key].MarketOrder_Sale(executed_price=self.stock_set[key].open.loc[self.next_date], amount=amount) #卖
                    self.Cash_Transfer(from_stock=key, to_stock='pool', amount=self.stock_set[key].cash_holding)
            except:
                pass  


#(4)初始化回测系统
start_date = 202101
end_date = 202112
startcash = 100000
stock_set = {}
startcash_dict={}
stock_list = local_source.get_stock_list(cols="TS_CODE,NAME")["TS_CODE"]
for stock in pb(stock_list, desc='Importing stock data', colour='#ffffff'):
    data_stock = local_source.get_quotations_daily(cols='TRADE_DATE,TS_CODE,OPEN,CLOSE,HIGH,LOW',condition="TS_CODE = " + "'" + stock + "' and TRADE_DATE>=" + str(start_date) + " and TRADE_DATE<=" + str(end_date))
    if len(data_stock)==0:
        continue
    data_stock["TRADE_DATE"] = data_stock["TRADE_DATE"].astype(int)
    data_stock = value_strategy_funcs.degenerate_dailydata_to_monthlydata(data_stock,data_type='panel')
    data_stock = data_stock.set_index("TRADE_DATE")
    stock_set[stock] = data_stock
    startcash_dict[stock] = startcash

#(5)进行回测
test2 = BackTest_MultivariateStock_with_Strategy(stock_df_dict=stock_set, start_date=start_date, end_date=end_date, freq='monthly', startcash=len(stock_set)*100000, pct_commission_buy=0.00012, pct_commission_sale=0.00112, minimum_commission=5)
transaction_history, cash_history = test2.Multivariate_BackTest()





















