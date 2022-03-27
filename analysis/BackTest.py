# -*- coding: utf-8 -*-

import pandas as pd  
import matplotlib.pyplot as plt
from pylab import mpl
from data_source import local_source

mpl.rcParams['font.sans-serif'] = ['FangSong']  # 替换sans-serif字体


def get_all_tradedates(start_date, end_date, freq='daily'):
    if freq=='daily':
        date_list=local_source.get_indices_daily(cols='INDEX_CODE,TRADE_DATE',condition="INDEX_CODE = '000001.SH' and TRADE_DATE>=" + str(start_date) + " and TRADE_DATE<=" + str(end_date))["TRADE_DATE"].astype(int)
    if freq=='monthly':
        date_list=local_source.get_indices_monthly(cols='INDEX_CODE,TRADE_DATE',condition="INDEX_CODE = '000001.SH' and TRADE_DATE>=" + str(start_date) + "01" + " and TRADE_DATE<=" + str(end_date) + "31")["TRADE_DATE"].astype(int)
        date_list = date_list //100
    return date_list

def get_reference_index(start_date, end_date, freq='daily'):
    if freq=='daily':
        data=local_source.get_indices_daily(cols='TRADE_DATE,CLOSE',condition="INDEX_CODE = '000001.SH' and TRADE_DATE>=" + str(start_date) + " and TRADE_DATE<=" + str(end_date))
        data["TRADE_DATE"]=data["TRADE_DATE"].astype(int)
    if freq=='monthly':
        data=local_source.get_indices_monthly(cols='TRADE_DATE,CLOSE',condition="INDEX_CODE = '000001.SH' and TRADE_DATE>=" + str(start_date) + "01" + " and TRADE_DATE<=" + str(end_date) + "31")
        data["TRADE_DATE"]=data["TRADE_DATE"].astype(int) // 100
    data = data.set_index("TRADE_DATE")
    return data
    

class BackTest_UnivariateStock(object):
    def __init__(self, df, start_date, end_date, freq, startcash, pct_commission_buy=0.00012, pct_commission_sale=0.00112, minimum_commission=5):
     #df: 个股信息数据集, 至少需包含OPEN, CLOSE, HIGH, LOW; 包含TRADE_DATE或直接以日期为index
     #start_date, end_date, freq: 开始日期, 结束日期, 调仓频率(daily,monthly), 日期为int且格式要与频率相符
     #startcash: 初始资金
     #pct_commission_buy, pct_commission_sale, minimum_commission: 买单手续费比例, 卖单手续费比例, 最小手续费金额
        self.date_list = get_all_tradedates(start_date=start_date, end_date=end_date, freq=freq) #交易日表
        self.current_date = self.date_list.iloc[0]      #当前日期
        self.current_date_index = self.date_list[self.date_list==self.current_date].index[0] #当前日期在交易日表中的index
        self.next_date = self.date_list.iloc[1]         #下一个交易日日期

        self.freq = freq
        self.open = df['OPEN']                          #开盘价表
        self.close = df['CLOSE']                        #收盘价表
        self.high = df['HIGH']                          #最高价表
        self.low = df['LOW']                            #最低价表
        self.pct_commission_buy = pct_commission_buy    #买单手续费比例
        self.pct_commission_sale = pct_commission_sale  #卖单手续费比例
        self.minimum_commission = minimum_commission    #最小手续费金额
        self.cash_holding = startcash                   #空余资金
        self.shares_holding = 0                         #持仓股数
        self.cash_restricted = 0                        #因委托交易而被锁定的空余资金
        self.shares_restricted = 0                      #因委托交易而被锁定的持仓股数
        self.limit_orders_buy={}                        #委托限价买单字典, 格式为委托价格:委托数量
        self.limit_orders_sale={}                       #委托限价卖单字典, 格式为委托价格:委托数量
        self.transaction_history = pd.DataFrame(columns=["TRADE_DATE","ACTION","PRICE","AMOUNT","TRANSACTION_COST"]) #存放所有交易操作的记录
        self.cash_history = pd.DataFrame([[0,startcash,0,startcash,0]],columns=["TRADE_DATE","CASH_SPARED","CASH_INVESTED","CASH_TOTAL","POSITION"]) #存放每日结束后剩余资金量和持仓量信息

    def Notify_trade(self): #提醒交易发生
        print(self.transaction_history.iloc[-1])
        
    def Notify_daily(self):  #提醒每天资金状况
        print(self.cash_history.iloc[-1])        

    def MarketOrder_Buy(self, executed_price, amount): #amount为股数
        if amount==0: return
        commission = max(self.minimum_commission, amount*executed_price*self.pct_commission_buy)
        self.cash_holding = self.cash_holding - amount * executed_price - commission
        self.shares_holding = self.shares_holding + amount
        self.transaction_history = self.transaction_history.append({"TRADE_DATE":self.current_date, "ACTION":"MarketOrder_Buy", "PRICE":executed_price, "AMOUNT":amount, "TRANSACTION_COST":commission}, ignore_index=True)
        
    def MarketOrder_Sale(self, executed_price, amount):
        if amount==0: return
        commission = max(self.minimum_commission, amount*executed_price*self.pct_commission_sale)
        self.cash_holding = self.cash_holding + amount * executed_price - commission
        self.shares_holding = self.shares_holding - amount
        self.transaction_history = self.transaction_history.append({"TRADE_DATE":self.current_date, "ACTION":"MarketOrder_Sale", "PRICE":executed_price, "AMOUNT":amount, "TRANSACTION_COST":commission}, ignore_index=True)

    def LimitOrder_Buy_setup(self, ordered_price, amount):
        if amount==0: return
        commission = max(self.minimum_commission, amount*ordered_price*self.pct_commission_buy)        
        self.cash_holding = self.cash_holding - amount * ordered_price - commission
        self.cash_restricted = self.cash_restricted + amount * ordered_price + commission
        if ordered_price in self.limit_orders_buy.keys():
            self.limit_orders_buy[ordered_price] = self.limit_orders_buy[ordered_price] + amount
        else:
            self.limit_order_buy[ordered_price] = amount
        self.transaction_history = self.transaction_history.append({"TRADE_DATE":self.current_date, "ACTION":"LimitOrder_Buy_setup", "PRICE":ordered_price, "AMOUNT":amount, "TRANSACTION_COST":0}, ignore_index=True)
     
    def LimitOrder_Sale_setup(self, ordered_price, amount):
        if amount==0: return        
        self.shares_holding = self.shares_holding - amount
        self.shares_restricted = self.shares_restricted + amount
        if ordered_price in self.limit_orders_sale.keys():
            self.limit_orders_sale[ordered_price] = self.limit_orders_sale[ordered_price] + amount
        else:
            self.limit_order_sale[ordered_price] = amount       
        self.transaction_history = self.transaction_history.append({"TRADE_DATE":self.current_date, "ACTION":"LimitOrder_Sale_setup", "PRICE":ordered_price, "AMOUNT":amount, "TRANSACTION_COST":0}, ignore_index=True)
     
    def LimitOrder_Buy_execution(self, ordered_price):
        amount = self.limit_orders_buy[ordered_price]
        commission = max(self.minimum_commission, amount*ordered_price*self.pct_commission_buy)
        self.cash_restricted = self.cash_restricted - amount * ordered_price - commission
        self.shares_holding = self.shares_holding + amount
        del self.limit_orders_buy[ordered_price]
        self.transaction_history = self.transaction_history.append({"TRADE_DATE":self.current_date, "ACTION":"LimitOrder_Buy_execution", "PRICE":ordered_price, "AMOUNT":amount, "TRANSACTION_COST":commission}, ignore_index=True)
        
    def LimitOrder_Sale_execution(self, ordered_price):
        amount = self.limit_orders_sale[ordered_price]
        commission = max(self.minimum_commission, amount*ordered_price*self.pct_commission_sale)
        self.shares_restricted = self.shares_restricted - amount
        self.cash_holding = self.cash_restricted + amount * ordered_price - commission
        del self.limit_orders_sale[ordered_price]
        self.transaction_history = self.transaction_history.append({"TRADE_DATE":self.current_date, "ACTION":"LimitOrder_Sale_execution", "PRICE":ordered_price, "AMOUNT":amount, "TRANSACTION_COST":commission}, ignore_index=True)

    def LimitOrder_Buy_cancel(self,ordered_price,amount):
        if amount==0: return
        commission = max(self.minimum_commission, amount*ordered_price*self.pct_commission_buy) 
        self.cash_holding = self.cash_holding + amount * ordered_price + commission
        self.cash_restricted = self.cash_restricted - amount * ordered_price - commission
        if self.limit_orders_buy[ordered_price]==0: del self.limit_orders_buy[ordered_price]
        self.transaction_history = self.transaction_history.append({"TRADE_DATE":self.current_date, "ACTION":"LimitOrder_Buy_cancel", "PRICE":ordered_price, "AMOUNT":amount, "TRANSACTION_COST":0}, ignore_index=True)
        
    def LimitOrder_Sale_cancel(self,ordered_price,amount):
        if amount==0: return
        self.shares_holding = self.shares_holding + amount
        self.shares_restricted = self.shares_restricted - amount        
        if self.limit_orders_sale[ordered_price]==0: del self.limit_orders_sale[ordered_price]
        self.transaction_history = self.transaction_history.append({"TRADE_DATE":self.current_date, "ACTION":"LimitOrder_Sale_cancel", "PRICE":ordered_price, "AMOUNT":amount, "TRANSACTION_COST":0}, ignore_index=True)
    
    def maximum_amount_to_buy(self, executed_price):  #计算最大可买入数量(考虑手续费)
        amount = min( (self.cash_holding-self.minimum_commission)//executed_price, self.cash_holding//(executed_price*(1+self.pct_commission_buy)) )
        return max(amount,0) 
    
    def minimum_amount_to_sale(self, executed_price, cash_needed):  #计算需要筹到一定的资金最小需卖出数量(考虑手续费)
        amount = max((cash_needed+self.minimum_commission)//executed_price , cash_needed//(executed_price*(1+self.pct_commission_buy)) ) +1
        return amount        
    
    def MyStrategy(self):
        pass

    def Daily_Routine(self):
        self.current_date = self.date_list.iloc[self.current_date_index]
        try:
            self.next_date = self.date_list.iloc[self.current_date_index + 1]
        except:
            pass
        #检查有无当日内被交易的limit order
        try:
            intraday_max_price = max(self.high.loc[self.current_date], self.low.loc[self.current_date]) 
            intraday_min_price = min(self.high.loc[self.current_date], self.low.loc[self.current_date])
            for ordered_price in self.limit_orders_buy.keys():
                if ordered_price>intraday_min_price and ordered_price<intraday_max_price:
                    self.LimitOrder_Buy_execution(ordered_price)
            for ordered_price in self.limit_orders_sale.keys():
                if ordered_price>intraday_min_price and ordered_price<intraday_max_price:
                    self.LimitOrder_Sale_execution(ordered_price)            
            #计算资金状况并记录
            cash_spared = (self.cash_holding + self.cash_restricted)
            cash_invested = (self.shares_holding + self.shares_restricted) * self.close.loc[self.current_date]
            self.cash_history = self.cash_history.append({"TRADE_DATE":self.current_date, "CASH_SPARED":cash_spared, "CASH_INVESTED":cash_invested, "CASH_TOTAL":(cash_spared+cash_invested), "POSITION":(self.shares_holding+self.shares_restricted)}, ignore_index=True)
            #运行strategy
            self.MyStrategy()
            #进入下一天
            self.current_date_index = self.current_date_index + 1
        except:
            pass
    
    def report_final_result(self, test_mode=False):
        end_cash_total = self.cash_history["CASH_TOTAL"].iloc[-1]
        if test_mode == True: return end_cash_total
        start_cash=self.cash_history["CASH_TOTAL"].iloc[0]
        end_cash_invested = self.cash_history["CASH_INVESTED"].iloc[-1]
        print("起始资金: {start_cash}\n结束时总资金: {end_cash_total}\n结束时持仓资金: {end_cash_invested}\n收益率: {total_ret}".format(start_cash=start_cash, end_cash_total=end_cash_total, end_cash_invested=end_cash_invested, total_ret=(end_cash_total/start_cash)-1))
        print("参考: 期间内该股票价格变化率:", (self.close.iloc[-1]/self.close.iloc[0])-1)  
        if self.freq == 'monthly':
            x = pd.to_datetime(self.date_list, format="%Y%m")
        else:
            x = pd.to_datetime(self.date_list, format="%Y%m%d")
        y_cash = self.cash_history["CASH_TOTAL"][1:]
        if self.freq == 'monthly':
            fig = plt.figure(figsize=(len(x)//4, 8))   
        else:
            fig = plt.figure(figsize=(len(x)//20, 8)) 
        grid = plt.GridSpec(2,1)
        ax1 = fig.add_subplot(grid[0,0])          
        ax1.set_title("总资金变化图") 
        ax1.set_xlabel("Trade Date", labelpad=0, position=(0.5,1))           
        ax1.set_ylabel("Total Cash", labelpad=0, position=(1,0.5))        
        ax1.plot(x,y_cash, color='red')
        ax2 = fig.add_subplot(grid[1,0])          
        ax2.set_title("股价走势图") 
        ax2.set_xlabel("Trade Date", labelpad=0, position=(0.5,1))           
        ax2.set_ylabel("Stock Price", labelpad=0, position=(1,0.5))        
        ax2.plot(x,self.close)       
        fig.subplots_adjust(hspace=0.4, wspace=0.4)
        plt.show() 
        
    def BackTest(self):
        while self.current_date_index != len(self.date_list):
            self.Daily_Routine()
        self.report_final_result()   
        return self.transaction_history, self.cash_history



class BackTest_MultivariateStock(BackTest_UnivariateStock):
    def __init__(self, stock_df_dict, start_date, end_date, freq, startcash, pct_commission_buy=0.00012, pct_commission_sale=0.00112, minimum_commission=5):
        self.date_list = get_all_tradedates(start_date=start_date, end_date=end_date, freq=freq)
        self.stock_names = pd.Series(stock_df_dict.keys())
        stock_set={}
        for key in stock_df_dict.keys():
            stock_set[key]=BackTest_UnivariateStock(stock_df_dict[key], start_date, end_date, freq, 0, pct_commission_buy, pct_commission_sale, minimum_commission)
        self.stock_set = stock_set  #个股的账户的集合
        self.stock_list = list(stock_df_dict.keys())
        self.date_list = self.stock_set[self.stock_names.iloc[0]].date_list
        self.current_date = self.date_list.iloc[0]
        self.current_date_index = self.date_list[self.date_list==self.current_date].index[0]  
        self.next_date = self.date_list.iloc[1]
        self.freq = freq
        self.reference = get_reference_index(start_date=start_date, end_date = end_date, freq=freq)
        self.cash_holding_pool = startcash
        self.transaction_history = pd.DataFrame(columns=["TRADE_DATE","TS_CODE","ACTION","PRICE","AMOUNT","TRANSACTION_COST"]) #存放所有交易操作的记录
        self.cash_history = pd.DataFrame([[0,self.cash_holding_pool,0,self.cash_holding_pool]],columns=["TRADE_DATE","CASH_SPARED","CASH_INVESTED","CASH_TOTAL"]) #存放每日结束后剩余资金量和持仓量信息
        self.cash_history["TRADE_DATE"] = self.date_list
    
    def Cash_Transfer(self, from_stock, to_stock, amount):
        if amount == all: 
            amount = self.stock_set[from_stock].cash_holding
        if from_stock == 'pool':
            self.cash_holding_pool = self.cash_holding_pool - amount
        else:
            self.stock_set[from_stock].cash_holding = self.stock_set[from_stock].cash_holding - amount
        if to_stock == 'pool':
            self.cash_holding_pool = self.cash_holding_pool + amount   
        else:
            self.stock_set[to_stock].cash_holding = self.stock_set[to_stock].cash_holding + amount
    
    def Multivariate_MyStrategy(self):
        pass
    
    def Multivariate_DailyRoutine(self):
        self.current_date = self.date_list.iloc[self.current_date_index]
        try:
            self.next_date = self.date_list.iloc[self.current_date_index + 1]
        except:
            pass
        cash_spared = self.cash_holding_pool
        cash_invested = 0
        for key in self.stock_list:
            self.stock_set[key].Daily_Routine()
            self.Cash_Transfer(from_stock=key, to_stock='pool', amount=self.stock_set[key].cash_holding)
            cash_spared = cash_spared + self.stock_set[key].cash_history["CASH_SPARED"].iloc[-1]
            cash_invested = cash_invested + self.stock_set[key].cash_history["CASH_INVESTED"].iloc[-1]
        self.cash_history = self.cash_history.append({"TRADE_DATE":self.current_date, "CASH_SPARED":cash_spared, "CASH_INVESTED":cash_invested, "CASH_TOTAL":(cash_spared+cash_invested)}, ignore_index=True)            
        self.Multivariate_MyStrategy()
        self.current_date_index = self.current_date_index + 1
    
    def Multivariate_report_final_result(self, test_mode=False):
        for key in self.stock_list:
            transaction_history_piece = self.stock_set[key].transaction_history
            transaction_history_piece["TS_CODE"] = key
            transaction_history_piece = transaction_history_piece[["TRADE_DATE","TS_CODE","ACTION" ,"PRICE","AMOUNT","TRANSACTION_COST"]]
            self.transaction_history = self.transaction_history.append(transaction_history_piece)
        start_cash=self.cash_history["CASH_TOTAL"].iloc[0]
        end_cash_invested = self.cash_history["CASH_INVESTED"].iloc[-1]
        end_cash_total = self.cash_history["CASH_TOTAL"].iloc[-1]
        print("起始资金: {start_cash}\n结束时总资金: {end_cash_total}\n结束时持仓资金: {end_cash_invested}\n收益率: {total_ret}".format(start_cash=start_cash, end_cash_total=end_cash_total, end_cash_invested=end_cash_invested, total_ret=(end_cash_total/start_cash)-1))
        if self.freq == 'monthly':
            x = pd.to_datetime(self.date_list, format="%Y%m")
        else:
            x = pd.to_datetime(self.date_list, format="%Y%m%d")
        y_cash = self.cash_history["CASH_TOTAL"][1:]        
        if self.freq == 'monthly':
            fig = plt.figure(figsize=(len(x)//4, 8))   
        else:
            fig = plt.figure(figsize=(len(x)//20, 8)) 
        grid = plt.GridSpec(2,1)
        ax1 = fig.add_subplot(grid[0,0])          
        ax1.set_title("总资金变化图") 
        ax1.set_xlabel("Trade Date", labelpad=0, position=(0.5,1))           
        ax1.set_ylabel("Total Cash", labelpad=0, position=(1,0.5))        
        ax1.plot(x,y_cash, color='red')
        ax2 = fig.add_subplot(grid[1,0])          
        ax2.set_title("上证指数走势图") 
        ax2.set_xlabel("Trade Date", labelpad=0, position=(0.5,1))           
        ax2.set_ylabel("Index", labelpad=0, position=(1,0.5))        
        ax2.plot(x, self.reference)       
        fig.subplots_adjust(hspace=0.4, wspace=0.4)
        plt.show() 
        
    def Multivariate_BackTest(self):
        while self.current_date_index != len(self.date_list):
            self.Multivariate_DailyRoutine()
        self.Multivariate_report_final_result()   
        return self.transaction_history, self.cash_history



