import time
import pandas as pd
import tushare as ts
from data_api.date_getter import date_getter as dg
ts.set_token("3f73ca482044f78edd9694a4e06a0edd9431c24cbac31a07f275d7cf")
pro = ts.pro_api()


def get_index_quotation(index_code, frequency, size):
    """
    获取指数行情
    :param index_code: 上证指数 | 深证指数
    :param frequency: 日线 | 周线 | 月线
    :param size: 条数
    :return: 指数行情 dataframe
    """
    last_finished_trade_date = dg.latest_finished_trade_date
    data = None
    if frequency == 'DAILY':
        data = pro.index_daily(ts_code=index_code, start_date='20100101', end_date=last_finished_trade_date, fields='trade_date, open, close, low, high, vol')
    elif frequency == 'WEEKLY':
        data = pro.index_weekly(ts_code=index_code, start_date='20100101', end_date=last_finished_trade_date, fields='trade_date, open, close, low, high, vol')
    elif frequency == 'MONTHLY':
        data = pro.index_monthly(ts_code=index_code, start_date='20100101', end_date=last_finished_trade_date, fields='trade_date, open, close, low, high, vol')
    data = data[:size].iloc[::-1] if data is not None else None
    data.reset_index(inplace=True, drop=True)
    return data


def get_stock_list(index_suffix='ALL'):
    """
    获取某个交易所的股票列表
    :param index_suffix: 那个交易所？
    :return:股票代码列表
    """
    exchange = ''
    if index_suffix == 'SH':
        exchange = 'SSE'
    elif index_suffix == 'SZ':
        exchange = 'SZSE'
    data = pro.stock_basic(exchange=exchange, list_status='L', fields='ts_code,name,area,industry,market,list_status,list_date,delist_date,is_hs')
    return data


def get_stocks_change(frequency, date, exchange='ALL'):
    """
    获取某日某交易所股票涨跌数据
    :param exchange: 上交所 | 深交所
    :param frequency: 日线 | 周线 | 月线
    :param date: 日期
    :return: 当日股票涨跌数据 dataframe
    """
    data = None
    if frequency == 'DAILY':
        data = pro.daily(trade_date=date, fields='ts_code, change')
    elif frequency == 'WEEKLY':
        data = pro.weekly(trade_date=date, fields='ts_code, change')
    elif frequency == 'MONTHLY':
        data = pro.monthly(trade_date=date, fields='ts_code, change')
    if exchange != 'ALL':
        stock_list = get_stock_list(exchange)
        data = pd.merge(stock_list, data, how='inner')
    return data


def get_stock_details(ts_code, size, frequency='DAILY'):
    """
    获取某日某交易所股票涨跌数据
    :param frequency:
    :param size:
    :param ts_code: 股票代码
    :return: 股票数据 dataframe
    """
    start_date = dg.get_trade_date_before(size+30, frequency=frequency)
    data = ts.pro_bar(ts_code=ts_code, freq=frequency[0], adj='qfq', start_date=start_date, ma=[30])
    cols = ['ts_code', 'trade_date', 'open', 'close', 'low', 'high', 'pre_close', 'change', 'pct_chg', 'vol', 'amount', 'ma30', 'ma_v_30']
    data = data[cols]
    if len(data) >= size + 30:
        data = data[:-30]
    start_date = dg.get_trade_date_before(size, frequency=frequency)
    adj = pro.adj_factor(ts_code=ts_code, start_date=start_date)
    basic = pro.daily_basic(ts_code=ts_code, start_date=start_date, fields='ts_code,trade_date,turnover_rate,volume_ratio,pe,pe_ttm,pb,dv_ratio,dv_ttm')
    data = pd.merge(data, adj, how='left')
    data = pd.merge(data, basic, how='left')
    return data


def get_up_down_limits_statistic_details(date, exchange='ALL'):
    while True:
        try:
            data = pro.limit_list(trade_date=date, fields="ts_code,trade_date,fc_ratio,fl_ratio,fd_amount,first_time,last_time,open_times,strth,limit")
            break
        except Exception:
            time.sleep(5)
    if exchange != 'ALL':
        stock_list = get_stock_list(exchange)['ts_code']
        data = pd.merge(stock_list, data, how='inner')
    return data


if __name__ == '__main__':
    pass
    # data = get_stock_details('003039.SZ', 100, 'MONTHLY')
    # print(data)

