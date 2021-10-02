import pandas as pd

from data_api import spider
import numpy as np


def get_quotation_ma(data):
    k_ma = []
    vol_ma = []
    close = data['close']
    vol = data['vol']
    for i in range(30, len(data)):
        k_ma.append(np.mean(close[i-30:i]))
        vol_ma.append(np.mean(vol[i-30:i]))
    return {
        'k_ma': k_ma,
        'vol_ma': vol_ma
    }


def get_index_ad_line(index_suffix, frequency, size):
    ups = []
    downs = []
    ad_line = []
    trade_date_list = spider.get_trade_date_list(frequency, size)
    trade_date_list = trade_date_list['trade_date']
    ad_point = 0
    for date in trade_date_list:
        up, down = 0, 0
        data = spider.get_stocks_change(frequency, date, exchange=index_suffix)
        data = data['change']
        for i in data:
            if i > 0:
                up += 1
            elif i < 0:
                down += 1
        ups.append(up)
        downs.append(down)
        ad_point += up - down
        ad_line.append(ad_point)
    return {
        'date': trade_date_list,
        'ups': ups,
        'downs': downs,
        'ad_line': ad_line
    }


def get_max_limiting_stocks(data, date, con_stocks=None):
    pre_date = spider.get_trade_date_before(1, date)
    pre_data = spider.get_up_down_limits_statistic(pre_date)
    pre_stocks = pre_data[['ts_code', 'limit']]
    if con_stocks is None:
        stocks = data[['ts_code', 'limit']]
        con_stocks = pd.merge(stocks, pre_stocks)
    else:
        con_stocks = pd.merge(con_stocks, pre_stocks)
    if len(con_stocks) == 0:
        return data
    else:
        for i in range(0, len(data)):
            if data['ts_code'][i] in con_stocks['ts_code'].values:
                data.at[i, 'con_days'] += 1
        return get_max_limiting_stocks(data, pre_date, con_stocks)


if __name__ == '__main__':
    pass
