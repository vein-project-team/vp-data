import pandas as pd
from tqdm import tqdm as pb
from data_api.date_getter import date_getter as dg
from data_api import spider
from numpy import mean


def get_quotation_ma(data):
    ma30 = pd.DataFrame(columns=['k_ma30', 'vol_ma30'])
    close = data['close']
    vol = data['vol']
    for i in range(30, len(data)):
        ma30.loc[i-30] = [mean(close[i-30:i]), mean(vol[i-30:i])]
    return ma30


def get_index_ad_line(index_suffix, frequency, size):
    ad = pd.DataFrame(columns=['ups', 'downs', 'ad_line'])
    trade_date_list = dg.get_trade_date_list_forward(frequency, size)
    trade_date_list = trade_date_list['trade_date']
    ad_point = 0
    for i in pb(range(0, len(trade_date_list)), desc='收集数据中', colour='#ffffff'):
        date = trade_date_list[i]
        up, down = 0, 0
        data = spider.get_stocks_change(frequency, date, exchange=index_suffix)
        data = data['change']
        for j in data:
            if j > 0:
                up += 1
            elif j < 0:
                down += 1
        ad_point += up - down
        ad.loc[i] = [up, down, ad_point]
    return ad


def get_max_limiting_stocks(data, date, exchange='ALL', con_stocks=None):
    pre_date = dg.get_trade_date_before(2, date)
    pre_data = spider.get_up_down_limits_statistic_details(pre_date, exchange)
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
        return get_max_limiting_stocks(data, pre_date, exchange, con_stocks)


if __name__ == '__main__':
    pass
