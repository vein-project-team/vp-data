from data_api import spider
import numpy as np
import utils


def get_quotation_ma(data):
    k_ma = []
    vol_ma = []
    close = data['close'].tolist()
    vol = data['vol'].tolist()
    for i in range(30, len(data)):
        k_ma.append(np.mean(close[i-30:i]))
        vol_ma.append(np.mean(vol[i-30:i]))
    return {
        'k_ma': k_ma,
        'vol_ma': vol_ma
    }


def get_index_daily_ad_line(index_suffix, size):
    ups = []
    downs = []
    ad_line = []
    trade_date_list = spider.get_trade_date_list(size)
    trade_date_list = trade_date_list['cal_date'].tolist()
    ad_point = 0
    for date in trade_date_list:
        up, down = 0, 0
        data = spider.get_stocks_change_daily(index_suffix, date)
        data = data['change'].tolist()
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


def get_index_weekly_ad_line(index_suffix, size):
    ups = []
    downs = []
    ad_line = []
    trade_week_list = spider.get_trade_week_list(size)
    trade_week_list = trade_week_list.tolist()
    ad_point = 0
    for date in trade_week_list:
        up, down = 0, 0
        data = spider.get_stocks_change_weekly(index_suffix, date)
        data = data['change'].tolist()
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
        'date': trade_week_list,
        'ups': ups,
        'downs': downs,
        'ad_line': ad_line
    }


if __name__ == '__main__':
    pass
    # print(get_index_weekly_ad_line('SH', 2))
