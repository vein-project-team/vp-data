from data_api import spider
import numpy as np
import utils


def get_index_ma(index_code, days, ma_days):
    k_ma = []
    vol_ma = []
    data = spider.get_index_daily(index_code, days + ma_days)
    data = data.iloc[::-1]
    close = data['close'].tolist()
    vol = data['vol'].tolist()
    for i in range(30, len(data)):
        k_ma.append(np.mean(close[i-30:i]))
        vol_ma.append(np.mean(vol[i-30:i]))
    return {
        'k_ma': k_ma,
        'vol_ma': vol_ma
    }


def get_index_AD_line(index_suffix, days, only_main_board=True):
    ad_point = 0
    ad_line = []
    trade_date_list = spider.get_trade_date_list(days)
    trade_date_list = trade_date_list['cal_date'].tolist()
    for date in trade_date_list:
        data = spider.get_up_down_daily(date)
        if only_main_board is True:
            if index_suffix == 'SH':
                data = utils.only_keep_sh_main_board(data)
            elif index_suffix == 'SZ':
                data = utils.only_keep_sz_main_board(data)
        data = data['change'].tolist()
        for i in data:
            if i > 0:
                ad_point += 1
            elif i < 0:
                ad_point -= 1
        ad_line.append(ad_point)
    return ad_line


if __name__ == '__main__':
    pass
