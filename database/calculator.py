from database import spider
from database import utils
import numpy as np


def get_index_ma(index_code, days, ma_days):
    ma = []
    data = spider.get_index_daily(index_code, days + ma_days)
    data = data['close'].tolist()
    data.reverse()
    for i in range(30, len(data)):
        ma.append(np.mean(data[i-30:i]))
    return ma


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
