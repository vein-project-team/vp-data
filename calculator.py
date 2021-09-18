import spider
import numpy as np


def get_index_ma(index_code, days, ma_days):
    ma = []
    data = spider.get_index_daily(index_code, days + ma_days)
    data = data['close'].tolist()
    data.reverse()
    for i in range(30, len(data)):
        ma.append(np.mean(data[i-30:i]))
    return ma


if __name__ == '__main__':
    get_index_ma('000001.SH', 200, 30)
