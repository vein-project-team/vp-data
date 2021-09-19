from database import spider
from database import calculator


def get_index_daily(index_code, days):
    date = spider.get_trade_date_list(days)
    date = date["cal_date"].tolist()
    data = spider.get_index_daily(index_code, days)
    close = data["close"].tolist()
    close.reverse()
    ma30 = calculator.get_index_ma(index_code, days, 30)
    vol = data['vol'].tolist()
    vol.reverse()
    adline = calculator.get_index_AD_line(index_code[-2:], days)
    for i in range(0, len(date)):
        close[i] = round(close[i], 2)
        ma30[i] = round(ma30[i], 2)
        vol[i] = round(vol[i], 2)
    return {
        index_code: {
            "date": date,
            "close": close,
            "ma30": ma30,
            "vol": vol,
            "adline": adline
        }
    }


if __name__ == '__main__':
    pass
    get_index_daily("000001.SH", 200)
