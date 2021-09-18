import spider
import calculator


def get_index_daily(index_code, days):
    date = spider.get_trade_date(days)
    date = date["cal_date"].tolist()
    data = spider.get_index_daily(index_code, days)
    data = data["close"].tolist()
    data.reverse()
    return {
        index_code: {
            "date": date,
            "data": data,
            "ma30": calculator.get_index_ma(index_code, days, 30)
        }
    }


if __name__ == '__main__':
    pass
    get_index_daily("000001.SH", 200)
