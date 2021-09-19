import tushare as ts
from database import utils

ts.set_token("3f73ca482044f78edd9694a4e06a0edd9431c24cbac31a07f275d7cf")
pro = ts.pro_api()


def get_start_end_date(days):
    """
    获取开始日期和结束日期
    :param days: 多少个交易日之前开始？
    :return: 开始日期和结束日期组成的dict
    """
    end_date = utils.get_today()
    trade_date_list = pro.query('trade_cal', is_open="1", start_date='20100101', end_date=end_date)
    length = len(trade_date_list)
    start_date = trade_date_list["cal_date"][length - days]
    end_date = trade_date_list["cal_date"][length - 1]
    return {
        "start_date": start_date,
        "end_date": end_date
    }


def get_trade_date_list(days):
    """
    获取从今天起往回数第 days 天内的交易日列表
    :param days: 回数天数
    :return: 交易日列表 dataframe
    """
    start_date, end_date = get_start_end_date(days).values()
    trade_date_list = pro.query('trade_cal', is_open="1", start_date=start_date, end_date=end_date)
    trade_date_list = trade_date_list
    return trade_date_list


def get_index_daily(index_code, days):
    """
    抓取指数行情
    :param index_code: 指数代码
    :param days: 抓取多少个交易日？
    :return: 指数日线dataframe
    """
    start_date, end_date = get_start_end_date(days).values()
    data = pro.index_daily(ts_code=index_code, start_date=start_date, end_date=end_date)
    return data


def get_up_down_daily(day):
    data = pro.daily(trade_date=day, fields='ts_code, change')
    return data


# def get_ma(code, days, ma_days):
#     start_date, end_date = get_start_end_date(days + ma_days).values()
#     df = ts.pro_bar(ts_code=code, start_date=start_date, end_date=end_date, ma=[ma_days])


if __name__ == '__main__':
    pass
