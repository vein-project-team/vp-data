import datetime
import pandas as pd
import tushare as ts
import utils

ts.set_token("3f73ca482044f78edd9694a4e06a0edd9431c24cbac31a07f275d7cf")
pro = ts.pro_api()


def get_latest_finished_trade_date():
    """
    获取最近的一个已经完成的交易日
    :return:  最近的一个已经完成的交易日 str
    """
    today = utils.get_today()
    now = datetime.datetime.now()
    hour = now.strftime('%H')
    day_counter = 3
    if int(hour) < 20:
        today = utils.get_days_before_today(1)
    while True:
        day = utils.get_days_before_today(day_counter)
        trade_date_list = pro.query('trade_cal', is_open="1", start_date=day, end_date=today)
        if len(trade_date_list) != 0:
            latest_finished_trade_date = trade_date_list['cal_date'].values[-1]
            return latest_finished_trade_date
        day_counter += 3


def get_trade_days_between(day1, day2):
    start_date = day1
    end_date = day2
    if int(day1) > int(day2):
        start_date, end_date = end_date, start_date
    trade_date_list = pro.query('trade_cal', is_open="1", start_date=start_date, end_date=end_date)
    return len(trade_date_list)


def get_trade_date_before(days):
    """
    获取开始日期和结束日期
    :param days: 多少个交易日之前开始？
    :return: 开始日期和结束日期组成的dict
    """
    end_date = get_latest_finished_trade_date()
    trade_date_list = pro.query('trade_cal', is_open="1", start_date='20100101', end_date=end_date)
    length = len(trade_date_list)
    start_date = trade_date_list["cal_date"][length - days]
    return start_date


def get_trade_week_list(weeks):
    data = get_index_weekly('000001.SH', weeks)
    return data['trade_date']


def get_trade_date_list(days):
    """
    获取从今天起往回数第 days 天内的交易日列表
    :param days: 回数天数
    :return: 交易日列表 dataframe
    """
    last_finished_trade_date = get_latest_finished_trade_date()
    trade_date_list = pro.query('trade_cal', is_open="1", start_date='20180101', end_date=last_finished_trade_date)
    return trade_date_list[-days:]


def get_index_daily(index_code, days):
    """
    抓取指数行情
    :param index_code: 指数代码
    :param days: 抓取多少个交易日？
    :return: 指数日线dataframe
    """
    last_finished_trade_date = get_latest_finished_trade_date()
    data = pro.index_daily(ts_code=index_code, start_date='20180101', end_date=last_finished_trade_date)
    return data[:days].iloc[::-1]


def get_index_weekly(index_code, weeks):
    """
    抓取指数行情
    :param index_code: 指数代码
    :param weeks: 抓取多少个交易周？
    :return: 指数日线dataframe
    """
    last_finished_trade_date = get_latest_finished_trade_date()
    data = pro.index_weekly(ts_code=index_code, start_date='20180101', end_date=last_finished_trade_date)
    return data[:weeks].iloc[::-1]


def get_stock_list(index_suffix):
    """
    获取某个交易所的股票列表
    :param index_suffix: 那个交易所？
    :return:股票代码列表
    """
    exchange = ''
    if index_suffix != 'ALL':
        exchange = 'SSE' if index_suffix == 'SH' else 'SZSE'
    data = pro.stock_basic(exchange=exchange, list_status='L', fields='ts_code')
    return data


def get_stocks_change_daily(exchange, day):
    """
    获取每日股票涨跌数据
    :param exchange: 那个交易所？
    :param day: 哪一天
    :return: 当天交易的股票涨跌数据 dataframe
    """
    stock_list = get_stock_list(exchange)
    data = pro.daily(trade_date=day, fields='ts_code, change')
    data = pd.merge(stock_list, data, how='inner')
    return data


def get_stocks_change_weekly(exchange, week):
    """
    获取每周股票涨跌数据
    :param exchange: 那个交易所？
    :param week: 哪周
    :return: 当周交易的股票涨跌数据 dataframe
    """
    stock_list = get_stock_list(exchange)
    data = pro.weekly(trade_date=week, fields='ts_code, change')
    data = pd.merge(stock_list, data, how='inner')
    return data


# def get_ma(code, days, ma_days):
#     start_date, end_date = get_start_end_date(days + ma_days).values()
#     df = ts.pro_bar(ts_code=code, start_date=start_date, end_date=end_date, ma=[ma_days])


if __name__ == '__main__':
    pass
