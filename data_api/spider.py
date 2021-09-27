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
    """
    获取两个交易日之间的交易日数量
    :param day1: 交易日1
    :param day2: 交易日2
    :return: 之间的交易日数量
    """
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


def get_trade_date_list(frequency, size):
    """
    获取交易日列表
    :param frequency: 日线 | 周线 | 月线
    :param size: 条数
    :return:
    """
    trade_date_list = pd.DataFrame()
    trade_date_list['trade_date'] = None
    if frequency == 'DAILY':
        last_finished_trade_date = get_latest_finished_trade_date()
        trade_date_list['trade_date'] = pro.query('trade_cal', is_open="1", start_date='20100101', end_date=last_finished_trade_date)['cal_date'][-size:]
    elif frequency == 'WEEKLY':
        data = get_index_quotation('000001.SH', 'WEEKLY', size)
        trade_date_list['trade_date'] = data['trade_date']
    elif frequency == 'MONTHLY':
        data = get_index_quotation('000001.SH', 'MONTHLY', size)
        trade_date_list['trade_date'] = data['trade_date']
    trade_date_list.reset_index(inplace=True, drop=True)
    return trade_date_list


def get_index_quotation(index_code, frequency, size):
    """
    获取指数行情
    :param index_code: 上证指数 | 深证指数
    :param frequency: 日线 | 周线 | 月线
    :param size: 条数
    :return: 指数行情 dataframe
    """
    data = None
    last_finished_trade_date = get_latest_finished_trade_date()
    if frequency == 'DAILY':
        data = pro.index_daily(ts_code=index_code, start_date='20100101', end_date=last_finished_trade_date)
    elif frequency == 'WEEKLY':
        data = pro.index_weekly(ts_code=index_code, start_date='20100101', end_date=last_finished_trade_date)
    elif frequency == 'MONTHLY':
        data = pro.index_monthly(ts_code=index_code, start_date='20100101', end_date=last_finished_trade_date)
    data = data[:size].iloc[::-1] if data is not None else None
    data.reset_index(inplace=True, drop=True)
    return data


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


def get_stocks_change(exchange, frequency, date):
    """
    获取某日某交易所股票涨跌数据
    :param exchange: 上交所 | 深交所
    :param frequency: 日线 | 周线 | 月线
    :param date: 日期
    :return: 当日股票涨跌数据 dataframe
    """
    data = None
    stock_list = get_stock_list(exchange)
    if frequency == 'DAILY':
        data = pro.daily(trade_date=date, fields='ts_code, change')
    elif frequency == 'WEEKLY':
        data = pro.weekly(trade_date=date, fields='ts_code, change')
    elif frequency == 'MONTHLY':
        data = pro.monthly(trade_date=date, fields='ts_code, change')
    data = pd.merge(stock_list, data, how='inner')
    return data


if __name__ == '__main__':
    pass
