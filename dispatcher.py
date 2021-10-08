import pandas as pd
from tqdm import tqdm as pb
from data_api import calculator
from data_api.date_getter import date_getter as dg
from database.db_reader import read_from_db
from data_api.spider import get_stock_list
from data_api.spider import get_index_quotation
from data_api.spider import get_up_down_limits_statistic_details
from data_api.spider import get_stock_details


def get_trade_date_list_from_api(frequency, size):
    """
    从API侧拉取交易日表
    :param frequency:
    :param size:
    :return:
    """
    data = dg.get_trade_date_list_forward(frequency, size)
    return data


def get_trade_date_list_from_db(frequency):
    """
    从数据库侧拉取交易日表
    :param frequency:
    :return:
    """
    data = read_from_db(f'''SELECT TRADE_DATE FROM TRADE_DATE_LIST_{frequency};''')
    return data


def get_stock_list_from_api(exchange):
    """
    从API侧拉取股票列表
    :param exchange:
    :return:
    """
    data = get_stock_list(exchange)
    data.fillna('NULL', inplace=True)
    return data


def get_stock_list_from_db(exchange):
    """
    从数据库侧拉取股票列表
    :param exchange:
    :return:
    """
    if exchange == 'all':
        data = pd.concat([
            read_from_db(f'''SELECT * FROM SZ_STOCK_LIST;'''),
            read_from_db(f'''SELECT * FROM SH_STOCK_LIST;''')]
        )
    else:
        data = read_from_db(f'''SELECT * FROM {exchange}_STOCK_LIST;''')
    return data


def get_index_quotation_from_api(index_suffix, frequency, size):
    """
    从API侧拉取指数行情
    :param index_suffix:
    :param frequency:
    :param size:
    :return:
    """
    index_code = ''
    if index_suffix == 'SH':
        index_code = '000001.SH'
    elif index_suffix == 'SZ':
        index_code = '399001.SZ'
    raw = get_index_quotation(index_code, frequency, size + 30)
    ma30 = calculator.get_quotation_ma(raw)
    ad = calculator.get_index_ad_line(index_code[-2:], frequency, size)
    data = raw[30:].reset_index(drop=True)
    data[['k_ma30', 'vol_ma30']] = ma30[['k_ma30', 'vol_ma30']]
    data[['ups', 'downs', 'ad_line']] = ad[['ups', 'downs', 'ad_line']]
    return round(data, 2)


def get_index_quotation_from_db(index_suffix, frequency):
    """
    从数据库侧拉取指数行情
    :param index_suffix:
    :param frequency:
    :return:
    """
    data = read_from_db(f'''SELECT * FROM {index_suffix}_INDEX_{frequency};''')
    return data


def get_stock_details_from_api(ts_code, size, frequency):
    """
    从API侧拉取个股行情
    :param ts_code:
    :param size:
    :param frequency:
    :return:
    """
    data = get_stock_details(ts_code, size, frequency)
    data = data.iloc[::-1].reset_index(drop=True)
    data.fillna('NULL', inplace=True)
    return data


def get_stocks_details_by_exchange_from_api(index_suffix, frequency, size):
    """
    从API侧根据交易所拉取一堆股票的行情
    :param index_suffix:
    :param frequency:
    :param size:
    :return:
    """
    stock_list = read_from_db(f'''SELECT TS_CODE FROM {index_suffix}_STOCK_LIST''')['TS_CODE']
    data = None
    for stock in pb(stock_list, desc='收集数据中', colour='#ffffff'):
        if data is None:
            data = get_stock_details_from_api(stock, size, frequency)
        else:
            data = pd.concat([data, get_stock_details_from_api(stock, size, frequency)])
    data = data.reset_index(drop=True)
    return round(data, 2)


def get_stock_details_from_db(stock, frequency):
    """
    从数据库侧拉取个股行情
    :param stock:
    :param frequency:
    :return:
    """
    data = read_from_db(f'''SELECT * FROM {stock[-2:]}_DETAILS_{frequency} WHERE TS_CODE = "{stock}"''')
    return data


def get_up_down_limits_statistic_details_from_api(size, exchange='ALL'):
    """
    从API侧拉取涨跌停统计数据
    :param size:
    :param exchange:
    :return:
    """
    date_list = dg.get_trade_date_list_forward('DAILY', size)
    data = None
    for date in pb(date_list['trade_date'], desc='收集数据中', colour='#ffffff'):
        raw = get_up_down_limits_statistic_details(date, exchange)
        raw['con_days'] = 1
        if data is None:
            data = calculator.get_max_limiting_stocks(raw, date, exchange)
        else:
            data = pd.concat([data, calculator.get_max_limiting_stocks(raw, date, exchange)], axis=0)
    return round(data.reset_index(drop=True), 2)


def get_up_down_limits_statistic_details_from_db():
    pass


if __name__ == '__main__':
    # print(get_index_quotation_from_db('SH', 'DAILY', 100))
    pass
