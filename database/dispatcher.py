import pandas as pd
from tqdm import tqdm as pb
from database import calculator
from database.date_getter import date_getter as dg
from database.db_reader import read_from_db
from database.spider import get_stock_list
from database.spider import get_index_quotation
from database.spider import get_up_down_limits_statistic_details
from database.spider import get_stock_details


def get_trade_date_list_from_api(frequency, size):
    data = dg.get_trade_date_list_forward(frequency, size)
    return data


def get_trade_date_list_from_db(frequency):
    data = read_from_db(f'''SELECT TRADE_DATE FROM TRADE_DATE_LIST_{frequency};''')
    return data


def get_stock_list_from_api(exchange):
    data = get_stock_list(exchange)
    data.fillna('NULL', inplace=True)
    return data


def get_stock_list_from_db(exchange):
    if exchange == 'all':
        data = pd.concat([
            read_from_db(f'''SELECT * FROM SZ_STOCK_LIST;'''),
            read_from_db(f'''SELECT * FROM SH_STOCK_LIST;''')]
        )
    else:
        data = read_from_db(f'''SELECT * FROM {exchange}_STOCK_LIST;''')

    return data


def get_index_quotation_from_api(index_code, frequency, size):
    raw = get_index_quotation(index_code, frequency, size + 30)
    ma30 = calculator.get_quotation_ma(raw)
    ad = calculator.get_index_ad_line(index_code[-2:], frequency, size)
    data = raw[30:].reset_index(drop=True)
    data[['k_ma30', 'vol_ma30']] = ma30[['k_ma30', 'vol_ma30']]
    data[['ups', 'downs', 'ad_line']] = ad[['ups', 'downs', 'ad_line']]
    return round(data, 2)


def get_index_quotation_from_db(index_suffix, frequency):
    data = read_from_db(f'''SELECT * FROM {index_suffix}_INDEX_{frequency};''')
    return data


def get_stock_details_from_api(ts_code, size, frequency):
    data = get_stock_details(ts_code, size, frequency)
    data = data.iloc[::-1].reset_index(drop=True)
    data.fillna('NULL', inplace=True)
    return data


def get_stocks_details_by_exchange_from_api(index_suffix, frequency, size):
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
    data = read_from_db(f'''SELECT * FROM {stock[-2:]}_DETAILS_{frequency} WHERE TS_CODE = "{stock}"''')
    return data


def get_up_down_limits_statistic_details_from_api(size, exchange='ALL'):
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
