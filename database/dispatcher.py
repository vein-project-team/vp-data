import pandas as pd
from tqdm import tqdm as pb
from database import spider
from database import calculator
from database.date_getter import date_getter as dg
from database.db_reader import read_from_db
from database.spider import get_stock_details
from database.settings import DB_SIZE


def get_trade_date_list_from_db(frequency):
    data = read_from_db(f'''SELECT TRADE_DATE FROM TRADE_DATE_LIST_{frequency};''')
    return data



def get_stock_list_from_api():
    pass


def get_stock_list_from_db(exchange, page):
    if exchange == 'all':
        data = pd.concat(
            read_from_db(f'''SELECT * FROM SZ_STOCK_LIST LIMIT 100 OFFSET {(page - 1) * 100};'''),
            read_from_db(f'''SELECT * FROM SH_STOCK_LIST LIMIT 100 OFFSET {(page - 1) * 100};''')
        )
    else:
        data = read_from_db(f'''SELECT * FROM {exchange}_STOCK_LIST LIMIT 100 OFFSET {(page -1 ) * 100};''')

    return {
        'stocks': [
            {
                'ts_code': ts_code,
                'name': name,
                'area': area,
                'industry': industry,
                'market': market,
                'list_date': list_date,
            } for ts_code, name, area, industry, market, list_status, list_date, delist_date, is_hs in data.values
        ]
    }


def get_index_quotation_from_api(index_code, frequency, size):
    raw = spider.get_index_quotation(index_code, frequency, size + 30)
    ma30 = calculator.get_quotation_ma(raw)
    ad = calculator.get_index_ad_line(index_code[-2:], frequency, size)
    data = raw[30:].reset_index(drop=True)
    data[['k_ma30', 'vol_ma30']] = ma30[['k_ma30', 'vol_ma30']]
    data[['ups', 'downs', 'ad_line']] = ad[['ups', 'downs', 'ad_line']]
    return round(data, 2)


def get_index_quotation_from_db(index_suffix, frequency, size):
    data = read_from_db(f'''SELECT * FROM {index_suffix}_INDEX_{frequency} LIMIT {DB_SIZE - size}, {DB_SIZE};''')
    return {
        "date": [date for date in data['TRADE_DATE']],
        "k_line": [
            [open, close, low, high] for open, close, low, high in data[['OPEN', 'CLOSE', 'LOW', 'HIGH']].values
        ],
        "vol": [vol for vol in data['VOL']],
        "k_ma30": [k_ma30 for k_ma30 in data['K_MA30']],
        "vol_ma30": [vol_ma30 for vol_ma30 in data['VOL_MA30']],
        "ups": [ups for ups in data['UPS']],
        "downs": [downs for downs in data['DOWNS']],
        "ad_line": [ad_line for ad_line in data['AD_LINE']]
    }


def get_stock_details_from_api(index_suffix, frequency, size):
    stock_list = read_from_db(f'''SELECT TS_CODE FROM {index_suffix}_STOCK_LIST''')['TS_CODE']
    data = None
    for stock in pb(stock_list, desc='收集数据中', colour='#ffffff'):
        if data is None:
            data = get_stock_details(stock, size, frequency)
        else:
            data = pd.concat([data, get_stock_details(stock, size, frequency)])
    data = data.reset_index(drop=True)
    return round(data, 2)


def get_stock_details_from_db(stock, frequency):
    data = read_from_db(f'''SELECT * FROM {stock[-2:]}_DETAILS_{frequency} WHERE TS_CODE = "{stock}"''')
    return {
        'date': [date for date in data['TRADE_DATE']],
        "k_line": [
            [open, close, low, high] for open, close, low, high in data[['OPEN', 'CLOSE', 'LOW', 'HIGH']].values
        ],
        "vol": [vol for vol in data['VOL']],
        "k_ma30": [k_ma30 for k_ma30 in data['K_MA30']],
        "vol_ma30": [vol_ma30 for vol_ma30 in data['VOL_MA30']],
    }


def get_up_down_limits_statistic_from_api(size, exchange='ALL'):
    date_list = dg.get_trade_date_list_forward('DAILY', size)
    data = None
    for date in pb(date_list['trade_date'], desc='收集数据中', colour='#ffffff'):
        raw = spider.get_up_down_limits_statistic_details(date, exchange)
        raw['con_days'] = 1
        if data is None:
            data = calculator.get_max_limiting_stocks(raw, date, exchange)
        else:
            data = pd.concat([data, calculator.get_max_limiting_stocks(raw, date, exchange)], axis=0)
    return round(data.reset_index(drop=True), 2)


if __name__ == '__main__':
    # print(get_index_quotation_from_db('SH', 'DAILY', 100))
    pass
