import pandas as pd
from database.dispatcher import get_trade_date_list_from_db
from database.dispatcher import get_stock_list_from_db
from database.dispatcher import get_stock_details_from_db
from database.dispatcher import get_index_quotation_from_db


def get_trade_date_list_by_frequency(frequency):
    data = get_trade_date_list_from_db(frequency)
    return [date for date in data['TRADE_DATE']]


def get_all_trade_date_list_json():
    return {
        'DAILY': get_trade_date_list_by_frequency('DAILY'),
        'WEEKLY': get_trade_date_list_by_frequency('WEEKLY'),
        'MONTHLY': get_trade_date_list_by_frequency('MONTHLY')
    }


def get_stock_list_json_by_exchange(exchange):
    data = get_stock_list_from_db(exchange)
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


def get_all_stock_list_json():
    return get_stock_list_json_by_exchange('all')


def get_stock_details_json(stock, frequency):
    data = get_stock_details_from_db(stock, frequency)
    return {
        'date': [date for date in data['TRADE_DATE']],
        "k_line": [
            [open, close, low, high] for open, close, low, high in data[['OPEN', 'CLOSE', 'LOW', 'HIGH']].values
        ],
        "vol": [vol for vol in data['VOL']],
        "k_ma30": [k_ma30 for k_ma30 in data['K_MA30']],
        "vol_ma30": [vol_ma30 for vol_ma30 in data['VOL_MA30']],
    }


def get_filled_stock_details_json(stock, frequency):
    full_date_list = get_trade_date_list_from_db(frequency)
    data = get_stock_details_from_db(stock, frequency)
    data = pd.merge(full_date_list, data, how='left')
    data.fillna('', inplace=True)
    return {stock: {
        'date': [date for date in data['TRADE_DATE']],
        "k_line": [
            [open, close, low, high] for open, close, low, high in data[['OPEN', 'CLOSE', 'LOW', 'HIGH']].values
        ],
        "vol": [vol for vol in data['VOL']],
        "k_ma30": [k_ma30 for k_ma30 in data['K_MA30']],
        "vol_ma30": [vol_ma30 for vol_ma30 in data['VOL_MA30']],
    }}


def get_index_quotation_json_by_frequency(index_suffix, frequency):
    data = get_index_quotation_from_db(index_suffix, frequency)
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


def get_index_quotation_json(index_suffix):
    return {
        index_suffix: {
            "daily": get_index_quotation_json_by_frequency(index_suffix, 'DAILY'),
            'weekly': get_index_quotation_json_by_frequency(index_suffix, 'WEEKLY'),
            'monthly': get_index_quotation_json_by_frequency(index_suffix, 'MONTHLY')
        }
    }


if __name__ == '__main__':
    print(get_all_stock_list_json())