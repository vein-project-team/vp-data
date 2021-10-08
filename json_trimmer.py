import pandas as pd
from dispatcher import get_trade_date_list_from_db
from dispatcher import get_stock_list_from_db
from dispatcher import get_stock_details_from_db
from dispatcher import get_index_quotation_from_db


def get_trade_date_list_by_frequency(frequency):
    """
    根据频率获取交易日
    :param frequency: 日线 | 周线 | 月线
    :return: 对应的交易日列表 list(string)
    """
    data = get_trade_date_list_from_db(frequency)
    return [date for date in data['TRADE_DATE']]


def get_all_trade_date_list_json():
    """
    返回一个完整的交易日表json
    :return: 交易日表json
    """
    return {
        'DAILY': get_trade_date_list_by_frequency('DAILY'),
        'WEEKLY': get_trade_date_list_by_frequency('WEEKLY'),
        'MONTHLY': get_trade_date_list_by_frequency('MONTHLY')
    }


def get_stock_list_json_by_exchange(exchange):
    """
    根据交易所获取对应的股票列表信息
    :param exchange: 交易所 SH | SZ
    :return: 对应的股票列表json
    """
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
    """
    获取全部股票列表
    :return: 完整的股票列表json
    """
    return get_stock_list_json_by_exchange('all')


def get_stock_details_json(stock, frequency):
    """
    获取个股行情数据
    :param stock: 股票代码
    :param frequency: 日线 | 月线 | 周线
    :return: 个股行情数据 json
    """
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
    """
    获取填充过的个股行情数据
    许多个股并不拥有完整的全交易日数据
    在这里进行填充可以在前端图中制造断层效果，直观好看
    缺失的数据填充为空字符串 ‘’
    :param stock: 股票代码
    :param frequency: 日线 | 月线 | 周线
    :return:个股行情数据 json
    """
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
    """
    获取某个指数的某个频率的行情
    :param index_suffix: 指数后缀 SH | SZ
    :param frequency: 日线 | 月线 | 周线
    :return: 对应的指数行情 json
    """
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
    """
    获取某个指数的全频率行情
    :param index_suffix: 指数后缀 SH | SZ
    :return: 该指数的完整行情json
    """
    return {
        index_suffix: {
            "daily": get_index_quotation_json_by_frequency(index_suffix, 'DAILY'),
            'weekly': get_index_quotation_json_by_frequency(index_suffix, 'WEEKLY'),
            'monthly': get_index_quotation_json_by_frequency(index_suffix, 'MONTHLY')
        }
    }


if __name__ == '__main__':
    print(get_all_stock_list_json())