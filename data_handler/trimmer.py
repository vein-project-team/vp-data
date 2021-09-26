from data_api import spider
from data_storage import reader
from data_handler import calculator
from utils import round_list


def get_index_daily_from_api(index_code, size):
    raw = spider.get_index_daily(index_code, size+30)
    ma30 = calculator.get_quotation_ma(raw)
    raw = raw[30:]
    ad_line_data = calculator.get_index_daily_ad_line(index_code[-2:], size)
    return {
        'date': raw['trade_date'].tolist(),
        'open': round_list(raw['open'].tolist()),
        'close': round_list(raw['close'].tolist()),
        'low': round_list(raw['low'].tolist()),
        'high': round_list(raw['high'].tolist()),
        'vol': round_list(raw['vol'].tolist()),
        'k_ma30': round_list(ma30['k_ma']),
        'vol_ma30': round_list(ma30['vol_ma']),
        'ups': ad_line_data['ups'],
        'downs': ad_line_data['downs'],
        'ad_line': ad_line_data['ad_line']
    }


def get_index_weekly_from_api(index_code, size):
    raw = spider.get_index_weekly(index_code, size+30)
    ma30 = calculator.get_quotation_ma(raw)
    raw = raw[30:]
    ad_line_data = calculator.get_index_weekly_ad_line(index_code[-2:], size)
    return {
        'date': raw['trade_date'].tolist(),
        'open': round_list(raw['open'].tolist()),
        'close': round_list(raw['close'].tolist()),
        'low': round_list(raw['low'].tolist()),
        'high': round_list(raw['high'].tolist()),
        'vol': round_list(raw['vol'].tolist()),
        'k_ma30': round_list(ma30['k_ma']),
        'vol_ma30': round_list(ma30['vol_ma']),
        'ups': ad_line_data['ups'],
        'downs': ad_line_data['downs'],
        'ad_line': ad_line_data['ad_line']
    }


def get_index_daily_from_db(index_suffix, size):
    data = reader.get_index_quotation(index_suffix, 'DAILY', size)
    return {
        "date": [data[i][0] for i in range(size)],
        "k_line": [
            [data[i][1], data[i][2], data[i][3], data[i][4]] for i in range(size)
        ],
        "vol": [data[i][5] for i in range(size)],
        "k_ma30": [data[i][6] for i in range(size)],
        "vol_ma30": [data[i][7] for i in range(size)],
        "ups": [data[i][8] for i in range(size)],
        "downs": [data[i][9] for i in range(size)],
        "ad_line": [data[i][10] for i in range(size)]
    }


def get_index_weekly_from_db(index_suffix, size):
    data = reader.get_index_quotation(index_suffix, 'WEEKLY', size)
    return {
        "date": [data[i][0] for i in range(size)],
        "k_line": [
            [data[i][1], data[i][2], data[i][3], data[i][4]] for i in range(size)
        ],
        "vol": [data[i][5] for i in range(size)],
        "k_ma30": [data[i][6] for i in range(size)],
        "vol_ma30": [data[i][7] for i in range(size)],
        "ups": [data[i][8] for i in range(size)],
        "downs": [data[i][9] for i in range(size)],
        "ad_line": [data[i][10] for i in range(size)]
    }


if __name__ == '__main__':
    pass
