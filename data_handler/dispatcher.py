from data_api import spider
from data_storage import reader
from data_handler import calculator
# import pandas as pd

# pd.set_option('display.max_rows', 500)
# pd.set_option('display.max_columns', 500)
# pd.set_option('display.width', 1000)


def get_index_quotation_from_api(index_code, frequency, size):
    raw = spider.get_index_quotation(index_code, frequency, size + 30)
    ma30 = calculator.get_quotation_ma(raw)
    raw = raw[30:].reset_index(drop=True)
    ad_line_data = calculator.get_index_ad_line(index_code[-2:], frequency, size)
    return {
        'date': raw['trade_date'],
        'open': round(raw['open'], 2),
        'close': round(raw['close'], 2),
        'low': round(raw['low'], 2),
        'high': round(raw['high'], 2),
        'vol': round(raw['vol'], 2),
        'k_ma30': round(ma30['k_ma'], 2),
        'vol_ma30': round(ma30['vol_ma'], 2),
        'ups': ad_line_data['ups'],
        'downs': ad_line_data['downs'],
        'ad_line': ad_line_data['ad_line']
    }


def get_index_quotation_from_db(index_suffix, frequency, size):
    data = reader.get_index_quotation(index_suffix, frequency, size)
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


def get_up_down_limits_statistic_from_api(date):
    raw = spider.get_up_down_limits_statistic(date)
    raw['con_days'] = 1
    data = calculator.get_max_limiting_stocks(raw, date)
    return data


if __name__ == '__main__':
    get_up_down_limits_statistic_from_api('20210930')
