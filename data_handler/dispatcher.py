from data_api import spider
from data_storage import reader
from data_handler import calculator
from utils import round_list


def get_index_quotation_from_api(index_code, frequency, size):
    raw = spider.get_index_quotation(index_code, frequency, size + 30)
    ma30 = calculator.get_quotation_ma(raw)
    raw = raw[30:]
    ad_line_data = calculator.get_index_ad_line(index_code[-2:], frequency, size)
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


def get_up_down_limits_statistic_from_api(date, exchange):
    raw = spider.get_up_down_limits_statistic(date, exchange)
    up_limits = raw.loc[raw['limit'] == 'U']
    down_limits = raw.loc[raw['limit'] == 'D']
    print(calculator.get_max_limiting_stocks(up_limits, date, exchange))
    data = {
        'date': date,
        'up_limits': len(up_limits),
        "down_limits": len(down_limits),
        'strongest': None,
        'weakest': None
    }


if __name__ == '__main__':
    get_up_down_limits_statistic_from_api('20210928', 'SH')
