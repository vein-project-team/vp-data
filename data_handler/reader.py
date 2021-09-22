# from data_api import spider
# from data_handler import calculator
from data_storage.filer import read_data


def get_index_daily(index_suffix, days):
    data = read_data(f'''
    SELECT * FROM (
    SELECT * FROM {index_suffix}_INDEX_DAILY
    ORDER BY DATE DESC LIMIT {days})
    ORDER BY DATE ASC;
    ''')

    return {
        index_suffix: {
            "date": [data[i][0] for i in range(days)],
            "k_line": [
                [data[i][1], data[i][2], data[i][3], data[i][4]] for i in range(days)
            ],
            "vol": [data[i][5] for i in range(days)],
            "k_ma30": [data[i][6] for i in range(days)],
            "vol_ma30": [data[i][7] for i in range(days)],
            "ad_line": [data[i][8] for i in range(days)]
        }
    }


if __name__ == '__main__':
    pass
    # get_index_daily("000001.SH", 200)
