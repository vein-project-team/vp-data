from dispatcher import get_trade_date_list_from_api, get_stocks_details_by_set_from_api
from dispatcher import get_stock_list_from_api
from dispatcher import get_index_quotation_from_api
from dispatcher import get_stocks_details_by_exchange_from_api
from dispatcher import get_up_down_limits_details_daily_from_api
from cacher import read_cache

DB_SIZE = 100
TABLE_NAMES = [
    'TRADE_DATE_LIST_DAILY',
    'TRADE_DATE_LIST_WEEKLY',
    'TRADE_DATE_LIST_MONTHLY',
    'SH_STOCK_LIST',
    'SZ_STOCK_LIST',
    'SH_INDEX_DAILY',
    'SH_INDEX_WEEKLY',
    'SH_INDEX_MONTHLY',
    'SH_DETAILS_DAILY',
    'SH_DETAILS_WEEKLY',
    'SH_DETAILS_MONTHLY',
    'SH_LIMITS_DETAILS_DAILY',
    'SH_LIMITS_STATISTIC_DAILY',
    'SZ_INDEX_DAILY',
    'SZ_INDEX_WEEKLY',
    'SZ_INDEX_MONTHLY',
    'SZ_DETAILS_DAILY',
    'SZ_DETAILS_WEEKLY',
    'SZ_DETAILS_MONTHLY',
    'SZ_LIMITS_DETAILS_DAILY',
    'SZ_LIMITS_STATISTIC_DAILY'
]

CREATION_SQL = {
    'TRADE_DATE_LIST_DAILY': '''
    CREATE TABLE IF NOT EXISTS TRADE_DATE_LIST_DAILY (
        TRADE_DATE CHAR(8) PRIMARY KEY
    );
    ''',
    'TRADE_DATE_LIST_WEEKLY': '''
    CREATE TABLE IF NOT EXISTS TRADE_DATE_LIST_WEEKLY (
        TRADE_DATE CHAR(8) PRIMARY KEY
    );
    ''',
    'TRADE_DATE_LIST_MONTHLY': '''
    CREATE TABLE IF NOT EXISTS TRADE_DATE_LIST_MONTHLY (
        TRADE_DATE CHAR(8) PRIMARY KEY
    );
    ''',
    'SH_STOCK_LIST': '''
    CREATE TABLE IF NOT EXISTS SH_STOCK_LIST (
        TS_CODE CHAR(9) PRIMARY KEY NOT NULL, -- 股票代码
        NAME TEXT NOT NULL, -- 股票名称
        AREA TEXT, -- 所属地区
        INDUSTRY TEXT, -- 所属行业
        MARKET CHAR(1), -- 市场类型，M: 主板，G: 创业板，K: 科创板
        LIST_STATUS CHAR(1) NOT NULL, -- 上市状态， L: 上市 D: 退市 P: 暂停上市
        LIST_DATE CHAR(8), -- 上市日期
        DELIST_DATE CHAR(8), -- 退市日期
        IS_HS CHAR(1) -- 是否沪深港通标的，N 否 H 沪股通 S 深股通
    );
    ''',
    'SZ_STOCK_LIST': '''
    CREATE TABLE IF NOT EXISTS SZ_STOCK_LIST (
        TS_CODE CHAR(9) PRIMARY KEY NOT NULL, -- 股票代码
        NAME TEXT NOT NULL, -- 股票名称
        AREA TEXT, -- 所属地区
        INDUSTRY TEXT, -- 所属行业
        MARKET CHAR(1), -- 市场类型，M: 主板，G: 创业板，K: 科创板
        LIST_STATUS CHAR(1) NOT NULL, -- 上市状态， L: 上市 D: 退市 P: 暂停上市
        LIST_DATE CHAR(8), -- 上市日期
        DELIST_DATE CHAR(8), -- 退市日期
        IS_HS CHAR(1) -- 是否沪深港通标的，N 否 H 沪股通 S 深股通
    );
    ''',
    'SH_INDEX_DAILY': '''
    CREATE TABLE IF NOT EXISTS SH_INDEX_DAILY (
        TRADE_DATE CHAR(8) PRIMARY KEY,
        OPEN REAL,
        CLOSE REAL,
        LOW REAL,
        HIGH REAL,
        VOL REAL,
        K_MA30 REAL,
        VOL_MA30 REAL,
        UPS REAL,
        DOWNS REAL,
        AD_LINE REAL
    );
    ''',
    'SH_INDEX_WEEKLY': '''
    CREATE TABLE IF NOT EXISTS SH_INDEX_WEEKLY (
        TRADE_DATE CHAR(8) PRIMARY KEY,
        OPEN REAL,
        CLOSE REAL,
        LOW REAL,
        HIGH REAL,
        VOL REAL,
        K_MA30 REAL,
        VOL_MA30 REAL,
        UPS REAL,
        DOWNS REAL,
        AD_LINE REAL
    );
    ''',
    'SH_INDEX_MONTHLY': '''
    CREATE TABLE IF NOT EXISTS SH_INDEX_MONTHLY (
        TRADE_DATE CHAR(8) PRIMARY KEY,
        OPEN REAL,
        CLOSE REAL,
        LOW REAL,
        HIGH REAL,
        VOL REAL,
        K_MA30 REAL,
        VOL_MA30 REAL,
        UPS REAL,
        DOWNS REAL,
        AD_LINE REAL
    );
    ''',
    'SH_DETAILS_DAILY': '''
    CREATE TABLE IF NOT EXISTS SH_DETAILS_DAILY (
        TS_CODE CHAR(9) NOT NULL, --股票代码
        TRADE_DATE CHAR(8) NOT NULL, --交易日
        OPEN REAL, -- 开盘价
        CLOSE REAL, -- 收盘价
        LOW REAL, -- 最低价
        HIGH REAL, -- 最高价
        PRE_CLOSE REAL, -- 昨收
        CHANGE REAL, -- 涨跌额
        PCT_CHANGE REAL, -- 涨跌幅
        VOL REAL, -- 成交量
        AMOUNT REAL, -- 成交额
        K_MA30 REAL, --30日均线
        VOL_MA30 REAL, --30日成交量均线
        ADJ_FACTOR REAL, -- 复权因子
        TURNOVER_RATE REAL, -- 换手率
        VOLUME_RATIO REAL, -- 量比
        PE REAL, -- 市盈率
        PE_TTM REAL, -- 滚动市盈率
        PB REAL, -- 市净率
        DV REAL, -- 股息率
        DV_TTM REAL, -- 滚动股息率
        PRIMARY KEY (TS_CODE, TRADE_DATE)
    );
    ''',
    'SH_DETAILS_WEEKLY': '''
    CREATE TABLE IF NOT EXISTS SH_DETAILS_WEEKLY (
        TS_CODE CHAR(9) NOT NULL, --股票代码
        TRADE_DATE CHAR(8) NOT NULL, --交易日
        OPEN REAL, -- 开盘价
        CLOSE REAL, -- 收盘价
        LOW REAL, -- 最低价
        HIGH REAL, -- 最高价
        PRE_CLOSE REAL, -- 昨收
        CHANGE REAL, -- 涨跌额
        PCT_CHANGE REAL, -- 涨跌幅
        VOL REAL, -- 成交量
        AMOUNT REAL, -- 成交额
        K_MA30 REAL, --30日均线
        VOL_MA30 REAL, --30日成交量均线
        ADJ_FACTOR REAL, -- 复权因子
        PE REAL, -- 市盈率
        PE_TTM REAL, -- 滚动市盈率
        PB REAL, -- 市净率
        DV REAL, -- 股息率
        DV_TTM REAL, -- 滚动股息率
        PRIMARY KEY (TS_CODE, TRADE_DATE)
    );
    ''',
    'SH_DETAILS_MONTHLY': '''
    CREATE TABLE IF NOT EXISTS SH_DETAILS_MONTHLY (
        TS_CODE CHAR(9) NOT NULL, --股票代码
        TRADE_DATE CHAR(8) NOT NULL, --交易日
        OPEN REAL, -- 开盘价
        CLOSE REAL, -- 收盘价
        LOW REAL, -- 最低价
        HIGH REAL, -- 最高价
        PRE_CLOSE REAL, -- 昨收
        CHANGE REAL, -- 涨跌额
        PCT_CHANGE REAL, -- 涨跌幅
        VOL REAL, -- 成交量
        AMOUNT REAL, -- 成交额
        K_MA30 REAL, --30日均线
        VOL_MA30 REAL, --30日成交量均线
        ADJ_FACTOR REAL, -- 复权因子
        PE REAL, -- 市盈率
        PE_TTM REAL, -- 滚动市盈率
        PB REAL, -- 市净率
        DV REAL, -- 股息率
        DV_TTM REAL, -- 滚动股息率
        PRIMARY KEY (TS_CODE, TRADE_DATE)
    );
    ''',
    'SH_LIMITS_DETAILS_DAILY': '''
    CREATE TABLE IF NOT EXISTS SH_LIMITS_DETAILS_DAILY (
        TS_CODE CHAR(9),
        TRADE_DATE CHAR(8),
        FC_RATIO REAL,
        FL_RATIO REAL,
        FD_AMOUNT REAL,
        FIRST_TIME TEXT,
        LAST_TIME TEXT, 
        OPEN_TIMES REAL,
        STRTH REAL, 
        LIMIT_TYPE CHAR(1),
        CON_DAYS REAL,
        PRIMARY KEY (TRADE_DATE, TS_CODE)
    );
    ''',
    'SH_LIMITS_STATISTIC_DAILY': '''
    CREATE TABLE IF NOT EXISTS SH_LIMITS_STATISTIC_DAILY (
        TRADE_DATE CHAR(8) PRIMARY KEY,
        UP_LIMITS_AMOUNT REAL,
        DOWN_LIMITS_AMOUNT REAL,
        MAX_UP_LIMIT_CON_DAYS REAL,
        MAX_DOWN_LIMIT_CON_DAYS REAL
    );
    ''',
    'SZ_INDEX_DAILY': '''
    CREATE TABLE IF NOT EXISTS SZ_INDEX_DAILY (
        TRADE_DATE CHAR(8) PRIMARY KEY,
        OPEN REAL,
        CLOSE REAL,
        LOW REAL,
        HIGH REAL,
        VOL REAL,
        K_MA30 REAL,
        VOL_MA30 REAL,
        UPS REAL,
        DOWNS REAL,
        AD_LINE REAL
    );
    ''',
    'SZ_INDEX_WEEKLY': '''
    CREATE TABLE IF NOT EXISTS SZ_INDEX_WEEKLY (
        TRADE_DATE CHAR(8) PRIMARY KEY,
        OPEN REAL,
        CLOSE REAL,
        LOW REAL,
        HIGH REAL,
        VOL REAL,
        K_MA30 REAL,
        VOL_MA30 REAL,
        UPS REAL,
        DOWNS REAL,
        AD_LINE REAL
    );
    ''',
    'SZ_INDEX_MONTHLY': '''
    CREATE TABLE IF NOT EXISTS SZ_INDEX_MONTHLY (
        TRADE_DATE CHAR(8) PRIMARY KEY,
        OPEN REAL,
        CLOSE REAL,
        LOW REAL,
        HIGH REAL,
        VOL REAL,
        K_MA30 REAL,
        VOL_MA30 REAL,
        UPS REAL,
        DOWNS REAL,
        AD_LINE REAL
    );
    ''',
    'SZ_DETAILS_DAILY': '''
    CREATE TABLE IF NOT EXISTS SZ_DETAILS_DAILY (
        TS_CODE CHAR(9) NOT NULL, --股票代码
        TRADE_DATE CHAR(8) NOT NULL, --交易日
        OPEN REAL, -- 开盘价
        CLOSE REAL, -- 收盘价
        LOW REAL, -- 最低价
        HIGH REAL, -- 最高价
        PRE_CLOSE REAL, -- 昨收
        CHANGE REAL, -- 涨跌额
        PCT_CHANGE REAL, -- 涨跌幅
        VOL REAL, -- 成交量
        AMOUNT REAL, -- 成交额
        K_MA30 REAL, --30日均线
        VOL_MA30 REAL, --30日成交量均线
        ADJ_FACTOR REAL, -- 复权因子
        TURNOVER_RATE REAL, -- 换手率
        VOLUME_RATIO REAL, -- 量比
        PE REAL, -- 市盈率
        PE_TTM REAL, -- 滚动市盈率
        PB REAL, -- 市净率
        DV REAL, -- 股息率
        DV_TTM REAL, -- 滚动股息率
        PRIMARY KEY (TS_CODE, TRADE_DATE)
    );
    ''',
    'SZ_DETAILS_WEEKLY': '''
    CREATE TABLE IF NOT EXISTS SZ_DETAILS_WEEKLY (
        TS_CODE CHAR(9) NOT NULL, --股票代码
        TRADE_DATE CHAR(8) NOT NULL, --交易日
        OPEN REAL, -- 开盘价
        CLOSE REAL, -- 收盘价
        LOW REAL, -- 最低价
        HIGH REAL, -- 最高价
        PRE_CLOSE REAL, -- 昨收
        CHANGE REAL, -- 涨跌额
        PCT_CHANGE REAL, -- 涨跌幅
        VOL REAL, -- 成交量
        AMOUNT REAL, -- 成交额
        K_MA30 REAL, --30日均线
        VOL_MA30 REAL, --30日成交量均线
        ADJ_FACTOR REAL, -- 复权因子
        PE REAL, -- 市盈率
        PE_TTM REAL, -- 滚动市盈率
        PB REAL, -- 市净率
        DV REAL, -- 股息率
        DV_TTM REAL, -- 滚动股息率
        PRIMARY KEY (TS_CODE, TRADE_DATE)
    );
    ''',
    'SZ_DETAILS_MONTHLY': '''
    CREATE TABLE IF NOT EXISTS SZ_DETAILS_MONTHLY (
        TS_CODE CHAR(9) NOT NULL, --股票代码
        TRADE_DATE CHAR(8) NOT NULL, --交易日
        OPEN REAL, -- 开盘价
        CLOSE REAL, -- 收盘价
        LOW REAL, -- 最低价
        HIGH REAL, -- 最高价
        PRE_CLOSE REAL, -- 昨收
        CHANGE REAL, -- 涨跌额
        PCT_CHANGE REAL, -- 涨跌幅
        VOL REAL, -- 成交量
        AMOUNT REAL, -- 成交额
        K_MA30 REAL, --30日均线
        VOL_MA30 REAL, --30日成交量均线
        ADJ_FACTOR REAL, -- 复权因子
        PE REAL, -- 市盈率
        PE_TTM REAL, -- 滚动市盈率
        PB REAL, -- 市净率
        DV REAL, -- 股息率
        DV_TTM REAL, -- 滚动股息率
        PRIMARY KEY (TS_CODE, TRADE_DATE)
    );
    ''',
    'SZ_LIMITS_DETAILS_DAILY': '''
    CREATE TABLE IF NOT EXISTS SZ_LIMITS_DETAILS_DAILY (
        TS_CODE CHAR(9),
        TRADE_DATE CHAR(8),
        FC_RATIO REAL,
        FL_RATIO REAL,
        FD_AMOUNT REAL,
        FIRST_TIME TEXT,
        LAST_TIME TEXT, 
        OPEN_TIMES REAL,
        STRTH REAL, 
        LIMIT_TYPE CHAR(1),
        CON_DAYS REAL,
        PRIMARY KEY (TRADE_DATE, TS_CODE)
    );
    ''',
    'SZ_LIMITS_STATISTIC_DAILY': '''
    CREATE TABLE IF NOT EXISTS SZ_LIMITS_STATISTIC_DAILY (
        TRADE_DATE CHAR(8) PRIMARY KEY,
        UP_LIMITS_AMOUNT REAL,
        DOWN_LIMITS_AMOUNT REAL,
        MAX_UP_LIMIT_CON_DAYS REAL,
        MAX_DOWN_LIMIT_CON_DAYS REAL
    );
    '''
}

TABLES_NEED_FILL = [
    'TRADE_DATE_LIST_DAILY',
    'TRADE_DATE_LIST_WEEKLY',
    'TRADE_DATE_LIST_MONTHLY',
    'SH_STOCK_LIST',
    'SZ_STOCK_LIST',
    'SH_INDEX_DAILY',
    'SH_INDEX_WEEKLY',
    'SH_INDEX_MONTHLY',
    'SH_DETAILS_DAILY',
    'SH_DETAILS_WEEKLY',
    'SH_DETAILS_MONTHLY',
    'SH_LIMITS_DETAILS_DAILY',
    'SZ_INDEX_DAILY',
    'SZ_INDEX_WEEKLY',
    'SZ_INDEX_MONTHLY',
    'SZ_DETAILS_DAILY',
    'SZ_DETAILS_WEEKLY',
    'SZ_DETAILS_MONTHLY',
    'SZ_LIMITS_DETAILS_DAILY',
]

TABLES_NEED_UPDATE_BY_STOCKS = [
    'SH_STOCK_LIST',
    'SZ_STOCK_LIST',
    'SH_DETAILS_DAILY',
    'SH_DETAILS_WEEKLY',
    'SH_DETAILS_MONTHLY',
    'SZ_DETAILS_DAILY',
    'SZ_DETAILS_WEEKLY',
    'SZ_DETAILS_MONTHLY',
]

TABLES_NEED_UPDATE_BY_DATE = [
    'TRADE_DATE_LIST_DAILY',
    'TRADE_DATE_LIST_WEEKLY',
    'TRADE_DATE_LIST_MONTHLY',
    'SH_INDEX_DAILY',
    'SH_INDEX_WEEKLY',
    'SH_INDEX_MONTHLY',
    'SH_DETAILS_DAILY',
    'SH_DETAILS_WEEKLY',
    'SH_DETAILS_MONTHLY',
    'SH_LIMITS_DETAILS_DAILY',
    'SZ_INDEX_DAILY',
    'SZ_INDEX_WEEKLY',
    'SZ_INDEX_MONTHLY',
    'SZ_DETAILS_DAILY',
    'SZ_DETAILS_WEEKLY',
    'SZ_DETAILS_MONTHLY',
    'SZ_LIMITS_DETAILS_DAILY',
]

TABLES_NEED_TRIM_BY_DATE = [
    'TRADE_DATE_LIST_DAILY',
    'TRADE_DATE_LIST_WEEKLY',
    'TRADE_DATE_LIST_MONTHLY',
    'SH_INDEX_DAILY',
    'SH_INDEX_WEEKLY',
    'SH_INDEX_MONTHLY',
    'SH_DETAILS_DAILY',
    'SH_DETAILS_WEEKLY',
    'SH_DETAILS_MONTHLY',
    'SH_LIMITS_DETAILS_DAILY',
    'SZ_INDEX_DAILY',
    'SZ_INDEX_WEEKLY',
    'SZ_INDEX_MONTHLY',
    'SZ_DETAILS_DAILY',
    'SZ_DETAILS_WEEKLY',
    'SZ_DETAILS_MONTHLY',
    'SZ_LIMITS_DETAILS_DAILY',
]

INIT_DATA_SOURCE = {
        'TRADE_DATE_LIST_DAILY': {
            'getter': get_trade_date_list_from_api,
            'getter_args': ['DAILY', DB_SIZE]
        },
        'TRADE_DATE_LIST_WEEKLY': {
            'getter': get_trade_date_list_from_api,
            'getter_args': ['WEEKLY', DB_SIZE]
        },
        'TRADE_DATE_LIST_MONTHLY': {
            'getter': get_trade_date_list_from_api,
            'getter_args': ['MONTHLY', DB_SIZE]
        },
        'SH_STOCK_LIST': {
            'getter': get_stock_list_from_api,
            'getter_args': ['SH']
        },
        'SZ_STOCK_LIST': {
            'getter': get_stock_list_from_api,
            'getter_args': ['SZ']
        },
        'SH_INDEX_DAILY': {
            'getter': get_index_quotation_from_api,
            'getter_args': ['SH', 'DAILY', DB_SIZE]
        },
        'SH_INDEX_WEEKLY': {
            'getter': get_index_quotation_from_api,
            'getter_args': ['SH', 'WEEKLY', DB_SIZE]
        },
        'SH_INDEX_MONTHLY': {
            'getter': get_index_quotation_from_api,
            'getter_args': ['SH', 'MONTHLY', DB_SIZE]
        },
        'SH_DETAILS_DAILY': {
            'getter': get_stocks_details_by_exchange_from_api,
            'getter_args': ['SH', 'DAILY', DB_SIZE]
        },
        'SH_DETAILS_WEEKLY': {
            'getter': get_stocks_details_by_exchange_from_api,
            'getter_args': ['SH', 'WEEKLY', DB_SIZE]
        },
        'SH_DETAILS_MONTHLY': {
            'getter': get_stocks_details_by_exchange_from_api,
            'getter_args': ['SH', 'MONTHLY', DB_SIZE]
        },
        'SH_LIMITS_DETAILS_DAILY': {
            'getter': get_up_down_limits_details_daily_from_api,
            'getter_args': [DB_SIZE, 'SH']
        },
        'SZ_INDEX_DAILY': {
            'getter': get_index_quotation_from_api,
            'getter_args': ['SZ', 'DAILY', DB_SIZE]
        },
        'SZ_INDEX_WEEKLY': {
            'getter': get_index_quotation_from_api,
            'getter_args': ['SZ', 'WEEKLY', DB_SIZE]
        },
        'SZ_INDEX_MONTHLY': {
            'getter': get_index_quotation_from_api,
            'getter_args': ['SZ', 'MONTHLY', DB_SIZE]
        },
        'SZ_DETAILS_DAILY': {
            'getter': get_stocks_details_by_exchange_from_api,
            'getter_args': ['SZ', 'DAILY', DB_SIZE]
        },
        'SZ_DETAILS_WEEKLY': {
            'getter': get_stocks_details_by_exchange_from_api,
            'getter_args': ['SZ', 'WEEKLY', DB_SIZE]
        },
        'SZ_DETAILS_MONTHLY': {
            'getter': get_stocks_details_by_exchange_from_api,
            'getter_args': ['SZ', 'MONTHLY', DB_SIZE]
        },
        'SZ_LIMITS_DETAILS_DAILY': {
            'getter': get_up_down_limits_details_daily_from_api,
            'getter_args': [DB_SIZE, 'SZ']
        },
    }


def update_data_source():
    report = read_cache('update_report')
    return {
        'TRADE_DATE_LIST_DAILY': {
            'update_by_date': {
                'getter': get_trade_date_list_from_api,
                'getter_args': ['DAILY', DB_SIZE]
            }
        },
        'TRADE_DATE_LIST_WEEKLY': {
            'update_by_date': {
                'getter': get_trade_date_list_from_api,
                'getter_args': ['WEEKLY', DB_SIZE]
            }
        },
        'TRADE_DATE_LIST_MONTHLY': {
            'update_by_date': {
                'getter': get_trade_date_list_from_api,
                'getter_args': ['MONTHLY', DB_SIZE]
            }
        },
        'SH_STOCK_LIST': {
            'update_by_date': {
                'getter': get_stock_list_from_api,
                'getter_args': ['SH']
            },
            # TODO: here can use data from report
            'update_by_stocks': {
                'getter': get_stock_list_from_api,
                'getter_args': ['SH']
            }
        },
        'SZ_STOCK_LIST': {
            'update_by_date': {
                'getter': get_stock_list_from_api,
                'getter_args': ['SZ']
            },
            'update_by_stocks': {
                'getter': get_stock_list_from_api,
                'getter_args': ['SH']
            }
        },
        'SH_INDEX_DAILY': {
            'update_by_date': {
                'getter': get_index_quotation_from_api,
                'getter_args': ['SH', 'DAILY', report['daily_update_size']]
            }

        },
        'SH_INDEX_WEEKLY': {
            'update_by_date': {
                'getter': get_index_quotation_from_api,
                'getter_args': ['SH', 'WEEKLY', report['weekly_update_size']]
            }
        },
        'SH_INDEX_MONTHLY': {
            'update_by_date': {
                'getter': get_index_quotation_from_api,
                'getter_args': ['SH', 'MONTHLY', report['monthly_update_size']]
            }
        },
        'SH_DETAILS_DAILY': {
            'update_by_date': {
                'getter': get_stocks_details_by_exchange_from_api,
                'getter_args': ['SH', 'DAILY', report['daily_update_size']]
            },
            'update_by_stocks': {
                'getter': get_stocks_details_by_set_from_api,
                'getter_args': [report['sh_added_stocks'], 'DAILY', DB_SIZE]
            }
        },
        'SH_DETAILS_WEEKLY': {
            'update_by_date': {
                'getter': get_stocks_details_by_exchange_from_api,
                'getter_args': ['SH', 'WEEKLY', report['weekly_update_size']]
            },
            'update_by_stocks': {
                'getter': get_stocks_details_by_set_from_api,
                'getter_args': [report['sh_added_stocks'], 'WEEKLY', DB_SIZE]
            }
        },
        'SH_DETAILS_MONTHLY': {
            'update_by_date': {
                'getter': get_stocks_details_by_exchange_from_api,
                'getter_args': ['SH', 'MONTHLY', report['monthly_update_size']]
            },
            'update_by_stocks': {
                'getter': get_stocks_details_by_set_from_api,
                'getter_args': [report['sh_added_stocks'], 'MONTHLY', DB_SIZE]
            }
        },
        'SH_LIMITS_DETAILS_DAILY': {
            'update_by_date': {
                'getter': get_up_down_limits_details_daily_from_api,
                'getter_args': [report['daily_update_size'], 'SH']
            }
        },
        'SZ_INDEX_DAILY': {
            'update_by_date': {
                'getter': get_index_quotation_from_api,
                'getter_args': ['SZ', 'DAILY', report['daily_update_size']]
            }

        },
        'SZ_INDEX_WEEKLY': {
            'update_by_date': {
                'getter': get_index_quotation_from_api,
                'getter_args': ['SZ', 'WEEKLY', report['weekly_update_size']]
            }
        },
        'SZ_INDEX_MONTHLY': {
            'update_by_date': {
                'getter': get_index_quotation_from_api,
                'getter_args': ['SZ', 'MONTHLY', report['monthly_update_size']]
            }
        },
        'SZ_DETAILS_DAILY': {
            'update_by_date': {
                'getter': get_stocks_details_by_exchange_from_api,
                'getter_args': ['SZ', 'DAILY', report['daily_update_size']]
            },
            'update_by_stocks': {
                'getter': get_stocks_details_by_set_from_api,
                'getter_args': [report['sz_added_stocks'], 'DAILY', DB_SIZE]
            }
        },
        'SZ_DETAILS_WEEKLY': {
            'update_by_date': {
                'getter': get_stocks_details_by_exchange_from_api,
                'getter_args': ['SZ', 'WEEKLY', report['weekly_update_size']]
            },
            'update_by_stocks': {
                'getter': get_stocks_details_by_set_from_api,
                'getter_args': [report['sz_added_stocks'], 'WEEKLY', DB_SIZE]
            }
        },
        'SZ_DETAILS_MONTHLY': {
            'update_by_date': {
                'getter': get_stocks_details_by_exchange_from_api,
                'getter_args': ['SZ', 'MONTHLY', report['monthly_update_size']]
            },
            'update_by_stocks': {
                'getter': get_stocks_details_by_set_from_api,
                'getter_args': [report['sz_added_stocks'], 'MONTHLY', DB_SIZE]
            }
        },
        'SZ_LIMITS_DETAILS_DAILY': {
            'update_by_date': {
                'getter': get_up_down_limits_details_daily_from_api,
                'getter_args': [report['daily_update_size'], 'SZ']
            }
        },
    }


if __name__ == '__main__':
    for table in TABLES_NEED_FILL:
        print(table)