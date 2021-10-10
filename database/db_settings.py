from dispatcher import get_trade_date_list_from_api, get_stocks_details_by_set_from_api
from dispatcher import get_stock_list_from_api
from dispatcher import get_index_quotation_from_api
from dispatcher import get_stocks_details_by_exchange_from_api
from dispatcher import get_up_down_limits_statistic_details_from_api
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
    'SH_LIMITS_STATISTIC_DETAILS',
    'SZ_INDEX_DAILY',
    'SZ_INDEX_WEEKLY',
    'SZ_INDEX_MONTHLY',
    'SZ_DETAILS_DAILY',
    'SZ_DETAILS_WEEKLY',
    'SZ_DETAILS_MONTHLY',
    'SZ_LIMITS_STATISTIC_DETAILS',
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
    'SH_LIMITS_STATISTIC_DETAILS',
    'SZ_INDEX_DAILY',
    'SZ_INDEX_WEEKLY',
    'SZ_INDEX_MONTHLY',
    'SZ_DETAILS_DAILY',
    'SZ_DETAILS_WEEKLY',
    'SZ_DETAILS_MONTHLY',
    'SZ_LIMITS_STATISTIC_DETAILS',
]


def init_data_source():
    return {
        'TRADE_DATE_LIST_DAILY': {
            'func': get_trade_date_list_from_api,
            'args': ['DAILY', DB_SIZE]
        },
        'TRADE_DATE_LIST_WEEKLY': {
            'func': get_trade_date_list_from_api,
            'args': ['WEEKLY', DB_SIZE]
        },
        'TRADE_DATE_LIST_MONTHLY': {
            'func': get_trade_date_list_from_api,
            'args': ['MONTHLY', DB_SIZE]
        },
        'SH_STOCK_LIST': {
            'func': get_stock_list_from_api,
            'args': ['SH']
        },
        'SZ_STOCK_LIST': {
            'func': get_stock_list_from_api,
            'args': ['SZ']
        },
        'SH_INDEX_DAILY': {
            'func': get_index_quotation_from_api,
            'args': ['SH', 'DAILY', DB_SIZE]
        },
        'SH_INDEX_WEEKLY': {
            'func': get_index_quotation_from_api,
            'args': ['SH', 'WEEKLY', DB_SIZE]
        },
        'SH_INDEX_MONTHLY': {
            'func': get_index_quotation_from_api,
            'args': ['SH', 'MONTHLY', DB_SIZE]
        },
        'SH_DETAILS_DAILY': {
            'func': get_stocks_details_by_exchange_from_api,
            'args': ['SH', 'DAILY', DB_SIZE]
        },
        'SH_DETAILS_WEEKLY': {
            'func': get_stocks_details_by_exchange_from_api,
            'args': ['SH', 'WEEKLY', DB_SIZE]
        },
        'SH_DETAILS_MONTHLY': {
            'func': get_stocks_details_by_exchange_from_api,
            'args': ['SH', 'MONTHLY', DB_SIZE]
        },
        'SH_LIMITS_STATISTIC_DETAILS': {
            'func': get_up_down_limits_statistic_details_from_api,
            'args': [DB_SIZE, 'SH']
        },
        'SZ_INDEX_DAILY': {
            'func': get_index_quotation_from_api,
            'args': ['SZ', 'DAILY', DB_SIZE]
        },
        'SZ_INDEX_WEEKLY': {
            'func': get_index_quotation_from_api,
            'args': ['SZ', 'WEEKLY', DB_SIZE]
        },
        'SZ_INDEX_MONTHLY': {
            'func': get_index_quotation_from_api,
            'args': ['SZ', 'MONTHLY', DB_SIZE]
        },
        'SZ_DETAILS_DAILY': {
            'func': get_stocks_details_by_exchange_from_api,
            'args': ['SZ', 'DAILY', DB_SIZE]
        },
        'SZ_DETAILS_WEEKLY': {
            'func': get_stocks_details_by_exchange_from_api,
            'args': ['SZ', 'WEEKLY', DB_SIZE]
        },
        'SZ_DETAILS_MONTHLY': {
            'func': get_stocks_details_by_exchange_from_api,
            'args': ['SZ', 'MONTHLY', DB_SIZE]
        },
        'SZ_LIMITS_STATISTIC_DETAILS': {
            'func': get_up_down_limits_statistic_details_from_api,
            'args': [DB_SIZE, 'SZ']
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
        'SH_LIMITS_STATISTIC_DETAILS': {
            'update_by_date': {
                'getter': get_up_down_limits_statistic_details_from_api,
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
        'SZ_LIMITS_STATISTIC_DETAILS': {
            'update_by_date': {
                'getter': get_up_down_limits_statistic_details_from_api,
                'getter_args': [report['daily_update_size'], 'SZ']
            }
        },
    }


CREATION_SQL = []
