from data_source import tushare_source
from database.db_checker import no_checker, light_checker

DATA_SOURCE = {
    'INDEX_LIST': {
        'checker': no_checker,
        'getter': tushare_source.get_index_list,
        'args': []
    },
    'STOCK_LIST': {
        'checker': no_checker,
        'getter': tushare_source.get_stock_list,
        'args': []
    },
    'INDICES_DAILY': {
        'checker': no_checker,
        'getter': tushare_source.get_indices_daily,
        'args': []
    },
    'INDICES_WEEKLY': {
        'checker': no_checker,
        'getter': tushare_source.get_indices_weekly,
        'args': []
    },
    'INDICES_MONTHLY': {
        'checker': no_checker,
        'getter': tushare_source.get_indices_monthly,
        'args': []
    },
    'QUOTATIONS_DAILY': {
        'checker': light_checker,
        'getter': tushare_source.get_quotations_daily,
        'args': []
    },
    'QUOTATIONS_WEEKLY': {
        'checker': light_checker,
        'getter': tushare_source.get_quotations_weekly,
        'args': []
    },
    'QUOTATIONS_MONTHLY': {
        'checker': light_checker,
        'getter': tushare_source.get_quotations_monthly,
        'args': []
    },
    'LIMITS_STATISTIC': {
        'checker': light_checker,
        'getter': tushare_source.get_limits_statistic,
        'args': []
    },
    'ADJ_FACTORS': {
        'checker': light_checker,
        'getter': tushare_source.get_adj_factors,
        'args': []
    },
    "INCOME_STATEMENTS": {
        'checker': light_checker,
        'getter': tushare_source.get_income_statements,
        'args': []
    }
}
