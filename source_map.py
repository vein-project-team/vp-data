from data_source import tushare_source
from database.db_checker import date_checker, manual_checker, no_checker, light_checker

DATA_SOURCE = {
    'STOCK_LIST': {
        'getter': tushare_source.get_stock_list,
        'checker': no_checker,
        'args': []
    },
    'INDICES_DAILY': {
        'getter': tushare_source.get_indices_daily,
        'checker': no_checker,
        'args': []
    },
    'INDICES_WEEKLY': {
        'getter': tushare_source.get_indices_weekly,
        'checker': no_checker,
        'args': []
    },
    'INDICES_MONTHLY': {
        'getter': tushare_source.get_indices_monthly,
        'checker': no_checker,
        'args': []
    },
    'QUOTATIONS_DAILY': {
        'getter': tushare_source.get_quotations_daily,
        'checker': date_checker,
        'args': []
    },
    'QUOTATIONS_WEEKLY': {
        'getter': tushare_source.get_quotations_weekly,
        'checker': date_checker,
        'args': ['weekly']
    },
    'QUOTATIONS_MONTHLY': {
        'getter': tushare_source.get_quotations_monthly,
        'checker': date_checker,
        'args': ['monthly']
    },
    'STOCK_INDICATORS_DAILY': {
        'getter': tushare_source.get_stock_indicators_daily,
        'checker': date_checker,
        'args': []
    },
    'LIMITS_STATISTIC': {
        'getter': tushare_source.get_limits_statistic,
        'checker': date_checker,
        'args': []
    },
    'ADJ_FACTORS': {
        'getter': tushare_source.get_adj_factors,
        'checker': date_checker,
        'args': []
    },
    "INCOME_STATEMENTS": {
        'getter': tushare_source.get_income_statements,
        'checker': date_checker,
        'args': ['quarter', 'END_DATE']
    },
    "BALANCE_SHEETS": {
        'getter': tushare_source.get_balance_sheets,
        'checker': date_checker,
        'args': ['quarter', 'END_DATE']
    },
    "STATEMENTS_OF_CASH_FLOWS": {
        'getter': tushare_source.get_cash_flows,
        'checker': date_checker,
        'args': ['quarter', 'END_DATE']
    },
    "INCOME_FORECASTS": {
        'getter': tushare_source.get_income_forecasts,
        'checker': date_checker,
        'args': ['quarter', 'END_DATE']
    },
    "FINANCIAL_INDICATORS": {
        'getter': tushare_source.get_financial_indicators,
        'checker': date_checker,
        'args': ['quarter', 'END_DATE']
    }
    
}
