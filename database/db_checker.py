from utils import log

from database.db_reader import read_from_db
from database.date_getter import date_getter as dg


def light_check(func, table_name):
    pass


def check_trade_date_list_table(frequency):
    end_date = dg.get_trade_date_before(1, frequency=frequency)
    data = read_from_db(f'''SELECT * FROM TRADE_DATE_LIST_{frequency} WHERE TRADE_DATE = {end_date}''')
    if len(data) != 0:
        log(f'检查到表 TRADE_DATE_LIST_{frequency} 包含最新数据，无需更新。')
        return True
    else:
        log(f'表 TRADE_DATE_LIST_{frequency} 未包含最新数据，需要更新。')
        return False


def check_stock_list_table(index_suffix):
    data = read_from_db(f'''SELECT * FROM {index_suffix}_STOCK_LIST;''')
    if len(data) != 0:
        log(f'检查到表 {index_suffix}_STOCK_LIST 包含最新数据，无需更新。')
        return True
    else:
        log(f'表 {index_suffix}_STOCK_LIST 未包含最新数据，需要更新。')
        return False


def check_details_table(index_suffix, frequency):
    data = read_from_db(f'''SELECT TS_CODE FROM {index_suffix}_DETAILS_{frequency};''')
    if len(data) != 0:
        log(f'检查到表 {index_suffix}_DETAILS_{frequency} 包含最新数据，无需更新。')
        return True
    else:
        log(f'表 {index_suffix}_DETAILS_{frequency} 未包含最新数据，需要更新。')
        return False


def check_index_quotation_table(index_suffix, frequency):
    end_date = dg.get_trade_date_before(1, frequency=frequency)
    data = read_from_db(f'''SELECT * FROM {index_suffix}_INDEX_{frequency} WHERE TRADE_DATE = {end_date}''')
    if len(data) != 0:
        log(f'检查到表 {index_suffix}_INDEX_{frequency} 包含最新数据，无需更新。')
        return True
    else:
        log(f'表 {index_suffix}_INDEX_{frequency} 未包含最新数据，需要更新。')
        return False


def check_limits_statistic_details_table(index_suffix):
    end_date = dg.get_trade_date_before(1)
    data = read_from_db(f'''SELECT * FROM {index_suffix}_LIMITS_STATISTIC_DETAILS WHERE TRADE_DATE = {end_date}''')
    if len(data) != 0:
        log(f'检查到表 {index_suffix}_LIMITS_STATISTIC_DETAILS 包含最新数据，无需更新。')
        return True
    else:
        log(f'表 {index_suffix}_LIMITS_STATISTIC_DETAILS 未包含最新数据，需要更新。')
        return False


if __name__ == '__main__':
    check_trade_date_list_table('DAILY')
    check_trade_date_list_table('WEEKLY')
    check_trade_date_list_table('MONTHLY')
    check_index_quotation_table('SH', 'DAILY')
    check_index_quotation_table('SH', 'WEEKLY')
    check_index_quotation_table('SH', 'MONTHLY')
    check_index_quotation_table('SZ', 'DAILY')
    check_index_quotation_table('SZ', 'WEEKLY')
    check_index_quotation_table('SZ', 'MONTHLY')
    check_limits_statistic_details_table('SH')
    check_limits_statistic_details_table('SZ')
