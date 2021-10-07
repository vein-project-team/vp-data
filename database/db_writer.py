import sqlite3

import pandas as pd

from utils import log
import database.db_checker as dc
from database.date_getter import date_getter as dg
from database.spider import get_stock_list
from database.dispatcher import get_index_quotation_from_api, get_stock_details_from_api
from database.dispatcher import get_up_down_limits_statistic_from_api


def write_to_db(table_name, dataframe):
    log(f'正在填充数据进表：{table_name}')
    conn = sqlite3.connect('vein-project.db')
    cursor = conn.cursor()
    cursor.execute(f'''PRAGMA TABLE_INFO({table_name});''')
    columns = []
    for i in cursor.fetchall():
        columns.append(i[1])
    dataframe.columns = columns
    dataframe.to_sql('TEMP', conn, index=False)
    conn.execute(f'''REPLACE INTO {table_name} SELECT * FROM TEMP;''')
    conn.execute('''DROP TABLE TEMP;''')
    conn.commit()


def fill_tables(size):
    if not dc.check_trade_date_list_table('DAILY'):
        write_to_db('TRADE_DATE_LIST_DAILY', dg.get_trade_date_list_forward('DAILY', size))
    if not dc.check_trade_date_list_table('WEEKLY'):
        write_to_db('TRADE_DATE_LIST_WEEKLY', dg.get_trade_date_list_forward('WEEKLY', size))
    if not dc.check_trade_date_list_table('MONTHLY'):
        write_to_db('TRADE_DATE_LIST_MONTHLY', dg.get_trade_date_list_forward('MONTHLY', size))

    if not dc.check_stock_list_table('SH'):
        write_to_db('SH_STOCK_LIST', get_stock_list('SH'))
    if not dc.check_stock_list_table('SZ'):
        write_to_db('SZ_STOCK_LIST', get_stock_list('SZ'))

    if not dc.check_index_quotation_table('SH', 'DAILY'):
        write_to_db('SH_INDEX_DAILY', get_index_quotation_from_api('000001.SH', 'DAILY', size))
    if not dc.check_index_quotation_table('SH', 'WEEKLY'):
        write_to_db('SH_INDEX_WEEKLY', get_index_quotation_from_api('000001.SH', 'WEEKLY', size))
    if not dc.check_index_quotation_table('SH', 'MONTHLY'):
        write_to_db('SH_INDEX_MONTHLY', get_index_quotation_from_api('000001.SH', 'MONTHLY', size))

    if not dc.check_index_quotation_table('SZ', 'DAILY'):
        write_to_db('SZ_INDEX_DAILY', get_index_quotation_from_api('399001.SZ', 'DAILY', size))
    if not dc.check_index_quotation_table('SZ', 'WEEKLY'):
        write_to_db('SZ_INDEX_WEEKLY', get_index_quotation_from_api('399001.SZ', 'WEEKLY', size))
    if not dc.check_index_quotation_table('SZ', 'MONTHLY'):
        write_to_db('SZ_INDEX_MONTHLY', get_index_quotation_from_api('399001.SZ', 'MONTHLY', size))

    if not dc.check_details_table('SH', 'DAILY'):
        write_to_db('SH_DETAILS_DAILY', get_stock_details_from_api('SH', 'DAILY', size))
    if not dc.check_details_table('SH', 'WEEKLY'):
        write_to_db('SH_DETAILS_WEEKLY', get_stock_details_from_api('SH', 'WEEKLY', size))
    if not dc.check_details_table('SH', 'MONTHLY'):
        write_to_db('SH_DETAILS_MONTHLY', get_stock_details_from_api('SH', 'MONTHLY', size))

    if not dc.check_details_table('SZ', 'DAILY'):
        write_to_db('SZ_DETAILS_DAILY', get_stock_details_from_api('SZ', 'DAILY', size))
    if not dc.check_details_table('SZ', 'WEEKLY'):
        write_to_db('SZ_DETAILS_WEEKLY', get_stock_details_from_api('SZ', 'WEEKLY', size))
    if not dc.check_details_table('SZ', 'MONTHLY'):
        write_to_db('SZ_DETAILS_MONTHLY', get_stock_details_from_api('SZ', 'MONTHLY', size))

    if not dc.check_limits_statistic_details_table('SH'):
        write_to_db('SH_LIMITS_STATISTIC_DETAILS', get_up_down_limits_statistic_from_api(size, 'SH'))
    if not dc.check_limits_statistic_details_table('SZ'):
        write_to_db('SZ_LIMITS_STATISTIC_DETAILS', get_up_down_limits_statistic_from_api(size, 'SZ'))


if __name__ == '__main__':
    pass
