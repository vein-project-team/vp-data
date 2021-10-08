import sqlite3
from utils import log
from database.db_checker import light_checker
from dispatcher import get_trade_date_list_from_api
from dispatcher import get_stock_list_from_api
from dispatcher import get_index_quotation_from_api, get_stocks_details_by_exchange_from_api
from dispatcher import get_up_down_limits_statistic_details_from_api
from database.db_settings import DB_SIZE, TABLE_NAMES


def write_to_db(table_name, dataframe):
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


def temp_searcher(table_name:str):
    words = table_name.split('_')
    data = None
    if 'TRADE_DATE_LIST' in table_name:
        data = get_trade_date_list_from_api(words[-1], DB_SIZE)
    elif 'STOCK_LIST' in table_name:
        data = get_stock_list_from_api(words[0])
    elif 'INDEX' in table_name:
        data = get_index_quotation_from_api(words[0], words[-1], DB_SIZE)
    elif 'DETAILS' in table_name:
        data = get_stocks_details_by_exchange_from_api(words[0], words[-1], DB_SIZE)
    elif 'LIMITS_STATISTIC_DETAILS' in table_name:
        data = get_up_down_limits_statistic_details_from_api(DB_SIZE, words[0])
    else:
        pass
    return data


def fill_table(table_name, data_searcher, checker):
    report = checker(table_name)
    if report['pass']:
        log(f'检测到表 {table_name} 包含 {report["records"]} 条数据，初始填充被跳过。')
        return

    dataframe = data_searcher(table_name)
    log(f'正在填充初始数据进表：{table_name}')
    write_to_db(table_name, dataframe)


def fill_tables():
    for table in TABLE_NAMES:
        fill_table(table, temp_searcher, light_checker)

    # for table in TABLE_NAMES:
    #     fill_table(table, temp_searcher, date_checker)

    # TODO SH_LIMITS_STATISTIC
    # TODO SH_LIMITS_STATISTIC




if __name__ == '__main__':
    temp_searcher('SH_INDEX_DAILY')
