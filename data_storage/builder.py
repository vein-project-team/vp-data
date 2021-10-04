import sqlite3
import os

from utils import log
from data_storage import filer

if not os.path.exists('vein-project.db'):
    log('正在创建数据库...')
else:
    log('数据库已存在...')


def db_init(db_size):
    log(f'当前数据库大小设定为：{db_size}')
    create_index_quotation_table('SH_INDEX_DAILY')
    create_index_quotation_table('SZ_INDEX_DAILY')
    create_index_quotation_table('SH_INDEX_WEEKLY')
    create_index_quotation_table('SZ_INDEX_WEEKLY')
    create_index_quotation_table('SH_INDEX_MONTHLY')
    create_index_quotation_table('SZ_INDEX_MONTHLY')
    create_limits_statistic_table()
    filer.fill_tables(db_size)
    filer.update_tables()
    filer.trim_tables(db_size)


def create_index_quotation_table(table_name):
    conn = sqlite3.connect('vein-project.db')
    log(f'检查或创建表：{table_name}...')
    conn.execute(f'''
    CREATE TABLE IF NOT EXISTS {table_name} (
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
    ''')


def create_limits_statistic_table():
    conn = sqlite3.connect('vein-project.db')
    log(f'检查或创建表：LIMITS_STATISTIC...')
    conn.execute(f'''
        CREATE TABLE IF NOT EXISTS LIMITS_STATISTIC (
            TRADE_DATE CHAR(8),
            TS_CODE CHAR(9),
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
        ''')


def complete():
    log(f'数据库初始化完成！')


if __name__ == '__main__':
    create_limits_statistic_table()