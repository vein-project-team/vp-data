import sqlite3
import os

from data_storage import filer

if not os.path.exists('vein-project.db'):
    print('正在创建数据库...')
else:
    print('数据库已存在...')
conn = sqlite3.connect('vein-project.db')


def db_init():
    create_trade_date_list_table()
    create_index_daily_table('SH_INDEX_DAILY')
    create_index_daily_table('SZ_INDEX_DAILY')
    filer.fill_tables(200)
    filer.update_tables()


def create_trade_date_list_table():
    print(f'检查或创建表：TRADE_DATE_LIST...')
    conn.execute('''
    CREATE TABLE IF NOT EXISTS TRADE_DATE_LIST (
        TRADE_DATE CHAR(8) PRIMARY KEY
    );
    ''')


def create_index_daily_table(table_name):
    print(f'检查或创建表：{table_name}...')
    conn.execute(f'''
    CREATE TABLE IF NOT EXISTS {table_name} (
        DATE CHAR(8) PRIMARY KEY,
        OPEN REAL,
        CLOSE REAL,
        LOW REAL,
        HIGH REAL,
        VOL REAL,
        K_MA30 REAL,
        VOL_MA30 REAL,
        AD_LINE REAL
    );
    ''')


def complete():
    print(f'数据库初始化完成！')
    conn.close()
