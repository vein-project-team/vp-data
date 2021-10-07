import sqlite3
import os
from utils import log


def db_init(db_size):
    if not os.path.exists('vein-project.db'):
        log('正在创建数据库...')
    else:
        log('数据库已存在...')
    log(f'当前数据库大小设定为：{db_size}')

    create_trade_date_list_table('DAILY')
    create_trade_date_list_table('WEEKLY')
    create_trade_date_list_table('MONTHLY')

    create_stock_list_table('SH')
    create_stock_list_table('SZ')

    create_index_quotation_table('SH', 'DAILY')
    create_index_quotation_table('SH', 'WEEKLY')
    create_index_quotation_table('SH', 'MONTHLY')
    create_limits_statistic_table('SH')
    create_limits_statistic_details_table('SH')

    create_index_quotation_table('SZ', 'DAILY')
    create_index_quotation_table('SZ', 'WEEKLY')
    create_index_quotation_table('SZ', 'MONTHLY')
    create_limits_statistic_table('SZ')
    create_limits_statistic_details_table('SZ')

    create_details_table('SH', 'DAILY')
    create_details_table('SH', 'WEEKLY')
    create_details_table('SH', 'MONTHLY')
    create_details_table('SZ', 'DAILY')
    create_details_table('SZ', 'WEEKLY')
    create_details_table('SZ', 'MONTHLY')

    log(f'数据库初始化完成！')


def create_trade_date_list_table(frequency):
    conn = sqlite3.connect('vein-project.db')
    log(f'检查或创建表：TRADE_DATE_LIST_{frequency}...')
    conn.execute(f'''
    CREATE TABLE IF NOT EXISTS TRADE_DATE_LIST_{frequency} (
        TRADE_DATE CHAR(8) PRIMARY KEY
    );
    ''')


def create_stock_list_table(index_suffix):
    conn = sqlite3.connect('vein-project.db')
    conn.execute(f'''
    CREATE TABLE IF NOT EXISTS {index_suffix}_STOCK_LIST (
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
    ''')


def create_index_quotation_table(index_suffix, frequency):
    conn = sqlite3.connect('vein-project.db')
    log(f'检查或创建表：{index_suffix}_INDEX_{frequency}...')
    conn.execute(f'''
    CREATE TABLE IF NOT EXISTS {index_suffix}_INDEX_{frequency} (
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


def create_details_table(index_suffix, frequency):
    conn = sqlite3.connect('vein-project.db')
    log(f'检查或创建表：{index_suffix}_DETAILS_{frequency}...')
    conn.execute(f'''
    CREATE TABLE IF NOT EXISTS {index_suffix}_DETAILS_{frequency} (
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
    ''')


def create_limits_statistic_table(index_suffix):
    conn = sqlite3.connect('vein-project.db')
    log(f'检查或创建表：{index_suffix}_LIMITS_STATISTIC...')
    conn.execute(f'''
    CREATE TABLE IF NOT EXISTS {index_suffix}_LIMITS_STATISTIC (
        TRADE_DATE CHAR(8) PRIMARY KEY,
        UP_LIMITS_AMOUNT REAL,
        DOWN_LIMITS_AMOUNT REAL,
        MAX_UP_LIMIT_CON_DAYS REAL,
        MAX_DOWN_LIMIT_CON_DAYS REAL
    );
    ''')


def create_limits_statistic_details_table(index_suffix):
    conn = sqlite3.connect('vein-project.db')
    log(f'检查或创建表：{index_suffix}_LIMITS_STATISTIC_DETAILS...')
    conn.execute(f'''
    CREATE TABLE IF NOT EXISTS {index_suffix}_LIMITS_STATISTIC_DETAILS (
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
    ''')


if __name__ == '__main__':
    db_init(120)
