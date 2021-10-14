import sqlite3
from database.db_settings import TABLES_NEED_TRIM_BY_DATE
from cacher import read_cache
from utils import log


def trim_table(table_name):
    conn = sqlite3.connect('vein-project.db')
    report = read_cache('update_report')
    trim_before_date = None
    if 'DAILY' in table_name:
        trim_before_date = report['new_daily_start_date']
    elif 'WEEKLY' in table_name:
        trim_before_date = report['new_weekly_start_date']
    elif 'MONTHLY' in table_name:
        trim_before_date = report['new_monthly_start_date']
    log(f'正在裁剪表 {table_name}...')
    conn.execute(f'''
    DELETE FROM {table_name} WHERE TRADE_DATE < {trim_before_date};
    ''')
    conn.commit()


def trim_tables():
    log('准备裁剪数据库...')
    for table in TABLES_NEED_TRIM_BY_DATE:
        trim_table(table)
    log('数据库裁剪完成。')
