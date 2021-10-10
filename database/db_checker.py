from pandas.io.sql import DatabaseError
from data_api.date_getter import date_getter as dg
from database.db_reader import read_from_db
from cacher import read_cache
from utils import log


def no_checker(table_name):
    log(f'{table_name} 将在不做任何检测的情况下更新。')
    return {
        'pass': False,
        'records': '*'
    }


def light_checker(table_name):
    """
    检测到该涨表存在且表中记录大于零条
    就算做检测通过
    :param table_name: 表名
    :return: 一个包含状态信息的report json
    """
    try:
        data = read_from_db(f'''SELECT COUNT(*) AS RECORDS FROM {table_name};''')
        flag = data['RECORDS'][0] > 0
        if flag:
            log(f'来自轻检测：{table_name} 不需要更新。')
        return {
            'pass': flag,
            'records': data['RECORDS'][0]
        }
    except DatabaseError:
        log(f'来自轻检测：{table_name} 在检测过程中遇到问题，将尝试更新。')
        return {
            'pass': False,
            'records': -1
        }


def date_checker(table_name):
    date = ''
    report = read_cache('update_report')
    if 'DAILY' in table_name or 'LIMITS' in table_name:
        date = report['new_daily_end_date']
    elif 'WEEKLY' in table_name:
        date = report['new_weekly_end_date']
    elif 'MONTHLY' in table_name:
        date = report['new_monthly_end_date']
    try:
        data = read_from_db(f'''SELECT COUNT(*) AS RECORDS FROM {table_name} WHERE TRADE_DATE = {date};''')
        flag = data['RECORDS'][0] > 0
        if flag:
            log(f'来自日期检测：{table_name} 不需要更新。')
        return {
            'pass': flag,
            'records': '*'
        }
    except DatabaseError:
        log(f'来自日期检测：{table_name} 在检测过程中遇到问题，将尝试更新。')
        return {
            'pass': False,
            'records': -1
        }


def stocks_checker(table_name):
    stocks = []
    report = read_cache('update_report')
    if 'SH' in table_name:
        stocks = report['sh_added_stocks']
    elif 'SZ' in table_name:
        stocks = report['sz_added_stocks']
    try:
        stocks_from_db = read_from_db(f'SELECT DISTINCT TS_CODE FROM {table_name};')['TS_CODE'].tolist()
        flag = set(stocks_from_db) <= set(stocks_from_db)
        if flag:
            log(f'来自股票列表检测：{table_name} 不需要更新。')
        return {
            'pass': flag,
            'records': '*'
        }
    except DatabaseError:
        log(f'来自日期检测：{table_name} 在检测过程中遇到问题，将尝试更新。')
        return {
            'pass': False,
            'records': '*'
        }



