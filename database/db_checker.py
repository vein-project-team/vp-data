from pandas.io.sql import DatabaseError
from database.db_reader import read_from_db


def get_db_status():
    return {
        'latest_trade_date_daily': read_from_db(
            'SELECT TRADE_DATE FROM QUOTATIONS_DAILY ORDER BY TRADE_DATE DESC LIMIT 1;'
        )['TRADE_DATE'][0],
        'latest_trade_date_weekly': read_from_db(
            'SELECT TRADE_dATE FROM QUOTATIONS_WEEKLY ORDER BY TRADE_DATE DESC  LIMIT 1;'
        )['TRADE_DATE'][0],
        'latest_trade_date_monthly': read_from_db(
            'SELECT TRADE_DATE FROM QUOTATIONS_MONTHLY ORDER BY TRADE_DATE DESC  LIMIT 1;'
        )['TRADE_DATE'][0]
    }


def no_checker(table_name):
    return {
        'type': '无检测',
        'pass': False
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
        return {
            'type': '轻检测',
            'pass': flag,
            'records': data['RECORDS'][0]
        }
    except DatabaseError:
        return {
            'type': '轻检测',
            'pass': False,
            'records': 'unknown'
        }



