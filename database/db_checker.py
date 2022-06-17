from database.db_reader import read_from_db, check_table_exist, check_table_not_empty
from database import date_getter

def get_standard_latest_trade_date(frequency='daily'):
    if frequency == 'quarter':
        return date_getter.get_quarter_end_date_before()
    else:
        return date_getter.get_trade_date_before(frequency=frequency.upper())


def get_latest_trade_date_from_table(table_name, column_name='TRADE_DATE'):
    return read_from_db(
        f'''SELECT {column_name} FROM {table_name} ORDER BY {column_name} DESC LIMIT 1;'''
    )[column_name][0]


def no_checker(table_name):
    return {
        'type': '无检测',
        'need_fill': True,
        'fill_controller': {}
    }


def light_checker(table_name):
    """
    检测到该涨表存在且表中记录大于零条
    就算做检测通过
    :param table_name: 表名
    :return: 一个包含状态信息的report json
    """
    if not check_table_exist(table_name):
        return {
            'type': '轻检测',
            'need_fill': False,
            'fill_controller': {}
        }
    if check_table_not_empty(table_name):
        return {
            'type': '轻检测',
            'need_fill': False,
            'fill_controller': {}
        }
    else:
        return {
            'type': '轻检测',
            'need_fill': True,
            'fill_controller': {}
        }


def date_checker(table_name, frequency='daily', column_name='TRADE_DATE'):
    if not check_table_not_empty(table_name):
        return {
            'type': '日期检测',
            'need_fill': True,
            'fill_controller': {}
        }
    table_latest_date = get_latest_trade_date_from_table(table_name, column_name)
    standard_latest_date = get_standard_latest_trade_date(frequency)
    passed = table_latest_date == standard_latest_date
    if passed == True:
        return {
            'type': '日期检测',
            'need_fill': False,
            'fill_controller': {
                'latest_date': table_latest_date
            }
        }
    else: 
        return {
            'type': '日期检测',
            'need_fill': True,
            'fill_controller': {
                'latest_date': table_latest_date
            }
        }


def manual_checker(table_name):
    while True:
        print(f'是否要更新表 {table_name} ? (y/n)', end=': ')
        user_resp = input().lower()
        if user_resp == 'y':
            print(f'请输入一个早于上次更新时间的日期 (yyyymmdd)', end=': ')
            latest_date = input()
            return {
                'type': '手动检测',
                'need_fill': True,
                'fill_controller': {
                    'latest_date': latest_date
                }
            }
        elif user_resp == 'n':
            return {
                'type': '手动检测',
                'need_fill': False,
                'fill_controller': {}
            }
        else: pass