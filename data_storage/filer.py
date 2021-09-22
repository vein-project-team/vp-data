import sqlite3
import utils
from data_api import spider
from data_handler import calculator
from data_storage import cacher


def fill_trade_date_list_table(days):
    conn = sqlite3.connect('vein-project.db')
    print('检查并填充数据进表：TRADE_DATE_LIST...')
    data = spider.get_trade_date_list(days)
    data = data['cal_date'].tolist()
    for date in data:
        conn.execute(f'''
        INSERT OR IGNORE INTO TRADE_DATE_LIST VALUES (
            {date}
        );
        ''')
    conn.commit()


def fill_index_daily_table(table_name, days):
    conn = sqlite3.connect('vein-project.db')
    print(f'检查并填充数据进表：{table_name}...')
    index_code = '000001.SH' if table_name[:2] == 'SH' else '399001.SZ'
    data = spider.get_index_daily(index_code, days)
    data = data.iloc[::-1]
    date = data['trade_date'].tolist()
    close = utils.round_list(data['close'].tolist())
    oppen = utils.round_list(data['open'].tolist())
    high = utils.round_list(data['high'].tolist())
    low = utils.round_list(data['low'].tolist())
    vol = utils.round_list(data['vol'].tolist())
    ma = calculator.get_index_ma(index_code, days, 30)
    k_ma = utils.round_list(ma['k_ma'])
    vol_ma = utils.round_list(ma['vol_ma'])
    ad_line = utils.round_list(calculator.get_index_AD_line(index_code[-2:], days))
    for i in range(0, len(close)):
        conn.execute(f'''
        INSERT OR IGNORE INTO {table_name} VALUES (
            {date[i]}, {oppen[i]}, {close[i]}, {low[i]}, {high[i]}, {vol[i]}, {k_ma[i]}, {vol_ma[i]}, {ad_line[i]}
        );
        ''')
    conn.commit()


def fill_tables(days):
    if cacher.has_cache('log'):
        print('发现有效的日志缓存，数据初始填充已跳过...')
        return
    else:
        print('正在进行数据初始填充...')
        latest_date = spider.get_latest_finished_trade_date()
        fill_trade_date_list_table(days)
        fill_index_daily_table('SH_INDEX_DAILY', days)
        fill_index_daily_table('SZ_INDEX_DAILY', days)
        print('数据初始填充已完成!')
        cacher.write_cache('log', {
            'latest_date': latest_date
        })


def update_tables():
    cache = cacher.read_cache('log')
    cache_date = cache['latest_date']
    latest_date = spider.get_latest_finished_trade_date()
    if latest_date == cache_date:
        print("数据无需更新!")
        return
    else:
        days = spider.get_trade_days_between(cache_date, latest_date)
        print(f'需要更新 {days} 个交易日的数据...')
        fill_trade_date_list_table(days)
        fill_index_daily_table('SH_INDEX_DAILY', days)
        fill_index_daily_table('SZ_INDEX_DAILY', days)
        cache['latest_date'] = latest_date
        cacher.override_cache('log', cache)
        print('数据更新完成！')


def trim_tables():
    pass


def read_data(sql):
    conn = sqlite3.connect('vein-project.db')
    c = conn.cursor()
    c.execute(sql)
    return list(c.fetchall())


if __name__ == '__main__':
    pass
