import sqlite3
from utils import log, round_list
from data_api import spider
from data_handler import calculator
from data_storage import cacher


def fill_trade_date_list_table(days):
    conn = sqlite3.connect('vein-project.db')
    log('检查并填充数据进表：TRADE_DATE_LIST...')
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
    log(f'检查并填充数据进表：{table_name}...')
    index_code = '000001.SH' if table_name[:2] == 'SH' else '399001.SZ'
    data = spider.get_index_daily(index_code, days)
    data = data.iloc[::-1]
    date = data['trade_date'].tolist()
    close = round_list(data['close'].tolist())
    oppen = round_list(data['open'].tolist())
    high = round_list(data['high'].tolist())
    low = round_list(data['low'].tolist())
    vol = round_list(data['vol'].tolist())
    ma = calculator.get_index_ma(index_code, days, 30)
    k_ma = round_list(ma['k_ma'])
    vol_ma = round_list(ma['vol_ma'])
    ad_line = round_list(calculator.get_index_AD_line(index_code[-2:], days))
    for i in range(0, len(close)):
        conn.execute(f'''
        INSERT OR IGNORE INTO {table_name} VALUES (
            {date[i]}, {oppen[i]}, {close[i]}, {low[i]}, {high[i]}, {vol[i]}, {k_ma[i]}, {vol_ma[i]}, {ad_line[i]}
        );
        ''')
    conn.commit()


def fill_tables(days):
    if cacher.has_cache('log'):
        log('发现有效的日志缓存，数据初始填充已跳过...')
        return
    else:
        log('正在进行数据初始填充...')
        latest_date = spider.get_latest_finished_trade_date()
        fill_trade_date_list_table(days)
        fill_index_daily_table('SH_INDEX_DAILY', days)
        fill_index_daily_table('SZ_INDEX_DAILY', days)
        log('数据初始填充已完成!')
        cacher.write_cache('log', {
            'latest_date': latest_date
        })


def update_tables():
    cache = cacher.read_cache('log')
    cache_date = cache['latest_date']
    latest_date = spider.get_latest_finished_trade_date()
    if latest_date == cache_date:
        log("数据无需更新!")
        return
    else:
        days = spider.get_trade_days_between(cache_date, latest_date) - 1
        log(f'需要更新 {days} 个交易日的数据...')
        fill_trade_date_list_table(days)
        fill_index_daily_table('SH_INDEX_DAILY', days)
        fill_index_daily_table('SZ_INDEX_DAILY', days)
        cache['latest_date'] = latest_date
        cacher.override_cache('log', cache)
        log('数据更新完成！')


def trim_trade_date_list_table(keep_records):
    conn = sqlite3.connect('vein-project.db')
    c = conn.cursor()
    c.execute('''
    SELECT COUNT(*) FROM TRADE_DATE_LIST;
    ''')
    total_records = c.fetchone()[0]
    need_trim_records = total_records - keep_records
    if need_trim_records > 0:
        log(f'交易日历表中当前有 {total_records} 条数据， 需裁剪掉前 {need_trim_records} 条...')
        c.execute(f'''
        DELETE FROM TRADE_DATE_LIST WHERE TRADE_DATE IN (SELECT TRADE_DATE FROM TRADE_DATE_LIST LIMIT {need_trim_records});
        ''')
        conn.commit()


def trim_index_daily_table(table_name, keep_records):
    if keep_records < 1:
        return
    conn = sqlite3.connect('vein-project.db')
    c = conn.cursor()
    c.execute(f'''
        SELECT COUNT(*) FROM {table_name};
    ''')
    total_records = c.fetchone()[0]
    need_trim_records = total_records - keep_records
    if need_trim_records > 0:
        log(f'表 {table_name} 中当前有 {total_records} 条数据， 需裁剪掉前 {need_trim_records} 条...')
        ad_line_offset = c.execute(f'''
            SELECT AD_LINE FROM {table_name} WHERE TRADE_DATE IN (SELECT TRADE_DATE FROM {table_name} LIMIT {need_trim_records}, {need_trim_records+1});
        ''').fetchone()
        c.execute(f'''
            UPDATE {table_name} SET AD_LINE = AD_LINE - {ad_line_offset[0]}
        ''')
        c.execute(f'''
           DELETE FROM {table_name} WHERE TRADE_DATE IN (SELECT TRADE_DATE FROM {table_name} LIMIT {need_trim_records});
        ''')
        conn.commit()


def trim_tables(keep_records):
    log('正在裁剪数据表...')
    trim_trade_date_list_table(keep_records)
    trim_index_daily_table('SH_INDEX_DAILY', keep_records)
    trim_index_daily_table('SZ_INDEX_DAILY', keep_records)
    log('数据裁剪完成！')


def read_data(sql):
    conn = sqlite3.connect('vein-project.db')
    c = conn.cursor()
    c.execute(sql)
    return list(c.fetchall())


if __name__ == '__main__':
    pass
