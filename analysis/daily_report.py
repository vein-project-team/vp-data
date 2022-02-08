from data_source import local_source
from data_source import date_getter
from tqdm import tqdm as pb

def gen_condition(date, goe, l):
    return f'''
            TRADE_DATE = {date} AND 
            NOT TS_CODE LIKE '3%' AND 
            NOT TS_CODE LIKE '8%' AND 
            NOT TS_CODE LIKE '68%' AND 
            (CLOSE - PRE_CLOSE) / PRE_CLOSE >= {goe} AND 
            (CLOSE - PRE_CLOSE) / PRE_CLOSE < {l}
            '''

def get_daily_report(date='latest'):
    
    if date == 'latest':
        date = date_getter.get_trade_date_before()

    # data = {
    #     '( ,-10%)': local_source.get_quotations_daily(
    #         cols='TS_CODE, CLOSE, PRE_CLOSE, ((CLOSE - PRE_CLOSE) / PRE_CLOSE)',
    #         condition=f'''
    #         TRADE_DATE = {date} AND 
    #         NOT TS_CODE LIKE '3%' AND 
    #         NOT TS_CODE LIKE '8%' AND 
    #         NOT TS_CODE LIKE '68%' AND 
    #         (CLOSE - PRE_CLOSE) / PRE_CLOSE < -0.1
    #         '''
    #     )
    # }
    data = dict()
    for i in  pb(range(-11, 11, 1), desc='正在生成报告', colour='#ffffff'):
        raw = local_source.get_quotations_daily(
            cols='TS_CODE, CLOSE, PRE_CLOSE, ((CLOSE - PRE_CLOSE) / PRE_CLOSE)',
            condition=gen_condition(date, i/100, (i+1)/100)
        )
        data[f'[{i}%, {i + 1}%)'] = {
            'raw': raw,
            'count': len(raw)
        }

    total = 0
    for k, v in data.items():
        total += v['count']
        print(k, v['count'])
    print(total)