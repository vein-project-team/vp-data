from data_source import local_source

if __name__ == '__main__':
    print(local_source.get_quotations_daily(
        cols='TS_CODE, CHANGE',
        condition='TRADE_DATE = "20220207" ORDER BY CHANGE DESC'
    ))
    print(local_source.get_stock_list(
        condition='TS_CODE = "688696.SH"'
    ))