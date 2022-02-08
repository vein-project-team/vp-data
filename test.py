from data_source import local_source

if __name__ == '__main__':
    print(local_source.get_quotations_daily(
        condition='TRADE_DATE = "20220208" AND TS_CODE = "603123.SH"'
    ))
    print(local_source.get_stock_list(
        condition='TS_CODE = "603123.SH"'
    ))