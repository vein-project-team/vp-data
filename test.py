from data_source import tushare_source

if __name__ == '__main__':
    tushare_source.get_stock_list()
    data_test=tushare_source.get_financial_indicators()
    print(data_test)
    