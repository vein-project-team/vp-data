from database.db_runner import setup
from database.dispatcher import get_index_quotation_from_db, get_stock_details_from_db
from database.dispatcher import get_trade_date_list_from_db
import time
import pandas as pd

if __name__ == '__main__':
    pass
    # print(get_trade_date_list_from_db('MONTHLY'))
    print(get_stock_details_from_db('003039.SZ', 'DAILY'))
    print(get_stock_details_from_db('003039.SZ', 'WEEKLY'))
    print(get_stock_details_from_db('003039.SZ', 'MONTHLY'))
    # print(get_index_quotation_from_db('SH', 'DAILY', 3))
    # while True:
    #     try:
    #         setup()
    #         break
    #     except ValueError:
    #         continue
    #     except ConnectionError:
    #         continue




