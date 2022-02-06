from data_source import local_source
from data_source import date_getter

def get_daily_report(date='latest'):
    
    if date == 'latest':
      date = date_getter.get_trade_date_before()
      print(date)
        