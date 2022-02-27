from analysis.ReportGener import ReportGener

from data_source import local_source
from data_source import date_getter

class StockListGener(ReportGener):

  folder_name = 'stock-lists'


  def __init__(self) -> None:
      super().__init__()


  def gen_mainboard_stocks(self):
    filename = 'mainboard-stocks'
    trade_date = date_getter.get_trade_date_before()
    data = local_source.get_stock_list(
      cols='''
      TS_CODE, NAME, CNSPELL, AREA, INDUSTRY
      ''',
      condition=f'''
      MARKET = '主板' AND STATUS = 'L' AND LIST_DATE < '{ trade_date }' ORDER BY TS_CODE
      '''
    )
    self.store(data, filename, override=True)
    return data


  def gen_no_st_mainboard_stocks(self):
    filename = 'no-st-mainboard-stocks'
    trade_date = date_getter.get_trade_date_before()
    data = local_source.get_stock_list(
      cols='''
      TS_CODE, NAME, CNSPELL, AREA, INDUSTRY
      ''',
      condition=f'''
      MARKET = '主板' AND NAME NOT LIKE '%ST%' AND STATUS = 'L' AND LIST_DATE < '{ trade_date }' ORDER BY TS_CODE
      '''
    )
    self.store(data, filename, override=True)
    return data


  def gen(self):
    pass

stock_list_gener = StockListGener()