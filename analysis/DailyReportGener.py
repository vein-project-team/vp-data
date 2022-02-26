from cgi import test
from pandas import DataFrame
from analysis.ReportGener import ReportGener
from analysis.StockListGener import stock_list_gener

import pandas as pd
from data_source import local_source
from data_source import date_getter

class DailyReportGener(ReportGener):

  folder_name = 'daily-reports'
  stock_list = DataFrame()


  def __init__(self, scope="default") -> None:
      super().__init__()
      if scope == 'default':
        self.stock_list = stock_list_gener.gen_no_st_mainboard_stocks()


  def gen_up_down_rank(self, date='latest') -> DataFrame:
    trade_date = date_getter.get_trade_date_before()
    if date != 'latest':
      trade_date = date
    filename = f'{trade_date}-up-down-rank'
    if (data := self.fetch(filename)) is not None:
      return data
    data = local_source.get_quotations_daily(
      cols='''
      TS_CODE, OPEN, CLOSE, LOW, HIGH, PRE_CLOSE, ROUND((CLOSE-PRE_CLOSE) / PRE_CLOSE, 2) AS PCT_CHANGE
      ''',
      condition=f'''
      TRADE_DATE = {trade_date} ORDER BY PCT_CHANGE DESC
      '''
    )

    t = data['TS_CODE'].isin(self.stock_list['TS_CODE'])
    data = data[t].reset_index(drop=True)

    self.store(data, filename)
    return data


  def gen_up_down_aggregation(self, date='latest') -> DataFrame:
    trade_date = date_getter.get_trade_date_before()
    if date != 'latest':
      trade_date = date
    filename = f'{trade_date}-up-down-aggregation'
    if data := self.fetch(filename) is not None:
      return data

    quotation_data = self.gen_up_down_rank(date)
    testing = quotation_data['PCT_CHANGE']
    aggregation = []
    for i in range(-10, 11, 1):
      lb = i / 100
      ub = (i + 1) / 100
      count = len(quotation_data[(lb <= testing) & (testing < ub)])
      aggregation.append(count)

    data = DataFrame({
      'range': [
        '[-0.1, -0.09)', '[-0.09, -0.08)', '[-0.08, -0.07)', '[-0.07, -0.06)', '[-0.06, -0.05)',
        '[-0.05, -0.04)', '[-0.04, -0.03)', '[-0.03, -0.02)', '[-0.02, -0.01)', '[-0.01, 0.00)',
        '[0.00, 0.01)', '[0.01, 0.02)', '[0.02, 0.03)', '[0.03, -0.04)', '[0.04, 0.05)',
        '[0.05, 0.06)', '[0.06, 0.07)', '[0.07, 0.08)', '[0.08, -0.09)', '[0.09, 0.1)', '[0.1, 0.11)'
      ],
      'count': aggregation
    })

    self.store(data, filename)
    return data


daily_report_gener = DailyReportGener()