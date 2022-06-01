from pandas import DataFrame
import pandas
from analysis.ReportGener import ReportGener
from analysis.StockListGener import stock_list_gener

import pandas as pd
from data_source import local_source
from data_source import date_getter
from data_source.DateGetter import DateGetter

class DailyReportGener(ReportGener):

  folder_name = 'daily-reports'
  stock_list = DataFrame()


  def __init__(self, scope="default") -> None:
      super().__init__()
      if scope == 'default':
        self.stock_list = stock_list_gener.buyable_stocks()


  def gen_up_down_rank(self, date='latest') -> DataFrame:
    trade_date = date_getter.get_trade_date_before()
    if date != 'latest':
      trade_date = date
    filename = f'{trade_date}-up-down-rank'
    if (data := self.fetch(filename)) is not None:
      return data

    data = local_source.get_quotations_daily(
      cols='''
      TS_CODE, OPEN, CLOSE, LOW, HIGH, PRE_CLOSE, ROUND((CLOSE-PRE_CLOSE) / PRE_CLOSE, 4) AS PCT_CHANGE
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
    if (data := self.fetch(filename)) is not None:
      return data

    quotation_data = self.gen_up_down_rank(date)
    testing = quotation_data['PCT_CHANGE']
    aggregation = []
    for i in range(-11, 11, 1):
      lb = i / 100
      ub = (i + 1) / 100
      count = len(quotation_data[(lb <= testing) & (testing < ub)])
      aggregation.append(count)

    data = DataFrame({
      'range': [
        '[-0.11, -0.10)', '[-0.10, -0.09)', '[-0.09, -0.08)', '[-0.08, -0.07)', '[-0.07, -0.06)', '[-0.06, -0.05)',
        '[-0.05, -0.04)', '[-0.04, -0.03)', '[-0.03, -0.02)', '[-0.02, -0.01)', '[-0.01, 0.00)',
        '[0.00, 0.01)', '[0.01, 0.02)', '[0.02, 0.03)', '[0.03, -0.04)', '[0.04, 0.05)',
        '[0.05, 0.06)', '[0.06, 0.07)', '[0.07, 0.08)', '[0.08, -0.09)', '[0.09, 0.10)', '[0.10, 0.11)'
      ],
      'count': aggregation
    })

    self.store(data, filename)
    return data


  def _gen_limits_rank(self, type, trade_date):
    date_list = date_getter.get_trade_date_list_forward(100)['TRADE_DATE'].values
    data = local_source.get_limits_statistic(
      condition=f'TRADE_DATE = { trade_date } AND LIMIT_TYPE = "{ type }" ORDER BY FD_AMOUNT DESC'
    )

    t = data['TS_CODE'].isin(self.stock_list['TS_CODE'])
    data = data[t].reset_index(drop=True)

    lianbans = []
    for stock in data['TS_CODE']:
      days = 1
      while True:
        previous_trade_date = date_list[-days-1]
        previous_bans = local_source.get_limits_statistic(
          cols='TS_CODE',
          condition=f'TRADE_DATE = { previous_trade_date } AND LIMIT_TYPE = "{ type }"'
        )['TS_CODE'].values
        if stock in previous_bans:
          days += 1
        else:
          lianbans.append(days)
          break
    
    data['LIANBAN'] = lianbans

    return data


  def gen_up_limits_rank(self, date='latest'):
    trade_date = date_getter.get_trade_date_before()
    if date != 'latest':
      trade_date = date
    filename = f'{trade_date}-up-limits_rank'
    if (data := self.fetch(filename)) is not None:
      return data

    data = self._gen_limits_rank('U', trade_date)

    self.store(data, filename)
    return data


  def gen_down_limits_rank(self, date='latest'):
    trade_date = date_getter.get_trade_date_before()
    if date != 'latest':
      trade_date = date
    filename = f'{trade_date}-down-limits_rank'
    if (data := self.fetch(filename)) is not None:
      return data

    data = self._gen_limits_rank('D', trade_date)

    self.store(data, filename)
    return data
    

daily_report_gener = DailyReportGener()