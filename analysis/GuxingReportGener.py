from datetime import date
import math
from operator import index

from numpy import append
from analysis.ReportGener import ReportGener
from pandas import DataFrame
from analysis.StockListGener import stock_list_gener
from data_source import local_source
from database import date_getter


class GuxingReportGener(ReportGener):

    folder_name = 'daily-reports'
    stock_list = DataFrame()
    indices_data = {
        'SH': DataFrame(),
        'SZ': DataFrame()
    }
    start_date = ""
    indices_changes = {
        'SH': [],
        'SZ': []
    }
    weighting_coe = [
        0.3, 0.3, 0.3, 
        0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 
        0.7, 0.7, 0.7,
        1, 1, 1
    ]

    def __init__(self, scope="default") -> None:
        super().__init__()
        if scope == 'default':
            self.stock_list = stock_list_gener.gen_buyable_stocks()
    
        self.start_date = date_getter.get_trade_date_before(16)

        self.indices_data['SH'] = local_source.get_indices_daily(
            "TRADE_DATE, CLOSE", f"INDEX_CODE LIKE '%SH' AND TRADE_DATE >= {self.start_date}"
        )
        self.indices_data['SZ'] = local_source.get_indices_daily(
            "TRADE_DATE, CLOSE", f"INDEX_CODE LIKE '%SZ' AND TRADE_DATE >= {self.start_date}"
        )
        for i in range(15):
            self.indices_changes['SH'].append(
                round(((self.indices_data['SH']['CLOSE'][i+1] - self.indices_data['SH']['CLOSE'][i]) / self.indices_data['SH']['CLOSE'][i])*100, 2)
            )
            self.indices_changes['SZ'].append(
                round(((self.indices_data['SZ']['CLOSE'][i+1] - self.indices_data['SZ']['CLOSE'][i]) / self.indices_data['SZ']['CLOSE'][i])*100, 2)
            )
        self.indices_data['SH'] = self.indices_data['SH'].iloc[1: , :]
        self.indices_data['SZ'] = self.indices_data['SZ'].iloc[1: , :]

    def calc_guxing_by_stock(self, stock):
        stock_data = local_source.get_quotations_daily(
            stock,
            cols="TS_CODE, TRADE_DATE, ((CLOSE - PRE_CLOSE) / PRE_CLOSE) AS CHANGE",
            condition=F"TRADE_DATE >= {self.start_date}"
        )
        index_type = stock_data['TS_CODE'][0][-2:]
        stock_changes = []
        for date in self.indices_data[index_type]['TRADE_DATE']:
            if date in stock_data['TRADE_DATE'].values:
                change = stock_data[stock_data['TRADE_DATE']==date]['CHANGE'].values[0]
                stock_changes.append(
                    round(change*100, 2)
                )
            else: stock_changes.append(0.00)
        stock_subt_index = []
        for i in range(15):
            stock_subt_index.append(stock_changes[i] - self.indices_changes[index_type][i])
            stock_subt_index[i] *= self.weighting_coe[i]
            stock_subt_index[i] = round(stock_subt_index[i], 2)
        return round(sum(stock_subt_index), 2)

    def get_guxing_report(self):
        trade_date = date_getter.get_trade_date_before()
        filename = f'{trade_date}-gx-report'
        if (data := self.fetch(filename)) is not None:
            return data

        guxing_report = DataFrame(columns=['STOCK_NAME', 'GX'])
        for stock in self.stock_list['NAME']:
            gx = self.calc_guxing_by_stock(stock)
            guxing_report = guxing_report.append({'STOCK_NAME': stock, 'GX': gx}, ignore_index=True)
        guxing_report = guxing_report.sort_values(by=['GX'])
        self.store(guxing_report, filename)
        return guxing_report
    
guxing_report_gener = GuxingReportGener()