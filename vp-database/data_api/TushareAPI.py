from data_api import DataAPI
import tushare as ts
import pandas as pd
from tqdm import tqdm as pb


class TushareAPI(DataAPI):

    def __init__(self):
        super().__init__()
        ts.set_token("3f73ca482044f78edd9694a4e06a0edd9431c24cbac31a07f275d7cf")
        self.query = ts.pro_api().query
        self.trade_date_list = {
            'daily': pd.DataFrame(),
            'weekly': pd.DataFrame(),
            'monthly': pd.DataFrame()
        }
        self.fields = {
            'INDEX_LIST': {
                'raw': 'ts_code,name,fullname,market,publisher,index_type,category,base_date,base_point,list_date,weight_rule,desc,exp_date',
                'ordered': ['ts_code', 'name', 'fullname', 'market', 'publisher', 'index_type', 'category', 'base_date', 'base_point',
                            'weight_rule', 'desc', 'list_date', 'exp_date']
            },
            'STOCK_LIST': {
                'raw': 'ts_code,name,area,industry,cnspell,market,exchange,list_status,list_date,delist_date,is_hs',
                'ordered': [
                    'ts_code', 'name', 'cnspell', 'exchange', "market", 'area', 'industry',
                    'list_status', 'list_date', 'delist_date', 'is_hs'
                ]
            },
            'INDICES_DAILY': {
                'raw': 'ts_code,trade_date,open,close,low,high,vol',
                'ordered': ['ts_code', 'trade_date', 'open', 'close', 'low', 'high', 'vol']
            },
            'INDICES_WEEKLY': {
                'raw': 'ts_code,trade_date,open,close,low,high,vol',
                'ordered': ['ts_code', 'trade_date', 'open', 'close', 'low', 'high', 'vol']
            },
            'INDICES_MONTHLY': {
                'raw': 'ts_code,trade_date,open,close,low,high,vol',
                'ordered': ['ts_code', 'trade_date', 'open', 'close', 'low', 'high', 'vol']
            },
            'QUOTATIONS_DAILY': {
                'raw': 'ts_code,trade_date,open,close,low,high,pre_close,change,vol,amount',
                'ordered': ['ts_code', 'trade_date', 'open', 'close', 'low', 'high', 'pre_close', 'change', 'vol', 'amount']
            },
            'QUOTATIONS_WEEKLY': {
                'raw': 'ts_code,trade_date,open,close,low,high,pre_close,change,vol,amount',
                'ordered': ['ts_code', 'trade_date', 'open', 'close', 'low', 'high', 'pre_close', 'change', 'vol', 'amount']
            },
            'QUOTATIONS_MONTHLY': {
                'raw': 'ts_code,trade_date,open,close,low,high,pre_close,change,vol,amount',
                'ordered': ['ts_code', 'trade_date', 'open', 'close', 'low', 'high', 'pre_close', 'change', 'vol', 'amount']
            },
            'LIMITS_STATISTIC': {
                'raw': 'trade_date,ts_code,fd_amount,first_time,last_time,open_times,limit',
                'ordered': ['ts_code', 'trade_date', 'limit', 'first_time', 'last_time', 'open_times', 'fd_amount']
            },
            'ADJ_FACTORS': {
                'raw': 'trade_date,ts_code,adj_factor',
                'ordered': ['ts_code', 'trade_date', 'adj_factor']
            }
        }

    def _get_fields(self, table_name):
        return self.fields[table_name]['raw']

    def _change_order(self, table_name, dataframe):
        cols = self.fields[table_name]['ordered']
        dataframe = dataframe[cols]
        return dataframe

    @staticmethod
    def _trim_date_list(date_list, start_date):
        return date_list[date_list > start_date]

    def get_index_list(self):
        table_name = 'INDEX_LIST'
        fields = self._get_fields(table_name)
        data = self._change_order(table_name, pd.concat([
            self.query('index_basic', market='SSE', fields=fields),
            self.query('index_basic', market='SZSE', fields=fields),
            self.query('index_basic', market='MSCI', fields=fields),
            self.query('index_basic', market='CSI', fields=fields),
            self.query('index_basic', market='CICC', fields=fields),
            self.query('index_basic', market='SW', fields=fields),
            self.query('index_basic', market='OTH', fields=fields)
        ], axis=0).reset_index(drop=True).fillna('NULL'))
        return self.convert_header(table_name, data)

    def get_stock_list(self):
        table_name = 'STOCK_LIST'
        fields = self._get_fields(table_name)
        data = self._change_order(table_name, pd.concat([
            self.query('stock_basic', exchange='SSE', list_status='L', fields=fields),
            self.query('stock_basic', exchange='SSE', list_status='P', fields=fields),
            self.query('stock_basic', exchange='SSE', list_status='D', fields=fields),
            self.query('stock_basic', exchange='SZSE', list_status='L', fields=fields),
            self.query('stock_basic', exchange='SZSE', list_status='P', fields=fields),
            self.query('stock_basic', exchange='SZSE', list_status='D', fields=fields)
        ], axis=0).reset_index(drop=True).fillna('NULL'))
        self.stock_list = data['ts_code']
        return self.convert_header(table_name, data)

    def _get_indices(self, table_name, frequency='daily'):
        fields = self._get_fields(table_name)
        data = self._change_order(table_name, pd.concat([
            self.query(f'index_{frequency}', ts_code='000001.SH', fields=fields).iloc[::-1],
            self.query(f'index_{frequency}', ts_code='399001.SZ', fields=fields).iloc[::-1]
        ], axis=0).reset_index(drop=True).fillna('NULL'))
        self.trade_date_list[frequency] = data['trade_date']
        data = self.convert_header(table_name, data)
        return data

    def get_indices_daily(self):
        return self._get_indices('INDICES_DAILY')

    def get_indices_weekly(self):
        return self._get_indices('INDICES_WEEKLY', 'weekly')

    def get_indices_monthly(self):
        return self._get_indices('INDICES_MONTHLY', 'monthly')

    def _get_quotations(self, table_name, start_date='', frequency='daily'):
        fields = self._get_fields(table_name)
        data = pd.DataFrame(columns=fields.split(','))
        trade_date_list = self.trade_date_list[frequency]
        if start_date != '':
            trade_date_list = self._trim_date_list(trade_date_list, start_date)
        if len(trade_date_list) == 0:
            return
        for trade_date in pb(trade_date_list, desc='长任务，请等待', colour='#ffffff'):
            next_data = None
            while True:
                try:
                    next_data = self.query(frequency, trade_date=trade_date, fields=fields)
                    break
                except Exception:
                    continue
            data = pd.concat([data, next_data], axis=0)
        data = self._change_order(table_name, data.reset_index(drop=True).fillna('NULL'))
        data = self.convert_header(table_name, data)
        return data

    def get_quotations_daily(self, start_date=''):
        return self._get_quotations('QUOTATIONS_DAILY', start_date)

    def get_quotations_weekly(self, start_date=''):
        return self._get_quotations('QUOTATIONS_WEEKLY', start_date, 'weekly')

    def get_quotations_monthly(self, start_date=''):
        return self._get_quotations('QUOTATIONS_MONTHLY', start_date, 'monthly')

    def get_limits_statistic(self, start_date=''):
        table_name = 'LIMITS_STATISTIC'
        fields = self._get_fields(table_name)
        data = pd.DataFrame(columns=fields.split(','))
        trade_date_list = self.trade_date_list['daily']
        if start_date != '':
            trade_date_list = self._trim_date_list(trade_date_list, start_date)
        if len(trade_date_list) == 0:
            return
        for trade_date in pb(trade_date_list, desc='长任务，请等待', colour='#ffffff'):
            next_data = None
            while True:
                try:
                    next_data = self.query('limit_list', trade_date=trade_date, fields=fields)
                    break
                except Exception:
                    continue
            data = pd.concat([data, next_data], axis=0)
        data = data.reset_index(drop=True).fillna('NULL')
        data = self._change_order(table_name, data)
        data = self.convert_header(table_name, data)
        return data

    def get_adj_factors(self, start_date=''):
        table_name = 'ADJ_FACTORS'
        fields = self._get_fields(table_name)
        data = pd.DataFrame(columns=fields.split(','))
        trade_date_list = self.trade_date_list['daily']
        if start_date != '':
            trade_date_list = self._trim_date_list(trade_date_list, start_date)
        if len(trade_date_list) == 0:
            return
        for trade_date in pb(trade_date_list, desc='长任务，请等待', colour='#ffffff'):
            next_data = None
            while True:
                try:
                    next_data = self.query('adj_factor', ts_code='', trade_date=trade_date, fields=fields)
                    break
                except Exception:
                    continue
            data = pd.concat([data, next_data], axis=0)
        data = data.reset_index(drop=True).fillna('NULL')
        data = self._change_order(table_name, data)
        data = self.convert_header(table_name, data)
        return data