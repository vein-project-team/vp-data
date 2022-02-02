from data_source import DataSource
import tushare as ts
import pandas as pd
from tqdm import tqdm as pb


class TushareSource(DataSource):

    def __init__(self):
        super().__init__()
        ts.set_token(
            "3f73ca482044f78edd9694a4e06a0edd9431c24cbac31a07f275d7cf")
        self.query = ts.pro_api().query
        self.stock_list = pd.DataFrame()
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
            },
            'INCOME_STATEMENTS': {
                'raw': 'ts_code,ann_date,f_ann_date,end_date,report_type,comp_type,end_type,basic_eps,diluted_eps,total_revenue,revenue,int_income,prem_earned,comm_income,n_commis_income,n_oth_income,n_oth_b_income,prem_income,out_prem,une_prem_reser,reins_income,n_sec_tb_income,n_sec_uw_income,n_asset_mg_income,oth_b_income,fv_value_chg_gain,invest_income,ass_invest_income,forex_gain,total_cogs,oper_cost,int_exp,comm_exp,biz_tax_surchg,sell_exp,admin_exp,fin_exp,assets_impair_loss,prem_refund,compens_payout,reser_insur_liab,div_payt,reins_exp,oper_exp,compens_payout_refu,insur_reser_refu,reins_cost_refund,other_bus_cost,operate_profit,non_oper_income,non_oper_exp,nca_disploss,total_profit,income_tax,n_income,n_income_attr_p,minority_gain,oth_compr_income,t_compr_income,compr_inc_attr_p,compr_inc_attr_m_s,ebit,ebitda,insurance_exp,undist_profit,distable_profit,rd_exp,fin_exp_int_exp,fin_exp_int_inc,transfer_surplus_rese,transfer_housing_imprest,transfer_oth,adj_lossgain,withdra_legal_surplus,withdra_legal_pubfund,withdra_biz_devfund,withdra_rese_fund,withdra_oth_ersu,workers_welfare,distr_profit_shrhder,prfshare_payable_dvd,comshare_payable_dvd,capit_comstock_div,net_after_nr_lp_correct,credit_impa_loss,net_expo_hedging_benefits,oth_impair_loss_assets,total_opcost,amodcost_fin_assets,oth_income,asset_disp_income,continued_net_profit,end_net_profit',
                'ordered': ['ts_code', 'ann_date', 'f_ann_date', 'end_date', 'report_type', 'comp_type', 'end_type', 'basic_eps', 'diluted_eps', 'total_revenue', 'revenue', 'int_income', 'prem_earned', 'comm_income', 'n_commis_income', 'n_oth_income', 'n_oth_b_income', 'prem_income', 'out_prem', 'une_prem_reser', 'reins_income', 'n_sec_tb_income', 'n_sec_uw_income', 'n_asset_mg_income', 'oth_b_income', 'fv_value_chg_gain', 'invest_income', 'ass_invest_income', 'forex_gain', 'total_cogs', 'oper_cost', 'int_exp', 'comm_exp', 'biz_tax_surchg', 'sell_exp', 'admin_exp', 'fin_exp', 'assets_impair_loss', 'prem_refund', 'compens_payout', 'reser_insur_liab', 'div_payt', 'reins_exp', 'oper_exp', 'compens_payout_refu', 'insur_reser_refu', 'reins_cost_refund', 'other_bus_cost', 'operate_profit', 'non_oper_income', 'non_oper_exp', 'nca_disploss', 'total_profit', 'income_tax', 'n_income', 'n_income_attr_p', 'minority_gain', 'oth_compr_income', 't_compr_income', 'compr_inc_attr_p', 'compr_inc_attr_m_s', 'ebit', 'ebitda', 'insurance_exp', 'undist_profit', 'distable_profit', 'rd_exp', 'fin_exp_int_exp', 'fin_exp_int_inc', 'transfer_surplus_rese', 'transfer_housing_imprest', 'transfer_oth', 'adj_lossgain', 'withdra_legal_surplus', 'withdra_legal_pubfund', 'withdra_biz_devfund', 'withdra_rese_fund', 'withdra_oth_ersu', 'workers_welfare', 'distr_profit_shrhder', 'prfshare_payable_dvd', 'comshare_payable_dvd', 'capit_comstock_div', 'net_after_nr_lp_correct', 'credit_impa_loss', 'net_expo_hedging_benefits', 'oth_impair_loss_assets', 'total_opcost', 'amodcost_fin_assets', 'oth_income', 'asset_disp_income', 'continued_net_profit', 'end_net_profit']

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
            self.query('stock_basic', exchange='SSE',
                       list_status='L', fields=fields),
            self.query('stock_basic', exchange='SSE',
                       list_status='P', fields=fields),
            self.query('stock_basic', exchange='SSE',
                       list_status='D', fields=fields),
            self.query('stock_basic', exchange='SZSE',
                       list_status='L', fields=fields),
            self.query('stock_basic', exchange='SZSE',
                       list_status='P', fields=fields),
            self.query('stock_basic', exchange='SZSE',
                       list_status='D', fields=fields)
        ], axis=0).reset_index(drop=True).fillna('NULL'))
        self.stock_list = data['ts_code']
        return self.convert_header(table_name, data)

    def _get_indices(self, table_name, frequency='daily'):
        fields = self._get_fields(table_name)
        data = self._change_order(table_name, pd.concat([
            self.query(f'index_{frequency}',
                       ts_code='000001.SH', fields=fields).iloc[::-1],
            self.query(f'index_{frequency}',
                       ts_code='399001.SZ', fields=fields).iloc[::-1]
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
                    next_data = self.query(
                        frequency, trade_date=trade_date, fields=fields)
                    break
                except Exception:
                    continue
            data = pd.concat([data, next_data], axis=0)
        data = self._change_order(
            table_name, data.reset_index(drop=True).fillna('NULL'))
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
                    next_data = self.query(
                        'limit_list', trade_date=trade_date, fields=fields)
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
                    next_data = self.query(
                        'adj_factor', ts_code='', trade_date=trade_date, fields=fields)
                    break
                except Exception:
                    continue
            data = pd.concat([data, next_data], axis=0)
        data = data.reset_index(drop=True).fillna('NULL')
        data = self._change_order(table_name, data)
        data = self.convert_header(table_name, data)
        return data

    def get_income_statements(self):
        table_name = 'INCOME_STATEMENTS'
        fields = self._get_fields(table_name)
        data = pd.DataFrame(columns=fields.split(','))
        for stock in pb(self.stock_list, desc='长任务，请等待', colour='#ffffff'):
            next_data = None
            while True:
                try:
                    next_data = self._change_order(table_name, pd.concat([
                        self.query("income", ts_code=stock,
                                   report_type=1, fields=fields),
                        self.query("income", ts_code=stock,
                                   report_type=2, fields=fields),
                        self.query("income", ts_code=stock,
                                   report_type=3, fields=fields),
                        self.query("income", ts_code=stock,
                                   report_type=4, fields=fields),
                        self.query("income", ts_code=stock,
                                   report_type=5, fields=fields),
                        self.query("income", ts_code=stock,
                                   report_type=6, fields=fields),
                        self.query("income", ts_code=stock,
                                   report_type=7, fields=fields),
                        self.query("income", ts_code=stock,
                                   report_type=8, fields=fields),
                        self.query("income", ts_code=stock,
                                   report_type=9, fields=fields),
                        self.query("income", ts_code=stock,
                                   report_type=10, fields=fields),
                        self.query("income", ts_code=stock,
                                   report_type=11, fields=fields),
                        self.query("income", ts_code=stock,
                                   report_type=12, fields=fields)
                    ], axis=0).reset_index(drop=True).fillna('NULL'))
                    break
                except Exception:
                    continue
            data = pd.concat([data, next_data], axis=0)
        return data

    def get_balance_sheets(self, *args):
        return super().get_balance_sheets(*args)

    def get_cash_flows(self, *args):
        return super().get_cash_flows(*args)
