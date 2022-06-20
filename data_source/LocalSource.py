from pandas import DataFrame
from soupsieve import match
from data_source.DataSource import DataSource
from database.db_reader import read_from_db
import re


class LocalSource(DataSource):

    @staticmethod
    def get(table_name, cols='*', condition='1'):
        data = read_from_db(
            f'''
            SELECT {cols} FROM {table_name}
            WHERE {condition};
            '''
        )
        return data

    def get_stock_list(self, cols='*', condition='1'):
        return self.get('STOCK_LIST', cols, condition)

    def get_indices_daily(self, cols='*', condition='1'):
        return self.get('INDICES_DAILY', cols, condition)

    def get_indices_weekly(self, cols='*', condition='1'):
        return self.get('INDICES_WEEKLY', cols, condition)

    def get_indices_monthly(self, cols='*', condition='1'):
        return self.get('INDICES_MONTHLY', cols, condition)

    def get_quotations_daily(self, stock='', date='', cols='*', condition='1') -> DataFrame:
        """
        提取日线行情数据
        :param stock: 指定股票 | 代码 XXXXXX.XX | 全称 |
        :param date: 指定日期 | 某日 XXXXXXXX | 某月 XXXXXX | 某年 XXXX | 日期区间 XXXXXXXX-XXXXXXXX |
        """
        stock_con = '1'
        if re.match(r'[\d]{6}\.S[ZH]', stock):
            stock_con = f'TS_CODE = "{stock}"'
        elif len(stock) > 0:
            stock = self.get_stock_list(condition=f'NAME = "{stock}"')['TS_CODE'][0]
            stock_con = f'TS_CODE = "{stock}"'
        
        date_con = '1'
        if re.match(r'[\d]{8}', date):
            date_con = f'TRADE_DATE = {date}'
        elif re.match(r'[\d]{6}', date):
            date_con = f'TRADE_DATE BETWEEN {date}01 AND {date}31'
        elif re.match(r'[\d]{4}', date):
            date_con = f'TRADE_DATE BETWEEN {date}0101 AND {date}1231'
        elif re.match(r'[\d]{8}-[\d]{8}', date):
            date = date.split('-')
            date_con = f'TRADE_DATE BETWEEN {date[0]} AND {date[1]}'
            
        return self.get('QUOTATIONS_DAILY', cols, f'{stock_con} AND {date_con} AND ' + condition)

    def get_quotations_weekly(self, cols='*', condition='1'):
        return self.get('QUOTATIONS_WEEKLY', cols, condition)

    def get_quotations_monthly(self, cols='*', condition='1'):
        return self.get('QUOTATIONS_MONTHLY', cols, condition)

    def get_limits_statistic(self, cols='*', condition='1'):
        return self.get('LIMITS_STATISTIC', cols, condition)

    def get_adj_factors(self, cols='*', condition='1'):
        return self.get('ADJ_FACTORS', cols, condition)

    def get_income_statements(self, cols='*', condition='1'):
        return self.get('INCOME_STATEMENTS', cols, condition)

    def get_balance_sheets(self, cols='*', condition='1'):
        return self.get('BALANCE_SHEETS', cols, condition)

    def get_cash_flows(self, cols='*', condition='1'):
        return self.get('STATEMENTS_OF_CASH_FLOWS', cols, condition)

    def get_income_forecasts(self, cols='*', condition='1'):
        return self.get('INCOME_FORECASTS', cols, condition)

    def get_financial_indicators(self, cols='*', condition='1'):
        return self.get('FINANCIAL_INDICATORS', cols, condition)

    def get_stock_indicators_daily(self, cols='*', condition='1'):
        return self.get('STOCK_INDICATORS_DAILY', cols, condition)
