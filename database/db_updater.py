from dispatcher import get_trade_date_list_from_db
from dispatcher import get_trade_date_list_from_api
from dispatcher import get_stock_list_from_db
from dispatcher import get_stock_list_from_api
from database.db_settings import DB_SIZE, TABLES_NEED_UPDATE_BY_DATE, TABLES_NEED_UPDATE_BY_STOCKS, update_data_source
from data_api.date_getter import date_getter as dg
from database.db_writer import fill_table, DataGetter
from database.db_checker import date_checker, stocks_checker
from cacher import override_cache, read_cache
from utils import log
import numpy as np


class Updater:

    def __init__(self):
        pass

    def update_database(self):
        self.refresh()

        log('开始按照股票列表的变化更新数据')
        for table in TABLES_NEED_UPDATE_BY_STOCKS:
            getter = update_data_source()[table]['update_by_stocks']['getter']
            getter_args = update_data_source()[table]['update_by_stocks']['getter_args']
            fill_table(
                table,
                DataGetter(getter, getter_args),
                stocks_checker,
            )

        log('开始按照日期的变化更新数据')
        for table in TABLES_NEED_UPDATE_BY_DATE:
            log(f'准备更新表：{table}')
            getter = update_data_source()[table]['update_by_date']['getter']
            getter_args = update_data_source()[table]['update_by_date']['getter_args']
            fill_table(
                table,
                DataGetter(getter, getter_args),
                date_checker,
            )

    @staticmethod
    def refresh():
        report = read_cache('update_report')
        if report is not None:
            if report['new_daily_end_date'] == dg.get_latest_finished_trade_date():
                log('更新报告缓存无需更新。')
                return

        log('正在写入新的更新报告缓存...')
        old_daily_date_list = get_trade_date_list_from_db('DAILY')['TRADE_DATE']
        old_weekly_date_list = get_trade_date_list_from_db('WEEKLY')['TRADE_DATE']
        old_monthly_date_list = get_trade_date_list_from_db('MONTHLY')['TRADE_DATE']

        new_daily_date_list = get_trade_date_list_from_api('DAILY', DB_SIZE)['trade_date']
        new_weekly_date_list = get_trade_date_list_from_api('WEEKLY', DB_SIZE)['trade_date']
        new_monthly_date_list = get_trade_date_list_from_api('MONTHLY', DB_SIZE)['trade_date']

        sh_old_stock_list = get_stock_list_from_db('SH')['TS_CODE'].values
        sh_new_stock_list = get_stock_list_from_api('SH')['ts_code'].values
        sh_added_stocks = np.setdiff1d(sh_new_stock_list, sh_old_stock_list)
        sh_removed_stocks = np.setdiff1d(sh_old_stock_list, sh_new_stock_list)

        sz_old_stock_list = get_stock_list_from_db('SZ')['TS_CODE'].values
        sz_new_stock_list = get_stock_list_from_api('SZ')['ts_code'].values
        sz_added_stocks = np.setdiff1d(sz_new_stock_list, sz_old_stock_list)
        sz_removed_stocks = np.setdiff1d(sz_old_stock_list, sz_new_stock_list)

        report = {
            'old_daily_start_date': old_daily_date_list[0],
            'old_weekly_start_date': old_weekly_date_list[0],
            'old_monthly_start_date': old_monthly_date_list[0],
            'old_daily_end_date': old_daily_date_list[DB_SIZE - 1],
            'old_weekly_end_date': old_weekly_date_list[DB_SIZE - 1],
            'old_monthly_end_date': old_monthly_date_list[DB_SIZE - 1],

            'new_daily_start_date': new_daily_date_list[0],
            'new_weekly_start_date': new_weekly_date_list[0],
            'new_monthly_start_date': new_monthly_date_list[0],
            'new_daily_end_date': new_daily_date_list[DB_SIZE - 1],
            'new_weekly_end_date': new_weekly_date_list[DB_SIZE - 1],
            'new_monthly_end_date': new_monthly_date_list[DB_SIZE - 1],

            'daily_update_size': dg.get_trade_days_between(
                old_daily_date_list[DB_SIZE - 1], new_daily_date_list[DB_SIZE - 1]
            ) - 1,
            'weekly_update_size': dg.get_trade_days_between(
                old_weekly_date_list[DB_SIZE - 1], new_weekly_date_list[DB_SIZE - 1]
            ) - 1,
            'monthly_update_size': dg.get_trade_days_between(
                old_monthly_date_list[DB_SIZE - 1], new_monthly_date_list[DB_SIZE - 1]
            ) - 1,

            'sh_added_stocks': sh_added_stocks.tolist(),
            'sh_removed_stocks': sh_removed_stocks.tolist(),

            'sz_added_stocks': sz_added_stocks.tolist(),
            'sz_removed_stocks': sz_removed_stocks.tolist()
        }

        override_cache('update_report', report)


updater = Updater()
