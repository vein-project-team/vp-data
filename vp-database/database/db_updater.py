from database.db_writer import fill_table, DataGetter
from database.db_checker import no_checker
from settings import TABLES_NEED_UPDATE
from database.db_checker import get_db_status
from source_map import DATA_SOURCE
from utils import log


class Updater:

    def __init__(self):
        self.db_status = None

    def refresh(self):
        self.db_status = get_db_status()

    def update_database(self):
        self.refresh()
        for table_name in TABLES_NEED_UPDATE:
            log(f'准备更新表：{table_name}')
            getter = DATA_SOURCE[table_name]['getter']
            fill_table(
                table_name,
                DataGetter(getter, self.get_args(table_name)),
                no_checker
            )

    def get_args(self, table_name):
        if 'DAILY' or 'LIMITS' in table_name:
            return [self.db_status['latest_trade_date_daily']]
        elif 'WEEKLY' in table_name:
            return [self.db_status['latest_trade_date_weekly']]
        elif 'MONTHLY' in table_name:
            return [self.db_status['latest_trade_date_monthly']]


db_updater = Updater()