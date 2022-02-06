from data_source.DataSource import DataSource
from data_source.TushareSource import TushareSource
from data_source.LocalSource import LocalSource
from database.db_reader import check_table_exist

tushare_source = TushareSource()
local_source = LocalSource()
date_getter = None
if check_table_exist('INDICES_DAILY'):
  from data_source.DateGetter import DateGetter
  date_getter = DateGetter()
