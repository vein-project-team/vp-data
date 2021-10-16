from database.db_builder import create_tables
from database.db_trimmer import trim_table, trim_tables
from dispatcher import get_stock_info_from_db

if __name__ == '__main__':
    print(get_stock_info_from_db('600519.SH'))




