from data_handler import reader
from data_storage.builder import db_init

if __name__ == '__main__':
    db_init()
    print(reader.get_index_daily('SH', 10))
