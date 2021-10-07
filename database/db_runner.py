from database.db_builder import db_init
from database.db_writer import fill_tables
from database.settings import DB_SIZE
from utils import log


def setup():
    log('正在启动数据库...')
    db_init(DB_SIZE)
    fill_tables(DB_SIZE)
    log('数据库启动完成！')
