import sqlite3
from utils import log
from settings import DB_PATH, TABLE_NAMES, CREATION_SQL


def create_tables():
    conn = sqlite3.connect(DB_PATH)
    for table_name in TABLE_NAMES:
        # TODO 区分表的存在状态
        log(f'检查或创建表：{table_name}...')
        conn.execute(CREATION_SQL[table_name])
    log(f'数据库初始化完成！')
