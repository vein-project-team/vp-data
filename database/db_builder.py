import sqlite3
from utils import log
from database.db_settings import TABLE_NAMES, CREATION_SQL


def create_tables():
    conn = sqlite3.connect('vein-project.db')
    for table in TABLE_NAMES:
        log(f'检查或创建表：{table}...')
        conn.execute(CREATION_SQL[table])
    log(f'数据库初始化完成！')
