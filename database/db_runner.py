import os

import requests
from settings import DB_PATH
from database.db_builder import create_tables
from database.db_writer import fill_tables
from utils import log


def build_database():
    log('数据库准备初始化...')
    if not os.path.exists(DB_PATH):
        log('正在创建数据库...')
    else:
        log('数据库已存在...')
    create_tables()
    log('数据库启动完成！')


def fill_database():
    log('接下来进行数据填充...')
    while True:
        try:
            fill_tables()
            break
        except ConnectionError:
            log('与数据接口的链接出现问题，正在重连...')
            continue
        except requests.exceptions.ConnectionError:
            log('与数据接口的链接出现问题，正在重连...')
            continue
    log('数据库填充完成！')


def setup():
    build_database()
    fill_database()