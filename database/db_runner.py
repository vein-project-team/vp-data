import os

import requests
from database.db_builder import create_tables
from database.db_writer import fill_tables
from database.db_settings import DB_SIZE
from database.db_updater import updater
from utils import log


def setup_database():
    if not os.path.exists('vein-project.db'):
        log('正在创建数据库...')
    else:
        log('数据库已存在...')
    log(f'当前数据库大小设定为：{DB_SIZE}')
    create_tables()
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
    log('数据库启动完成！')


def update_database():
    if not os.path.exists('vein-project.db'):
        setup_database()
    else:
        log('数据库准备更新...')
    while True:
        try:
            updater.update_database()
            break
        except ConnectionError:
            log('与数据接口的链接出现问题，正在重连...')
            continue
        except requests.exceptions.ConnectionError:
            log('与数据接口的链接出现问题，正在重连...')
            continue
    log('数据库更新完成！')
