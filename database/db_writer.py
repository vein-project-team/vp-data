import sqlite3
from utils import log
from settings import DB_PATH, TABLES_NEED_FILL
from source_map import DATA_SOURCE


def write_to_db(table_name, dataframe):
    if dataframe is None or len(dataframe) == 0:
        log(f'没有发现需要添加到 {table_name} 中的数据。')
        return
    else:
        log(f'正在填充数据进表：{table_name}')
        conn = sqlite3.connect(DB_PATH)
        dataframe.to_sql('TEMP', conn, index=False)
        conn.execute(f'''REPLACE INTO {table_name} SELECT * FROM TEMP;''')
        conn.execute('''DROP TABLE TEMP;''')
        conn.commit()


class DataGetter:

    def __init__(self, getter, checker, args):
        self.getter = getter
        self.checker = checker
        self.args = args

    def get(self):
        report = self.checker(*self.args)
        table_name = self.args[0]
        if report['need_fill'] == False:
            log(f"{report['type']}: 检查得出，表 {table_name} 无需填充。")
            return None
        else:
            log(f'准备填充数据进表：{table_name}')
            return self.getter(fill_controller=report['fill_controller'])


def fill_table(table_name):
    checker = DATA_SOURCE[table_name]['checker']
    getter = DATA_SOURCE[table_name]['getter']
    args = [table_name, ]
    args += DATA_SOURCE[table_name]['args']
    dataframe = DataGetter(getter, checker, args).get()
    if dataframe is not None:
        write_to_db(table_name, dataframe)


def fill_tables():
    for table_name in TABLES_NEED_FILL:
        fill_table(table_name)


if __name__ == '__main__':
    pass
