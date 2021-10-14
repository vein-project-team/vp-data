import sqlite3
from utils import log
from database.db_checker import light_checker
from database.db_settings import TABLES_NEED_FILL, INIT_DATA_SOURCE, update_data_source


def get_table_cols(table_name):
    conn = sqlite3.connect('vein-project.db')
    cursor = conn.cursor()
    cursor.execute(f'''PRAGMA TABLE_INFO({table_name});''')
    columns = []
    for i in cursor.fetchall():
        columns.append(i[1])
    return columns


def write_to_db(table_name, dataframe):
    if dataframe is None or len(dataframe) == 0:
        return
    else:
        columns = get_table_cols(table_name)
        conn = sqlite3.connect('vein-project.db')
        dataframe.columns = columns
        dataframe.to_sql('TEMP', conn, index=False)
        conn.execute(f'''REPLACE INTO {table_name} SELECT * FROM TEMP;''')
        conn.execute('''DROP TABLE TEMP;''')
        conn.commit()


class DataGetter:

    def __init__(self, func, args):
        self.target = func
        self.args = args

    def get(self):
        return self.target(*self.args)


def fill_table(table_name, data_getter, checker):
    report = checker(table_name)
    if report['pass']:
        return
    else:
        log(f'正在填充数据进表：{table_name}')
        dataframe = data_getter.get()
        write_to_db(table_name, dataframe)


def fill_tables():
    for table in TABLES_NEED_FILL:
        getter = INIT_DATA_SOURCE[table]['getter']
        getter_args = INIT_DATA_SOURCE[table]['getter_args']
        fill_table(
            table,
            DataGetter(getter, getter_args),
            light_checker
        )


if __name__ == '__main__':
    pass
