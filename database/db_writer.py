import sqlite3
from utils import log
from database.db_checker import light_checker
from database.db_settings import TABLE_NAMES, update_data_source, init_data_source


def write_to_db(table_name, dataframe):
    if dataframe is None or len(dataframe) == 0:
        return
    conn = sqlite3.connect('vein-project.db')
    cursor = conn.cursor()
    cursor.execute(f'''PRAGMA TABLE_INFO({table_name});''')
    columns = []
    for i in cursor.fetchall():
        columns.append(i[1])
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

    log(f'正在填充数据进表：{table_name}')
    dataframe = data_getter.get()
    write_to_db(table_name, dataframe)


def fill_tables():
    for table in TABLE_NAMES:
        func = init_data_source()[table]['func']
        args = init_data_source()[table]['args']
        fill_table(
            table,
            DataGetter(func, args),
            light_checker,
        )

    # TODO SH_LIMITS_STATISTIC
    # TODO SH_LIMITS_STATISTIC


if __name__ == '__main__':
    pass
