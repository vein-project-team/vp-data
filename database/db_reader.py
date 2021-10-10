import sqlite3
from pandas import read_sql


def read_from_db(sql: str):
    conn = sqlite3.connect('vein-project.db')
    dataframe = read_sql(sql, conn)
    return dataframe


def read_table(table_name, cols='*'):
    dataframe = read_from_db(f'''SELECT {cols} FROM {table_name};''')
    return dataframe
