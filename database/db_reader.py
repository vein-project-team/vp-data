import sqlite3
from pandas import read_sql


def read_from_db(sql: str):
    conn = sqlite3.connect('vein-project.db')
    dataframe = read_sql(sql, conn)
    return dataframe
