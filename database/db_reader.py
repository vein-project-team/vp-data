import sqlite3
from pandas import read_sql
from settings import DB_PATH

def check_table_exist(table_name):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute(f'''
        SELECT name FROM sqlite_master WHERE type = "table" AND name = "{table_name}";
    ''')
    result = cursor.fetchall()
    return len(result) > 0

def check_table_not_empty(table_name):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute(f'''
        SELECT COUNT(*) AS RECORDS FROM {table_name};
    ''')
    result = cursor.fetchall()
    return result[0][0] > 0

def read_from_db(sql):
    conn = sqlite3.connect(DB_PATH)
    dataframe = read_sql(sql, conn)
    return dataframe


def read_table_header(table_name):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute(f'''PRAGMA TABLE_INFO({table_name});''')
    columns = []
    for i in cursor.fetchall():
        columns.append(i[1])
    return columns


def read_table(table_name, cols='*'):
    dataframe = read_from_db(f'''SELECT {cols} FROM {table_name};''')
    return dataframe
