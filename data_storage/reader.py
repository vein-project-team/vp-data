import sqlite3


def read_data(sql):
    conn = sqlite3.connect('vein-project.db')
    c = conn.cursor()
    c.execute(sql)
    return list(c.fetchall())


def get_index_quotation(index_suffix, frequency, size):
    table_name = f'{index_suffix}_INDEX_{frequency}'
    data = read_data(f'''
    SELECT * FROM (
    SELECT * FROM {table_name}
    ORDER BY TRADE_DATE DESC LIMIT {size})
    ORDER BY TRADE_DATE ASC;
    ''')
    return data


if __name__ == '__main__':
    pass
