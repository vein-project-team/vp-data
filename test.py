from database.db_reader import check_table_not_empty

if __name__ == '__main__':
    print(not check_table_not_empty('INDICES_DAILY'))
    print(not check_table_not_empty('QUOTATIONS_DAILY'))