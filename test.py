from database import date_getter

if __name__ == "__main__":
    data = date_getter.get_quarter_end_date_list()
    for date in data['TRADE_DATE']:
        if date[-2:] != '30' and date[-2:] != '31':
            print(date)