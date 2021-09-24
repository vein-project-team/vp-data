import datetime
import time


def log(info):
    print(f"[{datetime.datetime.now().strftime('%H:%M:%S')}] {info}")


def round_list(the_list, points=2):
    for i in range(0, len(the_list)):
        the_list[i] = round(the_list[i], points)
    return the_list


def get_today():
    today = datetime.datetime.now().strftime('%Y%m%d')
    return today


def get_days_before_today(days):
    today = datetime.datetime.now()
    offset = datetime.timedelta(days=-days)
    the_day = (today + offset).strftime('%Y%m%d')
    return the_day


def get_days_between(day1, day2):
    time_array1 = time.strptime(day1, "%Y%m%d")
    timestamp_day1 = int(time.mktime(time_array1))
    time_array2 = time.strptime(day2, "%Y%m%d")
    timestamp_day2 = int(time.mktime(time_array2))
    result = (timestamp_day2 - timestamp_day1) // 60 // 60 // 24
    return abs(result)


def only_keep_sh_main_board(df):
    df = df.drop(df[(df.ts_code < "600000") | (df.ts_code > "680000")].index)
    return df


def only_keep_sz_main_board(df):
    df = df.drop(df[df.ts_code >= "100000"].index)
    return df


if __name__ == '__main__':
    pass
