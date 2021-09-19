import datetime


def get_today():
    today = datetime.date.today().__str__().replace("-", "")
    return today


def only_keep_sh_main_board(df):
    df = df.drop(df[(df.ts_code < "600000") | (df.ts_code > "680000")].index)
    return df


def only_keep_sz_main_board(df):
    df = df.drop(df[df.ts_code >= "100000"].index)
    return df
