import datetime
import time
from database.db_reader import read_from_db, check_table_exist


class DateGetter:

    def __init__(self):
        self.start_date = '20100101'
        self.today = self.get_today()
        self.latest_finished_trade_date = ''
        self.trade_date_list_daily = None
        self.trade_date_list_weekly = None
        self.trade_date_list_monthly = None
        self.quarter_end_date_list = None
        self.refresh()


    def refresh(self):
        """
        更新当前实例的数据
        """
        if check_table_exist('INDICES_DAILY'):
            self.latest_finished_trade_date = read_from_db(
                'SELECT TRADE_DATE FROM INDICES_DAILY ORDER BY TRADE_DATE DESC LIMIT 1;'
            )['TRADE_DATE'][0]
            self.trade_date_list_daily = read_from_db('SELECT TRADE_DATE FROM INDICES_DAILY;')
            self.trade_date_list_weekly = read_from_db('SELECT TRADE_DATE FROM INDICES_WEEKLY;')
            self.trade_date_list_monthly = read_from_db('SELECT TRADE_DATE FROM INDICES_MONTHLY;')

            quarter_end_date_list = read_from_db("SELECT DISTINCT TRADE_DATE FROM INDICES_MONTHLY WHERE SUBSTR(TRADE_DATE, 5, 2) IN ('03', '06', '09', '12')")
            quarter_end_date_list = quarter_end_date_list.replace(r'^([\d]{4}03)(\d{2})$', r'\g<1>31', regex=True)
            quarter_end_date_list = quarter_end_date_list.replace(r'^([\d]{4}06)(\d{2})$', r'\g<1>30', regex=True)
            quarter_end_date_list = quarter_end_date_list.replace(r'^([\d]{4}09)(\d{2})$', r'\g<1>30', regex=True)
            quarter_end_date_list = quarter_end_date_list.replace(r'^([\d]{4}12)(\d{2})$', r'\g<1>31', regex=True)
            self.quarter_end_date_list = quarter_end_date_list

    @staticmethod
    def get_today():
        """
        获取今天日期
        :return:今日日期 string 'YYYYMMDD'
        """
        today = datetime.datetime.now().strftime('%Y%m%d')
        return today

    @staticmethod
    def get_date_before_today(days):
        """
        获得前n天的日期
        :param days: 多少天前？
        :return: 日期 string 'YYYYMMDD'
        """
        today = datetime.datetime.now()
        offset = datetime.timedelta(days=-days)
        the_day = (today + offset).strftime('%Y%m%d')
        return the_day

    @staticmethod
    def get_days_between(day1, day2):
        """
        两个日期之内有多少天？
        :param day1: 日期1 string 'YYYYMMDD'
        :param day2: 日期2 string 'YYYYMMDD'
        :return: 天数 int
        """
        time_array1 = time.strptime(day1, "%Y%m%d")
        timestamp_day1 = int(time.mktime(time_array1))
        time_array2 = time.strptime(day2, "%Y%m%d")
        timestamp_day2 = int(time.mktime(time_array2))
        result = (timestamp_day2 - timestamp_day1) // 60 // 60 // 24
        return abs(result)

    def get_quarter_end_date_list(self):
        return self.quarter_end_date_list

    def get_quarter_end_date_before(self, date='last'):
        return self.quarter_end_date_list['TRADE_DATE'][len(self.quarter_end_date_list)-1]

    def get_trade_date_between(self, date1, date2, frequency='DAILY'):
        """
        获取两个日期之间的交易日表
        :param frequency:
        :param date1: 日期1 string 'YYYYMMDD'
        :param date2: 日期2 string 'YYYYMMDD'
        :return: 两个日期之间的交易日表 dataframe
        """
        t = None
        if frequency == 'DAILY':
            t = self.trade_date_list_daily
        elif frequency == 'WEEKLY':
            t = self.trade_date_list_weekly
        elif frequency == 'MONTHLY':
            t = self.trade_date_list_monthly
        trade_date_list = t.drop(t[(t.TRADE_DATE < date1) | (t.TRADE_DATE > date2)].index)
        return trade_date_list.reset_index(drop=True)

    def get_trade_days_between(self, day1, day2, frequency='DAILY'):
        """
        获取两个交易日之间的交易日数量
        :param frequency:
        :param day1: 交易日1
        :param day2: 交易日2
        :return: 之间的交易日数量 int
        """
        start_date = day1
        end_date = day2
        if int(day1) > int(day2):
            start_date, end_date = end_date, start_date
        trade_date_list = self.get_trade_date_between(start_date, end_date, frequency)
        return len(trade_date_list)

    def get_trade_date_before(self, days=1, date='last', frequency='DAILY'):
        """
        获取某日期n天前的交易日日期
        :param frequency:
        :param date: 日期，默认是最后一个完成的交易日
        :param days: 多少个交易日之前开始？
        :return: 日期 string 'YYYYMMDD'
        """
        end_date = date
        if date == 'last':
            end_date = self.latest_finished_trade_date
        trade_date_list = self.get_trade_date_between(self.start_date, end_date, frequency)
        length = len(trade_date_list)
        start_date = trade_date_list["TRADE_DATE"][length-days]
        return start_date

    def get_trade_date_list_forward(self, size, frequency='DAILY'):
        """
        获取一段时间内的交易日列表
        :param frequency: 日线 | 周线 | 月线
        :param size: 多少条记录？
        :return: 交易日列表 dataframe
        """
        if frequency == 'DAILY':
            return self.trade_date_list_daily[-size:].reset_index(drop=True)
        elif frequency == 'WEEKLY':
            return self.trade_date_list_weekly[-size:].reset_index(drop=True)
        elif frequency == 'MONTHLY':
            return self.trade_date_list_monthly[-size:].reset_index(drop=True)
        else:
            return None

