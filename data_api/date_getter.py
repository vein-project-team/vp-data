import datetime
import time
import tushare as ts


class DateGetter:

    def __init__(self):

        ts.set_token("3f73ca482044f78edd9694a4e06a0edd9431c24cbac31a07f275d7cf")
        self.pro = ts.pro_api()

        self.start_date = '20100101'
        self.today = self.get_today()
        self.latest_finished_trade_date = self.get_latest_finished_trade_date()

        finished_daily_trade_date_list = self.pro.index_daily(ts_code='000001.SH', start_date=self.start_date,
                                                              end_date=self.latest_finished_trade_date,
                                                              fields='trade_date')
        finished_weekly_trade_date_list = self.pro.index_weekly(ts_code='000001.SH', start_date=self.start_date,
                                                                end_date=self.latest_finished_trade_date,
                                                                fields='trade_date')
        finished_monthly_trade_date_list = self.pro.index_monthly(ts_code='000001.SH', start_date=self.start_date,
                                                                  end_date=self.latest_finished_trade_date,
                                                                  fields='trade_date')

        self.finished_daily_trade_date_list = finished_daily_trade_date_list.iloc[::-1].reset_index(drop=True)
        self.finished_weekly_trade_date_list = finished_weekly_trade_date_list.iloc[::-1].reset_index(drop=True)
        self.finished_monthly_trade_date_list = finished_monthly_trade_date_list.iloc[::-1].reset_index(drop=True)

    def update(self):
        """
        更新当前实例的数据
        """
        self.__init__()

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
        :param days:多少天前？
        :return:日期 string 'YYYYMMDD'
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
            t = self.finished_daily_trade_date_list
        elif frequency == 'WEEKLY':
            t = self.finished_weekly_trade_date_list
        elif frequency == 'MONTHLY':
            t = self.finished_monthly_trade_date_list
        trade_date_list = t.drop(t[(t.trade_date < date1) | (t.trade_date > date2)].index)
        return trade_date_list.reset_index(drop=True)

    def get_latest_finished_trade_date(self):
        """
        获取最近的一个已经完成的交易日
        :return:  最近的一个已经完成的交易日 string 'YYYYMMDD'
        """
        today = self.today
        now = datetime.datetime.now()
        hour = now.strftime('%H')
        day_counter = 3
        if int(hour) < 20:
            today = self.get_date_before_today(1)
        while True:
            day = self.get_date_before_today(day_counter)
            trade_date_list = self.pro.query('trade_cal', is_open="1", start_date=day, end_date=today)
            if len(trade_date_list) != 0:
                latest_finished_trade_date = trade_date_list['cal_date'].values[-1]
                return latest_finished_trade_date
            day_counter += 3

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

    def get_trade_date_before(self, days, date='last', frequency='DAILY'):
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
        start_date = trade_date_list["trade_date"][length-days]
        return start_date

    def get_trade_date_list_forward(self, frequency, size):
        """
        获取一段时间内的交易日列表
        :param frequency: 日线 | 周线 | 月线
        :param size: 多少条记录？
        :return: 交易日列表 dataframe
        """
        if frequency == 'DAILY':
            return self.finished_daily_trade_date_list[-size:].reset_index(drop=True)
        elif frequency == 'WEEKLY':
            return self.finished_weekly_trade_date_list[-size:].reset_index(drop=True)
        elif frequency == 'MONTHLY':
            return self.finished_monthly_trade_date_list[-size:].reset_index(drop=True)
        else:
            return None


date_getter = DateGetter()


if __name__ == '__main__':
    print(date_getter.get_trade_days_between('20211008', '20211015', 'MONTHLY'))
