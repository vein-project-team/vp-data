from analysis.DailyReportGener import daily_report_gener
from analysis.StockListGener import stock_list_gener

if __name__ == "__main__":
    print(daily_report_gener._gen_limits('U', '20220620'))