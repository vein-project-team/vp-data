from data_source import local_source

if __name__ == "__main__":
    data = local_source.get_quotations_daily(
        cols="TRADE_DATE, TS_CODE, PRE_CLOSE, CLOSE, ((CLOSE - PRE_CLOSE) / PRE_CLOSE) AS CHANGE",
        condition="TRADE_DATE = '20211108' AND ((CLOSE - PRE_CLOSE) / PRE_CLOSE) < -0.07"
    )
    print(data)