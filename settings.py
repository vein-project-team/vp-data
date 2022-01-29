DB_PATH = '.\\vein-project.db'
TABLE_NAMES = [
    'INDEX_LIST',
    'STOCK_LIST',
    'INDICES_DAILY',
    'INDICES_WEEKLY',
    'INDICES_MONTHLY',
    'QUOTATIONS_DAILY',
    'QUOTATIONS_WEEKLY',
    'QUOTATIONS_MONTHLY',
    'LIMITS_STATISTIC',
    'ADJ_FACTORS'
]

CREATION_SQL = {
    'INDEX_LIST': '''
    CREATE TABLE IF NOT EXISTS INDEX_LIST (
        INDEX_CODE CHAR(9) PRIMARY KEY,
        NAME TEXT,
        FULLNAME TEXT,
        MARKET CHAR(5),
        PUBLISHER TEXT,
        INDEX_TYPE TEXT, 
        CATEGORY TEXT, 
        BASE_DATE CHAR(8),
        BASE_POINT REAL,
        WEIGHT_RULE TEXT, 
        DESCRIPTION TEXT,
        LIST_DATE CHAR(8),
        EXP_DATE CHAR(8)
        
    );
    ''',
    'STOCK_LIST': '''
    CREATE TABLE IF NOT EXISTS STOCK_LIST (
        TS_CODE CHAR(9) PRIMARY KEY NOT NULL, -- 股票代码
        NAME TEXT NOT NULL, -- 股票名称
        CNSPELL CHAR(10), -- 拼音缩写
        EXCHANGE CHAR(4), -- 交易所代码
        MARKET CHAR(1), -- 市场类型，M: 主板，G: 创业板，K: 科创板
        AREA TEXT, -- 所属地区
        INDUSTRY TEXT, -- 所属行业
        STATUS CHAR(1) NOT NULL, -- 上市状态， L: 上市 D: 退市 P: 暂停上市
        LIST_DATE CHAR(8), -- 上市日期
        DELIST_DATE CHAR(8), -- 退市日期
        IS_HS CHAR(1) -- 是否沪深港通标的，N 否 H 沪股通 S 深股通
    );
    ''',
    'INDICES_DAILY': '''
    CREATE TABLE IF NOT EXISTS INDICES_DAILY (
        INDEX_CODE CHAR(9) NOT NULL,
        TRADE_DATE CHAR(8) NOT NULL,
        OPEN REAL,
        CLOSE REAL,
        LOW REAL,
        HIGH REAL,
        VOL REAL,
        PRIMARY KEY (INDEX_CODE, TRADE_DATE)
    );
    ''',
    'INDICES_WEEKLY': '''
    CREATE TABLE IF NOT EXISTS INDICES_WEEKLY (
        INDEX_CODE CHAR(9) NOT NULL,
        TRADE_DATE CHAR(8) NOT NULL,
        OPEN REAL,
        CLOSE REAL,
        LOW REAL,
        HIGH REAL,
        VOL REAL,
        PRIMARY KEY (INDEX_CODE, TRADE_DATE)
    );
    ''',
    'INDICES_MONTHLY': '''
    CREATE TABLE IF NOT EXISTS INDICES_MONTHLY (
        INDEX_CODE CHAR(9) NOT NULL,
        TRADE_DATE CHAR(8) NOT NULL,
        OPEN REAL,
        CLOSE REAL,
        LOW REAL,
        HIGH REAL,
        VOL REAL,
        PRIMARY KEY (INDEX_CODE, TRADE_DATE)
    );
    ''',
    'QUOTATIONS_DAILY': '''
    CREATE TABLE IF NOT EXISTS QUOTATIONS_DAILY (
        TS_CODE CHAR(9) NOT NULL, --股票代码
        TRADE_DATE CHAR(8) NOT NULL, --交易日
        OPEN REAL, -- 开盘价
        CLOSE REAL, -- 收盘价
        LOW REAL, -- 最低价
        HIGH REAL, -- 最高价
        PRE_CLOSE REAL, -- 昨收
        CHANGE REAL, -- 涨跌额
        VOL REAL, -- 成交量
        AMOUNT REAL, -- 成交额
        PRIMARY KEY (TS_CODE, TRADE_DATE)
    );
    ''',
    'QUOTATIONS_WEEKLY': '''
    CREATE TABLE IF NOT EXISTS QUOTATIONS_WEEKLY (
        TS_CODE CHAR(9) NOT NULL, --股票代码
        TRADE_DATE CHAR(8) NOT NULL, --交易日
        OPEN REAL, -- 开盘价
        CLOSE REAL, -- 收盘价
        LOW REAL, -- 最低价
        HIGH REAL, -- 最高价
        PRE_CLOSE REAL, -- 昨收
        CHANGE REAL, -- 涨跌额
        VOL REAL, -- 成交量
        AMOUNT REAL, -- 成交额
        PRIMARY KEY (TS_CODE, TRADE_DATE)
    );
    ''',
    'QUOTATIONS_MONTHLY': '''
    CREATE TABLE IF NOT EXISTS QUOTATIONS_MONTHLY (
        TS_CODE CHAR(9) NOT NULL, --股票代码
        TRADE_DATE CHAR(8) NOT NULL, --交易日
        OPEN REAL, -- 开盘价
        CLOSE REAL, -- 收盘价
        LOW REAL, -- 最低价
        HIGH REAL, -- 最高价
        PRE_CLOSE REAL, -- 昨收
        CHANGE REAL, -- 涨跌额
        VOL REAL, -- 成交量
        AMOUNT REAL, -- 成交额
        PRIMARY KEY (TS_CODE, TRADE_DATE)
    );
    ''',
    'LIMITS_STATISTIC': '''
    CREATE TABLE IF NOT EXISTS LIMITS_STATISTIC (
        TS_CODE CHAR(9) NOT NULL, --股票代码
        TRADE_DATE CHAR(8) NOT NULL, --交易日
        LIMIT_TYPE CHAR(1), --限制类型
        FIRST_TIME CHAR(8), --首限时间
        LAST_TIME CHAR(8), --终限时间
        OPEN_TIMES REAL, --打开次数
        FD_AMOUNT REAL, --封单金额
        PRIMARY KEY (TS_CODE, TRADE_DATE)
    );
    ''',
    'ADJ_FACTORS': '''
    CREATE TABLE IF NOT EXISTS ADJ_FACTORS (
        TS_CODE CHAR(9) NOT NULL, --股票代码
        TRADE_DATE CHAR(8) NOT NULL, --交易日
        ADJ_FACTOR REAL, --复权因子
        PRIMARY KEY (TS_CODE, TRADE_DATE)
    );
    '''
}

TABLES_NEED_FILL = [
    'INDEX_LIST',
    'STOCK_LIST',
    'INDICES_DAILY',
    'INDICES_WEEKLY',
    'INDICES_MONTHLY',
    'QUOTATIONS_DAILY',
    'QUOTATIONS_WEEKLY',
    'QUOTATIONS_MONTHLY',
    'LIMITS_STATISTIC',
    'ADJ_FACTORS'
]

TABLES_NEED_UPDATE = [
    'QUOTATIONS_DAILY',
    'QUOTATIONS_WEEKLY',
    'QUOTATIONS_MONTHLY',
    'LIMITS_STATISTIC',
    'ADJ_FACTORS'
]
