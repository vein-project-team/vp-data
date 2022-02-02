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
    ''',
    'INCOME_STATEMENTS':'''
    CREATE TABLE IF NOT EXISTS INCOME_STATEMENT (
        TS_CODE CHAR(9) NOT NULL, -- 股票代码
        ANN_DATE CHAR(8), -- 公告日期
        F_ANN_DATE CHAR(8), -- 实际公告日期
        END_DATE CHAR(8) NOT NULL, -- 报告期结束日期
        REPORT_TYPE CHAR(2) NOT NULL, -- 报告类型
        COMP_TYPE CHAR(1), -- 公司类型
        END_TYPE CHAR(1), -- 报告期编码
        BASIC_EPS REAL, -- 基本每股收益
        DILUTED_EPS REAL, -- 稀释每股收益
        TOTAL_REVENUE REAL, -- 营业总收入
        REVENUE REAL, -- 营业收入
        INT_INCOME REAL, -- 利息收入
        PREM_EARNED REAL, -- 已赚保费
        COMM_INCOME REAL, -- 手续费及佣金收入
        N_COMMIS_INCOME REAL, -- 手续费及佣金净收入
        N_OTH_INCOME REAL, -- 其他经营净收益
        N_OTH_B_INCOME REAL, -- 加:其他业务净收益
        PREM_INCOME REAL, -- 保险业务收入
        OUT_PREM REAL, -- 减:分出保费
        UNE_PREM_RESER REAL, -- 提取未到期责任准备金
        REINS_INCOME REAL, -- 其中:分保费收入
        N_SEC_TB_INCOME REAL, -- 代理买卖证券业务净收入
        N_SEC_UW_INCOME REAL, -- 证券承销业务净收入
        N_ASSET_MG_INCOME REAL, -- 受托客户资产管理业务净收入
        OTH_B_INCOME REAL, -- 其他业务收入
        FV_VALUE_CHG_GAIN REAL, -- 加:公允价值变动净收益
        INVEST_INCOME REAL, -- 加:投资净收益
        ASS_INVEST_INCOME REAL, -- 其中:对联营企业和合营企业的投资收益
        FOREX_GAIN REAL, -- 加:汇兑净收益
        TOTAL_COGS REAL, -- 营业总成本
        OPER_COST REAL, -- 减:营业成本
        INT_EXP REAL, -- 减:利息支出
        COMM_EXP REAL, -- 减:手续费及佣金支出
        BIZ_TAX_SURCHG REAL, -- 减:营业税金及附加
        SELL_EXP REAL, -- 减:销售费用
        ADMIN_EXP REAL, -- 减:管理费用
        FIN_EXP REAL, -- 减:财务费用
        ASSETS_IMPAIR_LOSS REAL, -- 减:资产减值损失
        PREM_REFUND REAL, -- 退保金
        COMPENS_PAYOUT REAL, -- 赔付总支出
        RESER_INSUR_LIAB REAL, -- 提取保险责任准备金
        DIV_PAYT REAL, -- 保户红利支出
        REINS_EXP REAL, -- 分保费用
        OPER_EXP REAL, -- 营业支出
        COMPENS_PAYOUT_REFU REAL, -- 减:摊回赔付支出
        INSUR_RESER_REFU REAL, -- 减:摊回保险责任准备金
        REINS_COST_REFUND REAL, -- 减:摊回分保费用
        OTHER_BUS_COST REAL, -- 其他业务成本
        OPERATE_PROFIT REAL, -- 营业利润
        NON_OPER_INCOME REAL, -- 加:营业外收入
        NON_OPER_EXP REAL, -- 减:营业外支出
        NCA_DISPLOSS REAL, -- 其中:减:非流动资产处置净损失
        TOTAL_PROFIT REAL, -- 利润总额
        INCOME_TAX REAL, -- 所得税费用
        N_INCOME REAL, -- 净利润(含少数股东损益)
        N_INCOME_ATTR_P REAL, -- 净利润(不含少数股东损益)
        MINORITY_GAIN REAL, -- 少数股东损益
        OTH_COMPR_INCOME REAL, -- 其他综合收益
        T_COMPR_INCOME REAL, -- 综合收益总额
        COMPR_INC_ATTR_P REAL, -- 归属于母公司(或股东)的综合收益总额
        COMPR_INC_ATTR_M_S REAL, -- 归属于少数股东的综合收益总额
        EBIT REAL, -- 息税前利润
        EBITDA REAL, -- 息税折旧摊销前利润
        INSURANCE_EXP REAL, -- 保险业务支出
        UNDIST_PROFIT REAL, -- 年初未分配利润
        DISTABLE_PROFIT REAL, -- 可分配利润
        RD_EXP REAL, -- 研发费用
        FIN_EXP_INT_EXP REAL, -- 财务费用:利息费用
        FIN_EXP_INT_INC REAL, -- 财务费用:利息收入
        TRANSFER_SURPLUS_RESE REAL, -- 盈余公积转入
        TRANSFER_HOUSING_IMPREST REAL, -- 住房周转金转入
        TRANSFER_OTH REAL, -- 其他转入
        ADJ_LOSSGAIN REAL, -- 调整以前年度损益
        WITHDRA_LEGAL_SURPLUS REAL, -- 提取法定盈余公积
        WITHDRA_LEGAL_PUBFUND REAL, -- 提取法定公益金
        WITHDRA_BIZ_DEVFUND REAL, -- 提取企业发展基金
        WITHDRA_RESE_FUND REAL, -- 提取储备基金
        WITHDRA_OTH_ERSU REAL, -- 提取任意盈余公积金
        WORKERS_WELFARE REAL, -- 职工奖金福利
        DISTR_PROFIT_SHRHDER REAL, -- 可供股东分配的利润
        PRFSHARE_PAYABLE_DVD REAL, -- 应付优先股股利
        COMSHARE_PAYABLE_DVD REAL, -- 应付普通股股利
        CAPIT_COMSTOCK_DIV REAL, -- 转作股本的普通股股利
        NET_AFTER_NR_LP_CORRECT REAL, -- 扣除非经常性损益后的净利润（更正前）
        CREDIT_IMPA_LOSS REAL, -- 信用减值损失
        NET_EXPO_HEDGING_BENEFITS REAL, -- 净敞口套期收益
        OTH_IMPAIR_LOSS_ASSETS REAL, -- 其他资产减值损失
        TOTAL_OPCOST REAL, -- 营业总成本（二）
        AMODCOST_FIN_ASSETS REAL, -- 以摊余成本计量的金融资产终止确认收益
        OTH_INCOME REAL, -- 其他收益
        ASSET_DISP_INCOME REAL, -- 资产处置收益
        CONTINUED_NET_PROFIT REAL, -- 持续经营净利润
        END_NET_PROFIT REAL, -- 终止经营净利润
        PRIMARY KEY (TS_CODE, END_DATE, REPORT_TYPE)
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
    'ADJ_FACTORS',
    'INCOME_STATEMENTS'
]

TABLES_NEED_UPDATE = [
    'QUOTATIONS_DAILY',
    'QUOTATIONS_WEEKLY',
    'QUOTATIONS_MONTHLY',
    'LIMITS_STATISTIC',
    'ADJ_FACTORS',
    'INCOME_STATEMENTS'
]
