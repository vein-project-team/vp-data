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
    'ADJ_FACTORS',
    'INCOME_STATEMENTS',
    'BALANCE_SHEETS',
    'STATEMENTS_OF_CASH_FLOWS',
    'INCOME_FORECASTS',
    'FINANCIAL_INDICATORS'
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
    CREATE TABLE IF NOT EXISTS INCOME_STATEMENTS (
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
    ''',
    'BALANCE_SHEETS':'''
    CREATE TABLE IF NOT EXISTS BALANCE_SHEETS (
        TS_CODE CHAR(9) NOT NULL, -- 股票代码
        ANN_DATE CHAR(8), -- 公告日期
        F_ANN_DATE CHAR(8), -- 实际公告日期
        END_DATE CHAR(8) NOT NULL, -- 报告期结束日期
        REPORT_TYPE CHAR(2) NOT NULL, -- 报告类型
        COMP_TYPE CHAR(1), -- 公司类型
        END_TYPE CHAR(1), -- 报告期编码      
        TOTAL_SHARE REAL, -- 期末总股本
        CAP_RESE REAL, -- 资本公积金
        UNDISTR_PORFIT REAL, -- 未分配利润
        SURPLUS_RESE REAL, -- 盈余公积金
        SPECIAL_RESE REAL, -- 专项储备
        MONEY_CAP REAL, -- 货币资金
        TRAD_ASSET REAL, -- 交易性金融资产
        NOTES_RECEIV REAL, -- 应收票据
        ACCOUNTS_RECEIV REAL, -- 应收账款
        OTH_RECEIV REAL, -- 其他应收款
        PREPAYMENT REAL, -- 预付款项
        DIV_RECEIV REAL, -- 应收股利
        INT_RECEIV REAL, -- 应收利息
        INVENTORIES REAL, -- 存货
        AMOR_EXP REAL, -- 长期待摊费用
        NCA_WITHIN_1Y REAL, -- 一年内到期的非流动资产
        SETT_RSRV REAL, -- 结算备付金
        LOANTO_OTH_BANK_FI REAL, -- 拆出资金
        PREMIUM_RECEIV REAL, -- 应收保费
        REINSUR_RECEIV REAL, -- 应收分保账款
        REINSUR_RES_RECEIV REAL, -- 应收分保合同准备金
        PUR_RESALE_FA REAL, -- 买入返售金融资产
        OTH_CUR_ASSETS REAL, -- 其他流动资产
        TOTAL_CUR_ASSETS REAL, -- 流动资产合计
        FA_AVAIL_FOR_SALE REAL, -- 可供出售金融资产
        HTM_INVEST REAL, -- 持有至到期投资
        LT_EQT_INVEST REAL, -- 长期股权投资
        INVEST_REAL_ESTATE REAL, -- 投资性房地产
        TIME_DEPOSITS REAL, -- 定期存款
        OTH_ASSETS REAL, -- 其他资产
        LT_REC REAL, -- 长期应收款
        FIX_ASSETS REAL, -- 固定资产
        CIP REAL, -- 在建工程
        CONST_MATERIALS REAL, -- 工程物资
        FIXED_ASSETS_DISP REAL, -- 固定资产清理
        PRODUC_BIO_ASSETS REAL, -- 生产性生物资产
        OIL_AND_GAS_ASSETS REAL, -- 油气资产
        INTAN_ASSETS REAL, -- 无形资产
        R_AND_D REAL, -- 研发支出
        GOODWILL REAL, -- 商誉
        LT_AMOR_EXP REAL, -- 长期待摊费用
        DEFER_TAX_ASSETS REAL, -- 递延所得税资产
        DECR_IN_DISBUR REAL, -- 发放贷款及垫款
        OTH_NCA REAL, -- 其他非流动资产
        TOTAL_NCA REAL, -- 非流动资产合计
        CASH_RESER_CB REAL, -- 现金及存放中央银行款项
        DEPOS_IN_OTH_BFI REAL, -- 存放同业和其它金融机构款项
        PREC_METALS REAL, -- 贵金属
        DERIV_ASSETS REAL, -- 衍生金融资产
        RR_REINS_UNE_PREM REAL, -- 应收分保未到期责任准备金
        RR_REINS_OUTSTD_CLA REAL, -- 应收分保未决赔款准备金
        RR_REINS_LINS_LIAB REAL, -- 应收分保寿险责任准备金
        RR_REINS_LTHINS_LIAB REAL, -- 应收分保长期健康险责任准备金
        REFUND_DEPOS REAL, -- 存出保证金
        PH_PLEDGE_LOANS REAL, -- 保户质押贷款
        REFUND_CAP_DEPOS REAL, -- 存出资本保证金
        INDEP_ACCT_ASSETS REAL, -- 独立账户资产
        CLIENT_DEPOS REAL, -- 其中：客户资金存款
        CLIENT_PROV REAL, -- 其中：客户备付金
        TRANSAC_SEAT_FEE REAL, -- 其中:交易席位费
        INVEST_AS_RECEIV REAL, -- 应收款项类投资
        TOTAL_ASSETS REAL, -- 资产总计
        LT_BORR REAL, -- 长期借款
        ST_BORR REAL, -- 短期借款
        CB_BORR REAL, -- 向中央银行借款
        DEPOS_IB_DEPOSITS REAL, -- 吸收存款及同业存放
        LOAN_OTH_BANK REAL, -- 拆入资金
        TRADING_FL REAL, -- 交易性金融负债
        NOTES_PAYABLE REAL, -- 应付票据
        ACCT_PAYABLE REAL, -- 应付账款
        ADV_RECEIPTS REAL, -- 预收款项
        SOLD_FOR_REPUR_FA REAL, -- 卖出回购金融资产款
        COMM_PAYABLE REAL, -- 应付手续费及佣金
        PAYROLL_PAYABLE REAL, -- 应付职工薪酬
        TAXES_PAYABLE REAL, -- 应交税费
        INT_PAYABLE REAL, -- 应付利息
        DIV_PAYABLE REAL, -- 应付股利
        OTH_PAYABLE REAL, -- 其他应付款
        ACC_EXP REAL, -- 预提费用
        DEFERRED_INC REAL, -- 递延收益
        ST_BONDS_PAYABLE REAL, -- 应付短期债券
        PAYABLE_TO_REINSURER REAL, -- 应付分保账款
        RSRV_INSUR_CONT REAL, -- 保险合同准备金
        ACTING_TRADING_SEC REAL, -- 代理买卖证券款
        ACTING_UW_SEC REAL, -- 代理承销证券款
        NON_CUR_LIAB_DUE_1Y REAL, -- 一年内到期的非流动负债
        OTH_CUR_LIAB REAL, -- 其他流动负债
        TOTAL_CUR_LIAB REAL, -- 流动负债合计
        BOND_PAYABLE REAL, -- 应付债券
        LT_PAYABLE REAL, -- 长期应付款
        SPECIFIC_PAYABLES REAL, -- 专项应付款
        ESTIMATED_LIAB REAL, -- 预计负债
        DEFER_TAX_LIAB REAL, -- 递延所得税负债
        DEFER_INC_NON_CUR_LIAB REAL, -- 递延收益-非流动负债
        OTH_NCL REAL, -- 其他非流动负债
        TOTAL_NCL REAL, -- 非流动负债合计
        DEPOS_OTH_BFI REAL, -- 同业和其它金融机构存放款项
        DERIV_LIAB REAL, -- 衍生金融负债
        DEPOS REAL, -- 吸收存款
        AGENCY_BUS_LIAB REAL, -- 代理业务负债
        OTH_LIAB REAL, -- 其他负债
        PREM_RECEIV_ADVA REAL, -- 预收保费
        DEPOS_RECEIVED REAL, -- 存入保证金
        PH_INVEST REAL, -- 保户储金及投资款
        RESER_UNE_PREM REAL, -- 未到期责任准备金
        RESER_OUTSTD_CLAIMS REAL, -- 未决赔款准备金
        RESER_LINS_LIAB REAL, -- 寿险责任准备金
        RESER_LTHINS_LIAB REAL, -- 长期健康险责任准备金
        INDEPT_ACC_LIAB REAL, -- 独立账户负债
        PLEDGE_BORR REAL, -- 其中:质押借款
        INDEM_PAYABLE REAL, -- 应付赔付款
        POLICY_DIV_PAYABLE REAL, -- 应付保单红利
        TOTAL_LIAB REAL, -- 负债合计
        TREASURY_SHARE REAL, -- 减:库存股
        ORDIN_RISK_RESER REAL, -- 一般风险准备
        FOREX_DIFFER REAL, -- 外币报表折算差额
        INVEST_LOSS_UNCONF REAL, -- 未确认的投资损失
        MINORITY_INT REAL, -- 少数股东权益
        TOTAL_HLDR_EQY_EXC_MIN_INT REAL, -- 股东权益合计(不含少数股东权益)
        TOTAL_HLDR_EQY_INC_MIN_INT REAL, -- 股东权益合计(含少数股东权益)
        TOTAL_LIAB_HLDR_EQY REAL, -- 负债及股东权益总计
        LT_PAYROLL_PAYABLE REAL, -- 长期应付职工薪酬
        OTH_COMP_INCOME REAL, -- 其他综合收益
        OTH_EQT_TOOLS REAL, -- 其他权益工具
        OTH_EQT_TOOLS_P_SHR REAL, -- 其他权益工具(优先股)
        LENDING_FUNDS REAL, -- 融出资金
        ACC_RECEIVABLE REAL, -- 应收款项
        ST_FIN_PAYABLE REAL, -- 应付短期融资款
        PAYABLES REAL, -- 应付款项
        HFS_ASSETS REAL, -- 持有待售的资产
        HFS_SALES REAL, -- 持有待售的负债
        COST_FIN_ASSETS REAL, -- 以摊余成本计量的金融资产
        FAIR_VALUE_FIN_ASSETS REAL, -- 以公允价值计量且其变动计入其他综合收益的金融资产
        CIP_TOTAL REAL, -- 在建工程(合计)(元)
        OTH_PAY_TOTAL REAL, -- 其他应付款(合计)(元)
        LONG_PAY_TOTAL REAL, -- 长期应付款(合计)(元)
        DEBT_INVEST REAL, -- 债权投资(元)
        OTH_DEBT_INVEST REAL, -- 其他债权投资(元)
        OTH_EQ_INVEST REAL, -- 其他权益工具投资(元)
        OTH_ILLIQ_FIN_ASSETS REAL, -- 其他非流动金融资产(元)
        OTH_EQ_PPBOND REAL, -- 其他权益工具:永续债(元)
        RECEIV_FINANCING REAL, -- 应收款项融资
        USE_RIGHT_ASSETS REAL, -- 使用权资产
        LEASE_LIAB REAL, -- 租赁负债
        CONTRACT_ASSETS REAL, -- 合同资产
        CONTRACT_LIAB REAL, -- 合同负债
        ACCOUNTS_RECEIV_BILL REAL, -- 应收票据及应收账款
        ACCOUNTS_PAY REAL, -- 应付票据及应付账款
        OTH_RCV_TOTAL REAL, -- 其他应收款(合计)（元）
        FIX_ASSETS_TOTAL REAL, -- 固定资产(合计)(元)
        PRIMARY KEY (TS_CODE, END_DATE, REPORT_TYPE)
    );
    ''',
    'STATEMENTS_OF_CASH_FLOWS':'''
    CREATE TABLE IF NOT EXISTS STATEMENTS_OF_CASH_FLOWS (
        TS_CODE CHAR(9) NOT NULL, -- 股票代码
        ANN_DATE CHAR(8), -- 公告日期
        F_ANN_DATE CHAR(8), -- 实际公告日期
        END_DATE CHAR(8) NOT NULL, -- 报告期结束日期
        REPORT_TYPE CHAR(2) NOT NULL, -- 报告类型
        COMP_TYPE CHAR(1), -- 公司类型
        END_TYPE CHAR(1), -- 报告期编码
        NET_PROFIT REAL, -- 净利润
        FINAN_EXP REAL, -- 财务费用
        C_FR_SALE_SG REAL, -- 销售商品、提供劳务收到的现金
        RECP_TAX_RENDS REAL, -- 收到的税费返还
        N_DEPOS_INCR_FI REAL, -- 客户存款和同业存放款项净增加额
        N_INCR_LOANS_CB REAL, -- 向中央银行借款净增加额
        N_INC_BORR_OTH_FI REAL, -- 向其他金融机构拆入资金净增加额
        PREM_FR_ORIG_CONTR REAL, -- 收到原保险合同保费取得的现金
        N_INCR_INSURED_DEP REAL, -- 保户储金净增加额
        N_REINSUR_PREM REAL, -- 收到再保业务现金净额
        N_INCR_DISP_TFA REAL, -- 处置交易性金融资产净增加额
        IFC_CASH_INCR REAL, -- 收取利息和手续费净增加额
        N_INCR_DISP_FAAS REAL, -- 处置可供出售金融资产净增加额
        N_INCR_LOANS_OTH_BANK REAL, -- 拆入资金净增加额
        N_CAP_INCR_REPUR REAL, -- 回购业务资金净增加额
        C_FR_OTH_OPERATE_A REAL, -- 收到其他与经营活动有关的现金
        C_INF_FR_OPERATE_A REAL, -- 经营活动现金流入小计
        C_PAID_GOODS_S REAL, -- 购买商品、接受劳务支付的现金
        C_PAID_TO_FOR_EMPL REAL, -- 支付给职工以及为职工支付的现金
        C_PAID_FOR_TAXES REAL, -- 支付的各项税费
        N_INCR_CLT_LOAN_ADV REAL, -- 客户贷款及垫款净增加额
        N_INCR_DEP_CBOB REAL, -- 存放央行和同业款项净增加额
        C_PAY_CLAIMS_ORIG_INCO REAL, -- 支付原保险合同赔付款项的现金
        PAY_HANDLING_CHRG REAL, -- 支付手续费的现金
        PAY_COMM_INSUR_PLCY REAL, -- 支付保单红利的现金
        OTH_CASH_PAY_OPER_ACT REAL, -- 支付其他与经营活动有关的现金
        ST_CASH_OUT_ACT REAL, -- 经营活动现金流出小计
        N_CASHFLOW_ACT REAL, -- 经营活动产生的现金流量净额
        OTH_RECP_RAL_INV_ACT REAL, -- 收到其他与投资活动有关的现金
        C_DISP_WITHDRWL_INVEST REAL, -- 收回投资收到的现金
        C_RECP_RETURN_INVEST REAL, -- 取得投资收益收到的现金
        N_RECP_DISP_FIOLTA REAL, -- 处置固定资产、无形资产和其他长期资产收回的现金净额
        N_RECP_DISP_SOBU REAL, -- 处置子公司及其他营业单位收到的现金净额
        STOT_INFLOWS_INV_ACT REAL, -- 投资活动现金流入小计
        C_PAY_ACQ_CONST_FIOLTA REAL, -- 购建固定资产、无形资产和其他长期资产支付的现金
        C_PAID_INVEST REAL, -- 投资支付的现金
        N_DISP_SUBS_OTH_BIZ REAL, -- 取得子公司及其他营业单位支付的现金净额
        OTH_PAY_RAL_INV_ACT REAL, -- 支付其他与投资活动有关的现金
        N_INCR_PLEDGE_LOAN REAL, -- 质押贷款净增加额
        STOT_OUT_INV_ACT REAL, -- 投资活动现金流出小计
        N_CASHFLOW_INV_ACT REAL, -- 投资活动产生的现金流量净额
        C_RECP_BORROW REAL, -- 取得借款收到的现金
        PROC_ISSUE_BONDS REAL, -- 发行债券收到的现金
        OTH_CASH_RECP_RAL_FNC_ACT REAL, -- 收到其他与筹资活动有关的现金
        STOT_CASH_IN_FNC_ACT REAL, -- 筹资活动现金流入小计
        FREE_CASHFLOW REAL, -- 企业自由现金流量
        C_PREPAY_AMT_BORR REAL, -- 偿还债务支付的现金
        C_PAY_DIST_DPCP_INT_EXP REAL, -- 分配股利、利润或偿付利息支付的现金
        INCL_DVD_PROFIT_PAID_SC_MS REAL, -- 其中:子公司支付给少数股东的股利、利润
        OTH_CASHPAY_RAL_FNC_ACT REAL, -- 支付其他与筹资活动有关的现金
        STOT_CASHOUT_FNC_ACT REAL, -- 筹资活动现金流出小计
        N_CASH_FLOWS_FNC_ACT REAL, -- 筹资活动产生的现金流量净额
        EFF_FX_FLU_CASH REAL, -- 汇率变动对现金的影响
        N_INCR_CASH_CASH_EQU REAL, -- 现金及现金等价物净增加额
        C_CASH_EQU_BEG_PERIOD REAL, -- 期初现金及现金等价物余额
        C_CASH_EQU_END_PERIOD REAL, -- 期末现金及现金等价物余额
        C_RECP_CAP_CONTRIB REAL, -- 吸收投资收到的现金
        INCL_CASH_REC_SAIMS REAL, -- 其中:子公司吸收少数股东投资收到的现金
        UNCON_INVEST_LOSS REAL, -- 未确认投资损失
        PROV_DEPR_ASSETS REAL, -- 加:资产减值准备
        DEPR_FA_COGA_DPBA REAL, -- 固定资产折旧、油气资产折耗、生产性生物资产折旧
        AMORT_INTANG_ASSETS REAL, -- 无形资产摊销
        LT_AMORT_DEFERRED_EXP REAL, -- 长期待摊费用摊销
        DECR_DEFERRED_EXP REAL, -- 待摊费用减少
        INCR_ACC_EXP REAL, -- 预提费用增加
        LOSS_DISP_FIOLTA REAL, -- 处置固定、无形资产和其他长期资产的损失
        LOSS_SCR_FA REAL, -- 固定资产报废损失
        LOSS_FV_CHG REAL, -- 公允价值变动损失
        INVEST_LOSS REAL, -- 投资损失
        DECR_DEF_INC_TAX_ASSETS REAL, -- 递延所得税资产减少
        INCR_DEF_INC_TAX_LIAB REAL, -- 递延所得税负债增加
        DECR_INVENTORIES REAL, -- 存货的减少
        DECR_OPER_PAYABLE REAL, -- 经营性应收项目的减少
        INCR_OPER_PAYABLE REAL, -- 经营性应付项目的增加
        OTHERS REAL, -- 其他
        IM_NET_CASHFLOW_OPER_ACT REAL, -- 经营活动产生的现金流量净额(间接法)
        CONV_DEBT_INTO_CAP REAL, -- 债务转为资本
        CONV_COPBONDS_DUE_WITHIN_1Y REAL, -- 一年内到期的可转换公司债券
        FA_FNC_LEASES REAL, -- 融资租入固定资产
        IM_N_INCR_CASH_EQU REAL, -- 现金及现金等价物净增加额(间接法)
        NET_DISM_CAPITAL_ADD REAL, -- 拆出资金净增加额
        NET_CASH_RECE_SEC REAL, -- 代理买卖证券收到的现金净额(元)
        CREDIT_IMPA_LOSS REAL, -- 信用减值损失
        USE_RIGHT_ASSET_DEP REAL, -- 使用权资产折旧
        OTH_LOSS_ASSET REAL, -- 其他资产减值损失
        END_BAL_CASH REAL, -- 现金的期末余额
        BEG_BAL_CASH REAL, -- 减:现金的期初余额
        END_BAL_CASH_EQU REAL, -- 加:现金等价物的期末余额
        BEG_BAL_CASH_EQU REAL, -- 减:现金等价物的期初余额
        PRIMARY KEY (TS_CODE, END_DATE, REPORT_TYPE)
    );
    ''',
    'INCOME_FORECASTS':'''
    CREATE TABLE IF NOT EXISTS INCOME_FORECASTS (
        TS_CODE CHAR(9) NOT NULL, -- 股票代码
        ANN_DATE CHAR(8), -- 公告日期
        END_DATE CHAR(8) NOT NULL, -- 报告期结束日期
        TYPE CHAR(4), -- 报告类型
        P_CHANGE_MIN REAL, -- 预告净利润变动幅度下限（%）
        P_CHANGE_MAX REAL, -- 预告净利润变动幅度上限（%）
        NET_PROFIT_MIN REAL, -- 预告净利润下限（万元）
        NET_PROFIT_MAX REAL, -- 预告净利润上限（万元）
        LAST_PARENT_NET REAL, -- 上年同期归属母公司净利润
        FIRST_ANN_DATE CHAR(8), -- 首次公告日
        SUMMARY TEXT, -- 业绩预告摘要
        CHANGE_REASON TEXT, -- 业绩变动原因
        PRIMARY KEY (TS_CODE, END_DATE)
    );
    ''',
    'FINANCIAL_INDICATORS':'''
    CREATE TABLE IF NOT EXISTS FINANCIAL_INDICATORS (    
        TS_CODE CHAR(9) NOT NULL, -- 股票代码
        ANN_DATE CHAR(8), -- 公告日期
        END_DATE CHAR(8) NOT NULL, -- 报告期结束日期
        EPS REAL, -- 基本每股收益
        DT_EPS REAL, -- 稀释每股收益
        TOTAL_REVENUE_PS REAL, -- 每股营业总收入
        REVENUE_PS REAL, -- 每股营业收入
        CAPITAL_RESE_PS REAL, -- 每股资本公积
        SURPLUS_RESE_PS REAL, -- 每股盈余公积
        UNDIST_PROFIT_PS REAL, -- 每股未分配利润
        EXTRA_ITEM REAL, -- 非经常性损益
        PROFIT_DEDT REAL, -- 扣除非经常性损益后的净利润（扣非净利润）
        GROSS_MARGIN REAL, -- 毛利
        CURRENT_RATIO REAL, -- 流动比率
        QUICK_RATIO REAL, -- 速动比率
        CASH_RATIO REAL, -- 保守速动比率
        INVTURN_DAYS REAL, -- 存货周转天数
        ARTURN_DAYS REAL, -- 应收账款周转天数
        INV_TURN REAL, -- 存货周转率
        AR_TURN REAL, -- 应收账款周转率
        CA_TURN REAL, -- 流动资产周转率
        FA_TURN REAL, -- 固定资产周转率
        ASSETS_TURN REAL, -- 总资产周转率
        OP_INCOME REAL, -- 经营活动净收益
        VALUECHANGE_INCOME REAL, -- 价值变动净收益
        INTERST_INCOME REAL, -- 利息费用
        DAA REAL, -- 折旧与摊销
        EBIT REAL, -- 息税前利润
        EBITDA REAL, -- 息税折旧摊销前利润
        FCFF REAL, -- 企业自由现金流量
        FCFE REAL, -- 股权自由现金流量
        CURRENT_EXINT REAL, -- 无息流动负债
        NONCURRENT_EXINT REAL, -- 无息非流动负债
        INTERESTDEBT REAL, -- 带息债务
        NETDEBT REAL, -- 净债务
        TANGIBLE_ASSET REAL, -- 有形资产
        WORKING_CAPITAL REAL, -- 营运资金
        NETWORKING_CAPITAL REAL, -- 营运流动资本
        INVEST_CAPITAL REAL, -- 全部投入资本
        RETAINED_EARNINGS REAL, -- 留存收益
        DILUTED2_EPS REAL, -- 期末摊薄每股收益
        BPS REAL, -- 每股净资产
        OCFPS REAL, -- 每股经营活动产生的现金流量净额
        RETAINEDPS REAL, -- 每股留存收益
        CFPS REAL, -- 每股现金流量净额
        EBIT_PS REAL, -- 每股息税前利润
        FCFF_PS REAL, -- 每股企业自由现金流量
        FCFE_PS REAL, -- 每股股东自由现金流量
        NETPROFIT_MARGIN REAL, -- 销售净利率
        GROSSPROFIT_MARGIN REAL, -- 销售毛利率
        COGS_OF_SALES REAL, -- 销售成本率
        EXPENSE_OF_SALES REAL, -- 销售期间费用率
        PROFIT_TO_GR REAL, -- 净利润/营业总收入
        SALEEXP_TO_GR REAL, -- 销售费用/营业总收入
        ADMINEXP_OF_GR REAL, -- 管理费用/营业总收入
        FINAEXP_OF_GR REAL, -- 财务费用/营业总收入
        IMPAI_TTM REAL, -- 资产减值损失/营业总收入
        GC_OF_GR REAL, -- 营业总成本/营业总收入
        OP_OF_GR REAL, -- 营业利润/营业总收入
        EBIT_OF_GR REAL, -- 息税前利润/营业总收入
        ROE REAL, -- 净资产收益率
        ROE_WAA REAL, -- 加权平均净资产收益率
        ROE_DT REAL, -- 净资产收益率(扣除非经常损益)
        ROA REAL, -- 总资产报酬率
        NPTA REAL, -- 总资产净利润
        ROIC REAL, -- 投入资本回报率
        ROE_YEARLY REAL, -- 年化净资产收益率
        ROA2_YEARLY REAL, -- 年化总资产报酬率
        ROE_AVG REAL, -- 平均净资产收益率(增发条件)
        OPINCOME_OF_EBT REAL, -- 经营活动净收益/利润总额
        INVESTINCOME_OF_EBT REAL, -- 价值变动净收益/利润总额
        N_OP_PROFIT_OF_EBT REAL, -- 营业外收支净额/利润总额
        TAX_TO_EBT REAL, -- 所得税/利润总额
        DTPROFIT_TO_PROFIT REAL, -- 扣除非经常损益后的净利润/净利润
        SALESCASH_TO_OR REAL, -- 销售商品提供劳务收到的现金/营业收入
        OCF_TO_OR REAL, -- 经营活动产生的现金流量净额/营业收入
        OCF_TO_OPINCOME REAL, -- 经营活动产生的现金流量净额/经营活动净收益
        CAPITALIZED_TO_DA REAL, -- 资本支出/折旧和摊销
        DEBT_TO_ASSETS REAL, -- 资产负债率
        ASSETS_TO_EQT REAL, -- 权益乘数
        DP_ASSETS_TO_EQT REAL, -- 权益乘数(杜邦分析)
        CA_TO_ASSETS REAL, -- 流动资产/总资产
        NCA_TO_ASSETS REAL, -- 非流动资产/总资产
        TBASSETS_TO_TOTALASSETS REAL, -- 有形资产/总资产
        INT_TO_TALCAP REAL, -- 带息债务/全部投入资本
        EQT_TO_TALCAPITAL REAL, -- 归属于母公司的股东权益/全部投入资本
        CURRENTDEBT_TO_DEBT REAL, -- 流动负债/负债合计
        LONGDEB_TO_DEBT REAL, -- 非流动负债/负债合计
        OCF_TO_SHORTDEBT REAL, -- 经营活动产生的现金流量净额/流动负债
        DEBT_TO_EQT REAL, -- 产权比率
        EQT_TO_DEBT REAL, -- 归属于母公司的股东权益/负债合计
        EQT_TO_INTERESTDEBT REAL, -- 归属于母公司的股东权益/带息债务
        TANGIBLEASSET_TO_DEBT REAL, -- 有形资产/负债合计
        TANGASSET_TO_INTDEBT REAL, -- 有形资产/带息债务
        TANGIBLEASSET_TO_NETDEBT REAL, -- 有形资产/净债务
        OCF_TO_DEBT REAL, -- 经营活动产生的现金流量净额/负债合计
        OCF_TO_INTERESTDEBT REAL, -- 经营活动产生的现金流量净额/带息债务
        OCF_TO_NETDEBT REAL, -- 经营活动产生的现金流量净额/净债务
        EBIT_TO_INTEREST REAL, -- 已获利息倍数(EBIT/利息费用)
        LONGDEBT_TO_WORKINGCAPITAL REAL, -- 长期债务与营运资金比率
        EBITDA_TO_DEBT REAL, -- 息税折旧摊销前利润/负债合计
        TURN_DAYS REAL, -- 营业周期
        ROA_YEARLY REAL, -- 年化总资产净利率
        ROA_DP REAL, -- 总资产净利率(杜邦分析)
        FIXED_ASSETS REAL, -- 固定资产合计
        PROFIT_PREFIN_EXP REAL, -- 扣除财务费用前营业利润
        NON_OP_PROFIT REAL, -- 非营业利润
        OP_TO_EBT REAL, -- 营业利润／利润总额
        NOP_TO_EBT REAL, -- 非营业利润／利润总额
        OCF_TO_PROFIT REAL, -- 经营活动产生的现金流量净额／营业利润
        CASH_TO_LIQDEBT REAL, -- 货币资金／流动负债
        CASH_TO_LIQDEBT_WITHINTEREST REAL, -- 货币资金／带息流动负债
        OP_TO_LIQDEBT REAL, -- 营业利润／流动负债
        OP_TO_DEBT REAL, -- 营业利润／负债合计
        ROIC_YEARLY REAL, -- 年化投入资本回报率
        TOTAL_FA_TRUN REAL, -- 固定资产合计周转率
        PROFIT_TO_OP REAL, -- 利润总额／营业收入
        Q_OPINCOME REAL, -- 经营活动单季度净收益
        Q_INVESTINCOME REAL, -- 价值变动单季度净收益
        Q_DTPROFIT REAL, -- 扣除非经常损益后的单季度净利润
        Q_EPS REAL, -- 每股收益(单季度)
        Q_NETPROFIT_MARGIN REAL, -- 销售净利率(单季度)
        Q_GSPROFIT_MARGIN REAL, -- 销售毛利率(单季度)
        Q_EXP_TO_SALES REAL, -- 销售期间费用率(单季度)
        Q_PROFIT_TO_GR REAL, -- 净利润／营业总收入(单季度)
        Q_SALEEXP_TO_GR REAL, -- 销售费用／营业总收入 (单季度)
        Q_ADMINEXP_TO_GR REAL, -- 管理费用／营业总收入 (单季度)
        Q_FINAEXP_TO_GR REAL, -- 财务费用／营业总收入 (单季度)
        Q_IMPAIR_TO_GR_TTM REAL, -- 资产减值损失／营业总收入(单季度)
        Q_GC_TO_GR REAL, -- 营业总成本／营业总收入 (单季度)
        Q_OP_TO_GR REAL, -- 营业利润／营业总收入(单季度)
        Q_ROE REAL, -- 净资产收益率(单季度)
        Q_DT_ROE REAL, -- 净资产单季度收益率(扣除非经常损益)
        Q_NPTA REAL, -- 总资产净利润(单季度)
        Q_OPINCOME_TO_EBT REAL, -- 经营活动净收益／利润总额(单季度)
        Q_INVESTINCOME_TO_EBT REAL, -- 价值变动净收益／利润总额(单季度)
        Q_DTPROFIT_TO_PROFIT REAL, -- 扣除非经常损益后的净利润／净利润(单季度)
        Q_SALESCASH_TO_OR REAL, -- 销售商品提供劳务收到的现金／营业收入(单季度)
        Q_OCF_TO_SALES REAL, -- 经营活动产生的现金流量净额／营业收入(单季度)
        Q_OCF_TO_OR REAL, -- 经营活动产生的现金流量净额／经营活动净收益(单季度)
        BASIC_EPS_YOY REAL, -- 基本每股收益同比增长率(%)
        DT_EPS_YOY REAL, -- 稀释每股收益同比增长率(%)
        CFPS_YOY REAL, -- 每股经营活动产生的现金流量净额同比增长率(%)
        OP_YOY REAL, -- 营业利润同比增长率(%)
        EBT_YOY REAL, -- 利润总额同比增长率(%)
        NETPROFIT_YOY REAL, -- 归属母公司股东的净利润同比增长率(%)
        DT_NETPROFIT_YOY REAL, -- 归属母公司股东的净利润-扣除非经常损益同比增长率(%)
        OCF_YOY REAL, -- 经营活动产生的现金流量净额同比增长率(%)
        ROE_YOY REAL, -- 净资产收益率(摊薄)同比增长率(%)
        BPS_YOY REAL, -- 每股净资产相对年初增长率(%)
        ASSETS_YOY REAL, -- 资产总计相对年初增长率(%)
        EQT_YOY REAL, -- 归属母公司的股东权益相对年初增长率(%)
        TR_YOY REAL, -- 营业总收入同比增长率(%)
        OR_YOY REAL, -- 营业收入同比增长率(%)
        Q_GR_YOY REAL, -- 营业总收入同比增长率(%)(单季度)
        Q_GR_QOQ REAL, -- 营业总收入环比增长率(%)(单季度)
        Q_SALES_YOY REAL, -- 营业收入同比增长率(%)(单季度)
        Q_SALES_QOQ REAL, -- 营业收入环比增长率(%)(单季度)
        Q_OP_YOY REAL, -- 营业利润同比增长率(%)(单季度)
        Q_OP_QOQ REAL, -- 营业利润环比增长率(%)(单季度)
        Q_PROFIT_YOY REAL, -- 净利润同比增长率(%)(单季度)
        Q_PROFIT_QOQ REAL, -- 净利润环比增长率(%)(单季度)
        Q_NETPROFIT_YOY REAL, -- 归属母公司股东的净利润同比增长率(%)(单季度)
        Q_NETPROFIT_QOQ REAL, -- 归属母公司股东的净利润环比增长率(%)(单季度)
        EQUITY_YOY REAL, -- 净资产同比增长率
        RD_EXP REAL, -- 研发费用
        PRIMARY KEY (TS_CODE, END_DATE)
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
    'INCOME_STATEMENTS',
    'BALANCE_SHEETS',
    'STATEMENTS_OF_CASH_FLOWS',
    'INCOME_FORECASTS',
    'FINANCIAL_INDICATORS'
]

TABLES_NEED_UPDATE = [
    'QUOTATIONS_DAILY',
    'QUOTATIONS_WEEKLY',
    'QUOTATIONS_MONTHLY',
    'LIMITS_STATISTIC',
    'ADJ_FACTORS'
]
