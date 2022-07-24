# -*- coding: utf-8 -*-

import pandas as pd
import numpy as np
from data_source import local_source


#example: 对个股计算内置alpha
from analysis import alphas_base
ts_code = '000002.SZ'
alphas = alphas_base.get_Alpha101_allalphas(ts_code=ts_code)
#GTJAalphas = alphas_base.get_GTJAalphas_allalphas(ts_code=ts_code)






'''
#未完成部分, 不要运行
from analysis import Financial_Statement_Analysis as FSA
from analysis import SinaFinance_Source as SF
from analysis import MyRandomIP

stocks_ind_used = FSA.FSA_Initializer(ts_code=ts_code)
FSA.FSA_Analyzer(stocks_ind_used)

min_date = 20220101
max_date = 20220401
proxy_list=MyRandomIP.GET_IP()
print("IP池获取完成。")
SF.SinaFinance_Initializer(ts_code=ts_code,min_date=min_date,max_date=max_date,proxy_list=proxy_list)
'''