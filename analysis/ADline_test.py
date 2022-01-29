# -*- coding: utf-8 -*-

from data_source import local_source
import pandas as pd

def test():
    A_D_LIST = pd.DataFrame(columns=['TRADE_DATE', 'net_advances', 'A_D',
                            'net_advances_compare_with_index', 'A_D_compare_with_index'])

    date_list = local_source.get_indices_daily(cols='TRADE_DATE')

    A_D = 0
    A_D_compare_with_index = 0


    for date_count in range(len(date_list)):
        date = date_list.loc[date_count, 'TRADE_DATE']

        quotations_piece = local_source.get_quotations_daily(
            cols='TS_CODE,TRADE_DATE,OPEN,CLOSE', condition='TRADE_DATE='+date)
        quotations_piece.set_index('TS_CODE', inplace=True)
        indices_piece = local_source.get_indices_daily(
            cols='INDEX_CODE,TRADE_DATE,OPEN,CLOSE', condition='TRADE_DATE='+date)
        indices_piece.set_index('INDEX_CODE', inplace=True)

        net_advances = len(quotations_piece[quotations_piece['OPEN'] < quotations_piece['CLOSE']])-len(
            quotations_piece[quotations_piece['OPEN'] > quotations_piece['CLOSE']])
        A_D = A_D+net_advances

        net_advances_compare_with_index = 0
        for stock in quotations_piece.index:
            amount_of_increase = (
                quotations_piece.loc[stock, 'CLOSE']-quotations_piece.loc[stock, 'OPEN'])/quotations_piece.loc[stock, 'OPEN']
            # 判断一个股票是不是属于深证指数的, 如果是就和深证指数比较，否则和上证指数比较；但是刚开始的一段时间没有深证数据,这种情况就仍用上证指数比较。
            if stock.split('.')[-1] == 'SZ':
                try:
                    amount_of_increase_index = (
                        indices_piece.loc['399001.SZ', 'CLOSE']-indices_piece.loc['399001.SZ', 'OPEN'])/indices_piece.loc['399001.SZ', 'OPEN']
                except:
                    amount_of_increase_index = (
                        indices_piece.loc['000001.SH', 'CLOSE']-indices_piece.loc['000001.SH', 'CLOSE'])/indices_piece.loc['000001.SH', 'OPEN']
            else:
                amount_of_increase_index = (
                    indices_piece.loc['000001.SH', 'CLOSE']-indices_piece.loc['000001.SH', 'CLOSE'])/indices_piece.loc['000001.SH', 'OPEN']
            if amount_of_increase > amount_of_increase_index:
                net_advances_compare_with_index = net_advances_compare_with_index+1
            if amount_of_increase < amount_of_increase_index:
                net_advances_compare_with_index = net_advances_compare_with_index-1
        A_D_compare_with_index = A_D_compare_with_index+net_advances_compare_with_index
        new = pd.Series({'TRADE_DATE': date, 'net_advances': net_advances, 'A_D': A_D,
                        'net_advances_compare_with_index': net_advances_compare_with_index, 'A_D_compare_with_index': A_D_compare_with_index})
        A_D_LIST = A_D_LIST.append(new, ignore_index=True)
        print("{date}腾落线相关数据计算完成。".format(date=date))

    A_D_LIST.to_csv("A_D_LIST.csv", index=False)
