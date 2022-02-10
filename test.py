import analysis.daily_report_test as drt

if __name__ == '__main__':
    # data = drt.get_limit_up_period('20220208', 5, 3)

    # for t in data: 
    #     print(t)

    print(drt.pct_chg_ranking('20220209', '1'))