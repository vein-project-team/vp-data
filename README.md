# vein-project: vp-data
database and data analysis system for the **vein project**

## The Vein Project

This project aim at creating an application to provide gamut of analysis features for Chinese stock market.

- Python based data analysis and API services: [vp-data](https://github.com/vein-project-team/vp-data)
- Typescript + Vue3 data presentation: [vp-renderer](https://github.com/vein-project-team/vp-renderer)

Current features:

- data collecting: collect data from specific data source
- data warehousing: sort and store data in local database for analysis
- easily create new tables and fetch new data within the framework
- automatic data update
- data analysis framework
- data presentation in modern webapp (early stage)

## install a instance

clone the repo and run

```bash
pip install --upgrade -r .\\requirements.txt
```

## collect data from source

If use `Tushare` to fetch data, Add a token to `data_source/token.py`, you need to create that file yourself. 

```python
TUSHARE_TOKEN = "XXXXXXXXXX"
```

Then run:

```
python main.py
```

The database file `vein-project.db` will be filled automatically, that may spend more than 20 hours.

## Perform simple analysis

Then we can use `local_source` module to perform simple analysis.

For example, find out trade date that SZ or SH index up (change in day) more than 3%

```python
from data_source import local_source

if __name__ == "__main__":
    data = local_source.get_indices_daily(
        cols="INDEX_CODE, TRADE_DATE, OPEN, CLOSE, ((CLOSE - OPEN) / OPEN) * 100 AS CHG_PCT",
        condition="((CLOSE - OPEN) / OPEN) * 100 > 3"
    )
    print(data)
```

The output data frame will like this:

```
    INDEX_CODE TRADE_DATE        OPEN       CLOSE    CHG_PCT
0    000001.SH   19901219     96.0500     99.9800   4.091619
1    000001.SH   19920528   1123.0400   1160.1700   3.306205
2    000001.SH   19920812    675.0000    781.2100  15.734815
3    000001.SH   19920813    797.1700    850.9400   6.745111
4    000001.SH   19920817    898.6600    939.4600   4.540093
..         ...        ...         ...         ...        ...
621  399001.SZ   20200310  11016.9238  11403.4665   3.508626
622  399001.SZ   20200313  10382.2213  10831.1250   4.323773
623  399001.SZ   20200706  12519.4965  12941.7205   3.372532
624  399001.SZ   20200713  13699.2226  14149.1440   3.284284
625  399001.SZ   20200729  13117.1882  13557.4411   3.356305

[626 rows x 5 columns]
```



For another example, find out the stocks down (compare to the previous close price) more than 7% on the date 20211108:

```python
from data_source import local_source

if __name__ == "__main__":
    data = local_source.get_quotations_daily(
        cols="TRADE_DATE, TS_CODE, PRE_CLOSE, CLOSE, ((CLOSE - PRE_CLOSE) / PRE_CLOSE) AS CHANGE",
        condition="TRADE_DATE = '20211108' AND ((CLOSE - PRE_CLOSE) / PRE_CLOSE) < -0.07"
    )
    print(data)
```

The output would be:

```
   TRADE_DATE    TS_CODE  PRE_CLOSE   CLOSE    CHANGE
0    20211108  001219.SZ      34.19   31.28 -0.085113
1    20211108  002135.SZ      10.67    9.77 -0.084349
2    20211108  002283.SZ      11.33   10.42 -0.080318
3    20211108  300077.SZ      25.37   23.00 -0.093417
4    20211108  300141.SZ      15.86   14.45 -0.088903
5    20211108  300204.SZ      12.91   11.91 -0.077459
6    20211108  300268.SZ      22.78   20.84 -0.085162
7    20211108  300458.SZ      66.34   60.20 -0.092554
8    20211108  300572.SZ      19.87   18.00 -0.094112
9    20211108  300702.SZ      41.89   38.85 -0.072571
10   20211108  300853.SZ      42.88   38.50 -0.102146
11   20211108  300903.SZ      26.46   24.31 -0.081255
12   20211108  301017.SZ      28.60   26.45 -0.075175
13   20211108  600171.SH      30.09   27.08 -0.100033
14   20211108  600310.SH       6.06    5.45 -0.100660
15   20211108  600335.SH       7.34    6.61 -0.099455
16   20211108  600483.SH      16.71   15.35 -0.081388
17   20211108  601218.SH       7.98    7.40 -0.072682
18   20211108  603090.SH      20.59   18.53 -0.100049
19   20211108  603317.SH      28.48   26.41 -0.072683
20   20211108  603667.SH      18.09   16.34 -0.096739
21   20211108  603688.SH      63.89   58.66 -0.081859
22   20211108  603978.SH      32.00   28.99 -0.094063
23   20211108  603990.SH      19.70   17.73 -0.100000
24   20211108  688185.SH     265.90  238.67 -0.102407
25   20211108  688386.SH      57.03   52.72 -0.075574
26   20211108  688613.SH      57.51   52.94 -0.079464
```

## Perform simple analysis

In most of situations, you can treat the complex analysis as the combination of several simple analysis, just create a py file and code the analysis you need. You are recommended to use the `ReportGener` class in `analysis` module as the template of your analysis file, it provide some basic functions such as `csv` file output helper. 
