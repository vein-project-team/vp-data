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

If use `Tushare` to fetch data, Add a token to `data_source/token.py`, you need to create that file yourself. 

```python
TUSHARE_TOKEN = "XXXXXXXXXX"
```

Then run:

```
python main.py
```

The database file `vein-project.db` will be filled automatically, that may spend more than 20 hours.
