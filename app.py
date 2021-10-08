import pandas as pd
from flask import Flask
from flask import request
from flask import render_template
import json_trimmer as jt

app = Flask(__name__)


@app.route('/')
def home_page():
    return render_template('home.html')


@app.route('/index-quotation')
def index_quotation():
    return render_template('index-quotation.html')


@app.route('/index-quotation-data')
def index_quotation_data():
    index_suffix = request.args.get('index')
    return jt.get_index_quotation_json(index_suffix)


@app.route('/stock-list')
def stock_list():
    return render_template('stock-list.html')


@app.route('/stock-list-data')
def stock_list_data():
    data = jt.get_all_stock_list_json()
    return data


@app.route('/stock-quotation')
def stock_quotation():
    stock = request.args.get('stock')
    return render_template('stock-quotation.html', stock=stock)


@app.route('/stock-quotation-data')
def stock_quotation_data():
    stock = request.args.get('stock')
    frequency = request.args.get('frequency')
    data = jt.get_filled_stock_details_json(stock, frequency)
    return data


if __name__ == '__main__':
    app.run()
