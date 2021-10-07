import pandas as pd
from flask import Flask
from flask import request
from flask import render_template
from database.dispatcher import get_index_quotation_from_db
from database.dispatcher import get_stock_list_from_db
from database.dispatcher import get_stock_details_from_db

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
    size = int(request.args.get('size'))
    return {
        index_suffix: {
            "daily": get_index_quotation_from_db(index_suffix, 'DAILY', size),
            'weekly': get_index_quotation_from_db(index_suffix, 'WEEKLY', size),
            'monthly': get_index_quotation_from_db(index_suffix, 'MONTHLY', size)
        }
    }


@app.route('/stock-list')
def stock_list():
    return render_template('stock-list.html')


@app.route('/stock-list-data')
def stock_list_data():
    exchange = request.args.get('exchange')
    page = int(request.args.get('page'))
    data = get_stock_list_from_db(exchange, page)
    return data


@app.route('/stock-quotation')
def stock_quotation():
    stock = request.args.get('stock')
    return render_template('stock-quotation.html', stock=stock)


@app.route('/stock-quotation-data')
def stock_quotation_data():
    stock = request.args.get('stock')
    frequency = request.args.get('frequency')
    return {
        stock: get_stock_details_from_db(stock, frequency)
    }


if __name__ == '__main__':
    app.run()
