from flask import Flask
from flask import request
from flask import render_template
import database.db_runner as db
import json_trimmer as jt
from database.db_updater import updater


db.run()
app = Flask(__name__)


@app.route('/')
def home_page():
    """
    返回主页
    :return: 主页
    """
    return render_template('home.html')


@app.route('/index-quotation')
def index_quotation():
    """
    返回大盘形态页
    :return:大盘形态页
    """
    return render_template('index-quotation.html')


@app.route('/index-quotation-data')
def index_quotation_data():
    """
    返回大盘形态数据
    :return: 大盘形态数据json
    """
    index_suffix = request.args.get('index')
    return jt.get_index_quotation_json(index_suffix)


@app.route('/stock-list')
def stock_list():
    """
    返回股票列表页
    :return: 股票列表页
    """
    return render_template('stock-list.html')


@app.route('/stock-list-data')
def stock_list_data():
    """
    返回股票列表数据
    :return: 股票列表数据
    """
    data = jt.get_all_stock_list_json()
    return data


@app.route('/stock-quotation')
def stock_quotation():
    """
    返回股票行情页
    :return:股票行情页
    """
    stock = request.args.get('stock')
    return render_template('stock-quotation.html', stock=stock)


@app.route('/stock-quotation-data')
def stock_quotation_data():
    """
    返回股票行情数据
    :return:股票行情数据
    """
    stock = request.args.get('stock')
    frequency = request.args.get('frequency')
    data = jt.get_filled_stock_details_json(stock, frequency)
    return data


if __name__ == '__main__':
    app.run()
