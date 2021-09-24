from flask import Flask
from flask import request
from flask import render_template
from data_storage.builder import db_init
from data_handler import reader

db_init(200)
app = Flask(__name__)


@app.route('/')
def home_page():
    return render_template('home.html')


@app.route('/index-daily')
def index_daily():
    return render_template('index-daily.html')


@app.route('/index-daily-data')
def index_daily_data():
    index_suffix = request.args.get('index')
    days = int(request.args.get('days'))
    return reader.get_index_daily(index_suffix, days)


if __name__ == '__main__':
    app.run()
