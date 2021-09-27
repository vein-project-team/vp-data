from settings import *
from flask import Flask
from flask import request
from flask import render_template
from data_storage.builder import db_init
from data_handler import dispatcher

db_init(DB_SIZE)
app = Flask(__name__)


@app.route('/')
def home_page():
    return render_template('home.html')


@app.route('/index-quotation')
def index_daily():
    return render_template('index-quotation.html')


@app.route('/index-quotation-data')
def index_daily_data():
    index_suffix = request.args.get('index')
    size = int(request.args.get('size'))
    return {
        index_suffix: {
            "daily": dispatcher.get_index_quotation_from_db(index_suffix, 'DAILY', size),
            'weekly': dispatcher.get_index_quotation_from_db(index_suffix, 'WEEKLY', size),
            'monthly': dispatcher.get_index_quotation_from_db(index_suffix, 'MONTHLY', size)
        }
    }


if __name__ == '__main__':
    app.run()
