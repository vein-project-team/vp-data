from settings import *
from flask import Flask
from flask import request
from flask import render_template
from data_storage.builder import db_init
from data_handler import trimmer

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
            "daily": trimmer.get_index_daily_from_db(index_suffix, size),
            'weekly': trimmer.get_index_weekly_from_db(index_suffix, size)
        }
    }


if __name__ == '__main__':
    app.run()
