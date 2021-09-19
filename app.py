from flask import Flask
from flask import request
from flask import render_template
from database import dispatcher

app = Flask(__name__)


@app.route('/')
def home_page():
    return render_template('home.html')


@app.route('/index-daily')
def index_daily():
    return render_template('index-daily.html')


@app.route('/index-daily-data')
def index_daily_data():
    index_code = request.args.get('code')
    days = int(request.args.get('days'))
    return dispatcher.get_index_daily(index_code, days)


if __name__ == '__main__':
    app.run()
