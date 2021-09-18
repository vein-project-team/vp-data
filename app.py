from flask import Flask
from flask import request
from flask import render_template
import trimmer

app = Flask(__name__)


@app.route('/')
def index_page():
    return render_template('index.html')


@app.route('/index-daily')
def get_index_daily():
    index_code = request.args.get('code')
    days = int(request.args.get('days'))
    return trimmer.get_index_daily(index_code, days)


if __name__ == '__main__':
    app.run()
