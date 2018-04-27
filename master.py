import os
import sqlite3

from dateutil import parser
from flask import Flask, g, render_template, request

app = Flask(__name__)

# Configuration
tablet_servers = []

# Database
database_path = 'twitter.db'
if not os.path.exists(database_path):
    from init import init_db
    init_db()


def dict_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d


def get_db():
    db = getattr(g, '_database', None)
    if db is None:
        db = g._database = sqlite3.connect(database_path)
        db.row_factory = dict_factory
    return db


# Initialization
with app.app_context():
    all_tweets = get_db().execute('SELECT * FROM Tweets').fetchall()
    all_tweets.sort(lambda x, y: cmp(parser.parse(x['created_at']), parser.parse(y['created_at'])))
    # TODO split all_tweets into tablets


@app.teardown_appcontext
def close_connection(exception):
    db = getattr(g, '_database', None)
    if db is not None:
        db.close()


@app.route('/', methods=['GET'])
def home():
    return render_template('home.html')


@app.route('/create/', methods=['POST'])
def create():
    return 'Added!'


@app.route('/read/', methods=['POST'])
def read():
    first = request.form['from']
    last = request.form['to']

    return ''


@app.route('/update/', methods=['POST'])
def update():
    return 'Updated!'


@app.route('/delete/', methods=['POST'])
def delete():
    return 'Deleted!'
