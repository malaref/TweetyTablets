import sqlite3

from dateutil.parser import parse
from flask import Flask, request, jsonify
from flask_cors import CORS
from util import Tweet, tweet_factory

app = Flask(__name__)
CORS(app)

# Database
db = sqlite3.connect(":memory:", check_same_thread=False)
db.row_factory = tweet_factory
db.execute('CREATE TABLE Tweets (id TEXT PRIMARY KEY, user TEXT, created_at TEXT, content TEXT)')
db.commit()

@app.route('/create/', methods=['POST'])
def create():
    Tweet({'id': request.form['id'],
           'user': request.form['user'],
           'created_at': request.form['created_at'],
           'content': request.form['content']}).insert_into(db)
    return 'Added!'


@app.route('/read/', methods=['POST'])
def read():
    first_datetime = parse(request.form['from'])
    last_datetime = parse(request.form['to'])
    tweets = []
    print first_datetime, last_datetime
    for tweet in db.execute('SELECT * FROM Tweets').fetchall():
        if last_datetime >= tweet.get_datetime() >= first_datetime:
            tweets.append(tweet.__dict__())
    return jsonify(tweets)


@app.route('/delete/', methods=['POST'])
def delete():
    db.execute('DELETE FROM Tweets WHERE id = ?',
               [request.form['id']])
    return 'Deleted!'
