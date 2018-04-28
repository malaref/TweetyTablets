import sqlite3

from dateutil.parser import parse
from flask import Flask, request, jsonify
from util import dict_factory, date_of_tweet, standard_datetime_format

app = Flask(__name__)

# Database
db = sqlite3.connect(":memory:")
db.row_factory = dict_factory
db.execute('CREATE TABLE Tweets (id text primary key, user text, created_at text, content text)')
db.commit()

@app.route('/create/', methods=['POST'])
def create():
    db.execute("INSERT INTO Tweets (user, created_at, content) VALUES (?,?,?)",
               [request.form['user'],
                standard_datetime_format(request.form['created_at']),
                request.form['content']])
    return 'Added successfully!'


@app.route('/read/', methods=['POST'])
def read():
    first_datetime = parse(request.form['from'])
    last_datetime = parse(request.form['to'])
    tweets = []
    for tweet in db.execute('SELECT * FROM Tweets').fetchall():
        if last_datetime >= date_of_tweet(tweet) >= first_datetime:
            tweets.append(tweet)
    return jsonify(tweets)


@app.route('/delete/', methods=['POST'])
def delete():
    db.execute('DELETE FROM Tweets WHERE id = ?',
               [request.form['id']])
    return 'Added successfully!'
