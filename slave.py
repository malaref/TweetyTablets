import sqlite3
import threading

from dateutil.parser import parse
from flask import Flask, request, jsonify
from flask_cors import CORS
from util import Tweet, tweet_factory, send_data

app = Flask(__name__)
CORS(app)

master = 'http://localhost:5000'
syncing_period = 30.0

# Database
db = sqlite3.connect(":memory:", check_same_thread=False)
db.row_factory = tweet_factory
db.execute('CREATE TABLE Tweets (id TEXT PRIMARY KEY, user TEXT, created_at TEXT, content TEXT)')
db.commit()

unsynced_created_tweets = []
unsynced_updated_tweets = []
unsynced_deleted_tweets = []

def create():
    Tweet({'id': request.form['id'],
           'user': request.form['user'],
           'created_at': request.form['created_at'],
           'content': request.form['content']}).insert_into(db)
    return 'Added!'

def read():
    first_datetime = parse(request.form['from'])
    last_datetime = parse(request.form['to'])
    tweets = []
    for tweet in db.execute('SELECT * FROM Tweets').fetchall():
        if last_datetime >= tweet.get_datetime() >= first_datetime:
            tweets.append(tweet.to_dict())
    return jsonify(tweets)

def update():
    db.execute('UPDATE Tweets SET user = ?, created_at = ?, content = ? WHERE id = ?',
               [request.form['user'], request.form['created_at'], request.form['content'], request.form['id']])
    return 'Updated!'

def delete():
    db.execute('DELETE FROM Tweets WHERE id = ?',
               [request.form['id']])
    return 'Deleted!'


@app.route('/create/', methods=['POST'])
def client_create():
    unsynced_created_tweets.append(request.form['id'])
    return create()

@app.route('/read/', methods=['POST'])
def client_read():
    return read()

@app.route('/update/', methods=['POST'])
def client_update():
    unsynced_updated_tweets.append(request.form['id'])
    return update()

@app.route('/delete/', methods=['POST'])
def client_delete():
    unsynced_deleted_tweets.append(request.form['id'])
    return delete()


@app.route('/master/create/', methods=['POST'])
def master_create():
    return create()


def sync():
    for tweet_id in unsynced_created_tweets:
        send_data(master + '/sync/create/',
                  db.execute('SELECT * FROM Tweets WHERE id = ?', [tweet_id]).fetchone().to_dict())
    for tweet_id in unsynced_updated_tweets:
        send_data(master + '/sync/update/',
                  db.execute('SELECT * FROM Tweets WHERE id = ?', [tweet_id]).fetchone().to_dict())
    for tweet_id in unsynced_deleted_tweets:
        send_data(master + '/sync/delete/', {'id': tweet_id})
    del unsynced_created_tweets[:]
    del unsynced_updated_tweets[:]
    del unsynced_deleted_tweets[:]
    db.commit()
    threading.Timer(syncing_period, sync).start()

sync()
