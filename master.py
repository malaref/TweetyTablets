import datetime
import os
import sqlite3
import uuid

from flask import Flask, request, render_template, jsonify
from flask_cors import CORS
from util import Tweet, tweet_factory, datetime_format, send_data

app = Flask(__name__)
CORS(app)

# Configuration
initial_tweets_count = 100
tablet_servers = ['http://localhost:5001', 'http://localhost:5002']
tablets_per_server = 2

# Database
database_path = 'tweets.db'
if not os.path.exists(database_path):
    import twitter
    twitter_api = twitter.Api(
        consumer_key=os.environ['CONSUMER_KEY'],
        consumer_secret=os.environ['CONSUMER_SECRET'],
        access_token_key=os.environ['ACCESS_TOKEN_KEY'],
        access_token_secret=os.environ['ACCESS_TOKEN_SECRET'])
    stream_sample = twitter_api.GetStreamSample()
    db = sqlite3.connect(database_path)
    db.execute(
        'CREATE TABLE Tweets (id TEXT PRIMARY KEY, user TEXT, created_at TEXT, content TEXT)')
    for _ in range(initial_tweets_count):
        next_tweet = stream_sample.next()
        while 'delete' in next_tweet:
            next_tweet = stream_sample.next()
        Tweet({'id': uuid.uuid4().hex,
               'user': next_tweet['user']['screen_name'],
               'created_at': next_tweet['created_at'],
               'content': next_tweet['text']}).insert_into(db)
    db.commit()
    db.close()
db = sqlite3.connect(database_path, check_same_thread=False)
db.row_factory = tweet_factory

# Initialization
all_tweets = db.execute('SELECT * FROM Tweets').fetchall()
number_of_tablets = len(tablet_servers) * tablets_per_server
first_datetime = min(all_tweets).get_datetime()
last_datetime = max(all_tweets).get_datetime()
timespan = (last_datetime - first_datetime) / number_of_tablets

# Helper functions
def get_server_index(tablet_index):
    return tablet_index // tablets_per_server

def get_tablet_index(tweet):
    return min(int((tweet.get_datetime() - first_datetime).total_seconds() / timespan.total_seconds()),
               number_of_tablets - 1)

def get_server(tweet):
    return tablet_servers[get_server_index(get_tablet_index(tweet))]

for tweet in all_tweets:
    send_data(get_server(tweet) + '/master/create/', tweet.to_dict())

@app.route('/', methods=['GET'])
def index():
    return render_template('index.html',
                           first_datetime=first_datetime.strftime(datetime_format),
                           last_datetime=last_datetime.strftime(datetime_format),
                           now=datetime.datetime.now().strftime(datetime_format),
                           new_id=uuid.uuid4().hex)


@app.route('/create/', methods=['POST'])
def create():
    return jsonify([get_server(Tweet({'created_at': request.form['created_at']}))])


@app.route('/read/', methods=['POST'])
def read():
    first_server_index = get_server_index(get_tablet_index(Tweet({'created_at': request.form['from']})))
    last_server_index = get_server_index(get_tablet_index(Tweet({'created_at': request.form['to']})))
    return jsonify([tablet_servers[server_index]
                    for server_index in range(first_server_index, last_server_index + 1)])

@app.route('/update/', methods=['POST'])
def update():
    return jsonify([get_server(
        Tweet({'id': request.form['id'],
               'user': request.form['user'],
               'created_at': request.form['created_at'],
               'content': request.form['content']}))])

@app.route('/delete/', methods=['POST'])
def delete():
    return jsonify([get_server(
        Tweet({'id': request.form['id'],
               'user': request.form['user'],
               'created_at': request.form['created_at'],
               'content': request.form['content']}))])


@app.route('/sync/create/', methods=['POST'])
def sync_create():
    Tweet({'id': request.form['id'],
           'user': request.form['user'],
           'created_at': request.form['created_at'],
           'content': request.form['content']}).insert_into(db)
    db.commit()
    return 'Synced!'

@app.route('/sync/update/', methods=['POST'])
def sync_update():
    db.execute('UPDATE Tweets SET user = ?, created_at = ?, content = ? WHERE id = ?',
               [request.form['user'], request.form['created_at'], request.form['content'], request.form['id']])
    db.commit()
    return 'Synced!'

@app.route('/sync/delete/', methods=['POST'])
def sync_delete():
    db.execute('DELETE FROM Tweets WHERE id = ?',
               [request.form['id']])
    db.commit()
    return 'Synced!'
