import datetime
import os
import sqlite3
import uuid

import requests
from flask import Flask, request, render_template, jsonify
from flask_cors import CORS
from util import Tweet, tweet_factory, datetime_format

app = Flask(__name__)
CORS(app)

# Configuration
initial_tweets_count = 100
tablet_servers = ['http://localhost:5001', 'http://localhost:5002']

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
number_of_tablets = len(tablet_servers) * 2
first_datetime = min(all_tweets).get_datetime()
last_datetime = max(all_tweets).get_datetime()
timespan = (last_datetime - first_datetime) / number_of_tablets

# Helper functions
def get_server_index(tablet_index):
    return tablet_index // 2

def get_tablet_index(tweet):
    return min(int((tweet.get_datetime() - first_datetime).total_seconds() / timespan.total_seconds()),
               number_of_tablets - 1)

def get_server(tweet):
    return tablet_servers[get_server_index(get_tablet_index(tweet))]

for tweet in all_tweets:
    requests.post(get_server(tweet) + '/create/',
                  data=tweet.__dict__())

@app.route('/', methods=['GET'])
def index():
    return render_template('index.html',
                           first_datetime=first_datetime.strftime(datetime_format),
                           last_datetime=last_datetime.strftime(datetime_format),
                           now=datetime.datetime.now().strftime(datetime_format),
                           new_id=uuid.uuid4().hex)


@app.route('/create/', methods=['POST'])
def create():
    tweet = Tweet({'created_at': request.form['created_at']})
    return jsonify([get_server(tweet)])


@app.route('/read/', methods=['POST'])
def read():
    first_server_index = get_server_index(get_tablet_index(Tweet({'created_at': request.form['from']})))
    last_server_index = get_server_index(get_tablet_index(Tweet({'created_at': request.form['to']})))
    return jsonify([tablet_servers[server_index]
                    for server_index in range(first_server_index, last_server_index + 1)])

@app.route('/update/', methods=['POST'])
def update():
    tweet = db.execute('SELECT * FROM Tweets WHERE id = ?', [request.form['id']]).fetchone()
    return jsonify([get_server(tweet)])

@app.route('/delete/', methods=['POST'])
def delete():
    tweet = db.execute('SELECT * FROM Tweets WHERE id = ?', [request.form['id']]).fetchone()
    return jsonify([get_server(tweet)])
