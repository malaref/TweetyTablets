import datetime
import os
import sqlite3

from flask import Flask, request, render_template, jsonify
from util import dict_factory, date_of_tweet, standard_datetime, standard_datetime_format, datetime_format

app = Flask(__name__)

# Configuration
initial_tweets_count = 100
tablet_servers = ['server1', 'server2']

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
    conn = sqlite3.connect(database_path)
    conn.execute(
        'CREATE TABLE Tweets (id INTEGER PRIMARY KEY AUTOINCREMENT, user TEXT, created_at TEXT, content TEXT)')
    for _ in range(initial_tweets_count):
        next_tweet = stream_sample.next()
        while "delete" in next_tweet:
            next_tweet = stream_sample.next()
        conn.execute('INSERT INTO Tweets (user, created_at, content) VALUES (?,?,?)',
                     [next_tweet["user"]["screen_name"],
                      standard_datetime_format(next_tweet["created_at"]),
                      next_tweet["text"]])
    conn.commit()
    conn.close()
db = sqlite3.connect(database_path)
db.row_factory = dict_factory

# Initialization
all_tweets = db.execute('SELECT * FROM Tweets').fetchall()
number_of_tablets = len(tablet_servers) * 2
first_datetime = date_of_tweet(min(all_tweets, key=date_of_tweet))
last_datetime = date_of_tweet(max(all_tweets, key=date_of_tweet))
timespan = (last_datetime - first_datetime) / number_of_tablets

print 'Done!'

# Helper functions
def get_server_by_tablet(tablet):
    return tablet // 2

def get_tablet_by_datetime(tweet_datetime):
    return min(int((tweet_datetime - first_datetime).total_seconds() / timespan.total_seconds()),
               number_of_tablets - 1)

def get_tablet_by_tweet(tweet):
    return get_tablet_by_datetime(date_of_tweet(tweet))


for tweet in all_tweets:
    pass


@app.route('/', methods=['GET'])
def index():
    return render_template('index.html',
                           first_datetime=first_datetime.strftime(datetime_format),
                           last_datetime=last_datetime.strftime(datetime_format),
                           now=datetime.datetime.now().strftime(datetime_format))


@app.route('/create/', methods=['POST'])
def create():
    return jsonify([get_server_by_tablet(get_tablet_by_datetime(standard_datetime(request.form['created_at'])))])


@app.route('/read/', methods=['POST'])
def read():
    first_server = get_server_by_tablet(get_tablet_by_datetime(standard_datetime(request.form['from'])))
    last_server = get_server_by_tablet(get_tablet_by_datetime(standard_datetime(request.form['to'])))
    return jsonify([i for i in range(first_server, last_server + 1)])


@app.route('/delete/', methods=['POST'])
def delete():
    tweet = db.execute('SELECT * FROM Tweets WHERE id = ?', [request.form['id']]).fetchone()
    return jsonify([get_server_by_tablet(get_tablet_by_tweet(tweet))])
