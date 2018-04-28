import datetime
import random
import requests

from dateutil.parser import parse

datetime_format = '%Y-%m-%dT%H:%M:%S'

def get_standard_datetime_str(s):
    return parse(s).strftime(datetime_format)

def get_standard_datetime(s):
    return parse(get_standard_datetime_str(s))

def get_datetime(tweet):
    return get_standard_datetime(tweet['created_at'])

class Tweet(object):

    def __init__(self, record):
        self.id = record.get('id')
        self.user = record.get('user')
        self.created_at = parse(record['created_at']).strftime(datetime_format)
        self.content = record.get('content')

    def __cmp__(self, other):
        return cmp(self.get_datetime(), other.get_datetime())

    def to_dict(self):
        return {'id': self.id, 'user': self.user, 'created_at': self.created_at, 'content': self.content}

    def insert_into(self, db):
        db.execute('INSERT INTO Tweets VALUES (?, ?,?,?)',
                   [self.id,
                    self.user,
                    self.created_at,
                    self.content])

    def get_datetime(self):
        return parse(self.created_at)


def tweet_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return Tweet(d)


def random_date(start, end):
    return start + datetime.timedelta(seconds=random.randint(0, int((end - start).total_seconds())))

def send_data(url, data):
    requests.post(url, data=data)
