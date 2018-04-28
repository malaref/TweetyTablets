from dateutil.parser import parse

datetime_format = '%Y-%m-%dT%H:%M:%S'

def standard_datetime_format(s):
    return parse(s).strftime(datetime_format)

def standard_datetime(s):
    return parse(standard_datetime_format(s))

def dict_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d

def date_of_tweet(tweet):
    return standard_datetime(tweet['created_at'])


