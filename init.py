def init_db():
    import twitter, os

    twitter_api = twitter.Api(
        consumer_key=os.environ['CONSUMER_KEY'],
        consumer_secret=os.environ['CONSUMER_SECRET'],
        access_token_key=os.environ['ACCESS_TOKEN_KEY'],
        access_token_secret=os.environ['ACCESS_TOKEN_SECRET'])
    stream_sample = twitter_api.GetStreamSample()

    import sqlite3
    conn = sqlite3.connect('twitter.db')

    conn.execute(
        'CREATE TABLE Tweets (id text primary key, user text, created_at text, content text)')
    for _ in range(1000):
        tweet = stream_sample.next()
        while "delete" in tweet:
            tweet = stream_sample.next()
        conn.execute("INSERT INTO Tweets VALUES (?,?,?,?)",
                     [tweet["id"], tweet["user"]["screen_name"], tweet["created_at"], tweet["text"]])
    conn.commit()
    conn.close()


if __name__ == '__main__':
    init_db()
