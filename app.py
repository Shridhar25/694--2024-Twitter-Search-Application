from flask import Flask, render_template, request
import pymongo
import psycopg2
import re
from datetime import datetime
from nltk.corpus import wordnet
import json
import time
from flask_paginate import Pagination, get_page_args

app = Flask(__name__)

# Connect to PostgreSQL Connection
conn = psycopg2.connect(
    dbname="twitter_database",
    user="postgres",
    password="shridhar",
    host="localhost",
    port="5432"
)
conn.autocommit = True

# Connect to MongoDB database
mongo_client = pymongo.MongoClient("mongodb+srv://ashtikarshridhar:shridhar@cluster0.ft49nzt.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0")
mongo_db = mongo_client["Twitter_DB"]
tweet_collection = mongo_db["Tweets"]
quoted_tweet_collection = mongo_db["Quoted-Tweets"]
retweet_collection = mongo_db["Retweets"]

class LRUCache1:
    def __init__(self, capacity, checkpoint_interval):
        self.capacity = capacity
        self.cache = {}
        self.access_times = {}
        self.checkpoint_interval = checkpoint_interval
        self.last_checkpoint_time = time.time()

    def get(self, key):
        if key in self.cache:
            self.access_times[key] = time.time()
            return self.cache[key]
        return None
    
    def put(self, key, value):
        if len(self.cache) >= self.capacity:
            self.evict_least_recently_used()

        self.cache[key] = value
        self.access_times[key] = time.time()

        if time.time() - self.last_checkpoint_time > self.checkpoint_interval:
            self.checkpoint()
            self.last_checkpoint_time = time.time()

    def process_tweets_for_cache(self, tweets):
        for tweet in tweets:
            # Exclude the _id field from the tweet
            tweet_without_id = {key: value for key, value in tweet.items() if key != '_id'}
            self.put(tweet_without_id['Tweet_Id'], tweet_without_id)
        self.checkpoint()
        
    def evict_least_recently_used(self):
        if not self.cache:
            return

        oldest_key = min(self.access_times, key=self.access_times.get)
        del self.cache[oldest_key]
        del self.access_times[oldest_key]

    def checkpoint(self):
        with open('cache_check-point.json', 'w') as f:
            json.dump({'cache': self.cache, 'access_times': self.access_times}, f)
        self.last_checkpoint_time = time.time()


    def load_checkpoint(self):
        try:
            with open('cache_check-point.json', 'r') as f:
                data = json.load(f)
                self.cache = data['cache']
                self.access_times = data['access_times']
                self.last_checkpoint_time = time.time()
        except FileNotFoundError:
            pass
        
    def search_tweet(self, tweet_id):
        for key, value in self.cache.items():
            if key == tweet_id:
                return value
        return None
    

def search_by_hashtag(hashtag):
    if not isinstance(hashtag, list):
        hashtag = [hashtag]
    
    tweets = list(tweet_collection.find({"Hashtag": {"$in": hashtag}}))
    retweets = list(retweet_collection.find({"Hashtag": {"$in": hashtag}}))
    quoted_tweets = list(quoted_tweet_collection.find({"Hashtag": {"$in": hashtag}}))
    
    combined_result = tweets + retweets + quoted_tweets
    
    cache.process_tweets_for_cache(combined_result)
    return combined_result

def search_tweets_by_string(query_string):
    query = {"Text": {"$regex": re.compile(re.escape(query_string), re.IGNORECASE)}}
    
    tweets = list(tweet_collection.find(query))
    retweets = list(retweet_collection.find(query))
    quoted_tweets = list(quoted_tweet_collection.find(query))
    
    combined_result = tweets + retweets + quoted_tweets
    cache.process_tweets_for_cache(combined_result)
    return combined_result

def search_by_user_name(user_name):
    tweets = list(tweet_collection.find({"User_Name": user_name}))
    retweets = list(retweet_collection.find({"User_Name": user_name}))
    quoted_tweets = list(quoted_tweet_collection.find({"User_Name": user_name}))
    
    combined_result = tweets + retweets + quoted_tweets
    cache.process_tweets_for_cache(combined_result)
    return combined_result

def search_by_screen_name(screen_name):
    tweets = list(tweet_collection.find({"User_Screen_Name":screen_name }))
    retweets = list(retweet_collection.find({"User_Screen_Name": screen_name}))
    quoted_tweets = list(quoted_tweet_collection.find({"User_Screen_Name": screen_name}))
    
    combined_result = tweets + retweets + quoted_tweets
    cache.process_tweets_for_cache(combined_result)
    return combined_result

def get_most_liked_posts():
    tweets = list(tweet_collection.find().sort("Likes_Count", pymongo.DESCENDING).limit(10))
    retweets = list(retweet_collection.find().sort("Likes_Count", pymongo.DESCENDING).limit(10))
    quoted_tweets = list(quoted_tweet_collection.find().sort("Likes_Count", pymongo.DESCENDING).limit(10)) 
    
    combined_result = tweets + retweets + quoted_tweets
    filter_most_liked_tweets(combined_result)
    
    return combined_result[0:10]
    
def get_most_retweeted_posts():
    tweets = list(tweet_collection.find().sort("Retweet_Count", pymongo.DESCENDING).limit(10))
    retweets = list(retweet_collection.find().sort("Retweet_Count", pymongo.DESCENDING).limit(10))
    quoted_tweets = list(quoted_tweet_collection.find().sort("Retweet_Count", pymongo.DESCENDING).limit(10)) 
    
    combined_result = tweets + retweets + quoted_tweets
    filter_most_retweeted_tweets(combined_result)
    
    return combined_result[0:10]

def get_most_quoted_posts():
    tweets = list(tweet_collection.find().sort("Quote_count", pymongo.DESCENDING).limit(10))
    retweets = list(retweet_collection.find().sort("Quote_count", pymongo.DESCENDING).limit(10))
    quoted_tweets = list(quoted_tweet_collection.find().sort("Quote_count", pymongo.DESCENDING).limit(10))
    
    combined_result = tweets + retweets + quoted_tweets
    filter_most_quoted_tweets(combined_result)
    
    return combined_result[0:10]

def get_synonyms(word):
    synonyms = []
    for syn in wordnet.synsets(word):
        for lemma in syn.lemmas():
            synonyms.append(lemma.name())
    return list(set(synonyms))[:10]

def search_tweets_by_keyword(string):
    synonyms = get_synonyms(string)
    query = {"$or": [{"Text": {"$regex": re.compile(re.escape(syn), re.IGNORECASE)}} for syn in [string] + synonyms]}
    
    tweets = list(tweet_collection.find(query))
    retweets = list(retweet_collection.find(query))
    quoted_tweets = list(quoted_tweet_collection.find(query))
    
    combined_result = tweets + retweets + quoted_tweets
    cache.process_tweets_for_cache(combined_result)
    return combined_result

def filter_positive_tweets(tweets):
    positive_tweets = [tweet for tweet in tweets if tweet.get("sentiment") == "positive"]
    return positive_tweets

def filter_negative_tweets(tweets):
    negative_tweets = [tweet for tweet in tweets if tweet.get("sentiment") == "negative"]
    return negative_tweets

def filter_neutral_tweets(tweets):
    neutral_tweets = [tweet for tweet in tweets if tweet.get("sentiment") == "neutral"]
    return neutral_tweets

def filter_most_liked_tweets(tweets): 
    sorted_tweets = sorted(tweets, key=lambda x: x.get("Likes_Count", 0), reverse=True)
    return sorted_tweets

def filter_most_retweeted_tweets(tweets):
    sorted_tweets = sorted(tweets, key=lambda x: x.get("Retweet_Count", 0), reverse=True)
    return sorted_tweets

def filter_most_quoted_tweets(tweets):
    sorted_tweets = sorted(tweets, key=lambda x: x.get("Quote_count", 0), reverse=True)
    return sorted_tweets

def filter_most_recent_tweets(tweets):
    sorted_tweets = sorted(tweets, key=lambda x: datetime.strptime(x.get("created_at", "1970-01-01 00:00:00"), "%Y-%m-%d %H:%M:%S"), reverse=True)
    return sorted_tweets

def filter_tweets_by_dates(tweets, from_date, to_date):
    sorted_tweets = sorted(tweets, key=lambda x: datetime.strptime(x.get("created_at", "1970-01-01 00:00:00"), "%Y-%m-%d %H:%M:%S"), reverse=True)
    from_date = datetime.strptime(from_date, "%Y-%m-%d")
    to_date = datetime.strptime(to_date, "%Y-%m-%d")
    filtered_tweets = [tweet for tweet in sorted_tweets if from_date <= datetime.strptime(tweet.get("created_at", "1970-01-01 00:00:00"), "%Y-%m-%d %H:%M:%S") <= to_date]
    return filtered_tweets



def filter_most_follwed_accounts(tweets):    
    id_str_list = [tweet['id_str'] for tweet in tweets]
    id_str_tuple = tuple(id_str_list)
    cur = conn.cursor()
    cur.execute("SELECT id_str, followers_count FROM user_details WHERE id_str IN %s", (id_str_tuple,))
    rows = cur.fetchall()

    follower_count_dict = {row[0]: row[1] for row in rows}
    for tweet in tweets:
        tweet['id_str'] = tweet.pop('User_Id', None)

    tweets.sort(key=lambda tweet: follower_count_dict.get(tweet['id_str'], 0), reverse=True)


cache = LRUCache1(capacity=2000, checkpoint_interval=120)


@app.route('/')
def index():
    return render_template('index.html')

@app.route('/user_results', methods=['GET', 'POST'])
def user_results():
    name = request.form['name']
    query = "SELECT * FROM user_details WHERE name = %s"
    cur = conn.cursor()
    cur.execute(query, (name,))
    rows = cur.fetchall()
 
    tweets = tweet_collection.find({'User_Name': name})

    tweets_by_user = search_by_user_name(name)

    return render_template('user_results.html', rows=rows, tweets=tweets, tweets_by_user=tweets_by_user)

@app.route('/tweet_results', methods=['GET', 'POST'])
def tweet_results():

    relevance = request.form.get('relevance')
    search_type = request.form.get('search_type')
    query_string = request.form.get('query_string')
    from_date = request.form.get('from_date')
    to_date = request.form.get('to_date')

    # Search Type
    if search_type == 'text':
        tweets = search_tweets_by_string(query_string)
    elif search_type == 'hashtag':
        tweets = search_by_hashtag(query_string)
    elif search_type == 'username':
        tweets = search_by_user_name(query_string)
    elif search_type == 'screen_name':
        tweets = search_by_screen_name(query_string)
    elif search_type == 'advanced_search':
        tweets = search_tweets_by_keyword(query_string)
    else:
        tweets = []

    # Date Filter
    filter_tweets_by_dates(tweets, from_date, to_date)

    # Relevance
    if relevance == 'most_followed':
        tweets = filter_most_follwed_accounts(tweets)
    elif relevance == 'most_retweet':
        tweets = filter_most_retweeted_tweets(tweets)
    elif relevance == 'most_quoted':
        tweets = filter_most_quoted_tweets(tweets)
    elif relevance == 'most_recent':
        tweets = filter_tweets_by_dates(tweets)
    else:
        tweets = filter_most_liked_tweets(tweets)
    
    return render_template('tweet_results.html',tweets=tweets)

@app.route('/top_likes')
def top_likes():
    tweets = get_most_liked_posts()
    return render_template("top_likes.html",tweets=tweets)

@app.route('/top_quotes')
def top_quotes():
    tweets = get_most_quoted_posts()
    return render_template("top_quotes.html",tweets=tweets)

@app.route('/top_retweets')
def top_retweets():
    tweets = get_most_retweeted_posts()
    return render_template("top_retweets.html",tweets=tweets)

if __name__ == '__main__':
    app.run(debug=True)