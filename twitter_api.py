from tweepy.streaming import StreamListener
from tweepy import OAuthHandler, Cursor, API
from tweepy import Stream
from tweepy import RateLimitError
import twitter_credentials
import json, sys,time

#####
#site : https://developer.twitter.com/en/docs/tweets/search/guides/build-standard-query
# usr : @codinghorror

# # # # TWITTER CLIENT # # # #
class TwitterClient():
    def __init__(self, twitter_user=None):
        self.auth = TwitterAuthenticator().authenticate_twitter_app()
        self.twitter_client = API(self.auth, wait_on_rate_limit=True)
        self.twitter_user = twitter_user

    def get_user_time_line_tweets(self, num_tweets, name_id=None):
        # get the tweets of a given user
        tweets = []
        for tweet in Cursor(self.twitter_client.user_timeline, id=name_id).items(num_tweets):
            tweets.append(tweet)
        return tweets

    def get_friend_list(self, num_friends, name_id=None):
        # get the friend of a given user
        friend_list = []
        for friend in Cursor(self.twitter_client.friends, id=name_id).items(num_friends):
            friend_list.append(friend)
        return friend_list

    def get_home_time_line_tweets(self, num_tweets, name_id=None):
        # the top tweet of a user in the time_line
        home_timeline_tweets = []
        for tweet in Cursor(self.twitter_client.home_timeline, id=name_id).items(num_tweets):
            home_timeline_tweets.append(tweet)
        return home_timeline_tweets

    def get_replies(self, name, num=100):
        replies = []
        non_bmp_map = dict.fromkeys(range(0x10000, sys.maxunicode + 1), 0xfffd)
        for full_tweets in Cursor(self.twitter_client.user_timeline, screen_name=name, timeout=999999).items(10):
            for tweet in Cursor(self.twitter_client.search, q='to:' + name, timeout=999999).items(num):
                if hasattr(tweet, 'in_reply_to_status_id_str'):
                    if tweet.in_reply_to_status_id_str == full_tweets.id_str:
                        replies.append(tweet.text)
            print("Tweet :", full_tweets.text.translate(non_bmp_map))
            for elements in replies:
                print("Replies :", elements)
            replies = []




    def search_by_id_tweet(self, tweetIDs):
        tweets = self.twitter_client.statuses_lookup(tweetIDs)
        for tweet in tweets:
            tweet = tweet._json
            print (str(tweet['id'])+"___"+str(tweet['text']))
        return tweets

    def search_mentions(self, max_tweets, query):
        list_results=[]
        for mentions in Cursor(self.twitter_client.mentions_timeline).items(max_tweets): #q=query
            # process mentions here
            print "in"
            list_results.append(mentions)
            print mentions.text
        return list_results

    def limit_handled(self,cursor):
        """
        # In this example, the handler is time.sleep(15 * 60),
        # but you can of course handle it in any way you want.

        example:

        for follower in limit_handled(tweepy.Cursor(api.followers).items()):
            if follower.friends_count < 300:
                print follower.screen_name
        """
        while True:
            try:
                yield cursor.next()
            except RateLimitError:
                time.sleep(15 * 60)

# # # TWITTER AUTH # # #
class TwitterAuthenticator():

    def authenticate_twitter_app(self):
        auth = OAuthHandler(twitter_credentials.CONSUMER_KEY, twitter_credentials.CONSUMER_SECRET)
        auth.set_access_token(twitter_credentials.ACCESSS_TOKEN, twitter_credentials.ACCESS_TOKEN_SECRET)
        return auth


class TwitterStreamer():
    """
    Class for streaming and processing live tweets
    """

    def __init__(self):
        self.twitter_authenticator = TwitterAuthenticator()

    def stream_tweets(self, fetched_tweets_filename, hash_tag_list):
        # This handel Twitter authentication and the connection to the Twitter Streaming API
        listener = StdOutListener(fetched_tweets_filename)
        auth = self.twitter_authenticator.authenticate_twitter_app()
        stream = Stream(auth, listener)
        stream.filter(track=hash_tag_list)


class StdOutListener(StreamListener):
    """
    This a basic listener class that just print received tweets to stdout
    """

    def __init__(self, fetched_tweets_filename):
        self.fetched_tweets_filename = fetched_tweets_filename

    def on_data(self, raw_data):
        try:
            print (raw_data)
            with open(self.fetched_tweets_filename, 'a') as f:
                f.write(raw_data)
            return True
        except BaseException as e:
            print("Error on_data: %s" % str(e))
        return True

    def on_error(self, status_code):
        if status_code == 420:
            # return false on_data method in case rate limit occurs
            return False
        print (status_code)


if __name__ == "__main__":
    # hastg_arr = ['donald trump', 'hillary clinton']
    # tweet_filename = "tweets.json"
    # tw_stream = TwitterStreamer()
    # tw_stream.stream_tweets(tweet_filename, hastg_arr)

    # ###################################################

    usr_name = 'codinghorror'
    id_str=['983464620705722369']
    twitter_client = TwitterClient()

    #str_tweets = twitter_client.get_replies_v2(usr_name)

    #str_tweets = (twitter_client.search_by_id_tweet(id_str))
    str_tweets = (twitter_client.get_user_time_line_tweets(30, usr_name))
    with open('/home/ise/NLP/{}_DATA.json'.format(usr_name), 'a') as outfile:
        for tweet_i in str_tweets:
            outfile.write('\n')
            json.dump(tweet_i._json, outfile)
            outfile.write('\n')
