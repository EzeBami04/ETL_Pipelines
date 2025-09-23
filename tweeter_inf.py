import requests
import tweepy
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from functools import cache
import duckdb
import pandas as pd
import psycopg2
from .inst import remove_emojis
from .database import connect_to_database
import os
import time
from dotenv import load_dotenv
import logging

load_dotenv()

#============================== Config =====================================
logging.getLogger().setLevel(logging.INFO)
bearer_token = os.getenv("x_bearer_token")
client = tweepy.Client(bearer_token=bearer_token, wait_on_rate_limit=True)
#=============================================================================

def user_data(username):
    """
    Fetch user profile (and optionally tweets) from Twitter API.

    Args:
        client: Tweepy client
        username (str): Twitter username
        fetch_tweets (bool): If True, also fetch recent tweets

    Returns:
        dict: user data
    """
    while True:
        try:
            # Get profile info
            users_response = client.get_users(
                usernames=[username],
                user_fields=[
                    "id", "name", "username", "description", 
                    "public_metrics", "created_at", "verified", "location"
                ],
                expansions=["pinned_tweet_id"]
            )
            if not users_response.data:
                logging.warning(f"No user data found for {username}")
                return None

            user = users_response.data[0]
            user_data = {
                "id": user.id,
                "name": user.name,
                "username": user.username,
                "description": user.description,
                "followers_count": user.public_metrics["followers_count"],
                "following_count": user.public_metrics["following_count"],
                "tweet_count": user.public_metrics["tweet_count"],
                "listed_count": user.public_metrics["listed_count"],
                "created_at": str(user.created_at),
                "verified": user.verified,
                "location": user.location,
                "tweets": []  # always included for consistency
            }

            # ✅ Only fetch tweets if requested
            if fetch_tweets:
                tweets_response = client.get_users_tweets(
                    user.id,
                    max_results=5,
                    tweet_fields=["created_at", "public_metrics", "text"]
                )
                if tweets_response.data:
                    user_data["tweets"] = [
                        {
                            "id": tweet.id,
                            "text": tweet.text,
                            "created_at": str(tweet.created_at),
                            "retweet_count": tweet.public_metrics["retweet_count"],
                            "reply_count": tweet.public_metrics["reply_count"],
                            "like_count": tweet.public_metrics["like_count"],
                            "quote_count": tweet.public_metrics["quote_count"],
                        }
                        for tweet in tweets_response.data
                    ]

            return user_data

        except tweepy.TooManyRequests as e:
            # Smarter backoff
            reset_time = int(e.response.headers.get("x-rate-limit-reset", time.time() + 900))
            sleep_for = max(reset_time - int(time.time()), 60)
            logging.warning(f"Rate limit hit. Sleeping {sleep_for}s...")
            time.sleep(sleep_for)
            continue
        except Exception as e:
            logging.error(f"Unexpected error fetching data for {username}: {e}")
            return None


def x_data(username):
    logging.info(f"getting data for @{username}")
    data = user_data(username)

    if not data:
        logging.warning(f"No data for {username}")
        return

    columns = ["created_at",
               "username", "id", "bio", "location", "profile_image_url", 
               "followers", "is_verified", "published_at", "text", 
               "likes", "retweets", "comments_count"]             
    df = pd.DataFrame(data)
    column = [col for col in columns if col in df.columns]
    df = df[column]
    duck = duckdb.connect()
    duck.register("df", df)
    #====================== Type Casting and Data cleansing ===========================
    df['username'] = df["username"].astype(str).apply(remove_emojis)
    df['id'] = df["id"].astype(str)
    df['bio'] = df["bio"].astype(str).apply(remove_emojis).replace(r'[@#"/]', ' ', regex=True)
    df['location'] = df["location"].astype(str)
    df['profile_image_url'] = df["profile_image_url"].astype(str)
    df['followers'] = df["followers"].fillna(0).astype(int)
    df['is_verified'] = df['is_verified'].astype(bool)
    df['created_at'] = pd.to_datetime(df['created_at'], errors="coerce")
    df['published_at'] = pd.to_datetime(df['published_at'], errors="coerce")
    df['text'] = df["text"].astype(str).apply(remove_emojis).replace(r'[@#"/]', ' ', regex=True)
    df['likes'] = df['likes'].astype(int)
    df['retweets'] = df['retweets'].astype(int)
    df['comments_count'] = df['comments_count'].astype(int)
    df_clean = duck.execute("""
                    SELECT created_at, username, id, 
                        REPLACE(bio, '|', ' ') AS bio, 
                        location, profile_image_url, 
                        followers, is_verified, 
                        published_at, 
                        REPLACE(text, '|', ' ') AS text, 
                        likes, retweets, comments_count 
                    FROM df
                """).fetchdf()

    records = df_clean.to_records(index=False)
    query = f"""
                INSERT INTO influencer_x({', '.join(columns)})
                VALUES ({', '.join(['%s'] * len(columns))})
                
                ON CONFLICT (id)  
                DO UPDATE SET
                    created_at = EXCLUDED.created_at,
                    username = EXCLUDED.username,
                    bio = EXCLUDED.bio,
                    location = EXCLUDED.location,
                    profile_image_url = EXCLUDED.profile_image_url,
                    followers = EXCLUDED.followers,
                    is_verified = EXCLUDED.is_verified,
                    published_at = EXCLUDED.published_at,
                    text = EXCLUDED.text,
                    likes = EXCLUDED.likes,
                    retweets = EXCLUDED.retweets,
                    comments_count = EXCLUDED.comments_count"""

    engine = connect_to_database()
    if engine:
        try:
            with engine.cursor() as cursor:
                cursor.execute("SET sceham_path TO PUBLIC") 
                engine.commit() 

                cursor.execute("""CREATE TABLE IF NOT EXISTS influencer_x(created_at TIMESTAMP,
                            username Text, id Varchar(50) PRIMARY KEY, bio Text, location Text, profile_image_url Text, 
                            followers INT, is_verified Boolean, published_at TIMESTAMP, text Text, likes INT, retweets INT, comments_count INT)"""
                            )  
                engine.commit()

                cursor.executemany(query, records)
                engine.commit()
        except psycopg2.DatabaseError as e:
            engine.rollback()
        finally:
            if cursor:
                cursor.close()
            if engine:
                engine.close()


#Test 
if __name__ == "__main__":
    usernames = "DONJAZZY"
    
    x_data(usernames)



# def user_data(usernames):
#     results = []
#     # max 100 usernames per call
#     chunk_size = 2

#     for i in range(0, len(usernames), chunk_size):
#         chunk = usernames[i:i + chunk_size]
#         try:
            
#             users_response = client.get_users(
#                 usernames=chunk,
#                 user_fields=["created_at", "description", "location",
#                              "profile_image_url", "public_metrics",
#                              "verified", "is_identity_verified"]
#             )

#             if not users_response.data:
#                 logging.warning(f"No user data returned for {chunk}")
#                 continue

#             for user in users_response.data:
#                 try:
#                     #=========== Parsing Json response ======================
#                     created = user.created_at
#                     user_id = user.id
#                     username = user.username
#                     bio = user.description or ""
#                     location = user.location or ""
#                     profile_image_url = user.profile_image_url or ""
#                     followers = user.public_metrics.get("followers_count", 0)
#                     is_verified = user.verified

                    
#                     tweets = client.get_users_tweets(
#                         user_id,
#                         max_results=5,
#                         tweet_fields=["created_at", "public_metrics", "text"]
#                     )

#                     published_at, tweet_text, tweets_likes, tweets_retweets, tweets_comments = [], [], [], [], []
#                     if tweets.data:
#                         for tweet in tweets.data:
#                             published_at.append(tweet.created_at)
#                             tweet_text.append(tweet.text)
#                             tweets_likes.append(tweet.public_metrics.get("like_count", 0))
#                             tweets_retweets.append(tweet.public_metrics.get("retweet_count", 0))
#                             tweets_comments.append(tweet.public_metrics.get("reply_count", 0))

#                     # ================ Data  Mapping ==========================
#                     user_info = {
#                         "created_at": created,
#                         "username": username,
#                         "id": str(user_id),
#                         "bio": bio,
#                         "location": location,
#                         "profile_image_url": profile_image_url,
#                         "followers": followers,
#                         "is_verified": is_verified,
#                         "published_at": published_at,
#                         "text": tweet_text,
#                         "likes": tweets_likes,
#                         "retweets": tweets_retweets,
#                         "comments_count": tweets_comments
#                     }
#                     results.append(user_info)

#                 except Exception as e:
#                     logging.error(f"Error processing user {user.username}: {e}")
#                     results.append({"username": user.username, "error": str(e)})

#         except tweepy.TooManyRequests as e:
#             logging.warning("Rate limit hit. Waiting...")
#             time.sleep(900)
#             continue
#         except Exception as e:
#             logging.error(f"Error fetching chunk {chunk}: {e}")
#             for uname in chunk:
#                 results.append({"username": uname, "error": str(e)})

#     return results

