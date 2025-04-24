from airflow import DAG
from airflow.operators.python import PythonOperator

import tweepy
import pandas as pd
import os
import time
from datetime import datetime, timedelta

from playwright.sync_api import sync_playwright

from dotenv import load_dotenv

load_dotenv()

#=====================Config====================#
bearer_token = os.getenv("x_bearer_token")
client = tweepy.Client(bearer_token=bearer_token, wait_on_rate_limit=True)

start_time = (datetime.utcnow() - timedelta(days=7)).isoformat(timespec="milliseconds") + "Z"
end_time = datetime.utcnow().isoformat(timespec="milliseconds") + "Z"

min_followers=50000


keywords = [
    "influencer", "fashion", "blogger", "model", "photography", "style", "beauty", "lifestyle", "makeup", "travel",
    "fitness", "motivation", "entrepreneur", "digital", "creator", "content", "marketing", "branding", "coach",
    "artist", "music", "love", "wellness", "inspiration"
    ]

def scrape_twitter_usernames(keyword, max_pages=5):
    usernames = []
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        query = f'site:twitter.com "{keyword}"'
        page.goto(f"https://www.google.com/search?q={query}")
        
        for _ in range(max_pages):
            links = page.locator("a:has-text('twitter.com')").all()
            for link in links:
                href = link.get_attribute("href")
                if href and "twitter.com/" in href:
                    user = href.split("twitter.com/")[1].split("/")[0].split("?")[0]
                    if user and user.lower() != "home":
                        usernames.append(user)
            try:
                next_button = page.locator("a#pnnext")
                if next_button.is_visible():
                    next_button.click()
                    time.sleep(2)
                else:
                    break
            except Exception:
                break
        browser.close()
    return list(set(usernames))

def get_userdata(username):
    try:
        profile = client.get_user(username=username, user_fields=["id", "username", "description", "public_metrics"])
        user = profile.data
        follower_count = user.public_metrics["followers_count"]

        if follower_count < min_followers:
            return []

        tweets = client.get_users_tweets(
            id=user.id,
            tweet_fields=["id", "created_at", "text", "public_metrics"],
            max_results=5,
            start_time=start_time,
            end_time=end_time
        )

        user_tweets = []
        if tweets.data:
            for tweet in tweets.data:
                user_tweets.append({
                    "user_id": user.id,
                    "username": user.username,
                    "bio": user.description,
                    "followers": follower_count,
                    "text": tweet.text,
                    "created_at": tweet.created_at,
                    "likes": tweet.public_metrics["like_count"],
                    "retweets": tweet.public_metrics["retweet_count"]
                })
        return user_tweets
    except Exception as e:
        print(f"Failed to fetch data for @{username}: {e}")
        return []

def tweeter_influencers(**context):
    all_data = []
    seen_usernames = set()

    for keyword in keywords:
        usernames = scrape_twitter_usernames(keyword)

        for username in usernames:
            if username in seen_usernames:
                continue
            seen_usernames.add(username)

            user_data = get_userdata(username)
            if user_data:
                all_data.extend(user_data)
            time.sleep(1) 

    if all_data:
        df = pd.DataFrame(all_data)
    data = df.to_json(orient='records')
    df_json = context['ti'].xcom_push(key='raw_df', value='data')
    return df_json
    


def stage_to_sqlite_task(**context):
    df_json = context['ti'].xcom_pull(key='raw_df')
    df = pd.read_json(df_json, orient='records')
    conn = sqlite3.connect('influencers.db')
    df.to_sql('staging', conn, if_exists='replace', index=False)
    conn.close()
    
def filter_keywords_from_bio(text, keywords):
    text = text.encode('ascii', 'ignore').decode() 
    words = re.findall(r'\b\w+\b', text.lower())    
    return ' '.join([word for word in words if word in keywords])

def transform_data(**context):
    conn = sqlite3.connect('/tmp/influencers.db')
    df = pd.read_sql_query("SELECT * FROM staging", conn)
    df.drop(df['user_id'], inplace=True, axis=1)

    df["bio"] = df["bio"].apply(lambda x: filter_keywords_from_bio(x, keywords))

    df.to_sql('transformed', conn, if_exists='replace', index=False)
    context['ti'].xcom_push(key='transformed_df', value=df.to_json(orient='records'))
    conn.close()

def load_to_mysql_task(**context):
    df_json = context['ti'].xcom_pull(key='transformed_df')
    df = pd.read_json(df_json, orient='records')

    engine = create_engine('mysql+pymysql://user:Mysql@localhost:3306/influencer')
    df.to_sql('instagram_influencers', con=engine, if_exists='replace', index=False)

def export_to_powerbi_task(**context):
    df_json = context['ti'].xcom_pull(key='transformed_df')
    df = pd.read_json(df_json, orient='records')

    
    df.to_csv('/tmp/influencers_for_powerbi.csv', index=False)
    



with DAG(
    'influencer_dashboard_Instagram',
    default_args=default_args,
    schedule_interval=timedelta(days=1),
    catchup=False,
    ) as dag:
    


    extract = PythonOperator(
        task_id='extract_profiles',
        python_callable=tweeter_influencers,
        provide_context=True
        retries=4,
        retry_delay=timedelta(minutes=5),
        priority_weight=4,
        dag=dag
        )
    
    

    stage = PythonOperator(
    task_id='stage_to_sqlite',
    python_callable=stage_to_sqlite_task,
    provide_context=True,
    priority_weight=3,
    dag=dag
    )
       
    transform = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data,
        provide_context=True,
        priority_weight=2,
        dag=dag
        )

    load_mysql = PythonOperator(
        task_id='load_mysql',
        python_callable=load_to_mysql,
        provide_context=True,
        dag=dag
        )

    refresh_powerbi = PythonOperator(
        task_id='refresh_powerbi',
        python_callable=load_powerbi_refresh
        )

extract >> stage>> transform >> [load_mysql, refresh_powerbi]
