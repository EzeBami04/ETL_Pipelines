from sqlalchemy import create_engine, MetaData
from functools import wraps
import os
from dotenv import load_dotenv
import logging
import requests
import time
import urllib.parse
import random

import mysql.connector.errors as MySQLConnectorError 
load_dotenv()

logging.basicConfig(level=logging.INFO, format='[%(asctime)s], [%(levelname)s], [%(message)s]', datefmt='%Y-%m-%d %H:%M:%S')
# ===== config
FB_PAGE_ID = os.getenv("FB_PAGE_ID")
access_token = os.getenv("fb_token")
graph_api = "v22.0"
min_followers = 50000
metadata = MetaData()
INSTAGRAM_NON_PROFILE_PATHS = {"explore", "p", "reel", "stories", "tv", "accounts"}



def random_sleep(a=1, b=3): # Reduced default sleep times slightly
    """ Sleeps for a random duration to mimic human behavior. """
    time.sleep(random.uniform(a, b))

def get_all_proxies():
    customer_id = "hl_8d90267d"
    zone_name = "quantum"
    password = "v4vzyr0ixuob"
    proxy_host = "brd.superproxy.io"
    proxy_port = 33335

    country_codes = ["us", "ng", "za", "ke", "eg", "gh", "tz", "ug"]
    proxies_list = []

    for country_code in country_codes:
        proxy_username = f"brd-customer-{customer_id}-zone-{zone_name}-country-{country_code}"
        proxy_url = f"http://{proxy_username}:{password}@{proxy_host}:{proxy_port}"
        proxies_list.append(proxy_url)

    return proxies_list



def extract_username_from_url(url):
    try:
        path = urllib.parse.urlparse(url).path.strip("/")
        path_parts = url.split("instagram.com/")
        if len(path_parts) > 1:
            path = path_parts[1].split("/")[0]
            if path and path not in INSTAGRAM_NON_PROFILE_PATHS:
                return path
    except IndexError:
        return None
    return None

def retry_on_rate_limit(max_retries=4, backoff_base=60): 
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(max_retries):
                result = func(*args, **kwargs)
                if result == "RATE_LIMIT":
                    sleep_time = backoff_base * (2 ** attempt) # Exponential backoff is better
                    logging.warning(f"Rate limit hit. Sleeping for {sleep_time} seconds before retrying {func.__name__} (attempt {attempt + 1}/{max_retries})...")
                    time.sleep(sleep_time)
                    continue
                return result
            logging.error(f"Exceeded max retries for {func.__name__}. Giving up.")
            return None
        return wrapper
    return decorator

def get_instagram_business_id(page_id):
    """ Fetches the Instagram Business Account ID linked to a Facebook Page. """
    url = f"https://graph.facebook.com/{graph_api}/{page_id}"
    params = {
        'fields': 'instagram_business_account',
        'access_token': access_token,
    }
    logging.info(f"Attempting to fetch Instagram Business ID for FB Page ID: {page_id}")
    try:
        res = requests.get(url, params=params, timeout=15)
        res.raise_for_status()
        data = res.json()
        ig_account_info = data.get('instagram_business_account')
        if ig_account_info and 'id' in ig_account_info:
            ig_id = ig_account_info['id']
            logging.info(f"Successfully retrieved Instagram Business ID: {ig_id}")
            return ig_id
        else:
            logging.error(f"Error: 'instagram_business_account' field not found or missing 'id'. Response: {data}")
            logging.error("Ensure the Facebook Page is connected to an Instagram Business/Creator account.")
            return None
    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching Instagram Business ID: {e}")
        if hasattr(e, 'response') and e.response is not None:
            logging.error(f"Response Status Code: {e.response.status_code}")
            logging.error(f"Response Text: {e.response.text}")
        return None
    except Exception as e:
        logging.error(f"An unexpected error occurred in get_instagram_business_id: {e}")
        return None

@retry_on_rate_limit(max_retries=4, backoff_base=60)
def validate_and_fetch_user(username, ig_business_id):
    """ Validates a username via Facebook Graph API and fetches data if criteria are met. """
    # proxy = random.choice(get_all_proxies()) if get_all_proxies() else None
    fields = (
        f'business_discovery.username({username})'
        '{id,username,profile_picture_url,name,biography,followers_count,media_count,media.limit(10){caption,like_count,comments_count,timestamp,media_url,permalink}}'
    )
    url = f'https://graph.facebook.com/{graph_api}/{ig_business_id}'
    params = {
        'fields': fields,
        'access_token': access_token
    }

    logging.info(f"Validating @{username} via Graph API...")
    try:
        res = requests.get(url, params=params, timeout=60)
        res.raise_for_status()

        if res.status_code in [429] or (res.status_code == 400 and "rate limit" in res.text.lower()): # Check for 400 with rate limit message
            random_sleep(240, 360)
            logging.warning(f"Rate limit hit for @{username}. Response: {res.text[:200]}")
            return "rate_limit retry later"
        if not res.headers.get('content-type', '').startswith('application/json'):
            logging.error(f"Non-JSON response for @{username}: {res.text[:500]}")
            return None
        if res.status_code == 403:
            random_sleep(960, 1200) 
            logging.warning(f"403 Forbidden for @{username}. Retrying after sleep...")
            return "Sleep and retry"
        try:
            data = res.json()
        except ValueError as e:
            logging.error(f"Failed to parse JSON for @{username}: {e}. Response: {res.text[:500]}")
            return None
        if "error" in data:
            error_code = data["error"].get("code")
            
            if error_code in [4, 17, 613] or "rate limit" in data["error"].get("message", "").lower():
                logging.warning(f"API Error (potential rate limit) for @{username}: {data['error'].get('message', 'No message')}")
                return "RATE_LIMIT"
            logging.info(f"API Error for @{username}: {data['error'].get('message', 'No message')}. Skipping.")
            return None

        user_data = data.get("business_discovery")
        if not user_data:
            logging.info(f"  'business_discovery' field missing for @{username}, likely not a discoverable Business/Creator account. Response: {data}")
            return None

        followers = user_data.get("followers_count", 0)
        logging.info(f"  @{username} found. Followers: {followers}")
        if followers >= min_followers:
            logging.info(f"  @{username} meets follower threshold ({min_followers}).")
            return user_data
        else:
            logging.info(f"  @{username} does not meet follower threshold ({followers} < {min_followers}). Skipping.")
            return None

    except requests.exceptions.Timeout:
        logging.warning(f"  Timeout error validating @{username}. Skipping.")
        return None
    except requests.exceptions.RequestException as e:
        logging.warning(f"  Network error validating @{username}: {e}")
        return None
    except Exception as e:
        logging.warning(f" An unexpected error occurred validating @{username}: {e}")
        return None

def create_db_connection():
    """ Create a database connection engine. """
    username = os.getenv("db_username")
    pwd = os.getenv("db_pass")
    port = 10780
    database_name = os.getenv("db_name")
    host = os.getenv("db_host")
    try:
        logging.info("Attempting to connect to the database...")
        
        engine = create_engine(f"mysql+pymysql://{username}:{pwd}@{host}:{port}/{database_name}",
                               pool_size=10, max_overflow=20) 
        metadata.create_all(engine) 
        logging.info("Database connection established.")
        return engine
    except MySQLConnectorError as e:
        logging.error(f"MySQL connection error: {e}")
        return None
    except Exception as e:
        logging.error(f"Error connecting to database: {e}")
        return None
