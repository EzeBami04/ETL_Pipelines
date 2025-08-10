"""Instagram Profile Finder
Scrapes Instagram profile links from Google Search using keywords.
"""
from __future__ import annotations

from apify import Actor
from bs4 import BeautifulSoup
from httpx import AsyncClient
import urllib.parse
import asyncio
import random
from concurrent.futures import ThreadPoolExecutor, as_completed
from .wrk import (create_db_connection, validate_and_fetch_user, 
                 extract_username_from_url)
import pandas as pd
from sqlalchemy import Table, Column, Integer, BigInteger, Text, TIMESTAMP, MetaData
import os
from dotenv import load_dotenv
from sqlalchemy import  MetaData
import logging

load_dotenv()

# ===== Configuration and Initialization =====
logging.basicConfig(level=logging.INFO, format='[%(asctime)s], [%(levelname)s], [%(message)s]', datefmt='%Y-%m-%d %H:%M:%S')
FB_PAGE_ID = os.getenv("FB_PAGE_ID")
access_token = os.getenv("fb_token")
graph_api = "v22.0"
min_followers = 50000
metadata = MetaData()
ig_id= os.getenv("ig_business_id")

#====================configuration ===========
INSTAGRAM_NON_PROFILE_PATHS = {"explore", "p", "reel", "stories", "tv", "accounts"}
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/114.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 Version/15.1 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/113.0.0.0 Safari/537.36",
]

def get_all_proxies():
    customer_id = "hl_8d90267d"
    zone_name = "quantum"
    password = "v4vzyr0ixuob"
    proxy_host = "brd.superproxy.io"
    proxy_port = 33335

    country_codes = ["us", "ng", "za", "ke", "eg", "gh", "tz", "ug", "uk", "ca", "in"]
    proxies_list = []

    for country_code in country_codes:
        proxy_username = f"brd-customer-{customer_id}-zone-{zone_name}-country-{country_code}"
        proxy_url = f"http://{proxy_username}:{password}@{proxy_host}:{proxy_port}"
        proxies_list.append(proxy_url)

    return proxies_list

proxy_string = random.choice(get_all_proxies()) if get_all_proxies() else None
async def main() -> None:
    """Scrape Instagram profiles from Google Search using keywords."""
    async with Actor:
        actor_input = await Actor.get_input() or {}
        keywords = [kw.strip() for kw in actor_input.get("keywords", []) if kw.strip()]
        if not keywords:
            raise ValueError('Missing or empty "keywords" list in input!')
 
        links = []

        async with AsyncClient(proxy=proxy_string, verify=False) as client:
            for kw in keywords:
                query = f"site:instagram.com {kw}"
                encoded_query = urllib.parse.quote_plus(query)
                base_url = f"https://www.google.com/search?q={encoded_query}"
                Actor.log.info(f"  Starting search for keyword: '{kw}'")

                for page in range(0, 13):  # up to 130 results
                    paginated_url = f"{base_url}&start={page * 10}"
                    headers = {
                        "User-Agent": random.choice(USER_AGENTS)
                    }

                    Actor.log.info(f"  Fetching page {page + 1} for keyword '{kw}'")

                    try:
                        response = await client.get(
                            paginated_url,
                            headers=headers,
                            follow_redirects=True
                        )
                        await asyncio.sleep(random.uniform(2, 5))
                        response.raise_for_status()
                    except Exception as e:
                        Actor.log.error(f" Request failed on page {page + 1} for keyword '{kw}': {type(e).__name__}: {str(e)}")
                        continue

                    soup = BeautifulSoup(response.content, "lxml")
                    for a in soup.select("a"):
                        href = a.get("href", "")
                        if "instagram.com" in href:
                            if href.startswith("https://www.instagram.com/"):
                                if href not in [l["profile_url"] for l in links]:
                                    links.append({"profile_url": href})

                    await asyncio.sleep(random.uniform(2, 5))

        Actor.log.info(f" Found {len(links)} unique Instagram profiles .")
        await Actor.push_data(links)

    link = [l for l in links if "instagram.com" in l["profile_url"]]
    Actor.log.info(f"  Found {len(link)} Instagram profile links.")
    usernames = set()
    if not link: 
        Actor.log.warning("No Instagram profiles found.")
    else:
        Actor.log.info(f"Found {len(link)} Instagram profiles: {link}")
        for item in link:
            url = item["profile_url"]
            username = extract_username_from_url(url)
            if username:
                usernames.add(username)

        Actor.log.info(f"Extracted usernames: {usernames}")

    if not access_token or not FB_PAGE_ID:
        logging.error("Missing 'fb_token' or 'fb_page_id' in .env.")
        return

    ig_business_id = ig_id
    if not ig_business_id:
        logging.error("Unable to get Instagram business ID. Exiting.")
        return

    logging.info(f"Using IG Business ID: {ig_business_id}")
    
    validated_influencer_data = []
    validated_usernames = set()

    logging.info("--- Starting parallel validation with Facebook Graph API ---")
    
    with ThreadPoolExecutor(max_workers=3) as executor: 
        future_to_username = {
            executor.submit(validate_and_fetch_user, username, ig_business_id): username
            for username in list(usernames) 
            }

        for future in as_completed(future_to_username):
            username = future_to_username[future]
            try:
                user_details = future.result()
                if user_details:
                    validated_usernames.add(user_details.get("username"))
                    posts = user_details.get("media", {}).get("data", [])
                    if not posts:
                        validated_influencer_data.append({
                            "user_id": user_details.get("id"),
                            "username": user_details.get("username"),
                            "profile_url": f"https://www.instagram.com/{user_details.get('username')}/",
                            "name": user_details.get("name"),
                            "profile_picture_url": user_details.get("profile_picture_url"),
                            "bio": user_details.get("biography"),
                            "follower_count": user_details.get("followers_count"),
                            "media_count": user_details.get("media_count"),
                            "post_caption": None,
                            "like_count": None,
                            "comments_count": None,
                            "timestamp": None,
                            "post_media_url": None,
                            "post_permalink": None,
                        })
                    else:
                        for post in posts:
                            validated_influencer_data.append({
                                "user_id": user_details.get("id"),
                                "username": user_details.get("username"),
                                "profile_url": f"https://www.instagram.com/{user_details.get('username')}/",
                                "name": user_details.get("name"),
                                "profile_picture_url": user_details.get("profile_picture_url"),
                                "bio": user_details.get("biography"),
                                "follower_count": user_details.get("followers_count"),
                                "media_count": user_details.get("media_count"),
                                "post_caption": post.get("caption"),
                                "like_count": post.get("like_count"),
                                "comments_count": post.get("comments_count"),
                                "timestamp": post.get("timestamp"),
                                "post_media_url": post.get("media_url"),
                                "post_permalink": post.get("permalink"),
                            })
            except Exception as e:
                logging.error(f"Error processing @{username}: {e}")

    logging.info(f"--- Finished validation. Valid influencers: {len(validated_usernames)}. Total rows (posts): {len(validated_influencer_data)}")

    if validated_influencer_data:
        df = pd.DataFrame(validated_influencer_data)
        columns_order = [
            "user_id","username", "name", "profile_url", "follower_count", "bio", "media_count",
            "profile_picture_url", "timestamp", "post_caption", "like_count",
            "comments_count", "post_media_url", "post_permalink"
        ]
        df_columns = [col for col in columns_order if col in df.columns]
        df = df[df_columns]

        # Data Cleaning and Type Conversion
        df['bio'] = df['bio'].astype(str).replace("/", "", regex=True)
        df['follower_count'] = df['follower_count'].fillna(0).astype(int)
        df['like_count'] = df['like_count'].fillna(0).astype(int)
        df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
        df['profile_picture_url'] = df['profile_picture_url'].str.rstrip("/")
        df['post_media_url'] = df['post_media_url'].str.rstrip("/")
        df['comments_count'] = df['comments_count'].fillna(0).astype(int)
        df['bio'] = df['bio'].astype(str).str.replace(r'@\w+', '', regex=True)
        df['post_caption'] = df['post_caption'].fillna("").astype(str).str.replace(r'http\S+|www\S+|https\S+', '', regex=True)
        df['post_caption'] = df['post_caption'].astype(str).str.replace(r'@\w+', '', regex=True)

        # Define table schema using SQLAlchemy
        influencer_table = Table(
            "influencer_instagram",
            metadata,
            Column("id", Integer, primary_key=True, autoincrement=True),
            Column("user_id", BigInteger),
            Column("username", Text),
            Column("name", Text),
            Column("profile_url", Text),
            Column("follower_count", BigInteger),
            Column("bio", Text),
            Column("media_count", BigInteger),
            Column("profile_picture_url", Text),
            Column("timestamp", TIMESTAMP),
            Column("post_caption", Text),
            Column("like_count", BigInteger),
            Column("comments_count", BigInteger),
            Column("post_media_url", Text),
            Column("post_permalink", Text)
        )

        engine = create_db_connection()
        if engine:

            try:
                
                df.to_sql("influencer_instagram", con=engine,
                          if_exists="append", index=False, method='multi')
                logging.info(f"Data successfully saved to database table 'influencer_instagram'.")
            except Exception as e:
                logging.error(f"Error saving data to database: {e}")
        else:
            logging.error("Failed to establish database connection. Data not saved.")
    else:
        logging.warning("No influencers met the criteria after validation, or no data could be fetched.")

