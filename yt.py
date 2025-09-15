import os
import requests

import duckdb
import pandas as pd

from psycopg2.extras import execute_values
from sqlalchemy import text
from requests.adapters import HTTPAdapter
from requests.sessions import Session
from urllib3.util import Retry
from datetime import datetime

from .database import connect_to_database
from .inst import remove_emojis
import csv
import re
import logging
from dotenv import load_dotenv


load_dotenv()

#======================== Config ==========================================
logging.basicConfig(format="%(asctime)s - %(message)s", level=logging.INFO)
API_KEY = os.getenv("YOUTUBE_API_KEY")

# ============Setup HTTP session with retry mechanism
session = Session()
retry = Retry(total=2, backoff_factor=5, status_forcelist=[400, 429, 403, 500])
session.mount("https://", HTTPAdapter(max_retries=retry))

# API endpoints
CHANNELS_URL = "https://www.googleapis.com/youtube/v3/channels"
PLAYLIST_ITEMS_URL = "https://www.googleapis.com/youtube/v3/playlistItems"
VIDEOS_URL = "https://www.googleapis.com/youtube/v3/videos"

def read_usernames(file_path):
    """Read usernames from CSV file, removing leading '@'."""
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            reader = csv.reader(f)
            return [row[0].strip() for row in reader if row]
    except Exception as e:
        logging.error(f"Error reading usernames: {e}")
        return []

def clean_text(text):
    """Clean text data by removing unwanted characters and normalizing."""
    if not text:
        return ""
    text = re.sub(r'\s+', ' ', text.strip())
    return text[:65535]
# ============== Data Ingestion and Parsing Json============================
def get_channel_details(username, api_key):
    """Fetch channel details from YouTube API."""
    params = {
        "part": "snippet,contentDetails,statistics",
        "forHandle": username,
        "key": api_key
    }
    try:
        res = session.get(CHANNELS_URL, params=params)
        logging.info(f"Fetching channel details for {username}, Status: {res.status_code}")
        if res.status_code == 200:
            data = res.json()
            if "items" in data and data["items"]:
                channel = data["items"][0]
                return {
                    "channel_id": channel["id"],
                    "channel_title": clean_text(channel["snippet"].get("title", "")),
                    "channel_description": clean_text(channel["snippet"].get("description", "")),
                    "channel_created_at": channel["snippet"].get("publishedAt"),
                    "profile_url": f"https://www.youtube.com/@{username}",
                    "thumbnail_url": channel["snippet"]["thumbnails"]["high"]["url"],
                    "subscriber_count": int(channel["statistics"].get("subscriberCount", 0)),
                    "total_video_count": int(channel["statistics"].get("videoCount", 0)),
                    "total_view_count": int(channel["statistics"].get("viewCount", 0)),
                    "uploads_playlist_id": channel["contentDetails"]["relatedPlaylists"]["uploads"]
                }
            logging.warning(f"No channel found for username: {username}")
            return None
        logging.error(f"Channel request failed: {res.text}")
        return None
    except Exception as e:
        logging.error(f"Error fetching channel details for {username}: {e}")
        return None

def get_channel_videos(playlist_id, api_key, max_results=5):
    """Fetch recent videos from channel's uploads playlist."""
    params = {
        "part": "snippet,contentDetails",
        "playlistId": playlist_id,
        "maxResults": max_results,
        "key": api_key
    }
    try:
        res = session.get(PLAYLIST_ITEMS_URL, params=params)
        if res.status_code != 200:
            logging.error(f"Playlist fetch failed: {res.text}")
            return []
        data = res.json()
        videos = []
        for item in data.get("items", []):
            video_id = item["contentDetails"]["videoId"]
            videos.append({
                "video_id": video_id,
                "video_title": clean_text(item["snippet"]["title"]),
                "video_description": clean_text(item["snippet"].get("description", "")),
                "video_published_at": item["contentDetails"]["videoPublishedAt"],
                "video_url": f"https://www.youtube.com/watch?v={video_id}"
            })
        return videos
    except Exception as e:
        logging.error(f"Error fetching videos for playlist {playlist_id}: {e}")
        return []

def get_video_stats(video_ids, api_key):
    """Fetch statistics for given video IDs."""
    if not video_ids:
        return {}
    params = {
        "part": "statistics",
        "id": ",".join(video_ids),
        "key": api_key
    }
    try:
        res = session.get(VIDEOS_URL, params=params)
        if res.status_code != 200:
            logging.error(f"Video stats fetch failed: {res.text}")
            return {}
        data = res.json()
        stats = {}
        for item in data.get("items", []):
            stats[item["id"]] = {
                "video_views": int(item["statistics"].get("viewCount", 0)),
                "video_likes": int(item["statistics"].get("likeCount", 0)),
                "video_comments": int(item["statistics"].get("commentCount", 0)),
            }
        return stats
    except Exception as e:
        logging.error(f"Error fetching video stats: {e}")
        return {}


def youtube_data_pipeline(usernames, api_key, max_videos=10):
    """Main pipeline to fetch and process YouTube data."""
    rows = []
    for username in usernames:
        logging.info(f"Processing username: {username}")
        channel_data = get_channel_details(username, api_key)
        if not channel_data:
            continue
        videos = get_channel_videos(channel_data["uploads_playlist_id"], api_key, max_results=max_videos)
        video_ids = [v["video_id"] for v in videos]
        video_stats = get_video_stats(video_ids, api_key)
        for video in videos:
            vid = video["video_id"]
            video_stat = video_stats.get(vid, {})

            #========================= Parse response and Map Schema ======================#
            row = {
                "channel_id": channel_data["channel_id"],
                "username": username,
                "channel_title": channel_data["channel_title"],
                "channel_description": channel_data["channel_description"],
                "subscriber_count": channel_data["subscriber_count"],
                "total_view_count": channel_data["total_view_count"],
                "total_video_count": channel_data["total_video_count"],
                "uploads_playlist_id": channel_data["uploads_playlist_id"],
                "channel_created_at": channel_data["channel_created_at"],
                "profile_url": channel_data["profile_url"],
                "thumbnail_url": channel_data["thumbnail_url"],
                "video_id": video["video_id"],
                "video_title": video["video_title"],
                "video_description": video["video_description"],
                "video_published_at": video["video_published_at"],
                "video_url": video["video_url"],
                "video_views": video_stat.get("video_views", 0),
                "video_likes": video_stat.get("video_likes", 0),
                "video_comments": video_stat.get("video_comments", 0),
                "created_at": datetime.utcnow(),
                "updated_at": datetime.utcnow()
                }
            rows.append(row)
    if not rows:
        logging.warning("No data fetched from YouTube API")
        return None
    return rows

def create_youtube_data_table(conn):
    """Create the YouTube data table if it doesn't exist."""
    create_table_query = """
    CREATE TABLE IF NOT EXISTS influencer_youtube (
        id SERIAL PRIMARY KEY,
        channel_id VARCHAR(100) NOT NULL,
        username VARCHAR(100) NOT NULL,
        channel_title TEXT,
        channel_description TEXT,
        subscriber_count BIGINT,
        total_view_count BIGINT,
        total_video_count BIGINT,
        uploads_playlist_id VARCHAR(100),
        channel_created_at TIMESTAMP,
        profile_url VARCHAR(255),
        thumbnail_url VARCHAR(255),
        video_id VARCHAR(100),
        video_title TEXT,
        video_description TEXT,
        video_published_at TIMESTAMP,
        video_url VARCHAR(255),
        video_views BIGINT,
        video_likes BIGINT,
        video_comments BIGINT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE (channel_id, video_id)
    );
    """
    try:
        with conn.cursor() as cursor:
            cursor.execute(create_table_query)
            conn.commit()
        logging.info("YouTube data table created or already exists")
    except Exception as e:
        logging.error(f"Error creating table: {e}")
        conn.rollback()


# usernames = run
from psycopg2.extras import execute_values

def youtube_data(usernames):
    """Main execution function."""
    data = youtube_data_pipeline(usernames, API_KEY, max_videos=10)
    columns = [
        "channel_id","username", "channel_title", "channel_description", "subscriber_count",
        "total_view_count", "total_video_count", "uploads_playlist_id","channel_created_at","profile_url", "thumbnail_url",
        "video_id","video_title", "video_description", "video_published_at", 
        "video_url", "video_views", "video_likes", "video_comments", "created_at", "updated_at"
    ]

    df = pd.DataFrame(data)
    df = df[[col for col in columns if col in df.columns]]
    if df.empty:
        logging.warning("DataFrame is empty after filtering. Skipping.")
        return

    # ==================== Clean and cast types===================
    df['channel_id'] = df['channel_id'].astype(str)
    df['username'] = df['username'].astype(str)
    df['channel_title'] = df['channel_title'].astype(str)
    df['channel_description'] = df['channel_description'].apply(remove_emojis).replace(r"[#@/]", ' ', regex=True)
    df['subscriber_count'] = df['subscriber_count'].fillna(0).astype(int)
    df['total_view_count'] = df['total_view_count'].fillna(0).astype(int)
    df['total_video_count'] = df['total_video_count'].fillna(0).astype(int)
    df['uploads_playlist_id'] = df['uploads_playlist_id'].astype(str)
    df['profile_url'] = df['profile_url'].astype(str)
    df['thumbnail_url'] = df['thumbnail_url'].astype(str)
    df['video_id'] = df['video_id'].astype(str)
    df['video_title'] = df['video_title'].astype(str).apply(remove_emojis).replace(r'[#@/&]', ' ', regex=True)
    df['video_description'] = df['video_description'].apply(remove_emojis).replace(r"[#@/&]", " ", regex=True)
    df['video_published_at'] = pd.to_datetime(df['video_published_at'], errors="coerce")
    df['video_url'] = df['video_url'].astype(str)
    df['video_views'] = df['video_views'].fillna(0).astype(int)
    df['video_likes'] = df['video_likes'].fillna(0).astype(int)
    df['video_comments'] = df['video_comments'].fillna(0).astype(int)
    df['created_at'] = pd.to_datetime(df['created_at'], errors="coerce")
    df['updated_at'] = pd.to_datetime(df['updated_at'], errors="coerce")

    # Convert DataFrame to list of tuples
    records = [tuple(row) for row in df.to_numpy()]

    engine = connect_to_database()
    
    if not engine:
        logging.error("Failed to connect to database")
        return
    create_youtube_data_table(engine)
    try:
        with engine.cursor() as cursor:
            col_names = ", ".join(columns)
            insert_query = f"""
                INSERT INTO influencer_youtube ({col_names})
                VALUES %s
                ON CONFLICT (channel_id, video_id) 
                DO UPDATE  SET 
                    channel_id = EXCLUDED.channel_id,
                    username = EXCLUDED.username, 
                    channel_title = EXCLUDED.channel_title, 
                    channel_description = EXCLUDED.channel_description, 
                    subscriber_count = EXCLUDED.subscriber_count,
                    total_view_count = EXCLUDED.total_view_count, 
                    total_video_count = EXCLUDED.total_video_count, 
                    uploads_playlist_id = EXCLUDED.uploads_playlist_id,
                    channel_created_at = EXCLUDED.channel_created_at,
                    profile_url = EXCLUDED.profile_url, 
                    thumbnail_url = EXCLUDED.thumbnail_url,
                    video_id = EXCLUDED.video_id,
                    video_title = EXCLUDED.video_title, 
                    video_description = EXCLUDED.video_description, 
                    video_published_at = EXCLUDED.video_published_at, 
                    video_url = EXCLUDED.video_url, 
                    video_views = EXCLUDED.video_views, 
                    video_likes = EXCLUDED.video_likes, 
                    video_comments = EXCLUDED.video_comments,
                    created_at = EXCLUDED.created_at, 
                    updated_at = EXCLUDED.updated_at;
            """
            execute_values(cursor, insert_query, records)
            engine.commit()
        logging.info(f"Inserted {len(records)} rows into influencer_youtube")
    except Exception as e:
        logging.error(f"Error inserting data: {e}")
        engine.rollback()
    finally:
        engine.close()

import csv
if __name__ == "__main__":
    path = r"C:/Users/Bamidele/Desktop/inf/usernames.csv"
    usernames = []
    with open(path, 'r', newline='', encoding='utf-8') as f:
        reader = csv.reader(f)
        for row in reader:
            username = row[0]
            if username:
                
                username = username.strip()
                if len(username) > 2: 
                    usernames.append(username)
    
    # usernames = ["patoranking", "wizkidayo"]

    youtube_data(usernames)
