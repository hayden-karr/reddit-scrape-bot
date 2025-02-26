import praw
import polars as pl
import requests
import os
import json
from pathlib import Path
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

# load environment variables
load_dotenv()

# Reddit API Setup (Replace with your credentials)
reddit = praw.Reddit(
    client_id= os.getenv("client_id"),
    client_secret=os.getenv("reddit_client_secret"),
    user_agent=os.getenv("user_agent")
)

# Configuration
SUBREDDIT_NAME = "dataengineering"  # Change as needed
SUBREDDIT_DIR = Path(f"reddit_data_{SUBREDDIT_NAME}") # Main subreddit folder
IMAGE_DIR = SUBREDDIT_DIR / f"images_{SUBREDDIT_NAME}" # Store images
CACHE_FILE = SUBREDDIT_DIR / f"cache_{SUBREDDIT_NAME}.json"  # Cache file for this subreddit
DATA_FILE = SUBREDDIT_DIR / f"reddit_posts_{SUBREDDIT_NAME}.parquet"  # Parquet file with subreddit name

# Create necessary directories
SUBREDDIT_DIR.mkdir(parents=True, exist_ok=True)
IMAGE_DIR.mkdir(exist_ok=True)

# Load cache 
cached_posts = set(json.load(open(CACHE_FILE, "r"))) if CACHE_FILE.exists() else set()

# Fetch posts lazily
def fetch_posts(subreddit_name, limit=None):
    subreddit = reddit.subreddit(subreddit_name)
    yield from subreddit.new(limit=limit)


# Fetch all comments
def get_all_comments(post):
    post.comments.replace_more(limit=0)
    return [comment.body for comment in post.comments.list()]

# multi-thread image processing
def download_image(image_url, post_id):
    try:
        response = requests.get(image_url, stream=True)
        if response.status_code == 200:
            image_path = IMAGE_DIR / f"{post_id}.webp"
            
            with open(image_path, "wb") as f:
                for chunk in response.iter_content(1024):
                    f.write(chunk)
            return str(image_path)
    except Exception as e:
        print(f"Image download failed: {e}")
        return None
    
# Process images concurrently
def process_images_concurrently(posts):
    with ThreadPoolExecutor() as executor:
        return list(executor.map(lambda p: download_image(p["image_url"], p["post_id"]) if p["image_url"] else None, posts))

# Save data incrementally
def save_parquet(new_data):
    if DATA_FILE.exists():
        existing_df = pl.read_parquet(DATA_FILE)
        df = pl.concat([existing_df, new_data], how="vertical").unique(subset=["post_id"])
    else:
        df = new_data
    df = df.sort("created_utc", descending=True) # sort by post time in desc so most recent at the top
    df.write_parquet(DATA_FILE, compression="zstd")


# Collecting Post Data
posts_data = [
    {
        "post_id": post.id,
        "title": post.title,
        "text": post.selftext,
        "created_utc": post.created_utc,
        "created_time": datetime.fromtimestamp(post.created_utc, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S'),
        "image_url": post.url if post.url.endswith(('.jpg', '.jpeg', '.png')) else None,
        "comments": get_all_comments(post),
    }
    for post in fetch_posts(SUBREDDIT_NAME, limit=10) if post.id not in cached_posts
]

# Fetch images in parallel
image_paths = process_images_concurrently(posts_data)

# Assign images
for i, image_path in enumerate(image_paths):
    posts_data[i]["image_path"] = image_path

# Save cache
with open(CACHE_FILE, "w") as f:
    json.dump(list(cached_posts), f)

# Convert to Polars DataFrame & save
if posts_data:
    new_df = pl.DataFrame(posts_data)
    save_parquet(new_df)

print(f"Scraping Complete! {len(posts_data)} new posts added.")