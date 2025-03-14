import requests
import polars as pl
from pathlib import Path
from datetime import datetime, timezone
from io import BytesIO
from PIL import Image
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
import argparse

# Session with retry mechanism
session = requests.Session()
retries = Retry(total=5, backoff_factor=0.3, status_forcelist=[500, 502, 503, 504])
adapter = HTTPAdapter(max_retries=retries)
session.mount("https://", adapter)

# Convert human-readable date to UTC timestamp
def to_utc_timestamp(date_str):
    dt = datetime.strptime(date_str, "%Y-%m-%d")
    return int(dt.replace(tzinfo=timezone.utc).timestamp())

# Fetch posts from PullPush
def fetch_pullpush_posts(subreddit, before=None, after=None, size=100):
    base_url = "https://api.pullpush.io/reddit/search/submission/"
    params = {
        "subreddit": subreddit,
        "size": size,
        "sort": "desc",
        "before": before,
        "after": after,
        "fields": ["id", "title", "selftext", "created_utc", "url"]
    }
    response = session.get(base_url, params=params)
    return response.json().get("data", [])

# Fetch comments from PullPush
def fetch_pullpush_comments(subreddit, before=None, after=None, size=100):
    base_url = "https://api.pullpush.io/reddit/search/comment/"
    params = {
        "subreddit": subreddit,
        "size": size,
        "sort": "desc",
        "before": before,
        "after": after,
        "fields": ["id", "link_id", "parent_id", "body", "created_utc"]
    }
    response = session.get(base_url, params=params)
    return response.json().get("data", [])

# Image download function with session
def download_image(image_url, file_name):
    if not image_url:
        return None
    image_path = IMAGE_DIR / f"{file_name}.webp"
    try:
        response = session.get(image_url, stream=True)
        response.raise_for_status()
        image = Image.open(BytesIO(response.content))
        image.save(image_path, "WEBP", quality=80)
        return str(image_path)
    except Exception as e:
        print(f"Failed to download image {image_url} without headers: {e}")
        try:
            headers = {"User-Agent": "Mozilla/5.0"}
            response = session.get(image_url, headers=headers, stream=True)
            response.raise_for_status()
            image = Image.open(BytesIO(response.content))
            image.save(image_path, "WEBP", quality=80)
            return str(image_path)
        except Exception as e:
            print(f"Failed to download image {image_url} with headers: {e}")
            return None

# Load existing data
def load_existing_data(file_path, schema):
    return pl.read_parquet(file_path) if file_path.exists() else pl.DataFrame(schema=schema)

# Merge and save data
def save_parquet(new_data, file_path, schema):
    existing_df = load_existing_data(file_path, schema)
    df = pl.concat([existing_df, new_data], how="vertical").unique(subset=[schema[0][0]])
    df.write_parquet(file_path, compression="zstd")
    print(f"Saved {len(new_data)} new records to {file_path}")

# Process and save posts
def process_pullpush_posts(subreddit, before=None, after=None):
    print(f"Fetching posts for r/{subreddit}...")
    posts = fetch_pullpush_posts(subreddit, before, after)
    if not posts:
        print("No new posts found.")
        return
    
    image_paths = []
    image_urls = []
    
    for p in posts:
        url = p.get("url", "")
        image_url = url if url.endswith(('.jpg', '.jpeg', '.png')) else None
        image_path = download_image(image_url, p["id"]) if image_url else None
        
        image_urls.append(image_url)
        image_paths.append(image_path)
    
    df = pl.DataFrame({
        "post_id": [p["id"] for p in posts],
        "title": [p["title"] for p in posts],
        "text": [p["selftext"] for p in posts],
        "created_utc": [int(p["created_utc"]) for p in posts],  
        "created_time": [datetime.fromtimestamp(p["created_utc"], tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S') for p in posts],
        "image_url": image_urls,
        "image_path": image_paths
    })

    post_schema = [
        ("post_id", pl.Utf8), ("title", pl.Utf8), ("text", pl.Utf8), 
        ("created_utc", pl.Int64), ("created_time", pl.Utf8), 
        ("image_url", pl.Utf8), ("image_path", pl.Utf8)
    ]
    save_parquet(df, POSTS_FILE, post_schema)

# Process and save comments
def process_pullpush_comments(subreddit, before=None, after=None):
    print(f"Fetching comments for r/{subreddit}...")
    comments = fetch_pullpush_comments(subreddit, before, after)
    if not comments:
        print("No new comments found.")
        return
    
    image_paths = []
    image_urls = []
    
    for c in comments:
        text = c.get("body", "")
        image_url = next((word for word in text.split() if word.startswith("http") and word.endswith(('.jpg', '.jpeg', '.png','.webp'))), None)
        image_path = download_image(image_url, f"comment_{c['id']}") if image_url else None
        
        image_urls.append(image_url)
        image_paths.append(image_path)
    
    df = pl.DataFrame({
        "comment_id": [c["id"] for c in comments],
        "post_id": [c["link_id"].split("_")[-1] for c in comments],  
        "parent_id": [c["parent_id"].split("_")[-1] for c in comments],
        "text": [c["body"] for c in comments],
        "created_utc": [int(c["created_utc"]) for c in comments],  
        "created_time": [datetime.fromtimestamp(c["created_utc"], tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S') for c in comments],
        "image_url": image_urls,
        "image_path": image_paths
    })
    
    comment_schema = [
        ("comment_id", pl.Utf8), ("post_id", pl.Utf8), ("parent_id", pl.Utf8), 
        ("text", pl.Utf8), ("created_utc", pl.Int64), ("created_time", pl.Utf8), 
        ("image_url", pl.Utf8), ("image_path", pl.Utf8)
    ]
    save_parquet(df, COMMENTS_FILE, comment_schema)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Scrape Reddit posts and comments using PullPush API.")
    parser.add_argument("--subreddit", type=str, required=True, help="Subreddit to fetch data from")
    parser.add_argument("--before", type=str, help="Fetch data before this date (YYYY-MM-DD)")
    parser.add_argument("--after", type=str, help="Fetch data after this date (YYYY-MM-DD)")
    parser.add_argument("--all", action="store_true", help="Fetch all data")
    args = parser.parse_args()

    before_timestamp = to_utc_timestamp(args.before) if args.before else None
    after_timestamp = to_utc_timestamp(args.after) if args.after else None

    SUBREDDIT_NAME = args.subreddit  # Use the passed subreddit name
    SUBREDDIT_DIR = Path("scraped_subreddits") / f"reddit_data_{SUBREDDIT_NAME}"
    IMAGE_DIR = SUBREDDIT_DIR / f"images_{SUBREDDIT_NAME}"
    POSTS_FILE = SUBREDDIT_DIR / f"reddit_posts_{SUBREDDIT_NAME}.parquet"
    COMMENTS_FILE = SUBREDDIT_DIR / f"reddit_comments_{SUBREDDIT_NAME}.parquet"

    # Ensure directories exist
    SUBREDDIT_DIR.mkdir(parents=True, exist_ok=True)
    IMAGE_DIR.mkdir(parents=True, exist_ok=True)

    print(f"Starting Reddit data collection for r/{SUBREDDIT_NAME}...")
    process_pullpush_posts(SUBREDDIT_NAME, before=before_timestamp, after=after_timestamp)
    process_pullpush_comments(SUBREDDIT_NAME, before=before_timestamp, after=after_timestamp)
    print("Data collection complete.")