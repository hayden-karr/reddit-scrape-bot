import praw
import polars as pl
import requests
import os
from pathlib import Path
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
import re
from urllib.parse import urlparse, parse_qs
from PIL import Image
from io import BytesIO
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Limit to 10 million pixels to prevent large decompressions
Image.MAX_IMAGE_PIXELS = 25000000 

# load environment variables
load_dotenv()

# Configuration
POST_LIMIT = 100
SUBREDDIT_NAME = "dataengineering"  # Change as needed
SUBREDDIT_DIR = Path("scraped_subreddits") / f"reddit_data_{SUBREDDIT_NAME}" # Main subreddit folder
IMAGE_DIR = SUBREDDIT_DIR / f"images_{SUBREDDIT_NAME}" # Store images
CACHE_FILE = SUBREDDIT_DIR / f"cache_{SUBREDDIT_NAME}.json"  # Cache file for this subreddit
DATA_FILE = SUBREDDIT_DIR / f"reddit_posts_{SUBREDDIT_NAME}.parquet"  # Parquet file with subreddit name
COMMENTS_FILE = SUBREDDIT_DIR / f"reddit_comments_{SUBREDDIT_NAME}.parquet" # Parquet file with subreddit name for comments

# Create necessary directories
SUBREDDIT_DIR.mkdir(parents=True, exist_ok=True)
IMAGE_DIR.mkdir(exist_ok=True)


# Configure retry strategy for requests
retry_strategy = Retry(
    total=3,                # Max retries
    backoff_factor=1,       # Exponential backoff (1s, 2s, 4s)
    status_forcelist=[429, 500, 502, 503, 504],  # Retry on these status codes
    allowed_methods=["GET"],  # Only retry for GET requests
)

# Mount retry strategy to a session
session = requests.Session()
session.mount("https://", HTTPAdapter(max_retries=retry_strategy))


# Reddit API Setup (Replace with your credentials)
reddit = praw.Reddit(
    client_id= os.getenv("client_id"),
    client_secret=os.getenv("reddit_client_secret"),
    user_agent=os.getenv("user_agent")
)


# Fetch posts lazily
def fetch_posts(subreddit_name, limit=POST_LIMIT):
    subreddit = reddit.subreddit(subreddit_name)
    yield from subreddit.new(limit=limit)

# Image download with retry mechanism
def download_image(image_url, item_id, is_comment=False):
    """Download an image and save it as WEBP, retrying with headers"""
    
    if not image_url:
        return None
    
    image_path = IMAGE_DIR / (f"comment_{item_id}.webp" if is_comment else f"{item_id}.webp")
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    }
    try:
        response = session.get(image_url, stream=True)  # Try without headers first
        response.raise_for_status()
    except requests.RequestException:
        try:
            response = session.get(image_url, headers=headers, stream=True)  # Retry with headers
            response.raise_for_status()
        except requests.RequestException as e:
            print(f"Image download failed after retries: {e}")
            return None

    # Process and save the image if successful
    try:
        image = Image.open(BytesIO(response.content))
        image.save(image_path, "WEBP", quality=80)
        return str(image_path)
    except Exception as e:
        print(f"Error processing image: {e}")
        return None


# Find a valid image URL in the comment text.
def extract_image_url(text):

    image_extensions = (".jpg", ".jpeg", ".png", ".webp")

    # Regex pattern for URLs
    url_pattern = r'https?://[^\s)]+'
    urls = re.findall(url_pattern, text)

    for url in urls:
        try:
            # Try to parse the URL
            parsed_url = urlparse(url)
            path = parsed_url.path.lower()

            # Check if the URL has a valid image extension
            if path.endswith(image_extensions):
                return url

            # Handle Reddit's image links with query parameters (e.g., ?format=pjpg)
            query_params = parse_qs(parsed_url.query)
            if "format" in query_params and query_params["format"][0] in ["jpg", "jpeg", "png", "webp"]:
                return url 

        except ValueError as e:
            # If there's a ValueError, print the error and skip to the next URL
            print(f"Skipping invalid URL: {url} due to error: {e}")
            continue  # Continue with the next URL

    return None  # No valid image found

# Save data incrementally
def save_posts_parquet(new_data):

    # Define schema
    post_schema = {
        "post_id": pl.Utf8,
        "title": pl.Utf8,
        "text": pl.Utf8,
        "created_utc": pl.Int64,
        "created_time": pl.Utf8,
        "image_url": pl.Utf8,
        "image_path": pl.Utf8
    }

    # Apply schema to new data
    new_data = new_data.cast(post_schema)

    if DATA_FILE.exists():
        existing_df = pl.read_parquet(DATA_FILE).cast(post_schema)
        df = pl.concat([existing_df, new_data], how="vertical").unique(subset=["post_id"])
    else:
        df = new_data

    df.sort("created_utc", descending=True).write_parquet(DATA_FILE, compression="zstd") # sort by post time in desc so most recent at the top
    
# Save Comments Data Separately
def save_comments_parquet(new_comments):

    # Define schema
    comment_schema = {
        "comment_id": pl.Utf8,
        "post_id": pl.Utf8,
        "parent_id": pl.Utf8,
        "text": pl.Utf8,
        "image_url": pl.Utf8,
        "image_path": pl.Utf8
    }

    new_comments = new_comments.cast(comment_schema)

    if COMMENTS_FILE.exists():
        existing_df = pl.read_parquet(COMMENTS_FILE).cast(comment_schema)
        df = pl.concat([existing_df, new_comments], how="vertical").unique(subset=["comment_id"])
    else:
        df = new_comments

    df.write_parquet(COMMENTS_FILE, compression="zstd")


# Collecting Post Data & Comments efficiently in a list comprehension

fetched_posts = fetch_posts(SUBREDDIT_NAME, limit=POST_LIMIT)

posts_data = [
    {
        "post_id": post.id,
        "title": post.title,
        "text": post.selftext,
        "created_utc": post.created_utc,
        "created_time": datetime.fromtimestamp(post.created_utc, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S'),
        "image_url": extract_image_url(post.url),
        "image_path": download_image(extract_image_url(post.url), post.id)
    }
    for post in fetched_posts 
]

comments_data = [
    {
        "comment_id": comment.id,
        "post_id": post.id,
        "parent_id": comment.parent_id.split('_')[-1] if "_" in comment.parent_id else None,
        "text": comment.body,
        "image_url": extract_image_url(comment.body),  # Extract image URL
        "image_path": download_image(extract_image_url(comment.body), comment.id, is_comment=True),

    }
    for post in fetch_posts(SUBREDDIT_NAME, limit=POST_LIMIT) 
    for _ in [post.comments.replace_more(limit=None)]  # Expand all hidden comments
    for comment in post.comments.list()
]

# Download images concurrently
with ThreadPoolExecutor() as executor:
    post_futures = {executor.submit(download_image, post["image_url"], post["post_id"]): post for post in posts_data}
    comment_futures = {executor.submit(download_image, comment["image_url"], comment["comment_id"], True): comment for comment in comments_data}

    for future in post_futures:
        post_futures[future]["image_path"] = future.result()

    for future in comment_futures:
        comment_futures[future]["image_path"] = future.result()

# Convert to Polars DataFrame & save
if posts_data:
    save_posts_parquet(pl.DataFrame(posts_data))

# Convert Comments to Polars DataFrame & save
if comments_data:
    save_comments_parquet(pl.DataFrame(comments_data))

print(f"Scraping Complete! {len(posts_data)} new posts added.")