import praw
import polars as pl
import requests
import os
import json
from pathlib import Path
from PIL import Image
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor

# load environment variables
load_dotenv()



# Reddit API Setup (Replace with your credentials)
reddit = praw.Reddit(
    client_id= os.getenv("client_id"),
    client_secret=os.getenv("reddit_client_secret"),
    user_agent=os.getenv("user_agent")
)

# Post limit if needed
POST_LIMIT = 5  # Number of posts to fetch

# Configuration
SUBREDDIT_NAME = "dataengineering"  # Change as needed
SUBREDDIT_DIR = Path(f"reddit_data_{SUBREDDIT_NAME}") # Main subreddit folder
IMAGE_DIR = SUBREDDIT_DIR / f"images_{SUBREDDIT_NAME}" # Store images
CACHE_FILE = SUBREDDIT_DIR / f"cache_{SUBREDDIT_NAME}.json"  # Cache file for this subreddit
DATA_FILE = SUBREDDIT_DIR / f"reddit_posts_{SUBREDDIT_NAME}.parquet"  # Parquet file with subreddit name
MAX_IMAGE_WIDTH = 800  # Resize images if larger
IMAGE_LOSSLESS = True  # Use PNG (True) or JPEG (False)

# Create necessary directories
SUBREDDIT_DIR.mkdir(parents=True, exist_ok=True)
IMAGE_DIR.mkdir(exist_ok=True)

# Load cache (IDs of previously collected posts)
if CACHE_FILE.exists():
    with open(CACHE_FILE, "r") as f:
        cached_posts = set(json.load(f))
else:
    cached_posts = set()

# Load existing Parquet file if it exists
if DATA_FILE.exists():
    existing_df = pl.read_parquet(DATA_FILE)
    existing_post_ids = set(existing_df["post_id"].to_list())
else:
    existing_df = None
    existing_post_ids = set()

# Function to Download & Process Image
def download_and_process_image(image_url, post_id, max_width=800, lossless=True):
    try:
        response = requests.get(image_url, stream=True)
        if response.status_code == 200:
            # Determine file extension
            ext = ".png" if lossless else ".jpg"
            image_filename = f"{post_id}{ext}"
            image_path = IMAGE_DIR / image_filename

            # Save original image
            with open(image_path, "wb") as f:
                for chunk in response.iter_content(1024):
                    f.write(chunk)

            # Open image for processing
            with Image.open(image_path) as img:
                original_format = img.format
                original_size = img.size  # (width, height)
                original_file_size = os.path.getsize(image_path) / 1024  # Convert to KB

                # Resize if too large
                if img.width > max_width:
                    ratio = max_width / img.width
                    new_height = int(img.height * ratio)
                    img = img.resize((max_width, new_height), Image.LANCZOS)

                # Save as lossless PNG or compressed JPEG
                if lossless:
                    img.save(image_path, format="PNG", optimize=True)
                else:
                    img.save(image_path, format="JPEG", quality=85, optimize=True)

                # Get new file size after compression
                new_file_size = os.path.getsize(image_path) / 1024  # Convert to KB

            return str(image_path), original_format, original_size[0], original_size[1], original_file_size, new_file_size
    except Exception as e:
        print(f"Failed to download/process {image_url}: {e}")
        return None, None, None, None, None, None

# Collecting Post Data
posts_data = []
subreddit = reddit.subreddit(SUBREDDIT_NAME)

# Get all posts with the newest first. Replace limit = none to POST_LIMIT to limit the amount of posts
for post in subreddit.hot(limit=None):  
    post_id = post.id
    if post_id in cached_posts or post_id in existing_post_ids:
        continue # skip aldeady collected posts
    
    title = post.title
    text = post.selftext
    image_url = post.url if post.url.endswith(('.jpg', '.jpeg', '.png')) else None

    # Process image if available
    image_path, img_format, img_width, img_height, orig_size, new_size = (None, None, None, None, None, None)
    if image_url:
        image_path, img_format, img_width, img_height, orig_size, new_size = download_and_process_image(image_url, post_id)

    # Extract all comments
    comments = []
    post.comments.replace_more(limit=None)
    for comment in post.comments.list():  
        comments.append(comment.body)

    # Store post data
    posts_data.append({
        "post_id": post_id,
        "title": title,
        "text": text,
        "image_path": image_path,
        "image_format": img_format,
        "image_width": img_width,
        "image_height": img_height,
        "original_size_kb": orig_size,
        "compressed_size_kb": new_size,
        "comments": comments
    })

    # Add to cache
    cached_posts.add(post_id)

# Save updated cache
with open(CACHE_FILE, "w") as f:
    json.dump(list(cached_posts), f)

# Convert new data to Polars DataFrame
if posts_data:
    new_df = pl.DataFrame(posts_data)

    # Merge with existing data if it exists
    if existing_df is not None:
        df = pl.concat([existing_df, new_df], how="vertical")
    else:
        df = new_df

    df = df.unique(subset=["post_id"])  # Removes duplicate posts before saving

    df = df.sort("post_id", descending=True)  # Sorts by latest post_id

    # Save updated Parquet file
    df.write_parquet(DATA_FILE)

print(f"Scraping Complete! {len(posts_data)} new posts added.")