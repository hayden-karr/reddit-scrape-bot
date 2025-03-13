from flask import Flask, render_template, send_from_directory, request, jsonify
import polars as pl
import os
from pathlib import Path
from math import ceil

app = Flask(__name__)

# Define paths
SUBREDDIT_NAME = "dataengineering"
BASE_DIR = Path(__file__).resolve().parent.parent / "scraped_subreddits"
SUBREDDIT_DIR = BASE_DIR / f"reddit_data_{SUBREDDIT_NAME}"
POSTS_FILE = SUBREDDIT_DIR / f"reddit_posts_{SUBREDDIT_NAME}.parquet"
COMMENTS_FILE = SUBREDDIT_DIR / f"reddit_comments_{SUBREDDIT_NAME}.parquet"
IMAGE_DIR = SUBREDDIT_DIR / f"images_{SUBREDDIT_NAME}"



# Parameters
chunk_size = 5 # Number of posts per chunk


# Ensure the image directory exists
os.makedirs(IMAGE_DIR, exist_ok=True)

# Function to load posts from the Parquet file
def load_posts():
    return pl.read_parquet(POSTS_FILE) if POSTS_FILE.exists() else None


# Function to load comments from the Parquet file
def load_comments():
    return pl.read_parquet(COMMENTS_FILE) if COMMENTS_FILE.exists() else None


# Function to format comments and replies
def format_comments(comments, parent_id):
    replies = comments.filter(pl.col("parent_id") == parent_id)

    return [
        {
            "comment_id": comment["comment_id"],
            "text": comment["text"],
            "image": comment["image_path"].split("\\")[-1] if comment["image_path"] else None,
            "replies": format_comments(comments, comment["comment_id"]),
        }
        for comment in replies.iter_rows(named=True)
    ]

# Serve Images
@app.route("/images/<filename>")
def serve_image(filename):
    return send_from_directory(IMAGE_DIR, filename)

# Route to load posts dynamically
@app.route("/load_posts")
def get_chunked_posts():
    chunk = int(request.args.get("chunk", 1))  # Get chunk number from request
    start_idx = (chunk - 1) * chunk_size
    end_idx = start_idx + chunk_size

    # Load posts and comments
    posts = load_posts()
    comments = load_comments()

    if posts is None or comments is None:
        return jsonify({"error": "No posts or comments found."}), 404
    
    chunked_posts = posts[start_idx:end_idx].select(["post_id", "title", "image_path", "text"]).to_dicts()


    data = [
        {
            "post_id": post["post_id"],
            "title": post["title"],
            "image": post["image_path"].split("\\")[-1] if post["image_path"] else None,
            "text": post["text"],
            "comments": format_comments(comments, post["post_id"]),
        }
        for post in chunked_posts
    ]

    return jsonify({"posts": data})

# Route to get total Chunks
@app.route("/totalChunks")
def get_total_chunks():
    posts = load_posts()
    
    if posts is None:
        return jsonify({"error": "No posts found."}), 404
    
    totalChunks = ceil(len(posts) / chunk_size)

    return jsonify({"totalChunks": totalChunks})  
    
# Route to render the main page
@app.route("/")
def index():
     return render_template("index.html", subreddit=SUBREDDIT_NAME)
    

if __name__ == "__main__":
    app.run(debug=True, port=8000)
