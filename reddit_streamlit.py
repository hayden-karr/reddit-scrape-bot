import streamlit as st
import polars as pl
from pathlib import Path
import os

# Load Data
SUBREDDIT_NAME = "dataengineering"
SUBREDDIT_DIR = Path(f"reddit_data_{SUBREDDIT_NAME}")
POSTS_FILE = SUBREDDIT_DIR / f"reddit_posts_{SUBREDDIT_NAME}.parquet"
COMMENTS_FILE = SUBREDDIT_DIR / f"reddit_comments_{SUBREDDIT_NAME}.parquet"
IMAGE_DIR = SUBREDDIT_DIR / f"images_{SUBREDDIT_NAME}"

# Load posts
def load_posts():
    return pl.read_parquet(POSTS_FILE) if POSTS_FILE.exists() else None

# Load comments
def load_comments():
    return pl.read_parquet(COMMENTS_FILE) if COMMENTS_FILE.exists() else None

def display_comments(comments, parent_id, level=0):
    """Recursively display nested comments with indentation."""
    replies = comments.filter(comments["parent_id"] == parent_id)

    for comment in replies.iter_rows(named=True):
        # Create indentation using Markdown and HTML for better readability
        indent = "&nbsp;" * (level * 4)  # Adjust for readability
        st.markdown(f"{indent} 🔹 **{comment['text']}**", unsafe_allow_html=True)

        # Display comment images if available
        if "image_path" in comment and comment["image_path"]:
            image_path = comment["image_path"]  # Use the stored path
            if os.path.isfile(image_path):
                st.image(image_path, width=250)
            else:
                st.warning(f"Comment image not found: {image_path}")

        # Recursively display replies, increasing indentation
        display_comments(comments, comment["comment_id"], level + 1)


# Streamlit UI
st.title(f"r/{SUBREDDIT_NAME} - Local Reddit Viewer")

posts = load_posts()
comments = load_comments()

if posts is not None and comments is not None:
    for post in posts.iter_rows(named=True):
        with st.container():
            st.subheader(post["title"])
            st.write(post["text"])
            if "image_path" in post and post["image_path"]:
                image_path = post["image_path"]  
                if os.path.isfile(image_path):  
                    st.image(image_path, use_container_width=True)
                else:
                    st.error(f"Image not found: {image_path}")
            st.write("---")
            st.write("**Comments:**")
            display_comments(comments, post["post_id"], level=0)
            st.write("---")
else:
    st.warning("No data found. Make sure you've scraped posts and comments.")

if st.button("Stop Execution"):
    st.warning("Execution stopped by user.")
    st.stop()

