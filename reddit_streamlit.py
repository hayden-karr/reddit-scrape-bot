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

# Display comments
def display_comments(comments, parent_id, level=0):
    replies = comments.filter(pl.col("parent_id") == parent_id)

    for comment in replies.iter_rows(named=True):
        # Create indentation using non-breaking spaces and a dash for each level
        indent = "&nbsp;" * (level * 5)  # 4 non-breaking spaces per indentation level
        dash_prefix = "-" * (level + 1)  # Number of dashes depends on indentation level
        formatted_text = f"{indent}{dash_prefix} {comment['text']}"

        # Display text with proper indentation and dashes using HTML <pre> tag
        st.markdown(f"<pre>{formatted_text}</pre>", unsafe_allow_html=True)

        # Display comment images if available
        if comment["image_path"]:
            image_path = comment["image_path"]
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
    # Pagination Setup
    batch_size = 25  # Number of posts per page
    total_pages = len(posts) // batch_size + (1 if len(posts) % batch_size > 0 else 0)

    # User selects the page
    page_number = st.number_input("Page", min_value=1, max_value=total_pages, step=1, value=1)

    # Slice data efficiently
    start_idx = (page_number - 1) * batch_size
    end_idx = start_idx + batch_size
    posts_batch = posts.slice(start_idx, batch_size)


    # Display posts
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
            with st.expander("**Comments:**"):
                display_comments(comments, post["post_id"], level=0)
            st.write("---")

    st.write(f"Page {page_number} of {total_pages}")
else:
    st.warning("No data found. Make sure you've scraped posts and comments.")


