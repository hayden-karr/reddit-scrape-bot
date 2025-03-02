import streamlit as st
import polars as pl
from pathlib import Path
import os
import time

# Load Data
SUBREDDIT_NAME = "dataengineering"
SUBREDDIT_DIR = Path(f"reddit_data_{SUBREDDIT_NAME}")
POSTS_FILE = SUBREDDIT_DIR / f"reddit_posts_{SUBREDDIT_NAME}.parquet"
COMMENTS_FILE = SUBREDDIT_DIR / f"reddit_comments_{SUBREDDIT_NAME}.parquet"
IMAGE_DIR = SUBREDDIT_DIR / f"images_{SUBREDDIT_NAME}"

# Chunks for post loading amount
chunk_size = 5

# Load posts
@st.cache_data
def load_posts():
    return pl.read_parquet(POSTS_FILE) if POSTS_FILE.exists() else None

# Load comments
@st.cache_data
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
        if "image_path" in comment and comment["image_path"]:
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

if "post_index" not in st.session_state:
    st.session_state.post_index = 0

def get_post_chunk(start, chunk_size):
    if posts is not None:
        return posts.slice(start, chunk_size)
    return None

post_chunk = get_post_chunk(st.session_state.post_index, chunk_size)

if post_chunk is not None:
    total_posts = len(posts)

    post_container = st.empty()

    with post_container.container():
        for post in post_chunk.iter_rows(named=True):
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
    
    col1, col2 = st.columns([1, 1])

    with col1:
        if st.session_state.post_index > 0:
            if st.button("Previous Posts"):
                st.session_state.post_index = max(0, st.session_state.post_index - chunk_size)
                st.rerun()
    with col2:
        if st.session_state.post_index + chunk_size < total_posts:
            if st.button("Next Posts"):
                with st.spinner("Loading more posts..."):
                    time.sleep(.5)  # Simulate smooth transition
                st.session_state.post_index += chunk_size
                st.rerun()
else:
    st.warning("No data found. Make sure you've scraped posts and comments.")


