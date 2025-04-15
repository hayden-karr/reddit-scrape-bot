"""
Reddit Subreddit Data Viewer Backend

This Flask application serves as a backend for viewing Reddit subreddit data,
including posts, comments, and associated images that have been previously scraped.

Now supports CLI arguments for subreddit and chunk size, and uses unified config.
"""

import os
import argparse
from math import ceil
from pathlib import Path
from typing import Dict, List, Optional

import polars as pl
from flask import Flask, jsonify, render_template, request, send_from_directory

from reddit_scraper.config import get_config


class RedditDataManager:
    """Manager for Reddit data access and processing."""

    def __init__(self, subreddit_name: str):
        """
        Initialize the Reddit data manager.

        Args:
            subreddit_name: Name of the subreddit to manage
        """
        self.subreddit_name = subreddit_name

        config = get_config()
        self.base_dir = config.storage.base_dir
        self.subreddit_dir = self.base_dir / f"reddit_data_{subreddit_name}"
        self.posts_file = self.subreddit_dir / f"reddit_posts_{subreddit_name}.parquet"
        self.comments_file = (
            self.subreddit_dir / f"reddit_comments_{subreddit_name}.parquet"
        )
        self.image_dir = self.subreddit_dir / f"images_{subreddit_name}"

        # Ensure image directory exists
        os.makedirs(self.image_dir, exist_ok=True)

        # Cache for loaded data
        self._posts_cache = None
        self._comments_cache = None

    def _extract_filename(self, path: Optional[str]) -> Optional[str]:
        """Extract filename from a path."""
        return path.split("\\")[-1] if path else None

    def load_posts(self) -> Optional[pl.DataFrame]:
        """Load posts from parquet file with caching."""
        if self._posts_cache is None and self.posts_file.exists():
            self._posts_cache = pl.read_parquet(self.posts_file)
        return self._posts_cache

    def load_comments(self) -> Optional[pl.DataFrame]:
        """Load comments from parquet file with caching."""
        if self._comments_cache is None and self.comments_file.exists():
            self._comments_cache = pl.read_parquet(self.comments_file)
        return self._comments_cache

    def format_comments(self, comments: pl.DataFrame, post_id: str, is_post: bool = True) -> List[Dict]:
        """
        Format comments and their replies recursively.

        Args:
            comments: DataFrame containing all comments
            post_id: ID of the post or comment
            is_post: Whether we're looking for top-level comments (True) or replies (False)

        Returns:
            List of formatted comments with nested replies
        """
        if is_post:
            # For top-level comments, filter by post_id
            replies = comments.filter(pl.col("post_id") == post_id)
        else:
            # For replies to comments, filter by parent_id
            replies = comments.filter(pl.col("parent_id") == post_id)

        return [
            {
                "comment_id": comment["id"],
                "text": comment["text"],
                "image": self._extract_filename(comment["image_path"]),
                "replies": self.format_comments(comments, comment["id"], is_post=False),
            }
            for comment in replies.iter_rows(named=True)
        ]

    def get_comments_for_post(self, post_id: str) -> List[Dict]:
        """Get comments for a specific post."""
        comments = self.load_comments()
        if comments is None:
            return []
        return self.format_comments(comments, post_id, is_post=True)

    def get_chunked_posts(self, chunk: int, chunk_size: int) -> Dict:
        """
        Get a chunk of posts with their comments.

        Args:
            chunk: Chunk number (1-based)
            chunk_size: Number of posts per chunk

        Returns:
            Dict with chunk ID and list of posts with their comments
        """
        posts = self.load_posts()
        comments = self.load_comments()

        if posts is None or comments is None:
            return {"id": chunk, "posts": []}

        start_idx = (chunk - 1) * chunk_size
        end_idx = start_idx + chunk_size

        chunked_posts = (
            posts[start_idx:end_idx]
            .select(["id", "title", "image_path", "text", "created_time"])
            .to_dicts()
        )

        formatted_posts = []
        for post in chunked_posts:
            post_comments = (
                self.format_comments(comments, post["id"], is_post=True)
                if comments is not None
                else []
            )

            formatted_posts.append(
                {
                    "id": post["id"],
                    "title": post["title"],
                    "image": self._extract_filename(post["image_path"]),
                    "text": post["text"],
                    "created_time": post["created_time"],
                    "comments": post_comments,
                    "commentCount": len(post_comments),
                }
            )

        return {"id": chunk, "posts": formatted_posts}

    def get_total_chunks(self, chunk_size: int) -> int:
        """Calculate total number of chunks based on post count."""
        posts = self.load_posts()
        if posts is None:
            return 0
        return ceil(len(posts) / chunk_size)

    def get_comments_for_post(self, post_id: str) -> List[Dict]:
        """Get comments for a specific post."""
        comments = self.load_comments()
        if comments is None:
            return []
        return self.format_comments(comments, post_id)


def create_app(subreddit_name: str, chunk_size: int) -> Flask:
    app = Flask(__name__)
    config = get_config()
    app.config["SECRET_KEY"] = config.web.secret_key

    data_manager = RedditDataManager(subreddit_name)

    @app.route("/images/<filename>")
    def serve_image(filename):
        """Serve images from the image directory."""
        return send_from_directory(data_manager.image_dir, filename)

    @app.route("/api/chunks/<int:chunk>")
    def get_chunked_posts(chunk):
        """API endpoint to load posts dynamically by chunk."""
        chunk_data = data_manager.get_chunked_posts(chunk, chunk_size)
        if not chunk_data["posts"]:
            return jsonify({"error": "No posts found."}), 404
        return jsonify(chunk_data)

    @app.route("/api/chunks/count")
    def get_total_chunks():
        """API endpoint to get the total number of chunks."""
        total_chunks = data_manager.get_total_chunks(chunk_size)
        if total_chunks == 0:
            return jsonify({"error": "No posts found."}), 404
        return jsonify({"count": total_chunks})

    @app.route("/api/comments/<post_id>")
    def get_comments(post_id):
        """API endpoint to get comments for a specific post."""
        comments = data_manager.get_comments_for_post(post_id)
        return jsonify({"comments": comments})

    @app.route("/load_posts")
    def legacy_get_posts():
        """Legacy API endpoint for loading posts."""
        chunk = int(request.args.get("chunk", 1))
        return get_chunked_posts(chunk)

    @app.route("/totalChunks")
    def legacy_get_total_chunks():
        """API endpoint for getting total chunks."""
        return get_total_chunks()

    @app.route("/")
    def index():
        """Render the main page."""
        return render_template("index.html", subreddit=subreddit_name)

    return app


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Reddit Flask Data Viewer")
    parser.add_argument(
        "--subreddit",
        type=str,
        default=os.getenv("SUBREDDIT_NAME"),
        help="Subreddit to view (overrides config/env)",
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=None,
        help="Number of posts per chunk (overrides config/env)",
    )
    args = parser.parse_args()

    config = get_config()
    subreddit = args.subreddit or os.getenv("SUBREDDIT_NAME") 
    chunk_size = args.chunk_size or int(os.getenv("CHUNK_SIZE", 5))

    app = create_app(subreddit, chunk_size)
    app.run(debug=config.web.debug, host=config.web.host, port=config.web.port)

