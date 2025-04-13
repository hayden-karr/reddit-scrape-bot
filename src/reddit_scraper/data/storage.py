"""
Data storage utilities for Reddit Explorer.

This module provides functionality for storing and retrieving Reddit data
using Polars and Parquet files, optimized for performance and disk space.
"""

from pathlib import Path
from typing import Dict, List, Optional, Tuple, Union

import polars as pl

from reddit_scraper.config import (
    get_comments_file,
    get_config,
    get_posts_file,
    get_subreddit_dir,
)
from reddit_scraper.constants import COMMENT_SCHEMA, PARQUET_COMPRESSION, POST_SCHEMA
from reddit_scraper.core.models import RedditComment, RedditPost
from reddit_scraper.exceptions import StorageError
from reddit_scraper.utils.logging import get_logger

logger = get_logger(__name__)


class RedditDataStorage:
    """
    Storage manager for Reddit data.
    
    This class handles reading and writing Reddit data to Parquet files,
    providing an abstraction over the storage details.
    """

    def __init__(self, subreddit: str):
        """
        Initialize the storage manager for a specific subreddit.
        
        Args:
            subreddit: Name of the subreddit
        """
        self.subreddit = subreddit
        self.config = get_config()
        
        # Get file paths
        self.subreddit_dir = get_subreddit_dir(subreddit)
        self.posts_file = get_posts_file(subreddit)
        self.comments_file = get_comments_file(subreddit)
        
        # Ensure directories exist
        self.subreddit_dir.mkdir(parents=True, exist_ok=True)
        
        logger.info(f"Storage initialized for r/{subreddit}")
    
    def _get_compression(self) -> str:
        """Get the compression method to use for Parquet files."""
        if self.config.storage.use_compression:
            return self.config.storage.compression_method
        return "none"
    
    def save_posts(self, posts: List[RedditPost]) -> int:
        """
        Save posts to the Parquet file.
        
        Args:
            posts: List of RedditPost objects to save
            
        Returns:
            Number of posts saved
            
        Raises:
            StorageError: If there's an error saving the posts
        """
        if not posts:
            logger.warning("No posts to save")
            return 0
        
        try:
            # Convert posts to dictionaries
            post_dicts = [post.to_dict() for post in posts]
            
            # Create DataFrame from posts
            new_data = pl.DataFrame(post_dicts)
            
            # Apply schema to ensure consistent types
            schema = POST_SCHEMA
            new_data = new_data.cast(schema)
            
            # Load existing data if file exists
            if self.posts_file.exists():
                existing_df = pl.read_parquet(self.posts_file)
                # Append new data and remove duplicates
                df = pl.concat([existing_df, new_data], how="vertical")
                # Keep the last occurrence (latest info) for each post id, never delete old posts
                df = df.unique(subset=["id"], keep="last")
            else:
                df = new_data
            
            # Sort by creation time (newest first) and save
            df = df.sort("created_utc", descending=True)
            df.write_parquet(
                self.posts_file,
                compression=self._get_compression()
            )
            
            num_saved = len(posts)
            logger.info(f"Saved {num_saved} posts to {self.posts_file}")
            return num_saved
        except Exception as e:
            error_msg = f"Error saving posts: {e}"
            logger.error(error_msg)
            raise StorageError(error_msg)
    
    def save_comments(self, comments: List[RedditComment]) -> int:
        """
        Save comments to the Parquet file.
        
        Args:
            comments: List of RedditComment objects to save
            
        Returns:
            Number of comments saved
            
        Raises:
            StorageError: If there's an error saving the comments
        """
        if not comments:
            logger.warning("No comments to save")
            return 0
        
        try:
            # Convert comments to dictionaries
            comment_dicts = [comment.to_dict() for comment in comments]
            
            # Create DataFrame from comments
            new_data = pl.DataFrame(comment_dicts)
            
            # Apply schema to ensure consistent types
            schema = COMMENT_SCHEMA
            new_data = new_data.cast(schema)
            
            # Load existing data if file exists
            if self.comments_file.exists():
                existing_df = pl.read_parquet(self.comments_file)
                # Append new data and remove duplicates
                df = pl.concat([existing_df, new_data], how="vertical")
                # Keep the last occurrence (latest info) for each comment id, never delete old comments
                df = df.unique(subset=["id"], keep="last")
            else:
                df = new_data
            
            # Sort by creation time (newest first) and save
            df = df.sort("created_utc", descending=True)
            df.write_parquet(
                self.comments_file,
                compression=self._get_compression()
            )
            
            num_saved = len(comments)
            logger.info(f"Saved {num_saved} comments to {self.comments_file}")
            return num_saved
        except Exception as e:
            error_msg = f"Error saving comments: {e}"
            logger.error(error_msg)
            raise StorageError(error_msg)
    
    def load_posts(self, limit: Optional[int] = None) -> List[Dict]:
        """
        Load posts from the Parquet file.
        
        Args:
            limit: Maximum number of posts to load
            
        Returns:
            List of post dictionaries
            
        Raises:
            StorageError: If there's an error loading the posts
        """
        try:
            if not self.posts_file.exists():
                logger.warning(f"Posts file not found: {self.posts_file}")
                return []
            
            df = pl.read_parquet(self.posts_file)
            
            if limit:
                df = df.limit(limit)
            
            return df.to_dicts()
        except Exception as e:
            error_msg = f"Error loading posts: {e}"
            logger.error(error_msg)
            raise StorageError(error_msg)
    
    def load_comments(
        self, post_id: Optional[str] = None, limit: Optional[int] = None
    ) -> List[Dict]:
        """
        Load comments from the Parquet file.
        
        Args:
            post_id: Optional ID of the post to filter by
            limit: Maximum number of comments to load
            
        Returns:
            List of comment dictionaries
            
        Raises:
            StorageError: If there's an error loading the comments
        """
        try:
            if not self.comments_file.exists():
                logger.warning(f"Comments file not found: {self.comments_file}")
                return []
            
            df = pl.read_parquet(self.comments_file)
            
            if post_id:
                df = df.filter(pl.col("post_id") == post_id)
            
            if limit:
                df = df.limit(limit)
            
            return df.to_dicts()
        except Exception as e:
            error_msg = f"Error loading comments: {e}"
            logger.error(error_msg)
            raise StorageError(error_msg)
    
    def format_comments_tree(self, comments: List[Dict], parent_id: str) -> List[Dict]:
        """
        Format comments into a hierarchical tree structure.
        
        Args:
            comments: List of comment dictionaries
            parent_id: ID of the parent (post or comment) to format children for
            
        Returns:
            List of hierarchical comment dictionaries with replies
        """
        try:
            # Filter comments for the specified parent
            parent_comments = [
                c for c in comments
                if c.get("parent_id") == parent_id or 
                   (c.get("parent_id") is None and parent_id == c.get("post_id"))
            ]
            
            # Recursively format replies for each comment
            return [
                {
                    "comment_id": comment["id"],
                    "text": comment["text"],
                    "image": comment["image_path"].split("/")[-1] if comment.get("image_path") else None,
                    "replies": self.format_comments_tree(comments, comment["id"]),
                }
                for comment in parent_comments
            ]
        except Exception as e:
            error_msg = f"Error formatting comments tree: {e}"
            logger.error(error_msg)
            raise StorageError(error_msg)
    
    def get_total_posts(self) -> int:
        """
        Get the total number of posts in the storage.
        
        Returns:
            Number of posts
        """
        try:
            if not self.posts_file.exists():
                return 0
            
            df = pl.read_parquet(self.posts_file)
            return df.shape[0]
        except Exception as e:
            error_msg = f"Error getting total posts: {e}"
            logger.error(error_msg)
            raise StorageError(error_msg)
    
    def get_total_comments(self) -> int:
        """
        Get the total number of comments in the storage.
        
        Returns:
            Number of comments
        """
        try:
            if not self.comments_file.exists():
                return 0
            
            df = pl.read_parquet(self.comments_file)
            return df.shape[0]
        except Exception as e:
            error_msg = f"Error getting total comments: {e}"
            logger.error(error_msg)
            raise StorageError(error_msg)