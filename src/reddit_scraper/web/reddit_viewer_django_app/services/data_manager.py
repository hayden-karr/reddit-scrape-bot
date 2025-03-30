"""
Reddit Data Manager

This module provides services for loading and processing Reddit data from
previously scraped parquet files, including posts, comments, and associated images.
"""

import os
import logging
from math import ceil
from pathlib import Path
from typing import Dict, List, Optional, TypedDict, Any

import polars as pl

# Set up logging
logger = logging.getLogger(__name__)


class CommentDict(TypedDict):
    """Type definition for comment dictionary structure."""
    comment_id: str
    text: str
    image: Optional[str]
    replies: List['CommentDict']


class PostDict(TypedDict):
    """Type definition for post dictionary structure."""
    id: str
    title: str
    image: Optional[str]
    text: str
    created_time: str
    comments: List[CommentDict]
    commentCount: int


class ChunkDict(TypedDict):
    """Type definition for chunk dictionary structure."""
    id: int
    posts: List[PostDict]


class DataManagerException(Exception):
    """Base exception for all data manager errors."""
    pass


class DataNotFoundException(DataManagerException):
    """Exception raised when requested data is not found."""
    pass


class DataLoadingException(DataManagerException):
    """Exception raised when there's an error loading data."""
    pass


class RedditDataManager:
    """
    Manager for Reddit data access and processing.
    
    This service class handles loading and processing Reddit data from parquet files,
    including posts, comments, and associated images. It provides methods for
    retrieving posts in chunks, managing comments, and handling image paths.
    
    Attributes:
        subreddit_name (str): Name of the subreddit to manage
        base_dir (Path): Base directory for scraped data
        subreddit_dir (Path): Directory for this specific subreddit
        posts_file (Path): Path to the posts parquet file
        comments_file (Path): Path to the comments parquet file
        image_dir (Path): Directory containing post and comment images
    """

    def __init__(self, subreddit_name: str, base_dir: Optional[Path] = None):
        """
        Initialize the Reddit data manager.

        Args:
            subreddit_name: Name of the subreddit to manage
            base_dir: Optional override for the base directory path
        
        Raises:
            DataManagerException: If there's an error setting up the data manager
        """
        try:
            self.subreddit_name = subreddit_name
            
            # Use provided base_dir or default to a standard location
            if base_dir:
                self.base_dir = base_dir
            else:
                # Default to a location relative to the project
                self.base_dir = Path(__file__).resolve().parent.parent.parent.parent / "scraped_subreddits"
            
            self.subreddit_dir = self.base_dir / f"reddit_data_{subreddit_name}"
            self.posts_file = self.subreddit_dir / f"reddit_posts_{subreddit_name}.parquet"
            self.comments_file = (self.subreddit_dir / f"reddit_comments_{subreddit_name}.parquet")
            self.image_dir = self.subreddit_dir / f"images_{subreddit_name}"

            # Ensure image directory exists
            os.makedirs(self.image_dir, exist_ok=True)

            # Cache for loaded data
            self._posts_cache = None
            self._comments_cache = None
            
            logger.info(f"Initialized RedditDataManager for subreddit '{subreddit_name}'")
            logger.debug(f"Subreddit directory: {self.subreddit_dir}")
            logger.debug(f"Posts file: {self.posts_file}")
            logger.debug(f"Comments file: {self.comments_file}")
            logger.debug(f"Image directory: {self.image_dir}")
            
            # Validate data sources exist
            self._validate_data_sources()
            
        except Exception as e:
            logger.error(f"Error initializing RedditDataManager: {str(e)}", exc_info=True)
            raise DataManagerException(f"Failed to initialize data manager: {str(e)}") from e
    
    def _validate_data_sources(self) -> None:
        """
        Validate that required data sources exist.
        
        Raises:
            DataNotFoundException: If required data files don't exist
        """
        if not self.subreddit_dir.exists():
            logger.warning(f"Subreddit directory does not exist: {self.subreddit_dir}")
            os.makedirs(self.subreddit_dir, exist_ok=True)
            
        if not self.posts_file.exists():
            logger.warning(f"Posts file does not exist: {self.posts_file}")
            # We don't raise an exception here, as empty results might be valid
            
        if not self.comments_file.exists():
            logger.warning(f"Comments file does not exist: {self.comments_file}")
            # We don't raise an exception here, as empty results might be valid

    def _extract_filename(self, path: Optional[str]) -> Optional[str]:
        """
        Extract filename from a path.
        
        Args:
            path: The full path to extract a filename from
            
        Returns:
            The extracted filename or None if the path is None
        """
        if not path:
            return None
        
        try:
            # Handle both Windows and Unix-style paths
            if '\\' in path:
                return path.split("\\")[-1]
            else:
                return path.split("/")[-1]
        except Exception as e:
            logger.warning(f"Error extracting filename from path '{path}': {str(e)}")
            return None

    def load_posts(self) -> Optional[pl.DataFrame]:
        """
        Load posts from parquet file with caching.
        
        Returns:
            DataFrame containing post data or None if file doesn't exist
            
        Raises:
            DataLoadingException: If there's an error loading the posts
        """
        try:
            if self._posts_cache is None and self.posts_file.exists():
                logger.debug(f"Loading posts from {self.posts_file}")
                self._posts_cache = pl.read_parquet(self.posts_file)
                logger.info(f"Loaded {len(self._posts_cache) if self._posts_cache is not None else 0} posts for subreddit '{self.subreddit_name}'")
            return self._posts_cache
        except Exception as e:
            logger.error(f"Error loading posts: {str(e)}", exc_info=True)
            raise DataLoadingException(f"Failed to load posts: {str(e)}") from e

    def load_comments(self) -> Optional[pl.DataFrame]:
        """
        Load comments from parquet file with caching.
        
        Returns:
            DataFrame containing comment data or None if file doesn't exist
            
        Raises:
            DataLoadingException: If there's an error loading the comments
        """
        try:
            if self._comments_cache is None and self.comments_file.exists():
                logger.debug(f"Loading comments from {self.comments_file}")
                self._comments_cache = pl.read_parquet(self.comments_file)
                logger.info(f"Loaded {len(self._comments_cache) if self._comments_cache is not None else 0} comments for subreddit '{self.subreddit_name}'")
            return self._comments_cache
        except Exception as e:
            logger.error(f"Error loading comments: {str(e)}", exc_info=True)
            raise DataLoadingException(f"Failed to load comments: {str(e)}") from e

    def format_comments(self, comments: pl.DataFrame, parent_id: str) -> List[CommentDict]:
        """
        Format comments and their replies recursively.

        Args:
            comments: DataFrame containing all comments
            parent_id: ID of the parent post or comment

        Returns:
            List of formatted comments with nested replies
            
        Raises:
            DataManagerException: If there's an error formatting the comments
        """
        try:
            if comments is None:
                return []
                
            replies = comments.filter(pl.col("parent_id") == parent_id)
            
            formatted_replies = []
            for comment in replies.iter_rows(named=True):
                formatted_comment: CommentDict = {
                    "comment_id": comment["comment_id"],
                    "text": comment["text"],
                    "image": self._extract_filename(comment["image_path"]),
                    "replies": self.format_comments(comments, comment["comment_id"]),
                }
                formatted_replies.append(formatted_comment)
                
            return formatted_replies
        except Exception as e:
            logger.error(f"Error formatting comments for parent_id '{parent_id}': {str(e)}", exc_info=True)
            raise DataManagerException(f"Failed to format comments: {str(e)}") from e

    def get_chunked_posts(self, chunk: int, chunk_size: int) -> ChunkDict:
        """
        Get a chunk of posts with their comments.

        Args:
            chunk: Chunk number (1-based)
            chunk_size: Number of posts per chunk

        Returns:
            Dict with chunk ID and list of posts with their comments
            
        Raises:
            DataManagerException: If there's an error retrieving chunked posts
        """
        try:
            posts = self.load_posts()
            comments = self.load_comments()

            if posts is None:
                logger.warning(f"No posts found for subreddit '{self.subreddit_name}'")
                return {"id": chunk, "posts": []}

            # Calculate chunk indices
            start_idx = (chunk - 1) * chunk_size
            end_idx = start_idx + chunk_size
            
            # Log chunk information
            logger.debug(f"Getting chunk {chunk} (indices {start_idx}:{end_idx}) with size {chunk_size}")
            
            # Check if the start index is out of bounds
            if start_idx >= len(posts):
                logger.warning(f"Chunk start index ({start_idx}) exceeds post count ({len(posts)})")
                return {"id": chunk, "posts": []}

            # Get the chunked posts from the dataframe
            chunked_posts = (
                posts[start_idx:end_idx]
                .select(["post_id", "title", "image_path", "text", "created_time"])
                .to_dicts()
            )

            # Format each post with its comments
            formatted_posts: List[PostDict] = []
            for post in chunked_posts:
                post_id = post["post_id"]
                
                # Get comments for this post
                post_comments = (
                    self.format_comments(comments, post_id) if comments is not None else []
                )
                
                # Format the post with its comments
                formatted_post: PostDict = {
                    "id": post_id,
                    "title": post["title"],
                    "image": self._extract_filename(post["image_path"]),
                    "text": post["text"],
                    "created_time": post["created_time"],
                    "comments": post_comments,
                    "commentCount": len(post_comments)
                }
                
                formatted_posts.append(formatted_post)

            logger.info(f"Retrieved {len(formatted_posts)} posts for chunk {chunk}")
            return {"id": chunk, "posts": formatted_posts}
            
        except Exception as e:
            logger.error(f"Error getting chunked posts for chunk {chunk}: {str(e)}", exc_info=True)
            raise DataManagerException(f"Failed to get chunked posts: {str(e)}") from e

    def get_total_chunks(self, chunk_size: int) -> int:
        """
        Calculate total number of chunks based on post count.
        
        Args:
            chunk_size: Number of posts per chunk
            
        Returns:
            Total number of chunks
            
        Raises:
            DataManagerException: If there's an error calculating total chunks
        """
        try:
            posts = self.load_posts()
            if posts is None:
                logger.warning(f"No posts found when calculating total chunks for subreddit '{self.subreddit_name}'")
                return 0
                
            total_chunks = ceil(len(posts) / chunk_size)
            logger.debug(f"Total chunks: {total_chunks} (posts: {len(posts)}, chunk_size: {chunk_size})")
            return total_chunks
        except Exception as e:
            logger.error(f"Error calculating total chunks: {str(e)}", exc_info=True)
            raise DataManagerException(f"Failed to calculate total chunks: {str(e)}") from e
    
    def get_comments_for_post(self, post_id: str) -> List[CommentDict]:
        """
        Get comments for a specific post.
        
        Args:
            post_id: ID of the post to get comments for
            
        Returns:
            List of formatted comments with nested replies
            
        Raises:
            DataManagerException: If there's an error retrieving comments
        """
        try:
            comments = self.load_comments()
            if comments is None:
                logger.warning(f"No comments found for post '{post_id}'")
                return []
            
            formatted_comments = self.format_comments(comments, post_id)
            logger.debug(f"Retrieved {len(formatted_comments)} top-level comments for post '{post_id}'")
            return formatted_comments
        except Exception as e:
            logger.error(f"Error getting comments for post '{post_id}': {str(e)}", exc_info=True)
            raise DataManagerException(f"Failed to get comments: {str(e)}") from e
    
    def get_image_path(self, filename: str) -> Path:
        """
        Get the full path for an image file.
        
        Args:
            filename: Name of the image file
            
        Returns:
            Path to the image file
            
        Raises:
            DataNotFoundException: If the image file doesn't exist
        """
        try:
            image_path = self.image_dir / filename
            if not image_path.exists():
                logger.warning(f"Image not found: {image_path}")
                raise DataNotFoundException(f"Image '{filename}' not found")
            return image_path
        except Exception as e:
            if not isinstance(e, DataNotFoundException):
                logger.error(f"Error getting image path for '{filename}': {str(e)}", exc_info=True)
                raise DataManagerException(f"Failed to get image path: {str(e)}") from e
            raise