"""
PRAW-based Reddit scraper implementation.

This module implements the BaseScraper interface using the PRAW library
for accessing the Reddit API directly.
"""

import re
from datetime import datetime, timezone
from io import BytesIO
from pathlib import Path
from typing import Dict, List, Optional, Union, Generator
from urllib.parse import urlparse, parse_qs

import praw
from PIL import Image
from praw.models import Comment, Submission
from requests.exceptions import RequestException

from reddit_scraper.config import get_config, get_image_dir
from reddit_scraper.constants import (
    ContentType,
    DEFAULT_POST_LIMIT,
    IMAGE_FORMAT,
    IMAGE_QUALITY,
    MAX_IMAGE_PIXELS,
    VALID_IMAGE_EXTENSIONS,
)
from reddit_scraper.core.models import RedditComment, RedditPost
from reddit_scraper.exceptions import PRAWError, ScraperError
from reddit_scraper.scrapers.base import BaseScraper
from reddit_scraper.utils.http import create_retry_session, get_user_agent
from reddit_scraper.utils.logging import get_logger


logger = get_logger(__name__)


class PRAWScraper(BaseScraper):
    """
    Reddit scraper implementation using PRAW.
    
    This class uses the official PRAW library to access the Reddit API directly.
    It requires valid Reddit API credentials.
    """

    def __init__(self, subreddit: str):
        """
        Initialize the PRAW scraper.
        
        Args:
            subreddit: Name of the subreddit to scrape
        """
        super().__init__(subreddit)
        self.config = get_config()
        
        try:
            # Initialize the Reddit API client
            self.reddit = praw.Reddit(
                client_id=self.config.reddit_api.client_id,
                client_secret=self.config.reddit_api.client_secret,
                user_agent=self.config.reddit_api.user_agent,
            )
            
            # Initialize the subreddit object
            self.subreddit_obj = self.reddit.subreddit(subreddit)
            
            # Create an HTTP session for image downloads
            self.session = create_retry_session()
            
            # Create the image directory
            self.image_dir = get_image_dir(subreddit)
            
            # Configure image handling
            Image.MAX_IMAGE_PIXELS = MAX_IMAGE_PIXELS
            
            logger.info(f"PRAW scraper initialized for r/{subreddit}")
        except Exception as e:
            logger.error(f"Failed to initialize PRAW scraper: {e}")
            raise PRAWError(f"Failed to initialize PRAW scraper: {e}")
    
    def fetch_posts(
        self,
        limit: Optional[int] = DEFAULT_POST_LIMIT,
        before: Optional[Union[int, datetime]] = None,
        after: Optional[Union[int, datetime]] = None,
    ) -> Generator[RedditPost, None, None]:
        """
        Fetch posts from the subreddit using PRAW.
        
        Args:
            limit: Maximum number of posts to fetch
            before: Only fetch posts before this time/timestamp
            after: Only fetch posts after this time/timestamp
            
        Yields:
            RedditPost objects
        """
        try:
            # Convert datetime to timestamp if needed
            if isinstance(before, datetime):
                before = int(before.timestamp())
            if isinstance(after, datetime):
                after = int(after.timestamp())
            
            # Fetch posts from the subreddit
            posts = self.subreddit_obj.new(limit=limit)
            
            for post in posts:
                # Apply time filters if specified
                if before and post.created_utc >= before:
                    continue
                if after and post.created_utc <= after:
                    continue
                
                # Extract image URL from post
                image_url = self.extract_image_url(post.url)
                
                # Convert to RedditPost model
                reddit_post = RedditPost(
                    id=post.id,
                    title=post.title,
                    text=post.selftext,
                    created_utc=int(post.created_utc),
                    created_time=datetime.fromtimestamp(post.created_utc).strftime(
                        "%Y-%m-%d %H:%M:%S"
                    ),
                    image_url=image_url,
                    image_path=None,  # Will be set after downloading
                )
                
                yield reddit_post
        except Exception as e:
            logger.error(f"Error fetching posts from r/{self.subreddit}: {e}")
            raise PRAWError(f"Error fetching posts: {e}")
    
    def fetch_comments(
        self,
        post_id: Optional[str] = None,
        limit: Optional[int] = None,
        before: Optional[Union[int, datetime]] = None,
        after: Optional[Union[int, datetime]] = None,
    ) -> Generator[RedditComment, None, None]:
        """
        Fetch comments from a post using PRAW.
        
        Args:
            post_id: ID of the post to fetch comments for
            limit: Maximum number of comments to fetch (NOTE: PRAW might ignore this)
            before: Only fetch comments before this time/timestamp
            after: Only fetch comments after this time/timestamp
            
        Yields:
            RedditComment objects
        """
        if not post_id:
            logger.warning("No post_id provided for comment fetching, skipping")
            return
        
        try:
            # Convert datetime to timestamp if needed
            if isinstance(before, datetime):
                before = int(before.timestamp())
            if isinstance(after, datetime):
                after = int(after.timestamp())
            
            # Fetch the submission
            submission = self.reddit.submission(id=post_id)
            
            # Expand comment forest to get all comments
            submission.comments.replace_more(limit=None)
            
            # Process all comments
            for comment in submission.comments.list():
                # Apply time filters if specified
                if before and comment.created_utc >= before:
                    continue
                if after and comment.created_utc <= after:
                    continue
                
                # Extract parent ID (removing prefix if present)
                parent_id = comment.parent_id
                if parent_id.startswith("t1_"):  # Comment parent
                    parent_id = parent_id[3:]
                elif parent_id.startswith("t3_"):  # Post parent
                    parent_id = None  # Top-level comment
                
                # Convert to RedditComment model
                reddit_comment = RedditComment(
                    id=comment.id,
                    post_id=post_id,
                    parent_id=parent_id,
                    text=comment.body,
                    created_utc=int(comment.created_utc),
                    created_time=datetime.fromtimestamp(comment.created_utc).strftime(
                        "%Y-%m-%d %H:%M:%S"
                    ),
                    image_url=None,  # Will be set after extraction
                    image_path=None,  # Will be set after downloading
                )
                
                yield reddit_comment
                
                # Check if we've reached the limit
                if limit is not None:
                    limit -= 1
                    if limit <= 0:
                        break
        except Exception as e:
            logger.error(f"Error fetching comments for post {post_id}: {e}")
            raise PRAWError(f"Error fetching comments: {e}")
    
    def extract_image_url(self, text: str) -> Optional[str]:
        """
        Extract an image URL from text content.
        
        Args:
            text: Text to extract image URL from
            
        Returns:
            Extracted image URL, or None if no image was found
        """
        # If the text is already a URL, check if it's an image
        parsed_url = urlparse(text)
        if parsed_url.scheme and parsed_url.netloc:
            path = parsed_url.path.lower()
            
            # Check if the URL has a valid image extension
            if any(path.endswith(ext) for ext in VALID_IMAGE_EXTENSIONS):
                return text
            
            # Check for Reddit's image links with query parameters
            query_params = parse_qs(parsed_url.query)
            if "format" in query_params and query_params["format"][0] in [
                "jpg", "jpeg", "png", "webp", "gif"
            ]:
                return text
        
        # Otherwise, search for URLs in the text
        url_pattern = r'https?://[^\s)"]+'
        urls = re.findall(url_pattern, text)
        
        for url in urls:
            try:
                parsed_url = urlparse(url)
                path = parsed_url.path.lower()
                
                # Check if the URL has a valid image extension
                if any(path.endswith(ext) for ext in VALID_IMAGE_EXTENSIONS):
                    return url
                
                # Check for Reddit's image links with query parameters
                query_params = parse_qs(parsed_url.query)
                if "format" in query_params and query_params["format"][0] in [
                    "jpg", "jpeg", "png", "webp", "gif"
                ]:
                    return url
            except ValueError:
                continue
        
        return None
    
    def download_image(
        self,
        image_url: Optional[str],
        item_id: str,
        content_type: ContentType
    ) -> Optional[str]:
        """
        Download an image and save it.
        
        Args:
            image_url: URL of the image to download
            item_id: ID of the post or comment
            content_type: Whether this is a post or comment image
            
        Returns:
            Path to the saved image, or None if no image was downloaded
        """
        if not image_url:
            return None
        
        # Create the image filename
        prefix = "comment_" if content_type == ContentType.COMMENT else ""
        image_path = self.image_dir / f"{prefix}{item_id}.{IMAGE_FORMAT.lower()}"
        
        try:
            # First try without special headers
            response = self.session.get(image_url, stream=True)
            response.raise_for_status()
        except RequestException:
            try:
                # Retry with browser-like headers
                headers = {"User-Agent": get_user_agent()}
                response = self.session.get(image_url, headers=headers, stream=True)
                response.raise_for_status()
            except RequestException as e:
                logger.error(f"Failed to download image {image_url}: {e}")
                return None
        
        try:
            # Process and save the image
            image = Image.open(BytesIO(response.content))
            image.save(image_path, IMAGE_FORMAT, quality=IMAGE_QUALITY)
            logger.debug(f"Downloaded image to {image_path}")
            return str(image_path)
        except Exception as e:
            logger.error(f"Error processing image: {e}")
            return None
    
    @classmethod
    def get_name(cls) -> str:
        """Get the name of this scraper implementation."""
        return "praw"
