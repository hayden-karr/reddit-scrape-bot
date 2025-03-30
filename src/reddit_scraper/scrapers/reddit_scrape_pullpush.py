"""
PullPush-based Reddit scraper implementation with APIClient and ImageService.

This module implements the BaseScraper interface using the PullPush API,
properly utilizing the APIClient for HTTP requests.
"""

import time
from datetime import datetime
from typing import Generator, Optional, Set, Union

from reddit_scraper.config import get_config
from reddit_scraper.constants import (
    ContentType, 
    DEFAULT_POST_LIMIT, 
    PULLPUSH_BASE_URL,
    PULLPUSH_POST_FIELDS,
    PULLPUSH_COMMENT_FIELDS,
)
from reddit_scraper.core.models import RedditComment, RedditPost
from reddit_scraper.exceptions import PullPushError
from reddit_scraper.scrapers.base import BaseScraper
from reddit_scraper.services.image_service import ImageService
from reddit_scraper.utils.http import APIClient, get_user_agent
from reddit_scraper.utils.logging import get_logger

logger = get_logger(__name__)


class PullPushScraper(BaseScraper):
    """
    Reddit scraper implementation using the PullPush API.
    
    This class uses the PullPush API which doesn't require authentication
    but has some limitations compared to the official API.
    """
    
    def __init__(self, subreddit: str):
        """
        Initialize the PullPush scraper.
        
        Args:
            subreddit: Name of the subreddit to scrape
        """
        super().__init__(subreddit)
        
        # Initialize configuration
        self.config = get_config()
        
        # Create API client for PullPush
        self.api_client = APIClient(
            base_url=PULLPUSH_BASE_URL,
            user_agent=get_user_agent()
        )
        
        # Initialize the image service
        self.image_service = ImageService(subreddit)
        
        # Keep track of seen post and comment IDs to avoid duplicates
        self.seen_post_ids: Set[str] = set()
        self.seen_comment_ids: Set[str] = set()
        
        logger.info(f"PullPush scraper initialized for r/{subreddit}")
    
    def fetch_posts(
        self, 
        limit: Optional[int] = DEFAULT_POST_LIMIT,
        before: Optional[Union[int, datetime]] = None,
        after: Optional[Union[int, datetime]] = None,
    ) -> Generator[RedditPost, None, None]:
        """
        Fetch posts from the subreddit using the PullPush API with proper pagination.
        
        Args:
            limit: Maximum number of posts to fetch
            before: Only fetch posts before this time/timestamp
            after: Only fetch posts after this time/timestamp
            
        Yields:
            RedditPost objects
        """
        try:
            # Convert datetime to timestamp if needed
            before_ts = self._convert_to_timestamp(before)
            after_ts = self._convert_to_timestamp(after)
            
            # Set up pagination
            posts_fetched = 0
            batch_size = min(100, limit or 100)  # PullPush API max is 100
            next_before = before_ts
            
            logger.info(f"Fetching up to {limit} posts from r/{self.subreddit}")
            
            while limit is None or posts_fetched < limit:
                # Build request parameters
                params = {
                    "subreddit": self.subreddit,
                    "size": batch_size,
                    "sort": "desc",
                    "fields": ",".join(PULLPUSH_POST_FIELDS)
                }
                
                # Add filter parameters if provided
                if next_before is not None:
                    params["before"] = next_before
                elif after_ts is not None:
                    params["after"] = after_ts
                
                # Make the request using the API client
                logger.debug(f"Requesting posts with params: {params}")
                response = self.api_client.get("search/submission/", params=params)
                
                data = response.json().get("data", [])
                logger.debug(f"Received {len(data)} posts in this batch")
                
                if not data:
                    # No more posts to fetch
                    logger.info("No more posts to fetch")
                    break
                
                # Find the oldest post timestamp for next pagination
                try:
                    oldest_timestamp = min(post.get("created_utc", 0) for post in data)
                    # Subtract 1 to avoid duplication on the boundary
                    next_before = oldest_timestamp - 1
                    logger.debug(f"Oldest timestamp: {oldest_timestamp}, next_before: {next_before}")
                except (ValueError, KeyError) as e:
                    logger.error(f"Error calculating next pagination marker: {e}")
                    break
                
                # Process the batch of posts
                batch_yield_count = 0
                for post_data in data:
                    # Skip if we've already seen this post
                    post_id = post_data.get("id")
                    if post_id in self.seen_post_ids:
                        logger.debug(f"Skipping duplicate post: {post_id}")
                        continue
                    
                    # Apply time filters explicitly
                    created_utc = post_data.get("created_utc", 0)
                    if before_ts and created_utc >= before_ts:
                        continue
                    if after_ts and created_utc <= after_ts:
                        continue
                    
                    # Mark as seen to avoid duplicates
                    self.seen_post_ids.add(post_id)
                    
                    # Extract image URL from post using the image service
                    image_url = self.image_service.extract_image_url(post_data.get("url", ""))
                    
                    # Convert to RedditPost model
                    reddit_post = RedditPost(
                        id=post_id,
                        title=post_data.get("title", ""),
                        text=post_data.get("selftext", ""),
                        created_utc=int(created_utc),
                        created_time=datetime.fromtimestamp(created_utc).strftime(
                            "%Y-%m-%d %H:%M:%S"
                        ),
                        image_url=image_url,
                        image_path=None,  # Will be set after downloading
                    )
                    
                    yield reddit_post
                    batch_yield_count += 1
                    posts_fetched += 1
                    
                    # Check if we've reached the limit
                    if limit is not None and posts_fetched >= limit:
                        logger.info(f"Reached post limit of {limit}")
                        break
                
                # If we yielded 0 posts from this batch, but received data, 
                # it means all posts were filtered or duplicates - break to avoid infinite loop
                if batch_yield_count == 0 and data:
                    logger.info("No new posts to process in this batch")
                    break
                
                # Add a small delay between batch requests to be respectful
                time.sleep(1)
                
        except Exception as e:
            logger.error(f"Error fetching posts from r/{self.subreddit}: {e}")
            raise PullPushError(f"Error fetching posts: {e}")
    
    def fetch_comments(
        self,
        post_id: Optional[str] = None,
        limit: Optional[int] = None,
        before: Optional[Union[int, datetime]] = None,
        after: Optional[Union[int, datetime]] = None,
    ) -> Generator[RedditComment, None, None]:
        """
        Fetch comments from a post using the PullPush API with proper pagination.
        
        Args:
            post_id: ID of the post to fetch comments for
            limit: Maximum number of comments to fetch
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
            before_ts = self._convert_to_timestamp(before)
            after_ts = self._convert_to_timestamp(after)
            
            # Set up pagination
            comments_fetched = 0
            batch_size = min(100, limit or 100)  # PullPush API max is 100
            next_before = before_ts
            
            logger.info(f"Fetching up to {limit} comments for post {post_id}")
            
            # Reset seen comment IDs for this post
            self.seen_comment_ids = set()
            
            while limit is None or comments_fetched < limit:
                # Build request parameters
                params = {
                    "link_id": f"t3_{post_id}",  # PullPush API uses a t3_ prefix for post IDs
                    "size": batch_size,
                    "sort": "desc",
                    "fields": ",".join(PULLPUSH_COMMENT_FIELDS)
                }
                
                # Add filter parameters if provided
                if next_before is not None:
                    params["before"] = next_before
                elif after_ts is not None:
                    params["after"] = after_ts
                
                # Make the request using the API client
                logger.debug(f"Requesting comments with params: {params}")
                response = self.api_client.get("search/comment/", params=params)
                
                data = response.json().get("data", [])
                logger.debug(f"Received {len(data)} comments in this batch")
                
                if not data:
                    # No more comments to fetch
                    logger.info("No more comments to fetch for this post")
                    break
                
                # Find the oldest comment timestamp for next pagination
                try:
                    oldest_timestamp = min(comment.get("created_utc", 0) for comment in data)
                    # Subtract 1 to avoid duplication on the boundary
                    next_before = oldest_timestamp - 1
                    logger.debug(f"Oldest timestamp: {oldest_timestamp}, next_before: {next_before}")
                except (ValueError, KeyError) as e:
                    logger.error(f"Error calculating next pagination marker for comments: {e}")
                    break
                
                # Process the batch of comments
                batch_yield_count = 0
                for comment_data in data:
                    # Skip if we've already seen this comment
                    comment_id = comment_data.get("id")
                    if comment_id in self.seen_comment_ids:
                        logger.debug(f"Skipping duplicate comment: {comment_id}")
                        continue
                    
                    # Apply time filters explicitly
                    created_utc = comment_data.get("created_utc", 0)
                    if before_ts and created_utc >= before_ts:
                        continue
                    if after_ts and created_utc <= after_ts:
                        continue
                    
                    # Mark as seen to avoid duplicates
                    self.seen_comment_ids.add(comment_id)
                    
                    # Extract parent ID (removing prefix if present)
                    parent_id = comment_data.get("parent_id", "")
                    if parent_id.startswith("t1_"):  # Comment parent
                        parent_id = parent_id[3:]
                    elif parent_id.startswith("t3_"):  # Post parent
                        parent_id = None  # Top-level comment
                    
                    # Get comment text
                    text = comment_data.get("body", "")
                    
                    # Extract image URL from comment text using the image service
                    image_url = self.image_service.extract_image_url(text)
                    
                    # Convert to RedditComment model
                    reddit_comment = RedditComment(
                        id=comment_id,
                        post_id=post_id,
                        parent_id=parent_id,
                        text=text,
                        created_utc=int(created_utc),
                        created_time=datetime.fromtimestamp(created_utc).strftime(
                            "%Y-%m-%d %H:%M:%S"
                        ),
                        image_url=image_url,
                        image_path=None,  # Will be set after downloading
                    )
                    
                    yield reddit_comment
                    batch_yield_count += 1
                    comments_fetched += 1
                    
                    # Check if we've reached the limit
                    if limit is not None and comments_fetched >= limit:
                        logger.info(f"Reached comment limit of {limit}")
                        break
                
                # If we yielded 0 comments from this batch, but received data, 
                # it means all comments were filtered or duplicates - break to avoid infinite loop
                if batch_yield_count == 0 and data:
                    logger.info("No new comments to process in this batch")
                    break
                
                # Add a small delay between batch requests to be respectful
                time.sleep(1)
                
        except Exception as e:
            logger.error(f"Error fetching comments for post {post_id}: {e}")
            raise PullPushError(f"Error fetching comments: {e}")
    
    # Image methods delegate to the ImageService
    def extract_image_url(self, text: str) -> Optional[str]:
        """Extract an image URL from text content using the image service."""
        return self.image_service.extract_image_url(text)
    
    def download_image(
        self, 
        image_url: Optional[str], 
        item_id: str,
        content_type: ContentType
    ) -> Optional[str]:
        """Download an image from a URL using the image service."""
        return self.image_service.download_image(image_url, item_id, content_type)
    
    def _convert_to_timestamp(self, dt: Optional[Union[int, datetime]]) -> Optional[int]:
        """Convert a datetime object to a Unix timestamp if it isn't already."""
        if dt is None:
            return None
        
        if isinstance(dt, datetime):
            return int(dt.timestamp())
        
        return int(dt)
    
    @classmethod
    def get_name(cls) -> str:
        """Get the name of this scraper implementation."""
        return "pullpush"