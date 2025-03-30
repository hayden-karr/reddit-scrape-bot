"""
Base scraper functionality for Reddit Explorer.

This module defines an abstract base class that all scraper implementations
must inherit from, ensuring a consistent interface regardless of the
underlying API or method used.
"""

import abc
from datetime import datetime
from pathlib import Path
from typing import Dict, Generator, List, Optional, Tuple, Union

from reddit_scraper.constants import ContentType
from reddit_scraper.core.models import RedditComment, RedditPost, ScrapingResult


class BaseScraper(abc.ABC):
    """
    Abstract base class for Reddit scrapers.
    
    This class defines the interface that all scraper implementations must follow,
    regardless of whether they use PRAW, PullPush, or another method.
    """

    def __init__(self, subreddit: str):
        """
        Initialize the scraper for a specific subreddit.
        
        Args:
            subreddit: Name of the subreddit to scrape
        """
        self.subreddit = subreddit
        self.result = ScrapingResult(subreddit=subreddit)
    
    @abc.abstractmethod
    def fetch_posts(
        self, 
        limit: Optional[int] = None,
        before: Optional[Union[int, datetime]] = None,
        after: Optional[Union[int, datetime]] = None,
    ) -> Generator[RedditPost, None, None]:
        """
        Fetch posts from the subreddit.
        
        Args:
            limit: Maximum number of posts to fetch
            before: Only fetch posts before this time/timestamp
            after: Only fetch posts after this time/timestamp
            
        Yields:
            RedditPost objects
        """
        pass
    
    @abc.abstractmethod
    def fetch_comments(
        self,
        post_id: Optional[str] = None,
        limit: Optional[int] = None,
        before: Optional[Union[int, datetime]] = None,
        after: Optional[Union[int, datetime]] = None,
    ) -> Generator[RedditComment, None, None]:
        """
        Fetch comments from the subreddit or a specific post.
        
        Args:
            post_id: Optional post ID to fetch comments for
            limit: Maximum number of comments to fetch
            before: Only fetch comments before this time/timestamp
            after: Only fetch comments after this time/timestamp
            
        Yields:
            RedditComment objects
        """
        pass
    
    @abc.abstractmethod
    def download_image(
        self, 
        image_url: Optional[str], 
        item_id: str,
        content_type: ContentType
    ) -> Optional[str]:
        """
        Download an image from a URL and save it to the appropriate location.
        
        Args:
            image_url: URL of the image to download
            item_id: ID of the post or comment
            content_type: Whether this is a post or comment image
            
        Returns:
            Path to the saved image, or None if no image was downloaded
        """
        pass
    
    @abc.abstractmethod
    def extract_image_url(self, text: str) -> Optional[str]:
        """
        Extract an image URL from text content.
        
        Args:
            text: Text to extract image URL from
            
        Returns:
            Extracted image URL, or None if no image was found
        """
        pass
    
    def scrape(
        self,
        post_limit: Optional[int] = None,
        comment_limit: Optional[int] = None,
        before: Optional[Union[int, datetime]] = None,
        after: Optional[Union[int, datetime]] = None,
        download_images: bool = True,
    ) -> ScrapingResult:
        """
        Perform a complete scrape operation.
        
        This method orchestrates the scraping process by fetching posts,
        fetching comments for each post, and downloading images as needed.
        
        Args:
            post_limit: Maximum number of posts to fetch
            comment_limit: Maximum number of comments to fetch per post
            before: Only fetch content before this time/timestamp
            after: Only fetch content after this time/timestamp
            download_images: Whether to download images
            
        Returns:
            ScrapingResult object with statistics about the operation
        """
        try:
            # Fetch and process posts
            for post in self.fetch_posts(limit=post_limit, before=before, after=after):
                self.result.add_post()
                
                # Download post image if available
                if download_images and post.image_url:
                    image_path = self.download_image(
                        post.image_url, post.id, ContentType.POST
                    )
                    if image_path:
                        post.image_path = image_path
                        self.result.add_image()
                
                # Fetch and process comments
                if comment_limit is not None and comment_limit > 0:
                    for comment in self.fetch_comments(
                        post_id=post.id, limit=comment_limit, before=before, after=after
                    ):
                        self.result.add_comment()
                        
                        # Download comment image if available
                        if download_images:
                            image_url = self.extract_image_url(comment.text)
                            if image_url:
                                comment.image_url = image_url
                                image_path = self.download_image(
                                    image_url, comment.id, ContentType.COMMENT
                                )
                                if image_path:
                                    comment.image_path = image_path
                                    self.result.add_image()
        except Exception as e:
            self.result.add_error()
            raise e
        finally:
            self.result.complete()
        
        return self.result
    
    @classmethod
    @abc.abstractmethod
    def get_name(cls) -> str:
        """
        Get the name of this scraper implementation.
        
        Returns:
            String name of the scraper
        """
        pass