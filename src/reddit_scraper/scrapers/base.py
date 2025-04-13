"""
Base scraper functionality for Reddit Scraper.

This module defines an abstract base class that all scraper implementations
must inherit from, ensuring a consistent interface regardless of the
underlying API or method used for fetching posts and comments.
"""

import abc
from datetime import datetime, timezone
from typing import Generator, Optional, Union

from reddit_scraper.config import get_config
from reddit_scraper.constants import ContentType, RedditSort, TopTimeFilter
from reddit_scraper.core.models import RedditComment, RedditPost, ScrapingResult
from reddit_scraper.exceptions import ScraperError
from reddit_scraper.services.image_service import ImageService
from reddit_scraper.utils.logging import get_logger

logger = get_logger(__name__)


class BaseScraper(abc.ABC):
    """
    Abstract base class for Reddit scrapers.

    Defines the common interface for fetching posts and comments from a subreddit.
    Handles interaction with the ImageService for image-related tasks.
    Subclasses must implement the abstract methods `Workspace_posts` and `Workspace_comments`.

    Attributes:
        subreddit (str): The name of the subreddit being scraped.
        result (ScrapingResult): Stores statistics about the scraping operation.
        image_service (ImageService): Service used for handling image downloads and URL extraction.
        config (AppConfig): Application configuration object.
    """

    def __init__(self, subreddit: str, image_service: Optional[ImageService] = None):
        """
        Initialize the scraper for a specific subreddit.

        Args:
            subreddit (str): Name of the subreddit to scrape.
            image_service (Optional[ImageService]): An instance of ImageService.
                                                  If None, a default one is created.

        Raises:
            ScraperError: If scraper initialization fails.
        """
        if not subreddit:
            raise ScraperError("Subreddit name cannot be empty.")

        self.subreddit: str = subreddit
        self.result: ScrapingResult = ScrapingResult(subreddit=subreddit)
        self.config = get_config() # Load config in base class if needed by subclasses

        # Use provided image service or create a new one
        self.image_service: ImageService = image_service or ImageService(subreddit)

        logger.debug(f"{self.__class__.__name__} initialized for r/{subreddit}")

    @abc.abstractmethod
    def fetch_posts(
        self,
        limit: Optional[int] = None,
        sort_order: RedditSort = RedditSort.NEW,
        time_filter: TopTimeFilter = TopTimeFilter.ALL,
        before: Optional[Union[int, datetime]] = None,
        after: Optional[Union[int, datetime]] = None,
    ) -> Generator[RedditPost, None, None]:
        """
        Fetch posts from the subreddit based on sorting and time filters.

        Args:
            limit: Maximum number of posts to fetch. Defaults to None.
            sort_order: The order to sort posts (new, hot, top).
            time_filter: The time filter for 'top' or 'controversial' sorting.
            before: Only fetch posts created strictly before this timestamp or datetime.
                    Compatibility may vary depending on the scraper implementation and sort order.
            after: Only fetch posts created strictly after this timestamp or datetime.
                   Compatibility may vary depending on the scraper implementation and sort order.

        Yields:
            RedditPost: An individual post fetched from the subreddit.

        Raises:
            ScraperError: If an error occurs during the scraping process.
            NotImplementedError: If the method is not implemented by a subclass.
        """
        raise NotImplementedError("Subclasses must implement fetch_posts")

    @abc.abstractmethod
    def fetch_comments(
        self,
        post_id: str,
        limit: Optional[int] = None,
        before: Optional[Union[int, datetime]] = None,
        after: Optional[Union[int, datetime]] = None,
    ) -> Generator[RedditComment, None, None]:
        """
        Fetch comments from a specific post within the subreddit.

        Args:
            post_id: The ID of the post to fetch comments for.
            limit: Maximum number of comments to fetch/yield. Defaults to None.
            before: Only fetch comments created strictly before this time/timestamp.
            after: Only fetch comments created strictly after this time/timestamp.

        Yields:
            RedditComment: An individual comment fetched from the post.

        Raises:
            ScraperError: If an error occurs during the scraping process.
            ResourceNotFoundError: If the specified post_id is not found by the scraper.
            NotImplementedError: If the method is not implemented by a subclass.
        """
        raise NotImplementedError("Subclasses must implement fetch_comments")

    def extract_image_url(self, text: str) -> Optional[str]:
        """
        Extracts the first potential image URL found in the given text.

        Delegates the actual extraction logic to the ImageService.

        Args:
            text: The text content (e.g., post URL, comment body) to search within.

        Returns:
            Optional[str]: The extracted image URL, or None if no valid URL is found.
        """
        return self.image_service.extract_image_url(text)

    def download_image(
        self,
        image_url: Optional[str],
        item_id: str,
        content_type: ContentType
    ) -> Optional[str]:
        """
        Attempts to download an image from the given URL.

        Delegates the download and saving logic to the ImageService.
        Increments the scraper's result counters for images or errors.

        Args:
            image_url: The URL of the image to download.
            item_id: The unique ID of the post or comment associated with the image.
            content_type: The type of content (POST or COMMENT).

        Returns:
            Optional[str]: The local path to the saved image file if successful, otherwise None.
        """
        if not image_url:
            return None
        try:
            # Delegate actual download to the image service
            image_path = self.image_service.download_image(image_url, item_id, content_type)
            if image_path:
                self.result.add_image() # Increment image count on success
                return image_path
            else:
                # If image_service returns None without raising an error (e.g., skip existing)
                # we don't count it as a new image or an error here.
                # If download failed inside image_service, it should raise an exception.
                return None
        except Exception as e:
            # Log warning and count error if download fails
            logger.warning(
                f"Image download failed for {content_type.value} {item_id} from {image_url}: {e}",
                exc_info=True # Include traceback in log for debugging
            )
            self.result.add_error()
            return None

    @classmethod
    @abc.abstractmethod
    def get_name(cls) -> str:
        """
        Get the identifying name of this scraper implementation (e.g., 'praw').

        Returns:
            str: The unique name of the scraper.

        Raises:
            NotImplementedError: If the method is not implemented by a subclass.
        """
        raise NotImplementedError("Subclasses must implement get_name")

    @staticmethod
    def _to_timestamp(dt: Optional[Union[int, datetime]]) -> Optional[int]:
        """
        Helper method to convert datetime objects to UTC Unix timestamps.

        Args:
            dt: The datetime object or integer timestamp.

        Returns:
            Optional[int]: The UTC Unix timestamp as an integer, or None if input is None.
        """
        if dt is None:
            return None
        if isinstance(dt, datetime):
            # Ensure datetime is timezone-aware (assume UTC if naive)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            elif dt.tzinfo != timezone.utc:
                 dt = dt.astimezone(timezone.utc) # Convert to UTC
            return int(dt.timestamp())
        try:
            return int(dt) # Assume it's already a timestamp-like int/float
        except (ValueError, TypeError):
             logger.warning(f"Could not convert '{dt}' to timestamp, returning None.")
             return None