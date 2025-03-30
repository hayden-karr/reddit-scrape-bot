"""
Scraping service for Reddit Scraper.

This service orchestrates the scraping process, including data retrieval,
image downloading, and storage. It acts as a high-level interface for
the application to perform scraping operations.
"""

from datetime import datetime
from typing import List, Optional, Union

from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn, TimeElapsedColumn

from reddit_scraper.constants import ContentType, ScraperMethod
from reddit_scraper.core.models import RedditComment, RedditPost, ScrapingResult
from reddit_scraper.data.storage import RedditDataStorage
from reddit_scraper.exceptions import ScraperError, StorageError
from reddit_scraper.scrapers import create_scraper
from reddit_scraper .utils.logging import get_logger

logger = get_logger(__name__)
console = Console()


class ScrapingService:
    """
    Service for orchestrating Reddit scraping operations.
    
    This service provides high-level methods for scraping Reddit data,
    abstracting away the details of different scraping methods and storage.
    """

    def __init__(self, subreddit: str, method: str = ScraperMethod.PRAW):
        """
        Initialize the scraping service.
        
        Args:
            subreddit: Name of the subreddit to scrape
            method: Scraping method to use (PRAW or PullPush)
        """
        self.subreddit = subreddit
        self.method = method
        
        # Create the scraper
        self.scraper = create_scraper(method, subreddit)
        
        # Create the storage manager
        self.storage = RedditDataStorage(subreddit)
        
        logger.info(f"Scraping service initialized for r/{subreddit} using {method}")
    
    def scrape_and_store(
        self,
        post_limit: Optional[int] = None,
        comment_limit: Optional[int] = None,
        before: Optional[Union[int, datetime]] = None,
        after: Optional[Union[int, datetime]] = None,
        download_images: bool = True,
        show_progress: bool = True,
    ) -> ScrapingResult:
        """
        Scrape Reddit data and store it.
        
        This method orchestrates the full scraping process, including:
        - Fetching posts
        - Fetching comments for each post
        - Downloading images
        - Storing everything in the database
        
        Args:
            post_limit: Maximum number of posts to fetch
            comment_limit: Maximum number of comments to fetch per post
            before: Only fetch content before this time/timestamp
            after: Only fetch content after this time/timestamp
            download_images: Whether to download images
            show_progress: Whether to show a progress bar in the console
            
        Returns:
            ScrapingResult with statistics about the operation
        """
        logger.info(
            f"Starting scrape operation for r/{self.subreddit} using {self.method}"
        )
        
        result = ScrapingResult(subreddit=self.subreddit)
        posts: List[RedditPost] = []
        comments: List[RedditComment] = []
        
        # Show progress if requested
        if show_progress:
            with Progress(
                SpinnerColumn(),
                TextColumn("[bold blue]{task.description}"),
                TimeElapsedColumn(),
                console=console,
            ) as progress:
                task = progress.add_task(
                    f"[green]Scraping r/{self.subreddit} with {self.method}...", 
                    total=None
                )
                self._perform_scrape(
                    result, posts, comments, post_limit, comment_limit, 
                    before, after, download_images
                )
                progress.update(task, completed=True)
        else:
            self._perform_scrape(
                result, posts, comments, post_limit, comment_limit, 
                before, after, download_images
            )
        
        logger.info(
            f"Scrape completed: {len(posts)} posts, {len(comments)} comments"
        )
        
        # Save the scraped data
        try:
            self.storage.save_posts(posts)
            self.storage.save_comments(comments)
        except StorageError as e:
            logger.error(f"Error saving data: {e}")
            result.add_error()
        
        result.posts_count = len(posts)
        result.comments_count = len(comments)
        result.complete()
        
        return result
    
    def _perform_scrape(
        self,
        result: ScrapingResult,
        posts: List[RedditPost],
        comments: List[RedditComment],
        post_limit: Optional[int],
        comment_limit: Optional[int],
        before: Optional[Union[int, datetime]],
        after: Optional[Union[int, datetime]],
        download_images: bool,
    ) -> None:
        """
        Perform the actual scraping operation.
        
        This internal method performs the scraping process,
        populating the provided lists with results.
        
        Args:
            result: ScrapingResult to update with statistics
            posts: List to populate with scraped posts
            comments: List to populate with scraped comments
            post_limit: Maximum number of posts to fetch
            comment_limit: Maximum number of comments to fetch per post
            before: Only fetch content before this time/timestamp
            after: Only fetch content after this time/timestamp
            download_images: Whether to download images
        """
        try:
            # Fetch posts
            for post in self.scraper.fetch_posts(
                limit=post_limit, before=before, after=after
            ):
                posts.append(post)
                result.add_post()
                
                # Download post image if available
                if download_images and post.image_url:
                    image_path = self.scraper.download_image(
                        post.image_url, post.id, ContentType.POST
                    )
                    if image_path:
                        post.image_path = image_path
                        result.add_image()
                
                # Fetch and process comments for this post
                if comment_limit is not None and comment_limit > 0:
                    self._fetch_comments(
                        post.id, comments, result, comment_limit, 
                        before, after, download_images
                    )
        except Exception as e:
            logger.error(f"Error during scraping: {e}")
            result.add_error()
    
    def _fetch_comments(
        self,
        post_id: str,
        comments: List[RedditComment],
        result: ScrapingResult,
        limit: Optional[int],
        before: Optional[Union[int, datetime]],
        after: Optional[Union[int, datetime]],
        download_images: bool,
    ) -> None:
        """
        Fetch comments for a specific post.
        
        Args:
            post_id: ID of the post to fetch comments for
            comments: List to populate with scraped comments
            result: ScrapingResult to update with statistics
            limit: Maximum number of comments to fetch
            before: Only fetch comments before this time/timestamp
            after: Only fetch comments after this time/timestamp
            download_images: Whether to download images
        """
        try:
            for comment in self.scraper.fetch_comments(
                post_id=post_id, limit=limit, before=before, after=after
            ):
                comments.append(comment)
                result.add_comment()
                
                # Extract and download comment image if present
                if download_images:
                    image_url = self.scraper.extract_image_url(comment.text)
                    if image_url:
                        comment.image_url = image_url
                        image_path = self.scraper.download_image(
                            image_url, comment.id, ContentType.COMMENT
                        )
                        if image_path:
                            comment.image_path = image_path
                            result.add_image()
        except Exception as e:
            logger.error(f"Error fetching comments for post {post_id}: {e}")
            result.add_error()
    
    def get_available_data(self) -> dict:
        """
        Get statistics about available data for this subreddit.
        
        Returns:
            Dictionary with data statistics
        """
        try:
            return {
                "subreddit": self.subreddit,
                "total_posts": self.storage.get_total_posts(),
                "total_comments": self.storage.get_total_comments(),
            }
        except Exception as e:
            logger.error(f"Error getting data statistics: {e}")
            return {
                "subreddit": self.subreddit,
                "error": str(e),
            }