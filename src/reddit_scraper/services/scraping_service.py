"""
Scraping service for Reddit Scraper.

This service orchestrates the scraping process, handling the interaction
between the chosen scraper implementation, the image service, and data storage.
It acts as the primary high-level interface for initiating and managing
scraping operations based on various parameters including sorting.
"""

from datetime import datetime
from typing import List, Optional, Union, Generator, Any

from rich.console import Console
from rich.progress import (
    Progress, SpinnerColumn, TextColumn, TimeElapsedColumn, BarColumn, TaskProgressColumn
)

from reddit_scraper.constants import (
    ContentType, ScraperMethod, RedditSort, TopTimeFilter,
    DEFAULT_POST_LIMIT, DEFAULT_COMMENT_LIMIT
)
from reddit_scraper.core.models import RedditComment, RedditPost, ScrapingResult
from reddit_scraper.data.storage import RedditDataStorage # Or use DataManager if consolidated
from reddit_scraper.exceptions import ScraperError, StorageError, RedditScraperError, ResourceNotFoundError
from reddit_scraper.scrapers import create_scraper, BaseScraper
from reddit_scraper.services.image_service import ImageService
from reddit_scraper.utils.logging import get_logger

logger = get_logger(__name__)
console = Console()


class ScrapingService:
    """
    Service for orchestrating Reddit scraping operations.

    Provides high-level methods for scraping Reddit data based on specified
    sorting and filtering criteria, handling image downloads, and storing results.

    Attributes:
        subreddit (str): Name of the subreddit being targeted.
        method (str): The scraping method chosen (e.g., 'praw', 'pullpush').
        scraper (BaseScraper): The scraper instance for the chosen method.
        storage (RedditDataStorage): The storage manager instance.
        image_service (ImageService): The image service instance.
    """

    def __init__(self, subreddit: str, method: str = ScraperMethod.PRAW.value):
        """
        Initialize the scraping service.

        Args:
            subreddit (str): Name of the subreddit to scrape.
            method (str): Scraping method identifier (e.g., 'praw').

        Raises:
            ValueError: If the specified scraping method is invalid.
            RedditScraperError: If initialization of scraper, storage, or image service fails.
        """
        self.subreddit = subreddit
        self.method = method

        try:
            # Initialize dependent services first
            self.image_service = ImageService(subreddit)
            # Assuming RedditDataStorage handles its own path setup via config
            self.storage = RedditDataStorage(subreddit)

            # Create the scraper, passing the shared image service
            self.scraper: BaseScraper = create_scraper(method, subreddit, image_service=self.image_service)

            logger.info(f"Scraping service initialized for r/{subreddit} using {method}")
        except (ValueError, RedditScraperError) as e:
            logger.error(f"Failed to initialize ScrapingService: {e}", exc_info=True)
            raise
        except Exception as e:
            logger.error(f"Unexpected error initializing ScrapingService: {e}", exc_info=True)
            raise RedditScraperError(f"Unexpected error initializing ScrapingService: {e}") from e


    def scrape_and_store(
        self,
        post_limit: Optional[int] = DEFAULT_POST_LIMIT,
        comment_limit: Optional[int] = DEFAULT_COMMENT_LIMIT,
        sort_order: RedditSort = RedditSort.NEW,
        time_filter: TopTimeFilter = TopTimeFilter.ALL,
        before: Optional[Union[int, datetime]] = None,
        after: Optional[Union[int, datetime]] = None,
        download_images: bool = True,
        show_progress: bool = True,
    ) -> ScrapingResult:
        """
        Perform the complete scraping and storing operation with sorting and filtering.

        Fetches posts and their comments using the configured scraper according
        to the specified sort order and time filters. Downloads associated images
        if requested, and saves all collected data to the configured storage.

        Args:
            post_limit: Maximum number of posts to fetch and yield.
            comment_limit: Maximum number of comments to fetch/yield per post (0 or None for no comments).
            sort_order: The order to sort posts (new, hot, top).
            time_filter: The time filter for 'top' sorting (day, week, etc.).
            before: Only process content created strictly before this time/timestamp.
                    Applied post-fetch for some scraper/sort combinations.
            after: Only process content created strictly after this time/timestamp.
                   Applied post-fetch for some scraper/sort combinations.
            download_images: Whether to attempt downloading images associated with posts/comments.
            show_progress: Whether to display a progress bar in the console.

        Returns:
            ScrapingResult: An object containing statistics about the completed operation.
        """
        logger.info(
            f"Starting scrape_and_store for r/{self.subreddit} "
            f"(sort={sort_order.value}, time={time_filter.value}, "
            f"posts={post_limit}, comments={comment_limit}, images={download_images})"
        )
        # Initialize result tracker at the beginning
        result = ScrapingResult(subreddit=self.subreddit)

        # Lists to hold fetched data before saving
        all_posts: List[RedditPost] = []
        all_comments: List[RedditComment] = []

        try:
            # --- Scraping Phase ---
            if show_progress:
                with Progress(
                    SpinnerColumn(),
                    TextColumn("[progress.description]{task.description}", justify="right"),
                    BarColumn(bar_width=None),
                    TaskProgressColumn(),
                    TimeElapsedColumn(),
                    console=console,
                    transient=False, # Keep finished progress bars
                ) as progress:
                    # Determine total for post task if limit is set
                    post_total = post_limit if (post_limit and post_limit > 0) else None
                    post_task = progress.add_task(f"[cyan]Fetching {sort_order.value} posts...", total=post_total)
                    # Comment total is generally unknown upfront
                    comment_task = progress.add_task(f"[magenta]Fetching comments...", total=None)

                    self._execute_scrape_loop(
                        result=result,
                        all_posts=all_posts,
                        all_comments=all_comments,
                        post_limit=post_limit,
                        comment_limit=comment_limit,
                        sort_order=sort_order,
                        time_filter=time_filter,
                        before=before,
                        after=after,
                        download_images=download_images,
                        progress=progress,
                        post_task_id=post_task,
                        comment_task_id=comment_task
                    )
                    # Update progress descriptions upon completion
                    progress.update(post_task, description=f"[green]Fetched {result.posts_count} posts", completed=result.posts_count)
                    # Set comment total after fetching if possible
                    progress.update(comment_task, description=f"[green]Fetched {result.comments_count} comments", total=result.comments_count, completed=result.comments_count)
            else: # No progress bar
                self._execute_scrape_loop(
                    result=result,
                    all_posts=all_posts,
                    all_comments=all_comments,
                    post_limit=post_limit,
                    comment_limit=comment_limit,
                    sort_order=sort_order,
                    time_filter=time_filter,
                    before=before,
                    after=after,
                    download_images=download_images,
                    progress=None,
                    post_task_id=None,
                    comment_task_id=None
                )

            logger.info(f"Scraping fetch phase completed for r/{self.subreddit}. Found {len(all_posts)} posts, {len(all_comments)} comments.")

            # --- Data Storage Phase ---
            if not all_posts and not all_comments:
                 logger.info("No posts or comments fetched, skipping storage.")
            else:
                logger.info(f"Saving {len(all_posts)} posts and {len(all_comments)} comments...")
                posts_saved = self.storage.save_posts(all_posts)
                comments_saved = self.storage.save_comments(all_comments)
                logger.info(f"Storage complete. Saved {posts_saved} unique posts and {comments_saved} unique comments.")
                # Note: result counts reflect FETCHED items, save results reflect unique items stored.

        except (ScraperError, StorageError) as e:
            # Log specific errors from scraping or storage phases
            logger.error(f"Scraping/Storage failed for r/{self.subreddit}: {e}", exc_info=True)
            result.add_error() # Ensure error is counted in result summary
            # Propagate the error so CLI can report failure
            raise
        except Exception as e:
            # Catch any other unexpected exceptions
            logger.exception(f"An unexpected error occurred during scraping/storage for r/{self.subreddit}: {e}")
            result.add_error()
            # Wrap in a generic RedditScraperError before propagating
            raise RedditScraperError(f"Unhandled exception during scrape: {e}") from e
        finally:
            # Always mark the operation as complete to record end time
            result.complete()

        logger.info(f"Scrape_and_store finished for r/{self.subreddit}. Result: {result.model_dump(exclude_none=True)}")
        return result


    def _execute_scrape_loop(
        self,
        result: ScrapingResult,
        all_posts: List[RedditPost],
        all_comments: List[RedditComment],
        post_limit: Optional[int],
        comment_limit: Optional[int],
        sort_order: RedditSort,
        time_filter: TopTimeFilter,
        before: Optional[Union[int, datetime]],
        after: Optional[Union[int, datetime]],
        download_images: bool,
        progress: Optional[Progress],
        post_task_id: Optional[Any], # Using Any for Rich's TaskID type
        comment_task_id: Optional[Any]
    ) -> None:
        """
        Internal helper method to execute the main post and comment fetching loop.

        Iterates through posts yielded by the scraper, triggers comment fetching
        for each post, handles image downloading delegation, and updates progress.

        Args:
            result: The ScrapingResult object to update statistics.
            all_posts: List to append fetched RedditPost objects to.
            all_comments: List to append fetched RedditComment objects to.
            post_limit: Maximum number of posts to process.
            comment_limit: Maximum number of comments to fetch per post.
            sort_order: Sorting order for posts.
            time_filter: Time filter for 'top' sorting.
            before: Time filter for fetching content before this date/timestamp.
            after: Time filter for fetching content after this date/timestamp.
            download_images: Flag to enable/disable image downloads.
            progress: Optional Rich Progress instance for updates.
            post_task_id: Optional Rich TaskID for the post fetching task.
            comment_task_id: Optional Rich TaskID for the comment fetching task.

        Raises:
            ScraperError: Propagates scraper errors encountered during fetching.
        """
        post_processed_count = 0
        try:
            # Fetch posts using the configured scraper and parameters
            post_generator = self.scraper.fetch_posts(
                limit=post_limit,
                sort_order=sort_order,
                time_filter=time_filter,
                before=before,
                after=after
            )

            for post in post_generator:
                # Record the fetched post
                result.add_post()
                post_processed_count += 1
                all_posts.append(post)

                # Update post progress bar
                if progress and post_task_id:
                    description = f"[cyan]Processing {sort_order.value} posts... ({post_processed_count}/{post_limit or '?'})"
                    progress.update(post_task_id, advance=1, description=description)

                # --- Download Post Image ---
                if download_images and post.image_url:
                    logger.debug(f"Attempting to download image for post {post.id}: {post.image_url}")
                    # Delegate download to scraper's method (uses ImageService)
                    # Updates result counter internally
                    image_path = self.scraper.download_image(post.image_url, post.id, ContentType.POST)
                    logger.debug(f"Download result for post {post.id}: {image_path}")
                    if image_path:
                        post.image_path = image_path # Update model with local path
                        result.add_image()


                # --- Fetch Comments for this Post ---
                # Fetch comments only if limit is positive
                should_fetch_comments = comment_limit is not None and comment_limit > 0
                if should_fetch_comments:
                    current_post_comment_count = 0
                    try:
                        if progress and comment_task_id:
                            progress.update(comment_task_id, description=f"[magenta]Fetching comments for post {post.id[:6]}...")

                        # Fetch comments using scraper method
                        comment_generator = self.scraper.fetch_comments(
                            post_id=post.id,
                            limit=comment_limit, # Pass limit to scraper
                            before=before, # Pass time filters if needed for comments
                            after=after
                        )

                        for comment in comment_generator:
                            result.add_comment()
                            current_post_comment_count += 1
                            all_comments.append(comment)

                            # Update overall comment progress
                            if progress and comment_task_id:
                                progress.update(comment_task_id, advance=1, description=f"[magenta]Fetching comments... ({result.comments_count})")

                            # --- Download Comment Image ---
                            if download_images:
                                # Extract first (scraper's method uses ImageService)
                                comment_image_url = self.scraper.extract_image_url(comment.text)
                                if comment_image_url:
                                    comment.image_url = comment_image_url # Store extracted URL
                                    # Delegate download (scraper uses ImageService)
                                    comment_image_path = self.scraper.download_image(
                                        comment_image_url, comment.id, ContentType.COMMENT
                                    )
                                    if comment_image_path:
                                        comment.image_path = comment_image_path # Update model
                                        result.add_image()

                            # Check if comment limit *for this post* is reached
                            if current_post_comment_count >= comment_limit:
                                break

                    except ResourceNotFoundError:
                         # Log if post disappears between post fetch and comment fetch (rare)
                         logger.warning(f"Post {post.id} not found when attempting to fetch comments.")
                         result.add_error()
                    except ScraperError as comment_exc:
                        # Log specific scraper errors during comment fetch for this post
                        logger.error(f"Scraper error fetching comments for post {post.id}: {comment_exc}", exc_info=True)
                        result.add_error()
                    except Exception as comment_exc:
                        # Log unexpected errors during comment fetch for this post
                        logger.exception(f"Unexpected error fetching comments for post {post.id}: {comment_exc}")
                        result.add_error()
                    # Continue to the next post even if comment fetching failed for one post

                # Check if post processing limit is reached (redundant if post_generator respects limit, but safe)
                if post_limit is not None and post_processed_count >= post_limit:
                    logger.info(f"Reached post processing limit ({post_limit}).")
                    break

        except ScraperError as post_exc:
            # If the post generator itself fails, log and re-raise
            logger.error(f"Scraper error during post fetch loop ({sort_order.value}): {post_exc}", exc_info=True)
            result.add_error()
            raise # Propagate error to the main handler
        except Exception as post_exc:
             logger.exception(f"Unexpected error during post fetch loop ({sort_order.value}): {post_exc}")
             result.add_error()
             raise ScraperError(f"Unexpected error during post fetch loop: {post_exc}") from post_exc


    def get_available_data(self) -> dict:
        """
        Get statistics about available locally stored data for this subreddit.

        Returns:
            dict: A dictionary containing 'subreddit', 'total_posts', 'total_comments'.

        Raises:
            StorageError: If there's an error accessing the storage layer.
        """
        logger.debug(f"Getting available data stats for r/{self.subreddit}")
        try:
            total_posts = self.storage.get_total_posts()
            total_comments = self.storage.get_total_comments()
            logger.debug(f"Storage stats: {total_posts} posts, {total_comments} comments.")
            return {
                "subreddit": self.subreddit,
                "total_posts": total_posts,
                "total_comments": total_comments,
            }
        except StorageError as e:
            logger.error(f"Storage error getting data statistics for r/{self.subreddit}: {e}", exc_info=True)
            raise # Re-raise storage errors
        except Exception as e:
            logger.error(f"Unexpected error getting data statistics for r/{self.subreddit}: {e}", exc_info=True)
            # Wrap unexpected errors
            raise StorageError(f"Unexpected error getting data statistics: {e}") from e