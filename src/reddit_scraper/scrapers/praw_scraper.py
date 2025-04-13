"""
PRAW-based Reddit scraper implementation.

This module implements the BaseScraper interface using the PRAW library
for accessing the Reddit API directly. It handles different post sorting
methods and fetches comments, aiming for completeness using replace_more.
"""

from datetime import datetime, timezone
from typing import Generator, Optional, Union

import praw
import prawcore # Import for specific exceptions
from praw.models import MoreComments

from reddit_scraper.config import get_config
from reddit_scraper.constants import (
    ContentType, RedditSort, TopTimeFilter
)
from reddit_scraper.core.models import RedditComment, RedditPost
from reddit_scraper.exceptions import (
    PRAWError, ScraperError, ConfigurationError, ResourceNotFoundError
)
from reddit_scraper.scrapers.base import BaseScraper
from reddit_scraper.services.image_service import ImageService
from reddit_scraper.utils.logging import get_logger


logger = get_logger(__name__)


class PRAWScraper(BaseScraper):
    """
    Reddit scraper implementation using the PRAW library.

    Requires valid Reddit API credentials configured via `config.py`.
    Uses `replace_more(limit=None)` for fetching comments to ensure completeness,
    but be aware of potential memory usage on very large threads.

    Attributes:
        reddit (praw.Reddit): The authenticated PRAW instance.
        subreddit_obj (praw.Subreddit): The PRAW subreddit object.
    """

    SCRAPER_NAME = "praw"

    def __init__(self, subreddit: str, image_service: Optional[ImageService] = None):
        """
        Initialize the PRAW scraper.

        Args:
            subreddit (str): Name of the subreddit to scrape.
            image_service (Optional[ImageService]): Image service instance.

        Raises:
            ConfigurationError: If Reddit API credentials are missing or invalid.
            PRAWError: If PRAW fails to initialize or connect to Reddit.
        """
        super().__init__(subreddit, image_service)

        api_config = self.config.reddit_api
        if not (api_config.client_id and api_config.client_secret):
            raise ConfigurationError("Reddit API client_id and client_secret must be configured.")

        # Construct a polite and informative user agent
        user_agent = api_config.user_agent 

        try:
            # Initialize PRAW client
            self.reddit = praw.Reddit(
                client_id=api_config.client_id,
                client_secret=api_config.client_secret,
                user_agent=user_agent,
                
            )
            
            self.reddit.user.me()
            logger.info(f"PRAW authenticated as user: {self.reddit.user.me()}")

            # Get subreddit object and check if it seems valid (optional)
            self.subreddit_obj = self.reddit.subreddit(subreddit)
            logger.debug(f"PRAW Subreddit object created for r/{subreddit}")
            # Accessing an attribute forces PRAW to check if subreddit exists/is accessible
            _ = self.subreddit_obj.display_name

            logger.info(f"PRAW scraper initialized successfully for r/{self.subreddit}")

        except prawcore.exceptions.OAuthException as e:
            logger.error(f"PRAW Authentication failed: {e}", exc_info=True)
            raise ConfigurationError(f"PRAW Authentication failed. Check credentials/user agent: {e}") from e
        except prawcore.exceptions.NotFound as e:
             logger.error(f"Subreddit r/{subreddit} not found or inaccessible: {e}", exc_info=True)
             raise ResourceNotFoundError(resource_type="Subreddit", identifier=subreddit, detail=str(e)) from e
        except (praw.exceptions.PRAWException, prawcore.exceptions.PrawcoreException) as e:
            logger.error(f"PRAW initialization error: {e}", exc_info=True)
            raise PRAWError(f"Failed to initialize PRAW scraper: {e}") from e
        except Exception as e:
            logger.error(f"Unexpected error initializing PRAW scraper: {e}", exc_info=True)
            raise PRAWError(f"Unexpected error initializing PRAW scraper: {e}") from e


    def fetch_posts(
        self,
        limit: Optional[int] = None,
        sort_order: RedditSort = RedditSort.NEW,
        time_filter: TopTimeFilter = TopTimeFilter.ALL,
        before: Optional[Union[int, datetime]] = None,
        after: Optional[Union[int, datetime]] = None,
    ) -> Generator[RedditPost, None, None]:
        """
        Fetch posts from the subreddit using PRAW, supporting different sort orders.

        Args:
            limit: Maximum number of posts to fetch.
            sort_order: The order to sort posts (new, hot, top).
            time_filter: The time filter for 'top' sorting (e.g., 'day', 'week').
                         Only used if sort_order is TOP.
            before: Only yield posts created strictly before this time/timestamp.
                    Note: Filtering applied *after* fetching for non-'new' sorts.
            after: Only yield posts created strictly after this time/timestamp.
                   Note: Filtering applied *after* fetching for non-'new' sorts.

        Yields:
            RedditPost: An individual post fetched from the subreddit.

        Raises:
            PRAWError: If a PRAW-specific error occurs during fetching.
            ScraperError: For other unexpected errors.
        """
        logger.info(
            f"Fetching posts from r/{self.subreddit} "
            f"(sort={sort_order.value}, time={time_filter.value}, limit={limit})"
        )
        post_yield_count = 0
        try:
            before_ts = self._to_timestamp(before)
            after_ts = self._to_timestamp(after)

            # Select the appropriate PRAW subreddit method based on sort_order
            praw_time_filter_str = time_filter.value.lower() # PRAW expects lowercase strings

            if sort_order == RedditSort.NEW:
                posts_generator = self.subreddit_obj.new(limit=limit)
                logger.debug("Using subreddit.new()")
            elif sort_order == RedditSort.HOT:
                posts_generator = self.subreddit_obj.hot(limit=limit)
                logger.debug("Using subreddit.hot()")
            elif sort_order == RedditSort.TOP:
                posts_generator = self.subreddit_obj.top(time_filter=praw_time_filter_str, limit=limit)
                logger.debug(f"Using subreddit.top(time_filter='{praw_time_filter_str}')")
            # Add elif for CONTROVERSIAL, RISING if needed and implemented in constants/CLI
            else:
                logger.warning(f"Unsupported sort order '{sort_order.value}', defaulting to 'new'.")
                posts_generator = self.subreddit_obj.new(limit=limit)

            # Iterate through the posts returned by PRAW
            for post in posts_generator:
                post_created_utc = int(post.created_utc)

                # Apply time filtering *after* fetching for non-'new' sorts
                # PRAW doesn't support before/after timestamps on all listing types.
                if sort_order != RedditSort.NEW:
                    if before_ts and post_created_utc >= before_ts:
                        continue
                    if after_ts and post_created_utc <= after_ts:
                        continue

                # Extract image URL using the base class method (delegates to ImageService)
                # Ensure post.url exists, fall back to empty string if None
                image_url = self.extract_image_url(post.url or '')

                # Ensure post has an 'id'
                if not hasattr(post, "id") or post.id is None:
                    logger.warning(f"Skipping post with missing 'id': {vars(post) if hasattr(post, '__dict__') else post}")
                    continue

                # Create the Pydantic model
                # created_time will be automatically generated by the model validator
                try:
                    reddit_post = RedditPost(
                        id=post.id,
                        title=post.title or "[No Title Found]", # Handle potential None title
                        text=post.selftext or "", # Handle potential None selftext
                        created_utc=post_created_utc,
                        image_url=image_url,
                        image_path=None, # To be filled in by ScrapingService after download attempt
                    )
                except Exception as ex:
                    logger.error(f"Failed to construct RedditPost for post: {vars(post) if hasattr(post, '__dict__') else post}, Exception: {ex}")
                    continue

                yield reddit_post
                post_yield_count += 1

                # Check yield limit
                if limit is not None and post_yield_count >= limit:
                    logger.info(f"Reached post yield limit ({limit}) after filtering.")
                    break

            logger.info(f"Finished fetching posts for r/{self.subreddit}. Total yielded: {post_yield_count}")

        except (praw.exceptions.PRAWException, prawcore.exceptions.PrawcoreException) as e:
            logger.error(f"PRAW error fetching posts (sort={getattr(sort_order, 'value', sort_order)}): {e}", exc_info=True)
            raise PRAWError(f"PRAW error fetching posts ({getattr(sort_order, 'value', sort_order)}): {e}") from e
        except Exception as e:
            logger.error(f"Unexpected error fetching posts (sort={getattr(sort_order, 'value', sort_order)}): {e}", exc_info=True)
            raise ScraperError(f"Unexpected error fetching posts ({getattr(sort_order, 'value', sort_order)}): {e}") from e


    def fetch_comments(
        self,
        post_id: str,
        limit: Optional[int] = None,
        before: Optional[Union[int, datetime]] = None,
        after: Optional[Union[int, datetime]] = None,
    ) -> Generator[RedditComment, None, None]:
        """
        Fetch ALL comments for a specific post_id using PRAW's replace_more(limit=None).

        This method aims for completeness by loading the entire comment tree first.
        Warning: Can consume significant memory and time on large threads.

        Args:
            post_id: The ID of the post to fetch comments for.
            limit: Maximum total number of comments to *yield*. If None, yields all fetched.
            before: Only yield comments created strictly before this time/timestamp.
            after: Only yield comments created strictly after this time/timestamp.

        Yields:
            RedditComment: An individual comment fetched from the post.

        Raises:
            ResourceNotFoundError: If the post_id is not found.
            PRAWError: For PRAW-specific API or processing errors.
            ScraperError: For other unexpected errors during scraping.
        """
        logger.info(f"Fetching ALL comments for post {post_id} using replace_more(limit=None)...")
        comments_yielded_count = 0
        try:
            before_ts = self._to_timestamp(before)
            after_ts = self._to_timestamp(after)

            # Fetch the submission object
            submission = self.reddit.submission(id=post_id)
            # Accessing an attribute forces PRAW to check if post exists/is accessible
            _ = submission.title
            logger.debug(f"Fetched submission object for post {post_id}")

            # --- Load Entire Comment Tree ---
            # This is the potentially time/memory intensive step
            logger.debug(f"Calling replace_more(limit=None) for post {post_id}. This may take some time...")
            submission.comments.replace_more(limit=None)
            logger.debug(f"Finished replace_more for post {post_id}.")
            # --- End Load ---

            # Get the flattened list of all Comment objects
            all_comments = submission.comments.list()
            total_comments_found = len(all_comments)
            logger.info(f"Processing {total_comments_found} comments found for post {post_id}.")

            for comment in all_comments:
                # PRAW's list() should only contain Comment objects after replace_more(limit=None)
                if not isinstance(comment, praw.models.Comment):
                    logger.warning(f"Skipping non-comment object found in comments list: {type(comment)}")
                    continue # Should not happen, but safety check

                comment_created_utc = int(comment.created_utc)

                # Apply time filters
                if before_ts and comment_created_utc >= before_ts:
                    continue
                if after_ts and comment_created_utc <= after_ts:
                    continue

                # Extract parent ID (only the ID part)
                parent_full_id = comment.parent_id
                parent_id_only = None
                if parent_full_id and parent_full_id.startswith("t1_"): # Parent is comment
                    parent_id_only = parent_full_id[3:]
                # If parent starts with t3_, it's the submission, so parent_id_only remains None

                # Extract image URL from body using base class method (if applicable)
                image_url = self.extract_image_url(comment.body or '')

                # Ensure comment has an 'id'
                if not hasattr(comment, "id") or comment.id is None:
                    logger.warning(f"Skipping comment with missing 'id': {vars(comment) if hasattr(comment, '__dict__') else comment}")
                    continue

                # Create Pydantic model
                # created_time is handled by model validator
                reddit_comment = RedditComment(
                    id=comment.id,
                    post_id=post_id,
                    parent_id=parent_id_only,
                    text=comment.body or "", # Handle potential None body
                    created_utc=comment_created_utc,
                    image_url=image_url,
                    image_path=None, # To be filled in by ScrapingService
                )

                yield reddit_comment
                comments_yielded_count += 1

                # Check the yield limit if one was provided
                if limit is not None and comments_yielded_count >= limit:
                    logger.info(f"Reached comment yield limit ({limit}) for post {post_id}")
                    break

            logger.info(f"Finished processing comments for post {post_id}. Total yielded: {comments_yielded_count}")

        except prawcore.exceptions.NotFound:
            logger.error(f"Post not found: {post_id}")
            raise ResourceNotFoundError(resource_type="Post", identifier=post_id) from None
        except (praw.exceptions.PRAWException, prawcore.exceptions.PrawcoreException) as e:
            # Catch more specific PRAW errors if needed (e.g., Forbidden for private subreddits)
            logger.error(f"PRAW error fetching comments for post {post_id}: {e}", exc_info=True)
            raise PRAWError(f"Error fetching comments for post {post_id}: {e}") from e
        except Exception as e:
            logger.error(f"Unexpected error fetching comments for post {post_id}: {e}", exc_info=True)
            raise ScraperError(f"Unexpected error fetching comments for post {post_id}: {e}") from e

    @classmethod
    def get_name(cls) -> str:
        """Get the name of this scraper implementation."""
        return cls.SCRAPER_NAME