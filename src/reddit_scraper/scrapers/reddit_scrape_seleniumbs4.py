"""
Selenium-based Reddit scraper implementation.

This module implements the BaseScraper interface using Selenium and BeautifulSoup
for browser-based scraping, which doesn't require API credentials.
"""

import re
import time
from datetime import datetime
from io import BytesIO
from pathlib import Path
from typing import Dict, Generator, List, Optional, Union
from urllib.parse import urlparse

import requests
from PIL import Image
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from bs4 import BeautifulSoup
from webdriver_manager.chrome import ChromeDriverManager

from reddit_scraper.config import get_config, get_image_dir
from reddit_scraper.constants import (
    ContentType,
    DEFAULT_POST_LIMIT,
    DEFAULT_USER_AGENT,
    IMAGE_FORMAT,
    IMAGE_QUALITY,
    MAX_IMAGE_PIXELS,
    VALID_IMAGE_EXTENSIONS,
)
from reddit_scraper.core.models import RedditComment, RedditPost, ScrapingResult
from reddit_scraper.exceptions import ScraperError
from reddit_scraper.scrapers.base import BaseScraper
from reddit_scraper.utils.http import create_retry_session, get_user_agent
from reddit_scraper.utils.logging import get_logger

logger = get_logger(__name__)


class SeleniumScraper(BaseScraper):
    """
    Reddit scraper implementation using Selenium and BeautifulSoup.
    
    This class uses browser automation to scrape Reddit content directly,
    which doesn't require API credentials but is slower and more resource-intensive.
    """
    
    def __init__(self, subreddit: str):
        """
        Initialize the Selenium scraper.
        
        Args:
            subreddit: Name of the subreddit to scrape
        """
        super().__init__(subreddit)
        
        # Initialize configuration
        self.config = get_config()
        
        # Initialize webdriver
        self.driver = None
        
        # Create an HTTP session for image downloads
        self.session = create_retry_session()
        
        # Create the image directory
        self.image_dir = get_image_dir(subreddit)
        
        # Configure image handling
        Image.MAX_IMAGE_PIXELS = MAX_IMAGE_PIXELS
        
        logger.info(f"Selenium scraper initialized for r/{subreddit}")
    
    def _init_browser(self) -> None:
        """Initialize the Selenium WebDriver."""
        try:
            logger.info("Initializing browser")
            chrome_options = Options()
            
            # Run in headless mode
            chrome_options.add_argument("--headless")
            chrome_options.add_argument("--no-sandbox")
            chrome_options.add_argument("--disable-dev-shm-usage")
            chrome_options.add_argument("--disable-gpu")
            chrome_options.add_argument("--window-size=1920,1080")
            
            # Set a realistic user agent
            chrome_options.add_argument(f"--user-agent={DEFAULT_USER_AGENT}")
            
            # Install and set up ChromeDriver using webdriver-manager
            service = Service(ChromeDriverManager().install())
            
            # Initialize the Chrome driver
            self.driver = webdriver.Chrome(service=service, options=chrome_options)
            self.driver.set_page_load_timeout(30)
            
            logger.debug("Browser initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize browser: {e}")
            raise ScraperError(f"Failed to initialize browser: {e}")
    
    def _close_browser(self) -> None:
        """Close the browser if it's open."""
        if self.driver:
            logger.debug("Closing browser")
            self.driver.quit()
            self.driver = None
    
    def fetch_posts(
        self, 
        limit: Optional[int] = DEFAULT_POST_LIMIT,
        before: Optional[Union[int, datetime]] = None,
        after: Optional[Union[int, datetime]] = None,
    ) -> Generator[RedditPost, None, None]:
        """
        Fetch posts from the subreddit using Selenium.
        
        Args:
            limit: Maximum number of posts to fetch
            before: Only fetch posts before this time/timestamp
            after: Only fetch posts after this time/timestamp
            
        Yields:
            RedditPost objects
        """
        try:
            # Initialize browser if needed
            if not self.driver:
                self._init_browser()
            
            # Prepare URL for the subreddit
            url = f"https://www.reddit.com/r/{self.subreddit}/new/"
            
            # Convert datetime to timestamp if needed
            before_ts = self._convert_to_timestamp(before)
            after_ts = self._convert_to_timestamp(after)
            
            # Navigate to the subreddit
            logger.info(f"Navigating to {url}")
            self.driver.get(url)
            
            # Wait for content to load
            WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )
            
            # Scroll to load more content if needed
            posts_fetched = 0
            max_scroll_attempts = 20  # Limit the number of scrolls
            scroll_attempts = 0
            
            while posts_fetched < limit and scroll_attempts < max_scroll_attempts:
                # Parse the current page content
                soup = BeautifulSoup(self.driver.page_source, "html.parser")
                
                # Find all post elements
                post_elements = soup.find_all("div", {"data-testid": "post-container"})
                logger.debug(f"Found {len(post_elements)} post elements on page")
                
                # Process each post element
                for post_element in post_elements:
                    # Skip if we've reached the limit
                    if posts_fetched >= limit:
                        break
                    
                    try:
                        # Extract post data
                        post_id = self._extract_post_id(post_element)
                        if not post_id:
                            continue
                        
                        # Extract creation time
                        created_utc = self._extract_post_timestamp(post_element)
                        
                        # Apply time filters
                        if before_ts and created_utc >= before_ts:
                            continue
                        if after_ts and created_utc <= after_ts:
                            continue
                        
                        # Extract post title
                        title_element = post_element.find("h3")
                        title = title_element.text.strip() if title_element else ""
                        
                        # Extract post URL and text
                        post_link = post_element.find("a", {"data-testid": "post-title"})
                        post_url = f"https://www.reddit.com{post_link['href']}" if post_link else ""
                        
                        # Get the post content - need to navigate to the post page
                        post_text = ""
                        if post_url:
                            # Store current page to return later
                            current_url = self.driver.current_url
                            
                            # Navigate to post page
                            self.driver.get(post_url)
                            WebDriverWait(self.driver, 10).until(
                                EC.presence_of_element_located((By.TAG_NAME, "body"))
                            )
                            
                            # Extract post text
                            post_soup = BeautifulSoup(self.driver.page_source, "html.parser")
                            post_text_element = post_soup.find("div", {"data-testid": "post-content"})
                            if post_text_element:
                                post_text = post_text_element.text.strip()
                            
                            # Return to the subreddit page
                            self.driver.get(current_url)
                            WebDriverWait(self.driver, 10).until(
                                EC.presence_of_element_located((By.TAG_NAME, "body"))
                            )
                        
                        # Extract image URL
                        image_url = self._extract_image_url_from_element(post_element)
                        if not image_url:
                            image_url = self.extract_image_url(post_url)
                        
                        # Convert to RedditPost model
                        reddit_post = RedditPost(
                            id=post_id,
                            title=title,
                            text=post_text,
                            created_utc=created_utc,
                            created_time=datetime.fromtimestamp(created_utc).strftime(
                                "%Y-%m-%d %H:%M:%S"
                            ),
                            image_url=image_url,
                            image_path=None,  # Will be set after downloading
                        )
                        
                        yield reddit_post
                        posts_fetched += 1
                        
                    except Exception as e:
                        logger.warning(f"Error processing post element: {e}")
                
                # If we haven't reached the limit, scroll down to load more
                if posts_fetched < limit:
                    logger.debug(f"Scrolling for more posts ({posts_fetched}/{limit})")
                    self.driver.execute_script(
                        "window.scrollTo(0, document.body.scrollHeight);"
                    )
                    # Wait for new content to load
                    time.sleep(2)
                    scroll_attempts += 1
                
            logger.info(f"Fetched {posts_fetched} posts from r/{self.subreddit}")
            
        except Exception as e:
            logger.error(f"Error fetching posts from r/{self.subreddit}: {e}")
            raise ScraperError(f"Error fetching posts: {e}")
        finally:
            # Clean up browser to avoid memory leaks
            self._close_browser()
    
    def fetch_comments(
        self,
        post_id: Optional[str] = None,
        limit: Optional[int] = None,
        before: Optional[Union[int, datetime]] = None,
        after: Optional[Union[int, datetime]] = None,
    ) -> Generator[RedditComment, None, None]:
        """
        Fetch comments from a post using Selenium.
        
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
            # Initialize browser if needed
            if not self.driver:
                self._init_browser()
            
            # Prepare URL for the post
            url = f"https://www.reddit.com/r/{self.subreddit}/comments/{post_id}/"
            
            # Convert datetime to timestamp if needed
            before_ts = self._convert_to_timestamp(before)
            after_ts = self._convert_to_timestamp(after)
            
            # Navigate to the post
            logger.info(f"Navigating to post {url}")
            self.driver.get(url)
            
            # Wait for content to load
            WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )
            
            # Expand all comment threads
            try:
                expand_buttons = self.driver.find_elements(By.XPATH, "//button[contains(text(), 'Continue this thread')]")
                for button in expand_buttons:
                    self.driver.execute_script("arguments[0].click();", button)
                    time.sleep(1)
            except Exception as e:
                logger.warning(f"Error expanding comment threads: {e}")
            
            # Scroll to load more comments if needed
            comments_fetched = 0
            max_scroll_attempts = 10  # Limit the number of scrolls
            scroll_attempts = 0
            
            while (limit is None or comments_fetched < limit) and scroll_attempts < max_scroll_attempts:
                # Parse the current page content
                soup = BeautifulSoup(self.driver.page_source, "html.parser")
                
                # Find all comment elements
                comment_elements = soup.find_all("div", {"data-testid": "comment"})
                logger.debug(f"Found {len(comment_elements)} comment elements on page")
                
                # Process each comment element
                for comment_element in comment_elements:
                    # Skip if we've reached the limit
                    if limit is not None and comments_fetched >= limit:
                        break
                    
                    try:
                        # Extract comment ID
                        comment_id = self._extract_comment_id(comment_element)
                        if not comment_id:
                            continue
                        
                        # Extract parent ID
                        parent_element = comment_element.find_parent("div", {"data-testid": "comment"})
                        parent_id = self._extract_comment_id(parent_element) if parent_element else None
                        
                        # Extract creation time
                        created_utc = self._extract_comment_timestamp(comment_element)
                        
                        # Apply time filters
                        if before_ts and created_utc >= before_ts:
                            continue
                        if after_ts and created_utc <= after_ts:
                            continue
                        
                        # Extract comment text
                        text_element = comment_element.find("div", {"data-testid": "comment-content"})
                        text = text_element.text.strip() if text_element else ""
                        
                        # Convert to RedditComment model
                        reddit_comment = RedditComment(
                            id=comment_id,
                            post_id=post_id,
                            parent_id=parent_id,
                            text=text,
                            created_utc=created_utc,
                            created_time=datetime.fromtimestamp(created_utc).strftime(
                                "%Y-%m-%d %H:%M:%S"
                            ),
                            image_url=None,  # Will be set after extraction
                            image_path=None,  # Will be set after downloading
                        )
                        
                        yield reddit_comment
                        comments_fetched += 1
                        
                    except Exception as e:
                        logger.warning(f"Error processing comment element: {e}")
                
                # If we haven't reached the limit, scroll down to load more
                if limit is None or comments_fetched < limit:
                    logger.debug(f"Scrolling for more comments ({comments_fetched}/{limit if limit else 'unlimited'})")
                    self.driver.execute_script(
                        "window.scrollTo(0, document.body.scrollHeight);"
                    )
                    # Wait for new content to load
                    time.sleep(2)
                    scroll_attempts += 1
                
            logger.info(f"Fetched {comments_fetched} comments from post {post_id}")
            
        except Exception as e:
            logger.error(f"Error fetching comments for post {post_id}: {e}")
            raise ScraperError(f"Error fetching comments: {e}")
        finally:
            # Clean up browser to avoid memory leaks
            self._close_browser()
    
    def extract_image_url(self, text: str) -> Optional[str]:
        """
        Extract an image URL from text content.
        
        Args:
            text: Text to extract image URL from
            
        Returns:
            Extracted image URL, or None if no image was found
        """
        # If text is empty, return None
        if not text:
            return None
            
        # Check if text itself is an image URL
        try:
            parsed_url = urlparse(text)
            path = parsed_url.path.lower()
            
            # Check if the URL has a valid image extension
            if any(path.endswith(ext) for ext in VALID_IMAGE_EXTENSIONS):
                return text
            
            # Check for Reddit's image hosting domains
            if "i.redd.it" in text or "i.imgur.com" in text:
                return text
        except Exception:
            pass
        
        # Otherwise, search for image URLs in the text
        url_pattern = r'https?://[^\s)"]+\.(?:jpg|jpeg|png|gif|webp)(?:\?[^\s)]*)?'
        matches = re.findall(url_pattern, text, re.IGNORECASE)
        
        if matches:
            return matches[0]
        
        # Also check for Reddit and Imgur image links
        reddit_imgur_pattern = r'https?://(?:i\.redd\.it|i\.imgur\.com)/[^\s)]+'
        matches = re.findall(reddit_imgur_pattern, text, re.IGNORECASE)
        
        if matches:
            return matches[0]
        
        return None
    
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
        if not image_url:
            return None
        
        # Create the image filename
        prefix = "comment_" if content_type == ContentType.COMMENT else ""
        image_path = self.image_dir / f"{prefix}{item_id}.{IMAGE_FORMAT.lower()}"
        
        # Skip if already downloaded
        if image_path.exists():
            logger.debug(f"Image already exists at {image_path}")
            return str(image_path)
        
        try:
            # First try standard headers
            response = self.session.get(image_url, stream=True, timeout=10)
            response.raise_for_status()
        except requests.exceptions.RequestException:
            try:
                # Retry with browser-like headers
                headers = {"User-Agent": get_user_agent()}
                response = self.session.get(image_url, headers=headers, stream=True, timeout=10)
                response.raise_for_status()
            except requests.exceptions.RequestException as e:
                logger.warning(f"Failed to download image {image_url}: {e}")
                return None
        
        try:
            # Process and save the image
            image = Image.open(BytesIO(response.content))
            image.save(image_path, IMAGE_FORMAT, quality=IMAGE_QUALITY)
            logger.debug(f"Downloaded image to {image_path}")
            return str(image_path)
        except Exception as e:
            logger.warning(f"Error processing image: {e}")
            return None
    
    def _extract_post_id(self, element) -> Optional[str]:
        """Extract the post ID from a post element."""
        try:
            # Try to find the ID attribute
            id_attr = element.get("id")
            if id_attr and id_attr.startswith("t3_"):
                return id_attr[3:]
            
            # Try to find the post link which often contains the ID
            link = element.find("a", {"data-testid": "post-title"})
            if link and link.get("href"):
                href = link.get("href")
                match = re.search(r'/comments/([a-z0-9]+)/', href)
                if match:
                    return match.group(1)
        except Exception as e:
            logger.warning(f"Error extracting post ID: {e}")
        
        return None
    
    def _extract_comment_id(self, element) -> Optional[str]:
        """Extract the comment ID from a comment element."""
        try:
            if not element:
                return None
            
            # Try to find the ID attribute
            id_attr = element.get("id")
            if id_attr and id_attr.startswith("t1_"):
                return id_attr[3:]
            
            # Try to find it in the permalink
            permalink = element.find("a", {"data-testid": "permalink"})
            if permalink and permalink.get("href"):
                href = permalink.get("href")
                match = re.search(r'/comments/[a-z0-9]+/[^/]+/([a-z0-9]+)', href)
                if match:
                    return match.group(1)
        except Exception as e:
            logger.warning(f"Error extracting comment ID: {e}")
        
        return None
    
    def _extract_post_timestamp(self, element) -> int:
        """Extract the creation timestamp from a post element."""
        try:
            # Try to find the timestamp element
            time_element = element.find("time")
            if time_element and time_element.get("datetime"):
                return int(datetime.fromisoformat(time_element.get("datetime").replace("Z", "+00:00")).timestamp())
            
            # If we can't find a timestamp, use the current time as a fallback
            return int(datetime.now().timestamp())
        except Exception as e:
            logger.warning(f"Error extracting post timestamp: {e}")
            return int(datetime.now().timestamp())
    
    def _extract_comment_timestamp(self, element) -> int:
        """Extract the creation timestamp from a comment element."""
        try:
            # Try to find the timestamp element
            time_element = element.find("time")
            if time_element and time_element.get("datetime"):
                return int(datetime.fromisoformat(time_element.get("datetime").replace("Z", "+00:00")).timestamp())
            
            # If we can't find a timestamp, use the current time as a fallback
            return int(datetime.now().timestamp())
        except Exception as e:
            logger.warning(f"Error extracting comment timestamp: {e}")
            return int(datetime.now().timestamp())
    
    def _extract_image_url_from_element(self, element) -> Optional[str]:
        """Extract an image URL from a post element."""
        try:
            # Look for image elements
            img = element.find("img")
            if img and img.get("src"):
                src = img.get("src")
                if self._is_valid_image_url(src):
                    return src
            
            # Look for preview links
            links = element.find_all("a")
            for link in links:
                href = link.get("href")
                if href and self._is_valid_image_url(href):
                    return href
        except Exception as e:
            logger.warning(f"Error extracting image URL from element: {e}")
        
        return None
    
    def _is_valid_image_url(self, url: str) -> bool:
        """Check if a URL is a valid image URL."""
        try:
            if not url:
                return False
                
            # Check file extension
            parsed_url = urlparse(url)
            path = parsed_url.path.lower()
            
            # Check if the URL has a valid image extension
            if any(path.endswith(ext) for ext in VALID_IMAGE_EXTENSIONS):
                return True
            
            # Check for Reddit's image hosting domains
            if "i.redd.it" in url or "i.imgur.com" in url:
                return True
        except Exception:
            pass
        
        return False
    
    def _convert_to_timestamp(self, dt: Optional[Union[int, datetime]]) -> Optional[int]:
        """
        Convert a datetime object to a Unix timestamp if it isn't already.
        
        Args:
            dt: Datetime object or timestamp
            
        Returns:
            Unix timestamp as an integer, or None if dt is None
        """
        if dt is None:
            return None
        
        if isinstance(dt, datetime):
            return int(dt.timestamp())
        
        return int(dt)
    
    @classmethod
    def get_name(cls) -> str:
        """
        Get the name of this scraper implementation.
        
        Returns:
            String name of the scraper
        """
        return "browser"