#!/usr/bin/env python3
"""
Reddit Scraper - A tool to scrape Reddit data using Selenium and BeautifulSoup.

This script provides a flexible, configurable way to extract data from Reddit
while following industry best practices including rate limiting, error handling,
and proper logging.

Usage:
    python reddit_scraper.py subreddit <subreddit> [--limit N] [--sort TYPE]
    python reddit_scraper.py post <post_id> <subreddit> [--limit N]
    python reddit_scraper.py user <username> [--limit N]
    
Example:
    python reddit_scraper.py subreddit python --limit 25 --sort new
    python reddit_scraper.py post abcd123 python --limit 50
    python reddit_scraper.py user reddit_username --limit 30
"""

import argparse
import configparser
import csv
import json
import logging
import logging.handlers
import os
import random
import re
import sys
import time
from dataclasses import dataclass, asdict
from datetime import datetime
from typing import Dict, List, Optional, Union, Any, Tuple

import requests
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from webdriver_manager.chrome import ChromeDriverManager


# Configure logging
def setup_logging(log_level: str = "INFO", log_file: Optional[str] = None) -> None:
    """
    Set up logging configuration.
    
    Args:
        log_level: The logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_file: Optional file path to write logs to
    """
    numeric_level = getattr(logging, log_level.upper(), None)
    if not isinstance(numeric_level, int):
        raise ValueError(f"Invalid log level: {log_level}")

    log_format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    logging.basicConfig(level=numeric_level, format=log_format)
    
    # Create logger
    logger = logging.getLogger("reddit_scraper")
    
    # Add file handler if log_file is specified
    if log_file:
        # Use rotating file handler to prevent logs from growing too large
        handler = logging.handlers.RotatingFileHandler(
            log_file, maxBytes=10485760, backupCount=5
        )
        handler.setFormatter(logging.Formatter(log_format))
        logger.addHandler(handler)


# Data models
@dataclass
class RedditPost:
    """
    Represents a Reddit post.
    
    Attributes:
        post_id: Unique identifier for the post
        title: Title of the post
        author: Username of the post author
        created_utc: Unix timestamp of post creation time
        score: Current score (upvotes - downvotes)
        upvote_ratio: Ratio of upvotes to total votes
        url: URL of the linked content or post itself
        permalink: Reddit permalink to the post
        num_comments: Number of comments on the post
        content: Text content of the post
        subreddit: Subreddit where the post was submitted
        scraped_at: Timestamp when the post was scraped
    """
    post_id: str
    title: str
    author: str
    created_utc: int
    score: int
    upvote_ratio: float
    url: str
    permalink: str
    num_comments: int
    content: str
    subreddit: str
    scraped_at: datetime = datetime.now()


@dataclass
class RedditComment:
    """
    Represents a Reddit comment.
    
    Attributes:
        comment_id: Unique identifier for the comment
        post_id: ID of the post this comment belongs to
        author: Username of the comment author
        created_utc: Unix timestamp of comment creation time
        score: Current score (upvotes - downvotes)
        content: Text content of the comment
        parent_id: ID of the parent comment or post
        permalink: Reddit permalink to the comment
        subreddit: Subreddit where the comment was posted
        scraped_at: Timestamp when the comment was scraped
    """
    comment_id: str
    post_id: str
    author: str
    created_utc: int
    score: int
    content: str
    parent_id: str
    permalink: str
    subreddit: str
    scraped_at: datetime = datetime.now()


# Configuration management
class ConfigManager:
    """
    Handles configuration loading and validation.
    
    This class manages the application configuration, providing defaults
    and loading user-defined settings from a configuration file.
    """
    
    def __init__(self, config_file: str):
        """
        Initialize with a config file path.
        
        Args:
            config_file: Path to the configuration file
        """
        self.logger = logging.getLogger("reddit_scraper.config")
        self.config_file = config_file
        self.config = configparser.ConfigParser()
        
        # Default configuration
        self.config["DEFAULT"] = {
            "user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            "request_delay": "3",
            "random_delay": "True",
            "max_retries": "3",
            "timeout": "30",
            "headless": "True",
        }
        
        self.load_config()
    
    def load_config(self) -> None:
        """Load configuration from file."""
        try:
            if os.path.exists(self.config_file):
                self.logger.info(f"Loading configuration from {self.config_file}")
                self.config.read(self.config_file)
            else:
                self.logger.warning(f"Configuration file {self.config_file} not found. Using defaults.")
                # Create the default config file
                with open(self.config_file, 'w') as f:
                    self.config.write(f)
        except Exception as e:
            self.logger.error(f"Failed to load configuration: {e}")
            raise
    
    def get(self, section: str, option: str, fallback: Any = None) -> Any:
        """Get a configuration value."""
        return self.config.get(section, option, fallback=fallback)
    
    def getboolean(self, section: str, option: str, fallback: bool = None) -> bool:
        """Get a boolean configuration value."""
        return self.config.getboolean(section, option, fallback=fallback)
    
    def getint(self, section: str, option: str, fallback: int = None) -> int:
        """Get an integer configuration value."""
        return self.config.getint(section, option, fallback=fallback)
    
    def getfloat(self, section: str, option: str, fallback: float = None) -> float:
        """Get a float configuration value."""
        return self.config.getfloat(section, option, fallback=fallback)


# Browser Management
class BrowserManager:
    """
    Manages the Selenium browser instance.
    
    This class handles browser initialization, navigation, and interaction
    with web elements, implementing best practices for web scraping.
    """
    
    def __init__(self, config_manager: ConfigManager):
        """
        Initialize with a configuration manager.
        
        Args:
            config_manager: Configuration manager instance
        """
        self.logger = logging.getLogger("reddit_scraper.browser")
        self.config = config_manager
        self.driver = None
    
    def init_browser(self) -> None:
        """Initialize the browser with appropriate settings."""
        self.logger.info("Initializing browser")
        try:
            chrome_options = Options()
            
            # Set headless mode if configured
            if self.config.getboolean("DEFAULT", "headless", fallback=True):
                self.logger.debug("Running in headless mode")
                chrome_options.add_argument("--headless")
            
            # Add additional options for stability and performance
            chrome_options.add_argument("--no-sandbox")
            chrome_options.add_argument("--disable-dev-shm-usage")
            chrome_options.add_argument("--disable-gpu")
            chrome_options.add_argument("--disable-extensions")
            chrome_options.add_argument("--disable-popup-blocking")
            
            # Set user agent
            user_agent = self.config.get("DEFAULT", "user_agent")
            chrome_options.add_argument(f"--user-agent={user_agent}")
            
            # Install and set up ChromeDriver
            service = Service(ChromeDriverManager().install())
            
            # Initialize the Chrome driver
            self.driver = webdriver.Chrome(service=service, options=chrome_options)
            
            # Set page load timeout
            timeout = self.config.getint("DEFAULT", "timeout", fallback=30)
            self.driver.set_page_load_timeout(timeout)
            
            self.logger.info("Browser initialized successfully")
        except Exception as e:
            self.logger.error(f"Failed to initialize browser: {e}")
            raise
    
    def navigate_to(self, url: str) -> None:
        """
        Navigate to a URL.
        
        Args:
            url: The URL to navigate to
        """
        if not self.driver:
            self.init_browser()
        
        self.logger.info(f"Navigating to {url}")
        try:
            self.driver.get(url)
            self._add_delay()
        except Exception as e:
            self.logger.error(f"Failed to navigate to {url}: {e}")
            raise
    
    def get_page_source(self) -> str:
        """
        Get the current page source.
        
        Returns:
            The HTML source of the current page
        """
        if not self.driver:
            self.logger.error("Browser not initialized")
            raise RuntimeError("Browser not initialized")
        
        return self.driver.page_source
    
    def wait_for_element(self, by: str, value: str, timeout: int = None) -> Any:
        """
        Wait for an element to be present on the page.
        
        Args:
            by: The method to locate the element (ID, CSS_SELECTOR, etc.)
            value: The value to search for
            timeout: Maximum time to wait in seconds
            
        Returns:
            The web element when found
        """
        if not self.driver:
            self.logger.error("Browser not initialized")
            raise RuntimeError("Browser not initialized")
        
        if timeout is None:
            timeout = self.config.getint("DEFAULT", "timeout", fallback=30)
        
        self.logger.debug(f"Waiting for element {by}={value} with timeout {timeout}s")
        try:
            element = WebDriverWait(self.driver, timeout).until(
                EC.presence_of_element_located((getattr(By, by.upper()), value))
            )
            return element
        except Exception as e:
            self.logger.error(f"Failed to wait for element {by}={value}: {e}")
            raise
    
    def scroll_to_bottom(self, scroll_pause_time: float = None) -> None:
        """
        Scroll to the bottom of the page to load dynamic content.
        
        Args:
            scroll_pause_time: Time to pause between scrolls in seconds
        """
        if not self.driver:
            self.logger.error("Browser not initialized")
            raise RuntimeError("Browser not initialized")
        
        if scroll_pause_time is None:
            scroll_pause_time = self.config.getfloat("DEFAULT", "request_delay", fallback=3)
        
        self.logger.debug("Scrolling to bottom of page")
        try:
            # Get scroll height
            last_height = self.driver.execute_script("return document.body.scrollHeight")
            
            while True:
                # Scroll down to bottom
                self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                
                # Wait to load page
                time.sleep(scroll_pause_time)
                
                # Calculate new scroll height and compare with last scroll height
                new_height = self.driver.execute_script("return document.body.scrollHeight")
                if new_height == last_height:
                    break
                last_height = new_height
                
                self._add_delay()
        except Exception as e:
            self.logger.error(f"Failed to scroll to bottom: {e}")
            raise
    
    def _add_delay(self) -> None:
        """
        Add a delay between requests to avoid rate limiting.
        
        This implements a polite scraping approach with configurable
        delays and optional random jitter.
        """
        delay = self.config.getfloat("DEFAULT", "request_delay", fallback=3)
        random_delay = self.config.getboolean("DEFAULT", "random_delay", fallback=True)
        
        if random_delay:
            # Add random jitter to delay (between 0.5x and 1.5x of the configured delay)
            delay = delay * (0.5 + random.random())
        
        self.logger.debug(f"Adding delay of {delay:.2f}s")
        time.sleep(delay)
    
    def close(self) -> None:
        """Close the browser and release resources."""
        if self.driver:
            self.logger.info("Closing browser")
            self.driver.quit()
            self.driver = None


# Parser for Reddit content
class RedditParser:
    """
    Parses Reddit HTML content using BeautifulSoup.
    
    This class extracts structured data from Reddit's HTML, transforming
    it into Python objects.
    """
    
    def __init__(self):
        """Initialize the parser."""
        self.logger = logging.getLogger("reddit_scraper.parser")
    
    def parse_subreddit_page(self, html_content: str, subreddit: str) -> List[RedditPost]:
        """
        Parse a subreddit page and extract posts.
        
        Args:
            html_content: HTML content of the subreddit page
            subreddit: Name of the subreddit
            
        Returns:
            List of RedditPost objects
        """
        self.logger.info(f"Parsing subreddit page for r/{subreddit}")
        posts = []
        
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            post_elements = soup.find_all('div', class_='thing')
            
            for post_element in post_elements:
                try:
                    post_id = post_element.get('id', '').replace('thing_', '')
                    title_element = post_element.find('a', class_='title')
                    title = title_element.text.strip() if title_element else ''
                    
                    author_element = post_element.find('a', class_='author')
                    author = author_element.text.strip() if author_element else '[deleted]'
                    
                    permalink = post_element.find('a', class_='permalink')
                    permalink_url = permalink.get('href') if permalink else ''
                    
                    score_element = post_element.find('div', class_='score')
                    score_text = score_element.get('title', '0') if score_element else '0'
                    score = int(score_text) if score_text.isdigit() else 0
                    
                    time_element = post_element.find('time')
                    created_utc = int(time_element.get('datetime', '0')) if time_element else 0
                    
                    comments_element = post_element.find('a', class_='comments')
                    comments_text = comments_element.text.split()[0] if comments_element else '0'
                    num_comments = int(comments_text) if comments_text.isdigit() else 0
                    
                    url_element = post_element.find('a', class_='title')
                    url = url_element.get('href', '') if url_element else ''
                    
                    content = ''
                    upvote_ratio = 0.0
                    
                    post = RedditPost(
                        post_id=post_id,
                        title=title,
                        author=author,
                        created_utc=created_utc,
                        score=score,
                        upvote_ratio=upvote_ratio,
                        url=url,
                        permalink=permalink_url,
                        num_comments=num_comments,
                        content=content,
                        subreddit=subreddit
                    )
                    
                    posts.append(post)
                except Exception as e:
                    self.logger.warning(f"Failed to parse post: {e}")
            
            self.logger.info(f"Parsed {len(posts)} posts from r/{subreddit}")
            return posts
        except Exception as e:
            self.logger.error(f"Failed to parse subreddit page: {e}")
            return []
    
    def parse_post_page(self, html_content: str, post_id: str) -> Tuple[Optional[RedditPost], List[RedditComment]]:
        """
        Parse a post page and extract the post content and comments.
        
        Args:
            html_content: HTML content of the post page
            post_id: ID of the post
            
        Returns:
            Tuple containing the post and a list of comments
        """
        self.logger.info(f"Parsing post page for {post_id}")
        post = None
        comments = []
        
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Parse post content
            post_element = soup.find('div', id=f"thing_{post_id}")
            if post_element:
                try:
                    title_element = post_element.find('a', class_='title')
                    title = title_element.text.strip() if title_element else ''
                    
                    author_element = post_element.find('a', class_='author')
                    author = author_element.text.strip() if author_element else '[deleted]'
                    
                    permalink = post_element.find('a', class_='permalink')
                    permalink_url = permalink.get('href') if permalink else ''
                    
                    score_element = post_element.find('div', class_='score')
                    score_text = score_element.get('title', '0') if score_element else '0'
                    score = int(score_text) if score_text.isdigit() else 0
                    
                    time_element = post_element.find('time')
                    created_utc = int(time_element.get('datetime', '0')) if time_element else 0
                    
                    upvote_element = post_element.find('div', class_='upvoted')
                    upvote_text = upvote_element.text.strip().replace('%', '') if upvote_element else '0'
                    upvote_ratio = float(upvote_text) / 100 if upvote_text.isdigit() else 0.0
                    
                    url_element = post_element.find('a', class_='title')
                    url = url_element.get('href', '') if url_element else ''
                    
                    content_element = post_element.find('div', class_='usertext-body')
                    content = content_element.text.strip() if content_element else ''
                    
                    subreddit_element = post_element.find('a', class_='subreddit')
                    subreddit = subreddit_element.text.strip().replace('r/', '') if subreddit_element else ''
                    
                    comments_element = post_element.find('a', class_='comments')
                    comments_text = comments_element.text.split()[0] if comments_element else '0'
                    num_comments = int(comments_text) if comments_text.isdigit() else 0
                    
                    post = RedditPost(
                        post_id=post_id,
                        title=title,
                        author=author,
                        created_utc=created_utc,
                        score=score,
                        upvote_ratio=upvote_ratio,
                        url=url,
                        permalink=permalink_url,
                        num_comments=num_comments,
                        content=content,
                        subreddit=subreddit
                    )
                except Exception as e:
                    self.logger.warning(f"Failed to parse post content: {e}")
            
            # Parse comments
            comment_elements = soup.find_all('div', class_='comment')
            for comment_element in comment_elements:
                try:
                    comment_id = comment_element.get('id', '').replace('thing_', '')
                    
                    author_element = comment_element.find('a', class_='author')
                    author = author_element.text.strip() if author_element else '[deleted]'
                    
                    permalink = comment_element.find('a', class_='permalink')
                    permalink_url = permalink.get('href') if permalink else ''
                    
                    score_element = comment_element.find('span', class_='score')
                    score_text = score_element.text.split()[0] if score_element else '0'
                    score = int(score_text) if score_text.isdigit() else 0
                    
                    time_element = comment_element.find('time')
                    created_utc = int(time_element.get('datetime', '0')) if time_element else 0
                    
                    content_element = comment_element.find('div', class_='usertext-body')
                    content = content_element.text.strip() if content_element else ''
                    
                    parent_id_element = comment_element.find('a', class_='bylink')
                    parent_id = parent_id_element.get('href', '').split('/')[-2] if parent_id_element else ''
                    
                    subreddit_element = comment_element.find('a', class_='subreddit')
                    subreddit = subreddit_element.text.strip().replace('r/', '') if subreddit_element else ''
                    
                    comment = RedditComment(
                        comment_id=comment_id,
                        post_id=post_id,
                        author=author,
                        created_utc=created_utc,
                        score=score,
                        content=content,
                        parent_id=parent_id,
                        permalink=permalink_url,
                        subreddit=subreddit
                    )
                    
                    comments.append(comment)
                except Exception as e:
                    self.logger.warning(f"Failed to parse comment: {e}")
            
            self.logger.info(f"Parsed post {post_id} with {len(comments)} comments")
            return post, comments
        except Exception as e:
            self.logger.error(f"Failed to parse post page: {e}")
            return None, []


# Data storage
class DataStorage:
    """
    Handles storage of scraped data.
    
    This class provides methods to save scraped data in different formats,
    such as CSV and JSON.
    """
    
    def __init__(self, output_dir: str):
        """
        Initialize with an output directory.
        
        Args:
            output_dir: Directory where scraped data will be saved
        """
        self.logger = logging.getLogger("reddit_scraper.storage")
        self.output_dir = output_dir
        
        # Create output directory if it doesn't exist
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
            self.logger.info(f"Created output directory {output_dir}")
    
    def save_posts_to_csv(self, posts: List[RedditPost], filename: str = None) -> str:
        """
        Save posts to a CSV file.
        
        Args:
            posts: List of RedditPost objects to save
            filename: Optional filename to use
            
        Returns:
            Path to the saved file
        """
        if not posts:
            self.logger.warning("No posts to save")
            return ""
        
        if filename is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"reddit_posts_{timestamp}.csv"
        
        file_path = os.path.join(self.output_dir, filename)
        self.logger.info(f"Saving {len(posts)} posts to {file_path}")
        
        try:
            with open(file_path, 'w', newline='', encoding='utf-8') as f:
                fieldnames = posts[0].__dict__.keys()
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                for post in posts:
                    # Convert datetime objects to strings
                    post_dict = asdict(post)
                    if isinstance(post_dict['scraped_at'], datetime):
                        post_dict['scraped_at'] = post_dict['scraped_at'].isoformat()
                    writer.writerow(post_dict)
            
            self.logger.info(f"Saved {len(posts)} posts to {file_path}")
            return file_path
        except Exception as e:
            self.logger.error(f"Failed to save posts to CSV: {e}")
            return ""
    
    def save_comments_to_csv(self, comments: List[RedditComment], filename: str = None) -> str:
        """
        Save comments to a CSV file.
        
        Args:
            comments: List of RedditComment objects to save
            filename: Optional filename to use
            
        Returns:
            Path to the saved file
        """
        if not comments:
            self.logger.warning("No comments to save")
            return ""
        
        if filename is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"reddit_comments_{timestamp}.csv"
        
        file_path = os.path.join(self.output_dir, filename)
        self.logger.info(f"Saving {len(comments)} comments to {file_path}")
        
        try:
            with open(file_path, 'w', newline='', encoding='utf-8') as f:
                fieldnames = comments[0].__dict__.keys()
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                for comment in comments:
                    # Convert datetime objects to strings
                    comment_dict = asdict(comment)
                    if isinstance(comment_dict['scraped_at'], datetime):
                        comment_dict['scraped_at'] = comment_dict['scraped_at'].isoformat()
                    writer.writerow(comment_dict)
            
            self.logger.info(f"Saved {len(comments)} comments to {file_path}")
            return file_path
        except Exception as e:
            self.logger.error(f"Failed to save comments to CSV: {e}")
            return ""
    
    def save_to_json(self, data: List[Union[RedditPost, RedditComment]], filename: str = None) -> str:
        """
        Save data to a JSON file.
        
        Args:
            data: List of data objects to save
            filename: Optional filename to use
            
        Returns:
            Path to the saved file
        """
        if not data:
            self.logger.warning("No data to save")
            return ""
        
        if filename is None:
            data_type = "posts" if isinstance(data[0], RedditPost) else "comments"
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"reddit_{data_type}_{timestamp}.json"
        
        file_path = os.path.join(self.output_dir, filename)
        self.logger.info(f"Saving {len(data)} items to {file_path}")
        
        try:
            with open(file_path, 'w', encoding='utf-8') as f:
                json_data = []
                for item in data:
                    item_dict = asdict(item)
                    # Convert datetime objects to strings
                    if isinstance(item_dict['scraped_at'], datetime):
                        item_dict['scraped_at'] = item_dict['scraped_at'].isoformat()
                    json_data.append(item_dict)
                
                json.dump(json_data, f, indent=2, ensure_ascii=False)
            
            self.logger.info(f"Saved {len(data)} items to {file_path}")
            return file_path
        except Exception as e:
            self.logger.error(f"Failed to save data to JSON: {e}")
            return ""


# Main Scraper class
class RedditScraper:
    """
    Main class for scraping Reddit.
    
    This class orchestrates the scraping process, using the browser manager,
    parser, and data storage components.
    """
    
    def __init__(self, config_file: str = "config.ini", output_dir: str = "output"):
        """
        Initialize the scraper with configuration.
        
        Args:
            config_file: Path to the configuration file
            output_dir: Directory to save output data
        """
        # Set up logging
        setup_logging()
        self.logger = logging.getLogger("reddit_scraper")
        
        # Create configuration manager
        self.config = ConfigManager(config_file)
        
        # Create browser manager
        self.browser = BrowserManager(self.config)
        
        # Create parser
        self.parser = RedditParser()
        
        # Create storage
        self.storage = DataStorage(output_dir)
        
        # Check if robots.txt allows scraping
        self._check_robots_txt()
    
    def _check_robots_txt(self) -> None:
        """Check if robots.txt allows scraping."""
        self.logger.info("Checking robots.txt")
        try:
            response = requests.get("https://www.reddit.com/robots.txt")
            if response.status_code == 200:
                # Simplified check - in a real-world scenario, you would want to use a proper
                # robots.txt parser and check for specific user agents and paths
                if "Disallow: /r/" in response.text:
                    self.logger.warning("robots.txt may disallow scraping subreddits. Proceed with caution.")
                else:
                    self.logger.info("No explicit disallow for subreddits found in robots.txt.")
            else:
                self.logger.warning(f"Failed to fetch robots.txt: {response.status_code}")
        except Exception as e:
            self.logger.error(f"Error checking robots.txt: {e}")
    
    def scrape_subreddit(self, subreddit: str, limit: int = 10, sort_by: str = "hot") -> List[RedditPost]:
        """
        Scrape posts from a subreddit.
        
        Args:
            subreddit: Name of the subreddit to scrape
            limit: Maximum number of posts to scrape
            sort_by: Sorting method ('hot', 'new', 'top', etc.)
            
        Returns:
            List of RedditPost objects
        """
        self.logger.info(f"Scraping subreddit r/{subreddit} ({sort_by}, limit {limit})")
        
        # Validate inputs
        if not re.match(r'^[a-zA-Z0-9_]+$', subreddit):
            self.logger.error(f"Invalid subreddit name: {subreddit}")
            raise ValueError(f"Invalid subreddit name: {subreddit}")
        
        if sort_by not in ["hot", "new", "top", "rising", "controversial"]:
            self.logger.error(f"Invalid sort_by value: {sort_by}")
            raise ValueError(f"Invalid sort_by value: {sort_by}")
        
        url = f"https://www.reddit.com/r/{subreddit}/{sort_by}/.json?limit={limit}"
        
        try:
            # Use requests instead of Selenium for the initial request to get JSON directly
            headers = {
                'User-Agent': self.config.get("DEFAULT", "user_agent")
            }
            
            response = requests.get(url, headers=headers)
            
            if response.status_code != 200:
                self.logger.error(f"Failed to fetch subreddit: {response.status_code}")
                raise RuntimeError(f"Failed to fetch subreddit: {response.status_code}")
            
            data = response.json()
            posts = []
            
            for post_data in data.get('data', {}).get('children', []):
                post_data = post_data.get('data', {})
                
                post = RedditPost(
                    post_id=post_data.get('id', ''),
                    title=post_data.get('title', ''),
                    author=post_data.get('author', '[deleted]'),
                    created_utc=post_data.get('created_utc', 0),
                    score=post_data.get('score', 0),
                    upvote_ratio=post_data.get('upvote_ratio', 0.0),
                    url=post_data.get('url', ''),
                    permalink=post_data.get('permalink', ''),
                    num_comments=post_data.get('num_comments', 0),
                    content=post_data.get('selftext', ''),
                    subreddit=post_data.get('subreddit', subreddit)
                )
                
                posts.append(post)
            
            self.logger.info(f"Scraped {len(posts)} posts from r/{subreddit}")
            return posts
        except Exception as e:
            self.logger.error(f"Failed to scrape subreddit: {e}")
            raise
    
    def scrape_post_comments(self, post_id: str, subreddit: str, limit: int = 100) -> List[RedditComment]:
        """
        Scrape comments from a post.
        
        Args:
            post_id: ID of the post to scrape comments from
            subreddit: Name of the subreddit containing the post
            limit: Maximum number of comments to scrape
            
        Returns:
            List of RedditComment objects
        """
        self.logger.info(f"Scraping comments for post {post_id} in r/{subreddit} (limit {limit})")
        
        url = f"https://www.reddit.com/r/{subreddit}/comments/{post_id}/.json?limit={limit}"
        
        try:
            # Use requests to get JSON directly
            headers = {
                'User-Agent': self.config.get("DEFAULT", "user_agent")
            }
            
            response = requests.get(url, headers=headers)
            
            if response.status_code != 200:
                self.logger.error(f"Failed to fetch post: {response.status_code}")
                raise RuntimeError(f"Failed to fetch post: {response.status_code}")
            
            data = response.json()
            comments = []
            
            # Extract comments from the JSON response
            if len(data) >= 2:
                comment_data = data[1].get('data', {}).get('children', [])
                
                def extract_comments(items, parent_id=None):
                    """Recursively extract comments."""
                    result = []
                    
                    for item in items:
                        item_data = item.get('data', {})
                        
                        # Skip deleted comments and non-comments
                        if item.get('kind') != 't1':
                            continue
                        
                        comment = RedditComment(
                            comment_id=item_data.get('id', ''),
                            post_id=post_id,
                            author=item_data.get('author', '[deleted]'),
                            created_utc=item_data.get('created_utc', 0),
                            score=item_data.get('score', 0),
                            content=item_data.get('body', ''),
                            parent_id=parent_id or post_id,
                            permalink=item_data.get('permalink', ''),
                            subreddit=item_data.get('subreddit', subreddit)
                        )
                        
                        result.append(comment)
                        
                        # Extract replies
                        replies = item_data.get('replies', {})
                        if isinstance(replies, dict) and 'data' in replies:
                            children = replies.get('data', {}).get('children', [])
                            result.extend(extract_comments(children, item_data.get('id')))
                    
                    return result
                
                comments = extract_comments(comment_data)
            
            self.logger.info(f"Scraped {len(comments)} comments from post {post_id}")
            return comments
        except Exception as e:
            self.logger.error(f"Failed to scrape post comments: {e}")
            raise
    
    def scrape_user(self, username: str, limit: int = 50) -> Dict[str, List[Union[RedditPost, RedditComment]]]:
        """
        Scrape a user's posts and comments.
        
        Args:
            username: Reddit username to scrape
            limit: Maximum number of items to scrape
            
        Returns:
            Dictionary containing lists of posts and comments
        """
        self.logger.info(f"Scraping user u/{username} (limit {limit})")
        
        # Validate input
        if not re.match(r'^[a-zA-Z0-9_-]+$', username):
            self.logger.error(f"Invalid username: {username}")
            raise ValueError(f"Invalid username: {username}")
        
        result = {
            'posts': [],
            'comments': []
        }
        
        try:
            # Scrape posts
            posts_url = f"https://www.reddit.com/user/{username}/submitted/.json?limit={limit}"
            
            headers = {
                'User-Agent': self.config.get("DEFAULT", "user_agent")
            }
            
            posts_response = requests.get(posts_url, headers=headers)
            
            if posts_response.status_code == 200:
                posts_data = posts_response.json()
                
                for post_data in posts_data.get('data', {}).get('children', []):
                    post_data = post_data.get('data', {})
                    
                    post = RedditPost(
                        post_id=post_data.get('id', ''),
                        title=post_data.get('title', ''),
                        author=post_data.get('author', username),
                        created_utc=post_data.get('created_utc', 0),
                        score=post_data.get('score', 0),
                        upvote_ratio=post_data.get('upvote_ratio', 0.0),
                        url=post_data.get('url', ''),
                        permalink=post_data.get('permalink', ''),
                        num_comments=post_data.get('num_comments', 0),
                        content=post_data.get('selftext', ''),
                        subreddit=post_data.get('subreddit', '')
                    )
                    
                    result['posts'].append(post)
            
            # Add delay between requests
            time.sleep(self.config.getfloat("DEFAULT", "request_delay", fallback=3))
            
            # Scrape comments
            comments_url = f"https://www.reddit.com/user/{username}/comments/.json?limit={limit}"
            
            comments_response = requests.get(comments_url, headers=headers)
            
            if comments_response.status_code == 200:
                comments_data = comments_response.json()
                
                for comment_data in comments_data.get('data', {}).get('children', []):
                    comment_data = comment_data.get('data', {})
                    
                    comment = RedditComment(
                        comment_id=comment_data.get('id', ''),
                        post_id=comment_data.get('link_id', '').replace('t3_', ''),
                        author=comment_data.get('author', username),
                        created_utc=comment_data.get('created_utc', 0),
                        score=comment_data.get('score', 0),
                        content=comment_data.get('body', ''),
                        parent_id=comment_data.get('parent_id', '').replace('t1_', '').replace('t3_', ''),
                        permalink=comment_data.get('permalink', ''),
                        subreddit=comment_data.get('subreddit', '')
                    )
                    
                    result['comments'].append(comment)
            
            self.logger.info(f"Scraped {len(result['posts'])} posts and {len(result['comments'])} comments from user {username}")
            return result
        except Exception as e:
            self.logger.error(f"Failed to scrape user: {e}")
            raise
    
    def save_data(self, data, format: str = "csv") -> Dict[str, str]:
        """
        Save scraped data to files.
        
        Args:
            data: Data to save (list or dictionary)
            format: Format to save in ('csv' or 'json')
            
        Returns:
            Dictionary mapping data types to file paths
        """
        self.logger.info(f"Saving data in {format} format")
        
        result = {}
        
        try:
            if isinstance(data, list):
                if data and isinstance(data[0], RedditPost):
                    if format == "csv":
                        result['posts'] = self.storage.save_posts_to_csv(data)
                    elif format == "json":
                        result['posts'] = self.storage.save_to_json(data)
                elif data and isinstance(data[0], RedditComment):
                    if format == "csv":
                        result['comments'] = self.storage.save_comments_to_csv(data)
                    elif format == "json":
                        result['comments'] = self.storage.save_to_json(data)
            elif isinstance(data, dict):
                posts = data.get('posts', [])
                comments = data.get('comments', [])
                
                if posts:
                    if format == "csv":
                        result['posts'] = self.storage.save_posts_to_csv(posts)
                    elif format == "json":
                        result['posts'] = self.storage.save_to_json(posts)
                
                if comments:
                    if format == "csv":
                        result['comments'] = self.storage.save_comments_to_csv(comments)
                    elif format == "json":
                        result['comments'] = self.storage.save_to_json(comments)
            
            return result
        except Exception as e:
            self.logger.error(f"Failed to save data: {e}")
            raise
    
    def close(self) -> None:
        """Clean up resources."""
        self.logger.info("Closing Reddit scraper")
        if self.browser:
            self.browser.close()


# Command-line interface
def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(description="Reddit Scraper - Scrape data from Reddit")
    
    # Add subparsers for different scraping modes
    subparsers = parser.add_subparsers(dest="mode", help="Scraping mode")
    
    # Subreddit scraper
    subreddit_parser = subparsers.add_parser("subreddit", help="Scrape posts from a subreddit")
    subreddit_parser.add_argument("subreddit", help="Subreddit name (without r/)")
    subreddit_parser.add_argument("--limit", type=int, default=10, help="Maximum number of posts to scrape")
    subreddit_parser.add_argument("--sort", choices=["hot", "new", "top", "rising", "controversial"], default="hot", help="Sort order")
    
    # Post scraper
    post_parser = subparsers.add_parser("post", help="Scrape a post and its comments")
    post_parser.add_argument("post_id", help="Post ID")
    post_parser.add_argument("subreddit", help="Subreddit name (without r/)")
    post_parser.add_argument("--limit", type=int, default=100, help="Maximum number of comments to scrape")
    
    # User scraper
    user_parser = subparsers.add_parser("user", help="Scrape a user's posts and comments")
    user_parser.add_argument("username", help="Username (without u/)")
    user_parser.add_argument("--limit", type=int, default=50, help="Maximum number of items to scrape")
    
    # Common arguments
    parser.add_argument("--config", default="config.ini", help="Path to configuration file")
    parser.add_argument("--output-dir", default="output", help="Output directory for scraped data")
    parser.add_argument("--format", choices=["csv", "json"], default="csv", help="Output format")
    parser.add_argument("--log-level", choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"], default="INFO", help="Logging level")
    parser.add_argument("--log-file", help="Log file path")
    
    return parser.parse_args()


def main() -> None:
    """Main entry point for the script."""
    args = parse_args()
    
    # Set up logging
    setup_logging(args.log_level, args.log_file)
    logger = logging.getLogger("reddit_scraper")
    
    logger.info("Starting Reddit scraper")
    
    # Create scraper
    scraper = RedditScraper(args.config, args.output_dir)
    
    try:
        if args.mode == "subreddit":
            # Scrape subreddit
            posts = scraper.scrape_subreddit(args.subreddit, args.limit, args.sort)
            scraper.save_data(posts, args.format)
        elif args.mode == "post":
            # Scrape post comments
            comments = scraper.scrape_post_comments(args.post_id, args.subreddit, args.limit)
            scraper.save_data(comments, args.format)
        elif args.mode == "user":
            # Scrape user
            user_data = scraper.scrape_user(args.username, args.limit)
            scraper.save_data(user_data, args.format)
        else:
            logger.error("No mode specified. Use --help for usage information.")
            sys.exit(1)
    except Exception as e:
        logger.error(f"Error: {e}")
        sys.exit(1)
    finally:
        scraper.close()
    
    logger.info("Reddit scraper finished successfully")


if __name__ == "__main__":
    main()