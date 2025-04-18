"""
Application-wide constants for Reddit Explorer.

This module defines constants that are used throughout the application
to avoid magic numbers and strings, improving maintainability.
"""

from enum import Enum, auto
from typing import Dict, List, Set, Tuple

# File and data constants
DEFAULT_POST_LIMIT = 100
DEFAULT_COMMENT_LIMIT = None
DEFAULT_CHUNK_SIZE = 5
PARQUET_COMPRESSION = "zstd"

# Image constants
IMAGE_QUALITY = 80
IMAGE_FORMAT = "AVIF"
MAX_IMAGE_PIXELS = 25000000  # Limit to prevent decompression bombs

# Valid image extensions for URL detection
VALID_IMAGE_EXTENSIONS: Tuple[str, ...] = (
    ".jpg",
    ".jpeg",
    ".png",
    ".webp",
)

# HTTP constants
HTTP_RETRY_TOTAL = 3
HTTP_RETRY_BACKOFF_FACTOR = 1.0
HTTP_RETRY_STATUS_FORCELIST = [429, 500, 502, 503, 504]
DEFAULT_USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"

# Web app constants
POSTS_PER_PAGE = 5
DEFAULT_PORT = 8000

# API endpoints
PULLPUSH_BASE_URL = "https://api.pullpush.io/reddit/search"
PULLPUSH_SUBMISSION_ENDPOINT = f"{PULLPUSH_BASE_URL}/submission/"
PULLPUSH_COMMENT_ENDPOINT = f"{PULLPUSH_BASE_URL}/comment/"


class ScraperMethod(str, Enum):
    """Enumeration of supported scraper methods."""

    PRAW = "praw"
    PULLPUSH = "pullpush"
    BROWSER = "browser"


class ContentType(str, Enum):
    """Enumeration of supported content types."""

    POST = "post"
    COMMENT = "comment"

class RedditSort(str, Enum):
    """Enumeration for Reddit post sorting methods"""
    NEW = "new"
    HOT = "hot"
    TOP = "top"
    RISING = "rising"
    CONTROVERSIAL = "controversial"

class TopTimeFilter(str, Enum):
    """Enumeration for 'top' posts time filters."""
    HOUR = "hour"
    DAY = "day"
    WEEK = "week"
    MONTH = "month"
    YEAR = "year"
    ALL = "all"



# Schema definitions for Polars DataFrames
import polars as pl

POST_SCHEMA: Dict[str, pl.DataType] = {
    "id": pl.String,
    "title": pl.String,
    "text": pl.String,
    "created_utc": pl.Int64,
    "created_time": pl.String,
    "image_url": pl.String,
    "image_path": pl.String,
}

COMMENT_SCHEMA: Dict[str, pl.DataType] = {
    "id": pl.String,
    "post_id": pl.String,
    "parent_id": pl.String,
    "text": pl.String,
    "created_utc": pl.Int64,
    "created_time": pl.String,
    "image_url": pl.String,
    "image_path": pl.String,
}

# Default fields to fetch from PullPush API
PULLPUSH_POST_FIELDS = ["id", "title", "selftext", "created_utc", "url"]
PULLPUSH_COMMENT_FIELDS = ["id", "link_id", "parent_id", "body", "created_utc"]