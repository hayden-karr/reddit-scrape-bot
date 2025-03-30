"""
Configuration management for Reddit Scraper.

This module handles loading configuration from environment variables, 
configuration files, and provides defaults when needed.
"""

import os
from functools import lru_cache
from pathlib import Path
from typing import Dict, Optional, Union

from dotenv import load_dotenv
from pydantic import BaseModel, Field, field_validator

# Load environment variables from .env file
load_dotenv()


class RedditAPIConfig(BaseModel):
    """Configuration for Reddit API access."""

    client_id: str = Field(..., description="Reddit API client ID")
    client_secret: str = Field(..., description="Reddit API client secret")
    user_agent: str = Field(..., description="Reddit API user agent")

class StorageConfig(BaseModel):
    """Configuration for data storage."""

    base_dir: Path = Field(
        default=Path("data"), description="Base directory for storing scraped data"
    )
    use_compression: bool = Field(default=True, description="Use compression for storage")
    compression_method: str = Field(
        default="zstd", description="Compression method for Parquet files"
    )

    @field_validator("base_dir", pre=True)
    def validate_base_dir(cls, v):
        """Convert string to Path and ensure directory exists."""
        if isinstance(v, str):
            path = Path(v)
        else:
            path = v
        
        # Create directory if it doesn't exist
        path.mkdir(parents=True, exist_ok=True)
        return path


class WebConfig(BaseModel):
    """Configuration for the web application."""

    host: str = Field(default="127.0.0.1", description="Host to run the web server on")
    port: int = Field(default=8000, description="Port to run the web server on")
    debug: bool = Field(default=False, description="Run in debug mode")
    secret_key: str = Field(
        default_factory=lambda: os.urandom(24).hex(),
        description="Secret key for Flask sessions",
    )


class LoggingConfig(BaseModel):
    """Configuration for logging."""

    level: str = Field(
        default="INFO", description="Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)"
    )
    format: str = Field(
        default="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>",
        description="Logging format string",
    )
    save_to_file: bool = Field(
        default=True, description="Whether to save logs to file"
    )
    log_file: Optional[Path] = Field(
        default=Path("logs/reddit_explorer.log"), description="Path to log file"
    )
    rotation: str = Field(
        default="500 MB", description="Log rotation size"
    )
    retention: str = Field(
        default="10 days", description="Log retention period"
    )

    @field_validator("log_file", pre=True)
    def validate_log_file(cls, v, values):
        """Ensure log file directory exists if logging to file."""
        if values.get("save_to_file", True) and v is not None:
            if isinstance(v, str):
                path = Path(v)
            else:
                path = v
            path.parent.mkdir(parents=True, exist_ok=True)
            return path
        return v


class AppConfig(BaseModel):
    """Main application configuration."""

    reddit_api: RedditAPIConfig
    storage: StorageConfig = Field(default_factory=StorageConfig)
    web: WebConfig = Field(default_factory=WebConfig)
    logging: LoggingConfig = Field(default_factory=LoggingConfig)


@lru_cache()
def get_config() -> AppConfig:
    """
    Load and return the application configuration.
    
    Uses environment variables and default values.
    Results are cached for performance.
    """
    return AppConfig(
        reddit_api=RedditAPIConfig(
            client_id=os.getenv("REDDIT_CLIENT_ID", ""),
            client_secret=os.getenv("REDDIT_CLIENT_SECRET", ""),
            user_agent=os.getenv("REDDIT_USER_AGENT", "RedditScraper"),
        ),
        storage=StorageConfig(
            base_dir=Path(os.getenv("STORAGE_BASE_DIR", "data")),
            use_compression=os.getenv("STORAGE_USE_COMPRESSION", "true").lower() == "true",
            compression_method=os.getenv("STORAGE_COMPRESSION_METHOD", "zstd"),
        ),
        web=WebConfig(
            host=os.getenv("WEB_HOST", "127.0.0.1"),
            port=int(os.getenv("WEB_PORT", "8000")),
            debug=os.getenv("WEB_DEBUG", "false").lower() == "true",
            secret_key=os.getenv("WEB_SECRET_KEY", os.urandom(24).hex()),
        ),
        logging=LoggingConfig(
            level=os.getenv("LOG_LEVEL", "INFO"),
            save_to_file=os.getenv("LOG_SAVE_TO_FILE", "true").lower() == "true",
            log_file=Path(os.getenv("LOG_FILE", "logs/reddit_scraper.log")),
        ),
    )


def get_subreddit_dir(subreddit_name: str) -> Path:
    """Get the directory for a specific subreddit's data."""
    config = get_config()
    return config.storage.base_dir / f"reddit_data_{subreddit_name}"


def get_image_dir(subreddit_name: str) -> Path:
    """Get the directory for a specific subreddit's images."""
    subreddit_dir = get_subreddit_dir(subreddit_name)
    image_dir = subreddit_dir / f"images_{subreddit_name}"
    image_dir.mkdir(parents=True, exist_ok=True)
    return image_dir


def get_posts_file(subreddit_name: str) -> Path:
    """Get the path to the posts file for a specific subreddit."""
    subreddit_dir = get_subreddit_dir(subreddit_name)
    return subreddit_dir / f"reddit_posts_{subreddit_name}.parquet"


def get_comments_file(subreddit_name: str) -> Path:
    """Get the path to the comments file for a specific subreddit."""
    subreddit_dir = get_subreddit_dir(subreddit_name)
    return subreddit_dir / f"reddit_comments_{subreddit_name}.parquet"