"""
Validation functions for Reddit Scraper.

This module provides reusable validation functions for various inputs
and data structures used throughout the application.
"""

import re
import os
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Union
from urllib.parse import urlparse

from reddit_scraper.constants import VALID_IMAGE_EXTENSIONS
from reddit_scraper.exceptions import ValidationError
from reddit_scraper.utils.logging import get_logger

logger = get_logger(__name__)


def validate_subreddit_name(subreddit: str) -> bool:
    """
    Validate that a subreddit name follows Reddit's naming rules.
    
    Args:
        subreddit: Name of the subreddit to validate
        
    Returns:
        True if the name is valid, False otherwise
        
    Notes:
        Reddit subreddit names:
        - Can only contain letters, numbers, and underscores
        - Must be between 3 and 21 characters
        - Cannot start with a number
    """
    if not subreddit:
        return False
        
    # Remove 'r/' prefix if present
    if subreddit.startswith('r/'):
        subreddit = subreddit[2:]
    
    # Check length
    if len(subreddit) < 3 or len(subreddit) > 21:
        return False
    
    # Check if it starts with a number
    if subreddit[0].isdigit():
        return False
    
    # Check if it contains only allowed characters
    return bool(re.match(r'^[a-zA-Z0-9_]+$', subreddit))


def sanitize_subreddit_name(subreddit: str) -> str:
    """
    Sanitize a subreddit name by removing prefix and invalid characters.
    
    Args:
        subreddit: Name of the subreddit to sanitize
        
    Returns:
        Sanitized subreddit name
        
    Raises:
        ValidationError: If the subreddit name is invalid after sanitization
    """
    # Remove 'r/' prefix if present
    if subreddit.startswith('r/'):
        subreddit = subreddit[2:]
    
    # Remove any whitespace
    subreddit = subreddit.strip()
    
    # Replace invalid characters with underscores
    sanitized = re.sub(r'[^a-zA-Z0-9_]', '_', subreddit)
    
    # Ensure it doesn't start with a number
    if sanitized and sanitized[0].isdigit():
        sanitized = f"_{sanitized}"
    
    # Check final length
    if len(sanitized) < 3:
        raise ValidationError(f"Subreddit name '{subreddit}' is too short after sanitization")
    
    if len(sanitized) > 21:
        sanitized = sanitized[:21]
    
    logger.debug(f"Sanitized subreddit name from '{subreddit}' to '{sanitized}'")
    return sanitized


def validate_date_string(date_str: str) -> bool:
    """
    Validate that a string is a valid date in YYYY-MM-DD format.
    
    Args:
        date_str: Date string to validate
        
    Returns:
        True if the date is valid, False otherwise
    """
    if not date_str:
        return False
        
    try:
        datetime.strptime(date_str, "%Y-%m-%d")
        return True
    except ValueError:
        return False


def validate_url(url: str) -> bool:
    """
    Validate that a string is a valid URL.
    
    Args:
        url: URL to validate
        
    Returns:
        True if the URL is valid, False otherwise
    """
    if not url:
        return False
        
    try:
        result = urlparse(url)
        return all([result.scheme, result.netloc])
    except Exception:
        return False


def is_image_url(url: str) -> bool:
    """
    Check if a URL points to an image.
    
    Args:
        url: URL to check
        
    Returns:
        True if the URL points to an image, False otherwise
    """
    if not validate_url(url):
        return False
        
    try:
        parsed_url = urlparse(url)
        path = parsed_url.path.lower()
        
        # Check file extension
        return any(path.endswith(ext) for ext in VALID_IMAGE_EXTENSIONS)
    except Exception:
        return False


def validate_file_path(path: Union[str, Path]) -> bool:
    """
    Validate that a path exists and is a file.
    
    Args:
        path: Path to validate
        
    Returns:
        True if the path exists and is a file, False otherwise
    """
    try:
        if isinstance(path, str):
            path = Path(path)
        return path.exists() and path.is_file()
    except Exception:
        return False


def validate_directory_path(path: Union[str, Path]) -> bool:
    """
    Validate that a path exists and is a directory.
    
    Args:
        path: Path to validate
        
    Returns:
        True if the path exists and is a directory, False otherwise
    """
    try:
        if isinstance(path, str):
            path = Path(path)
        return path.exists() and path.is_dir()
    except Exception:
        return False


def validate_writable_directory(path: Union[str, Path]) -> bool:
    """
    Validate that a directory exists and is writable.
    
    Args:
        path: Path to validate
        
    Returns:
        True if the directory exists and is writable, False otherwise
    """
    try:
        if isinstance(path, str):
            path = Path(path)
        
        # Check if directory exists
        if not path.exists():
            path.mkdir(parents=True, exist_ok=True)
        
        # Check if it's a directory
        if not path.is_dir():
            return False
        
        # Check if it's writable by creating and removing a test file
        test_file = path / ".write_test"
        try:
            test_file.touch()
            test_file.unlink()
            return True
        except (PermissionError, OSError):
            return False
    except Exception:
        return False


def validate_api_response(response: Dict[str, Any], required_fields: List[str] = None) -> bool:
    """
    Validate that an API response contains the required fields.
    
    Args:
        response: API response to validate
        required_fields: List of fields that must be present in the response
        
    Returns:
        True if the response is valid, False otherwise
    """
    if required_fields is None:
        return True
        
    return all(field in response for field in required_fields)


def validate_post_data(post_data: Dict[str, Any]) -> bool:
    """
    Validate that post data contains the minimum required fields.
    
    Args:
        post_data: Post data to validate
        
    Returns:
        True if the post data is valid, False otherwise
    """
    required_fields = ["id", "title"]
    return validate_api_response(post_data, required_fields)


def validate_comment_data(comment_data: Dict[str, Any]) -> bool:
    """
    Validate that comment data contains the minimum required fields.
    
    Args:
        comment_data: Comment data to validate
        
    Returns:
        True if the comment data is valid, False otherwise
    """
    required_fields = ["id", "link_id"]
    return validate_api_response(comment_data, required_fields)