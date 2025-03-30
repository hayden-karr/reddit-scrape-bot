"""
Custom exceptions for Reddit Scraper.

This module defines application-specific exceptions that provide
clear error messages and help with error handling throughout the app.
"""

from typing import Any, Dict, Optional


class RedditScraperError(Exception):
    """Base exception for all Reddit Scraper errors."""

    def __init__(self, message: str, details: Optional[Dict[str, Any]] = None):
        """
        Initialize the exception with a message and optional details.
        
        Args:
            message: Human-readable error message
            details: Additional contextual information about the error
        """
        self.message = message
        self.details = details or {}
        super().__init__(self.message)

    def __str__(self) -> str:
        """Return a string representation of the error."""
        if self.details:
            return f"{self.message} - {self.details}"
        return self.message


class ConfigurationError(RedditScraperError):
    """Raised when there's an issue with the application configuration."""
    pass


class ScraperError(RedditScraperError):
    """Base exception for scraper-related errors."""
    pass


class APIError(ScraperError):
    """Raised when an API request fails."""
    
    def __init__(
        self, 
        message: str, 
        status_code: Optional[int] = None, 
        response_text: Optional[str] = None, 
        details: Optional[Dict[str, Any]] = None
    ):
        """
        Initialize with API-specific error information.
        
        Args:
            message: Human-readable error message
            status_code: HTTP status code if available
            response_text: Response text if available
            details: Additional contextual information
        """
        self.status_code = status_code
        self.response_text = response_text
        super().__init__(message, details)


class PRAWError(ScraperError):
    """Raised when a PRAW-specific error occurs."""
    pass


class PullPushError(ScraperError):
    """Raised when a PullPush-specific error occurs."""
    pass


class StorageError(RedditScraperError):
    """Raised when a data storage operation fails."""
    pass


class ImageError(RedditScraperError):
    """Raised when an image operation fails."""
    pass


class ValidationError(RedditScraperError):
    """Raised when input validation fails."""
    pass


class ResourceNotFoundError(RedditScraperError):
    """Raised when a requested resource is not found."""
    
    def __init__(self, resource_type: str, identifier: str, details: Optional[Dict[str, Any]] = None):
        """
        Initialize with resource-specific information.
        
        Args:
            resource_type: Type of resource that wasn't found (e.g., "subreddit", "post")
            identifier: Identifier of the resource
            details: Additional contextual information
        """
        message = f"{resource_type.capitalize()} not found: {identifier}"
        super().__init__(message, details)
        self.resource_type = resource_type
        self.identifier = identifier