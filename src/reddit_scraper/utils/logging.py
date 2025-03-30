"""
Logging configuration for Reddit Scaper.

This module configures the application's logging system using Loguru,
providing structured logs with consistent formatting and file rotation.
"""

import logging
import sys
from typing import Dict, Optional, Union

from loguru import logger

from reddit_scraper.config import get_config


# Remove default handler
logger.remove()


def configure_logging() -> None:
    """Configure the application's logging system."""
    config = get_config()
    log_config = config.logging

    # Add console handler
    logger.add(
        sys.stderr,
        level=log_config.level,
        format=log_config.format,
        colorize=True,
    )

    # Add file handler if enabled
    if log_config.save_to_file and log_config.log_file:
        logger.add(
            str(log_config.log_file),
            level=log_config.level,
            format=log_config.format,
            rotation=log_config.rotation,
            retention=log_config.retention,
            compression="zip",
        )

    # Intercept standard library logging
    class InterceptHandler(logging.Handler):
        def emit(self, record):
            # Get corresponding Loguru level if it exists
            try:
                level = logger.level(record.levelname).name
            except ValueError:
                level = record.levelno

            # Find caller from where originated the logged message
            frame, depth = logging.currentframe(), 2
            while frame.f_code.co_filename == logging.__file__:
                frame = frame.f_back
                depth += 1

            logger.opt(depth=depth, exception=record.exc_info).log(
                level, record.getMessage()
            )

    # Intercept stdlib logging
    logging.basicConfig(handlers=[InterceptHandler()], level=0, force=True)

    # Update logging level for third-party libraries
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("requests").setLevel(logging.WARNING)
    logging.getLogger("praw").setLevel(logging.INFO)

    logger.info("Logging system configured successfully")


def get_logger(name: Optional[str] = None) -> logger.__class__:
    """
    Get a logger instance with the specified name.
    
    This provides a consistent way to get a logger throughout the application,
    ensuring all logs use the same format and configuration.
    
    Args:
        name: Optional name for the logger (typically the module name)
        
    Returns:
        Configured logger instance
    """
    return logger.bind(name=name)


class RequestsLogger:
    """
    Logger for HTTP requests using the requests library.
    
    This class can be used with the hooks parameter in requests to log
    request and response details at the appropriate log levels.
    """

    def __init__(self, name: Optional[str] = None):
        """Initialize with an optional logger name."""
        self.logger = get_logger(name or "requests")

    def log_request(self, request, **kwargs) -> None:
        """Log a request before it's sent."""
        self.logger.debug(
            f"Request: {request.method} {request.url}",
            headers=dict(request.headers),
            body=request.body if hasattr(request, "body") else None,
        )

    def log_response(self, response, **kwargs) -> None:
        """Log a response after it's received."""
        self.logger.debug(
            f"Response: {response.status_code} {response.reason}",
            url=response.url,
            headers=dict(response.headers),
            elapsed=response.elapsed.total_seconds(),
        )

    def get_hooks(self) -> Dict[str, list]:
        """
        Get hooks for the requests library.
        
        Usage:
            session.request(method, url, ..., hooks=logger.get_hooks())
        """
        return {
            "request": [self.log_request],
            "response": [self.log_response],
        }