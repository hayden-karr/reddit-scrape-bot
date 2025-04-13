"""
Flask application package for Reddit Scraper.

Provides an alternative lightweight web interface for viewing Reddit data.
"""

import os
import sys
import logging
from typing import Optional
from pathlib import Path

from reddit_scraper.config import get_config
from reddit_scraper.utils.logging import get_logger

logger = get_logger(__name__)

def run_flask_app(
    host: Optional[str] = None,
    port: Optional[int] = None,
    debug: bool = False,
    subreddit: Optional[str] = None,
    chunk_size: Optional[int] = None
) -> None:
    """
    Run the Flask web application.

    Args:
        host: Host to bind the server to
        port: Port to bind the server to
        debug: Whether to run in debug mode
        subreddit: Subreddit to view (overrides config/env)
        chunk_size: Number of posts per chunk (overrides config/env)
    """
    try:
        # Get configuration
        config = get_config()

        # Use provided values or fall back to config
        run_host = host or config.web.host
        run_port = port or config.web.port
        run_debug = debug or config.web.debug
        subreddit = subreddit or os.getenv("SUBREDDIT_NAME")
        chunk_size = chunk_size or int(os.getenv("CHUNK_SIZE", 5))

        logger.info(f"Starting Flask web server at http://{run_host}:{run_port}/ (subreddit={subreddit}, chunk_size={chunk_size})")

        # Import the Flask app factory
        from .reddit_flask import create_app

        app = create_app(subreddit, chunk_size)
        app.run(
            debug=run_debug,
            host=run_host,
            port=run_port
        )

    except Exception as e:
        logger.error(f"Error starting Flask application: {e}", exc_info=True)
        sys.exit(1)