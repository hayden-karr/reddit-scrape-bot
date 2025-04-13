"""
Reddit Viewer API Dependencies

This module defines FastAPI dependencies that can be injected into the API endpoints.
"""

import logging
from typing import Annotated
from pathlib import Path

from fastapi import Depends, HTTPException, status
from django.conf import settings

from reddit_scraper.web.reddit_viewer.services.data_manager import (
    RedditDataManager,
    DataManagerException,
    DataNotFoundException
)

# Set up logging
logger = logging.getLogger(__name__)


def get_data_manager(subreddit_name: str = settings.SUBREDDIT_NAME) -> RedditDataManager:
    """
    Dependency to get a configured RedditDataManager instance.
    
    Args:
        subreddit_name: Name of the subreddit to manage, defaults to settings value
        
    Returns:
        Configured RedditDataManager instance
        
    Raises:
        HTTPException: If there's an error initializing the data manager
    """
    try:
        from reddit_scraper.config import get_config
        base_dir = get_config().storage.base_dir
        manager = RedditDataManager(subreddit_name, base_dir=base_dir)
        return manager
    except DataManagerException as e:
        logger.error(f"Error initializing data manager: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, 
            detail=f"Failed to initialize data manager: {str(e)}"
        )


# Create an annotated dependency for type hints
DataManagerDep = Annotated[RedditDataManager, Depends(get_data_manager)]