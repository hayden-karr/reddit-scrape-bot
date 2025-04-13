"""
Reddit Viewer Django Views

This module defines the Django views for the Reddit Viewer application.
"""

import logging
from typing import Dict, Any

from django.shortcuts import render
from django.conf import settings
from django.http import HttpRequest, HttpResponse, FileResponse
from django.views.decorators.http import require_GET
from django.views.decorators.cache import cache_control

from reddit_scraper.web.reddit_viewer.services.data_manager import (
    RedditDataManager, 
    DataManagerException,
    DataNotFoundException
)

# Set up logging
logger = logging.getLogger(__name__)


@require_GET
def index(request: HttpRequest) -> HttpResponse:
    """
    Render the main index page.
    
    Args:
        request: The HTTP request
        
    Returns:
        Rendered index page
    """
    try:
        context = {
            'subreddit': settings.SUBREDDIT_NAME,
            'chunk_size': settings.CHUNK_SIZE,
            'debug': settings.DEBUG,
        }
        logger.debug(f"Rendering index with context: {context}")
        return render(request, 'reddit_viewer/index.html', context)
    except Exception as e:
        logger.error(f"Error rendering index: {str(e)}", exc_info=True)
        return render(request, 'reddit_viewer/error.html', {
            'error': 'An error occurred while rendering the page',
            'detail': str(e) if settings.DEBUG else None
        }, status=500)


@require_GET
@cache_control(max_age=3600)  # Cache images for 1 hour
def serve_image(request: HttpRequest, filename: str) -> HttpResponse:
    """
    Serve an image file.
    
    Args:
        request: The HTTP request
        filename: Name of the image file to serve
        
    Returns:
        File response with the image
        
    Raises:
        Http404: If the image is not found
    """
    try:
        # Initialize data manager
        data_manager = RedditDataManager(settings.SUBREDDIT_NAME)
        
        # Get image path
        image_path = data_manager.get_image_path(filename)
        
        # Return file response
        return FileResponse(open(image_path, 'rb'), content_type='image/jpeg')
    except DataNotFoundException:
        logger.warning(f"Image not found: {filename}")
        return render(request, 'reddit_viewer/error.html', {
            'error': 'Image not found',
            'detail': f'The requested image "{filename}" could not be found'
        }, status=404)
    except Exception as e:
        logger.error(f"Error serving image {filename}: {str(e)}", exc_info=True)
        return render(request, 'reddit_viewer/error.html', {
            'error': 'Error serving image',
            'detail': str(e) if settings.DEBUG else None
        }, status=500)