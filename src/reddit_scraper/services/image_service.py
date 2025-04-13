"""
Image service for handling image operations.

This module provides functionality for extracting image URLs from text,
downloading images, and managing image storage.
"""

import re
from io import BytesIO
from pathlib import Path
from typing import Optional
from urllib.parse import urlparse, parse_qs

from PIL import Image
import requests

# Ensure AVIF support is registered with Pillow if pillow-avif-plugin is installed
try:
    import pillow_avif  # noqa: F401
except ImportError:
    pass

from reddit_scraper.config import get_image_dir
from reddit_scraper.constants import (
    ContentType, 
    IMAGE_FORMAT,
    IMAGE_QUALITY,
    MAX_IMAGE_PIXELS,
    VALID_IMAGE_EXTENSIONS
)
from reddit_scraper.utils.http import create_retry_session, get_user_agent
from reddit_scraper.utils.logging import get_logger

logger = get_logger(__name__)


class ImageService:
    """
    Service for handling image-related operations.
    
    This class handles extracting image URLs from text content,
    downloading images, and saving them to appropriate locations.
    """
    
    def __init__(self, subreddit: str):
        """
        Initialize the image service for a specific subreddit.
        
        Args:
            subreddit: Name of the subreddit to handle images for
        """
        self.subreddit = subreddit
        
        # Create an HTTP session for image downloads with retry capabilities
        self.session = create_retry_session()
        
        # Get the image directory for this subreddit
        self.image_dir = get_image_dir(subreddit)
        
        # Configure image handling
        Image.MAX_IMAGE_PIXELS = MAX_IMAGE_PIXELS
        
        logger.debug(f"ImageService initialized for r/{subreddit}")
    
    def extract_image_url(self, text: str) -> Optional[str]:
        """
        Extract an image URL from text content, robustly handling Reddit and Imgur links,
        query parameters, and malformed URLs.
        """
        image_extensions = (".jpg", ".jpeg", ".png", ".webp")

        # Regex pattern for URLs
        url_pattern = r'https?://[^\s)"]+'
        urls = re.findall(url_pattern, text)

        for url in urls:
            try:
                parsed_url = urlparse(url)
                path = parsed_url.path.lower()

                # Check if the URL has a valid image extension
                if path.endswith(image_extensions):
                    return url

                # Handle Reddit's image links with query parameters (e.g., ?format=pjpg)
                query_params = parse_qs(parsed_url.query)
                if "format" in query_params and query_params["format"][0] in ["jpg", "jpeg", "png", "webp"]:
                    return url

                # Handle Reddit-hosted images (e.g., https://i.redd.it/abc123.jpg, https://preview.redd.it/...)
                if "i.redd.it" in parsed_url.netloc or "preview.redd.it" in parsed_url.netloc or "i.imgur.com" in parsed_url.netloc:
                    return url

            except ValueError as e:
                logger.debug(f"Skipping invalid URL: {url} due to error: {e}")
                continue  # Continue with the next URL

        return None  # No valid image found
    
    def download_image(
        self,
        image_url: Optional[str],
        item_id: str,
        content_type: ContentType
    ) -> Optional[str]:
        """
        Download an image and save it to the appropriate location.
        
        Args:
            image_url: URL of the image to download
            item_id: ID of the post or comment
            content_type: Whether this is a post or comment image
            
        Returns:
            Path to the saved image, or None if no image was downloaded
        """
        if not image_url:
            return None
        
        # Create the image filename with appropriate prefix based on content type
        prefix = "comment_" if content_type == ContentType.COMMENT else ""
        image_path = self.image_dir / f"{prefix}{item_id}.{IMAGE_FORMAT.lower()}"
        
        # Skip if already downloaded
        if image_path.exists():
            logger.debug(f"Image already exists at {image_path}")
            return str(image_path)
        
        try:
            # First try without special headers
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
            # Process and save the image as AVIF if possible
            image = Image.open(BytesIO(response.content))
            avif_exts = [ext for ext, fmt in Image.registered_extensions().items() if fmt.lower() == "avif"]
            can_save_avif = "avif" in Image.registered_extensions().values() or "AVIF" in getattr(Image, "SAVE", {})
            if IMAGE_FORMAT.lower() == "avif":
                try:
                    # Try Pillow native AVIF support
                    image.save(image_path, "AVIF", quality=IMAGE_QUALITY)
                    logger.debug(f"Downloaded image to {image_path} (Pillow AVIF)")
                    return str(image_path)
                except Exception as pil_avif_exc:
                    # Try imageio as a fallback
                    try:
                        import imageio.v3 as iio
                        iio.imwrite(str(image_path), image)
                        logger.debug(f"Downloaded image to {image_path} (imageio AVIF)")
                        return str(image_path)
                    except Exception as imageio_exc:
                        logger.warning(f"AVIF not supported by Pillow or imageio. Pillow error: {pil_avif_exc}, imageio error: {imageio_exc}")
                        return None
            else:
                # Save in the requested format (e.g., JPEG, PNG)
                image.save(image_path, IMAGE_FORMAT, quality=IMAGE_QUALITY)
                logger.debug(f"Downloaded image to {image_path}")
                return str(image_path)
        except Exception as e:
            logger.warning(f"Error processing image: {e}")
            return None