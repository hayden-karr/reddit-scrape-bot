"""
Scraper factory for creating and managing scraper instances.

This module implements the factory pattern for creating different scraper
implementations based on the chosen method.
"""

from typing import Dict, Type

from reddit_scraper.constants import ScraperMethod
from reddit_scraper.scrapers.base import BaseScraper
from reddit_scraper.scrapers.praw_scraper import PRAWScraper
from reddit_scraper.scrapers.reddit_scrape_pullpush import PullPushScraper
from reddit_scraper.scrapers.reddit_scrape_seleniumbs4 import SeleniumScraper
from reddit_scraper.utils.logging import get_logger

logger = get_logger(__name__)

# Registry of scraper classes
_scraper_registry: Dict[str, Type[BaseScraper]] = {
    ScraperMethod.PRAW: PRAWScraper,
    ScraperMethod.PULLPUSH: PullPushScraper,
    ScraperMethod.BROWSER: SeleniumScraper,
}


def create_scraper(method: str, subreddit: str, image_service=None) -> BaseScraper:
    """
    Create a scraper instance for the specified method and subreddit.
    
    Args:
        method: Scraping method to use
        subreddit: Name of the subreddit to scrape
        image_service: Optional image service to pass to the scraper (if supported)
        
    Returns:
        Initialized scraper instance
        
    Raises:
        ValueError: If the specified method is not supported
    """
    if method not in _scraper_registry:
        raise ValueError(f"Unsupported scraper method: {method}")
    
    scraper_class = _scraper_registry[method]
    logger.info(f"Creating {scraper_class.__name__} for r/{subreddit}")
    return scraper_class(subreddit, image_service=image_service)


def get_available_scrapers() -> Dict[str, str]:
    """
    Get a dictionary of available scraper methods.
    
    Returns:
        Dictionary mapping scraper method constants to their names
    """
    return {method: scraper_class.get_name() 
            for method, scraper_class in _scraper_registry.items()}