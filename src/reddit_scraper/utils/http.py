"""
HTTP utilities for Reddit Scraper.

This module provides enhanced HTTP functionality with retry capabilities,
proper error handling, and consistent logging.
"""

from typing import Any, Dict, Optional, Union

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from reddit_scraper.constants import (
    DEFAULT_USER_AGENT,
    HTTP_RETRY_BACKOFF_FACTOR,
    HTTP_RETRY_STATUS_FORCELIST,
    HTTP_RETRY_TOTAL,
)
from reddit_scraper.exceptions import APIError
from reddit_scraper.utils.logging import RequestsLogger, get_logger

logger = get_logger(__name__)


def create_retry_session(
    retries: int = HTTP_RETRY_TOTAL,
    backoff_factor: float = HTTP_RETRY_BACKOFF_FACTOR,
    status_forcelist: Optional[list] = None,
    session: Optional[requests.Session] = None,
) -> requests.Session:
    """
    Create a requests Session with retry capabilities.
    
    Args:
        retries: Maximum number of retries
        backoff_factor: Backoff factor for retry delay calculation
        status_forcelist: List of HTTP status codes to retry on
        session: Existing session to configure (creates new one if None)
        
    Returns:
        Configured requests Session with retry capabilities
    """
    if status_forcelist is None:
        status_forcelist = HTTP_RETRY_STATUS_FORCELIST

    retry = Retry(
        total=retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
        allowed_methods=["GET", "POST", "HEAD"],  # Method whitelist
    )

    session = session or requests.Session()
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    
    # Setup logging
    requests_logger = RequestsLogger()
    session.hooks = requests_logger.get_hooks()

    return session


def get_user_agent(custom_agent: Optional[str] = None) -> str:
    """
    Get a user agent string to use for requests.
    
    Using a proper user agent helps avoid rate limits and blocking.
    
    Args:
        custom_agent: Optional custom user agent string
        
    Returns:
        User agent string
    """
    return custom_agent or DEFAULT_USER_AGENT


def handle_request_errors(func):
    """
    Decorator to handle request errors consistently.
    
    Catches requests-related exceptions and transforms them into
    application-specific exceptions with helpful context.
    """
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except requests.exceptions.HTTPError as e:
            status_code = e.response.status_code
            response_text = e.response.text
            url = e.response.url
            logger.error(f"HTTP error {status_code} for URL {url}")
            raise APIError(
                message=f"HTTP error: {e}",
                status_code=status_code,
                response_text=response_text,
                details={"url": url},
            )
        except requests.exceptions.ConnectionError as e:
            logger.error(f"Connection error: {e}")
            raise APIError(message=f"Connection error: {e}")
        except requests.exceptions.Timeout as e:
            logger.error(f"Request timed out: {e}")
            raise APIError(message=f"Request timed out: {e}")
        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed: {e}")
            raise APIError(message=f"Request failed: {e}")
    return wrapper


class APIClient:
    """
    Generic API client with error handling and retry capabilities.
    
    This class provides a foundation for API-specific clients,
    handling common concerns like retries, headers, and error parsing.
    """

    def __init__(
        self,
        base_url: str,
        session: Optional[requests.Session] = None,
        user_agent: Optional[str] = None,
        headers: Optional[Dict[str, str]] = None,
    ):
        """
        Initialize the API client.
        
        Args:
            base_url: Base URL for API requests
            session: Optional pre-configured session
            user_agent: Optional custom user agent
            headers: Optional additional headers
        """
        self.base_url = base_url.rstrip("/")
        self.session = create_retry_session(session=session)
        self.user_agent = get_user_agent(user_agent)
        
        # Set up default headers
        self.headers = {"User-Agent": self.user_agent}
        if headers:
            self.headers.update(headers)
        
        self.logger = get_logger(self.__class__.__name__)

    @handle_request_errors
    def request(
        self,
        method: str,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        data: Optional[Dict[str, Any]] = None,
        json: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
        timeout: int = 30,
    ) -> requests.Response:
        """
        Make an HTTP request to the API.
        
        Args:
            method: HTTP method (GET, POST, etc.)
            endpoint: API endpoint (will be appended to base_url)
            params: Optional query parameters
            data: Optional form data
            json: Optional JSON body
            headers: Optional additional headers
            timeout: Request timeout in seconds
            
        Returns:
            requests.Response object
        """
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        
        # Merge default headers with request-specific headers
        request_headers = self.headers.copy()
        if headers:
            request_headers.update(headers)
        
        self.logger.debug(
            f"Making {method} request to {url}",
            params=params,
            headers=request_headers,
        )
        
        response = self.session.request(
            method=method,
            url=url,
            params=params,
            data=data,
            json=json,
            headers=request_headers,
            timeout=timeout,
        )
        
        # Raise HTTPError for bad status codes
        response.raise_for_status()
        
        return response

    def get(self, endpoint: str, **kwargs) -> requests.Response:
        """Make a GET request to the API."""
        return self.request("GET", endpoint, **kwargs)

    def post(self, endpoint: str, **kwargs) -> requests.Response:
        """Make a POST request to the API."""
        return self.request("POST", endpoint, **kwargs)

    def put(self, endpoint: str, **kwargs) -> requests.Response:
        """Make a PUT request to the API."""
        return self.request("PUT", endpoint, **kwargs)

    def delete(self, endpoint: str, **kwargs) -> requests.Response:
        """Make a DELETE request to the API."""
        return self.request("DELETE", endpoint, **kwargs)