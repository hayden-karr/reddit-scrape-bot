"""
Database functionality for Reddit Scraper.

This module provides a lightweight SQLite database interface for storing
metadata and search indices. The main content is stored in Parquet files,
but this database can be used for quick lookups and tracking scrape history.
"""

import sqlite3
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Union, Any, Iterator

from reddit_scraper.config import get_config
from reddit_scraper.utils.logging import get_logger

logger = get_logger(__name__)


class RedditDatabase:
    """
    SQLite database manager for Reddit Scraper.
    
    This class provides methods for storing and retrieving metadata about
    scraping operations, including timestamps, post counts, and search indices.
    """
    
    def __init__(self, db_path: Optional[Path] = None):
        """
        Initialize the database connection.
        
        Args:
            db_path: Path to the SQLite database file.
                     If None, uses the default path in config.
        """
        config = get_config()
        self.db_path = db_path or (config.storage.base_dir / "reddit_scraper.db")
        
        # Ensure parent directory exists
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Initialize the database
        self._initialize_db()
        
        logger.debug(f"Database initialized at {self.db_path}")
    
    @contextmanager
    def _get_connection(self) -> Iterator[sqlite3.Connection]:
        """
        Get a database connection as a context manager.
        
        Yields:
            SQLite connection object
        """
        conn = sqlite3.connect(str(self.db_path))
        conn.row_factory = sqlite3.Row  # Return rows as dict-like objects
        try:
            yield conn
        finally:
            conn.close()
    
    def _initialize_db(self) -> None:
        """Initialize the database schema if it doesn't exist."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            
            # Create scrape_history table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS scrape_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    subreddit TEXT NOT NULL,
                    method TEXT NOT NULL,
                    posts_count INTEGER,
                    comments_count INTEGER,
                    images_count INTEGER,
                    start_time TIMESTAMP,
                    end_time TIMESTAMP,
                    success BOOLEAN
                )
            """)
            
            # Create subreddit_metadata table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS subreddit_metadata (
                    subreddit TEXT PRIMARY KEY,
                    last_scrape TIMESTAMP,
                    total_posts INTEGER,
                    total_comments INTEGER,
                    data_path TEXT
                )
            """)
            
            conn.commit()
    
    def record_scrape(
        self,
        subreddit: str,
        method: str,
        posts_count: int,
        comments_count: int,
        images_count: int,
        start_time: datetime,
        end_time: Optional[datetime] = None,
        success: bool = True
    ) -> int:
        """
        Record a scraping operation in the database.
        
        Args:
            subreddit: Name of the subreddit
            method: Scraping method used
            posts_count: Number of posts scraped
            comments_count: Number of comments scraped
            images_count: Number of images downloaded
            start_time: When scraping started
            end_time: When scraping finished (None if still in progress)
            success: Whether the scrape was successful
            
        Returns:
            ID of the created record
        """
        with self._get_connection() as conn:
            cursor = conn.cursor()
            
            # Insert the record
            cursor.execute("""
                INSERT INTO scrape_history (
                    subreddit, method, posts_count, comments_count, 
                    images_count, start_time, end_time, success
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                subreddit, method, posts_count, comments_count,
                images_count, start_time, end_time, success
            ))
            
            # Update the subreddit metadata
            cursor.execute("""
                INSERT INTO subreddit_metadata (
                    subreddit, last_scrape, total_posts, total_comments, data_path
                ) VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(subreddit) DO UPDATE SET
                    last_scrape = excluded.last_scrape,
                    total_posts = excluded.total_posts,
                    total_comments = excluded.total_comments,
                    data_path = excluded.data_path
            """, (
                subreddit, 
                end_time or start_time,
                posts_count,
                comments_count,
                str(get_config().storage.base_dir / f"reddit_data_{subreddit}")
            ))
            
            conn.commit()
            return cursor.lastrowid
    
    def get_scrape_history(self, subreddit: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Get history of scraping operations.
        
        Args:
            subreddit: Optional subreddit to filter by
            
        Returns:
            List of scrape history records
        """
        with self._get_connection() as conn:
            cursor = conn.cursor()
            
            if subreddit:
                cursor.execute("""
                    SELECT * FROM scrape_history
                    WHERE subreddit = ?
                    ORDER BY start_time DESC
                """, (subreddit,))
            else:
                cursor.execute("""
                    SELECT * FROM scrape_history
                    ORDER BY start_time DESC
                """)
            
            # Convert rows to dictionaries
            return [dict(row) for row in cursor.fetchall()]
    
    def get_subreddit_metadata(self, subreddit: str) -> Optional[Dict[str, Any]]:
        """
        Get metadata for a specific subreddit.
        
        Args:
            subreddit: Name of the subreddit
            
        Returns:
            Dictionary of metadata or None if not found
        """
        with self._get_connection() as conn:
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT * FROM subreddit_metadata
                WHERE subreddit = ?
            """, (subreddit,))
            
            row = cursor.fetchone()
            return dict(row) if row else None
    
    def list_subreddits(self) -> List[str]:
        """
        Get list of all subreddits in the database.
        
        Returns:
            List of subreddit names
        """
        with self._get_connection() as conn:
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT subreddit FROM subreddit_metadata
                ORDER BY subreddit
            """)
            
            return [row[0] for row in cursor.fetchall()]


# Create a singleton instance
_db_instance = None

def get_database() -> RedditDatabase:
    """
    Get the database instance.
    
    Returns:
        Singleton RedditDatabase instance
    """
    global _db_instance
    if _db_instance is None:
        _db_instance = RedditDatabase()
    return _db_instance