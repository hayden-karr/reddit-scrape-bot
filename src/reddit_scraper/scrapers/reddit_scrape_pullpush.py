#!/usr/bin/env python3
"""
Reddit Data Collector - Efficiently collects posts and comments from subreddits using the PullPush API.

This script fetches Reddit posts and comments from the PullPush API with proper pagination
to collect all available data beyond the default 100-item limit. It downloads associated
images and stores all data in Parquet files for efficient access and analysis.

Usage:
    python reddit_data_collector.py --subreddit SUBREDDIT [--before YYYY-MM-DD] [--after YYYY-MM-DD] [--all] [--no-images] [--batch-size SIZE]
"""

import argparse
import logging
from datetime import datetime, timezone
from io import BytesIO
from pathlib import Path
import re
from typing import Dict, List, Optional, Union, Set

import polars as pl
import requests
from PIL import Image
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


class RedditDataCollector:
    """Class for collecting Reddit data from PullPush API with proper pagination support."""

    # API endpoints
    BASE_SUBMISSION_URL = "https://api.pullpush.io/reddit/search/submission/"
    BASE_COMMENT_URL = "https://api.pullpush.io/reddit/search/comment/"
    COMMENT_URL = "https://api.pullpush.io/reddit/comment/"

    # Schema definitions for data consistency
    POST_SCHEMA = {
        "post_id": pl.Utf8,
        "title": pl.Utf8,
        "text": pl.Utf8,
        "created_utc": pl.Int64,
        "created_time": pl.Utf8,
        "image_url": pl.Utf8,
        "image_path": pl.Utf8
    }
    
    COMMENT_SCHEMA = {
        "comment_id": pl.Utf8,
        "post_id": pl.Utf8,
        "parent_id": pl.Utf8,
        "text": pl.Utf8,
        #"created_utc": pl.Int64,
        #"created_time": pl.Utf8,
        "image_url": pl.Utf8,
        "image_path": pl.Utf8
    }

    def __init__(
        self,
        subreddit: str,
        before: Optional[int] = None,
        after: Optional[int] = None,
        save_images: bool = True,
        batch_size: int = 100,
        collect_all: bool = False
    ):
        """
        Initialize the Reddit data collector.
        
        Args:
            subreddit: The subreddit name to collect data from
            before: UTC timestamp to fetch data before
            after: UTC timestamp to fetch data after
            save_images: Whether to download and save images
            batch_size: Number of items to fetch per request
            collect_all: Whether to collect all posts regardless of before/after filters
        """
        self.subreddit = subreddit
        self.before = before
        self.after = after
        self.save_images = save_images
        self.batch_size = batch_size
        self.collect_all = collect_all
        
        # Set up directory structure
        self.base_dir = Path("scraped_subreddits") / f"reddit_data_{subreddit}"
        self.image_dir = self.base_dir / "images"
        self.posts_file = self.base_dir / f"reddit_posts_{subreddit}.parquet"
        self.comments_file = self.base_dir / f"reddit_comments_{subreddit}.parquet"
        
        # Create directories
        self.base_dir.mkdir(parents=True, exist_ok=True)
        if save_images:
            self.image_dir.mkdir(parents=True, exist_ok=True)
        
        # Set up logging
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(levelname)s - %(message)s",
            handlers=[
                logging.StreamHandler(),
                logging.FileHandler(self.base_dir / f"{subreddit}_collection.log"),
            ],
        )
        self.logger = logging.getLogger(__name__)
        
        # Set up HTTP session with retry mechanism
        self.session = self._create_session()

    def _create_session(self) -> requests.Session:
        """Create and configure a requests Session with retry mechanism."""
        session = requests.Session()
        retries = Retry(
            total=5,
            backoff_factor=0.3,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"],
        )
        adapter = HTTPAdapter(max_retries=retries)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        session.headers.update({
            "User-Agent": "RedditDataCollector/1.0 (Python/requests)",
            "Accept": "application/json"
        })
        return session

    def collect_all_data(self) -> None:
        """Collect all posts and their comments from the subreddit."""
        self.logger.info(f"Starting data collection for r/{self.subreddit}")
        
        # Collect all posts
        posts_df = self.collect_all_posts()
        if posts_df.is_empty():
            self.logger.info("No posts found. Exiting.")
            return
            
        # Collect all comments for each post
        self.collect_all_comments(posts_df["post_id"].to_list())
        
        self.logger.info("Data collection complete")

    def collect_all_posts(self) -> pl.DataFrame:
        """
        Collect all posts from the subreddit with proper pagination.
        
        Returns:
            DataFrame containing all collected posts
        """
        self.logger.info(f"Collecting posts from r/{self.subreddit}")
        
        all_posts = []
        total_count = 0
        
        # For collecting all posts, temporarily reset date filters
        if self.collect_all:
            self.logger.info("Collecting ALL posts (ignoring date filters initially)")
            original_before, original_after = self.before, self.after
            self.before, self.after = None, None

        # Key fix: Proper pagination implementation
        next_before = self.before
        seen_post_ids = set()
        
        while True:
            posts = self._fetch_posts(before=next_before, after=self.after)
            if not posts:
                self.logger.info("No more posts found.")
                break
                
            # Filter out any posts we've already seen (avoid duplicates)
            new_posts = [post for post in posts if post["id"] not in seen_post_ids]
            if not new_posts:
                self.logger.info("No new posts found (pagination complete).")
                break
                
            # Add new post IDs to our seen set
            seen_post_ids.update(post["id"] for post in new_posts)
            
            all_posts.extend(new_posts)
            total_count += len(new_posts)
            self.logger.info(f"Collected {len(new_posts)} posts. Total: {total_count}")
            
            # Check if we got fewer posts than batch size (might be at the end)
            if len(posts) < self.batch_size:
                self.logger.info(f"Received fewer than {self.batch_size} posts; likely reached the end.")
                break
                
            # Update pagination marker for next batch
            # Key fix: Find the oldest post timestamp for proper pagination
            try:
                oldest_timestamp = min(post["created_utc"] for post in posts)
                # Very important: subtract 1 from the timestamp to avoid getting the same post again
                next_before = oldest_timestamp - 1
                self.logger.debug(f"Next pagination marker: {next_before}")
            except (ValueError, KeyError) as e:
                self.logger.error(f"Error setting pagination marker: {e}")
                break
        
        # Restore original filters if needed
        if self.collect_all:
            self.before, self.after = original_before, original_after
            # Apply filters after collection if specified
            if self.before or self.after:
                all_posts = self._filter_posts_by_date(all_posts)

        if not all_posts:
            self.logger.warning("No posts collected.")
            return pl.DataFrame()
            
        # Process posts into DataFrame
        posts_df = self._process_posts(all_posts)
        
        # Save posts to parquet file
        self._save_to_parquet(posts_df, self.posts_file, "post_id")
        
        return posts_df
    
    def _filter_posts_by_date(self, posts: List[Dict]) -> List[Dict]:
        """
        Filter posts by date range when using --all with date filters.
        
        Args:
            posts: List of post dictionaries
            
        Returns:
            Filtered list of posts
        """
        filtered = posts
        
        if self.after:
            filtered = [p for p in filtered if p["created_utc"] >= self.after]
            
        if self.before:
            filtered = [p for p in filtered if p["created_utc"] <= self.before]
            
        self.logger.info(f"Filtered {len(posts)} posts to {len(filtered)} posts within date range")
        return filtered
    
    def _fetch_posts(self, before: Optional[int] = None, after: Optional[int] = None) -> List[Dict]:
        """
        Fetch posts from PullPush API.
        
        Args:
            before: UTC timestamp to fetch posts before
            after: UTC timestamp to fetch posts after
            
        Returns:
            List of post dictionaries
        """
        params = {
            "subreddit": self.subreddit,
            "size": self.batch_size,
            "sort": "desc",
            "fields": "id,title,selftext,created_utc,url,num_comments"
        }
        
        # Add date filters if provided
        if before is not None:
            params["before"] = before
        if after is not None:
            params["after"] = after
        
        try:
            response = self.session.get(self.BASE_SUBMISSION_URL, params=params)
            response.raise_for_status()
            data = response.json().get("data", [])
            self.logger.debug(f"API returned {len(data)} posts")
            return data
        except requests.RequestException as e:
            self.logger.error(f"Error fetching posts: {e}")
            return []

    def collect_all_comments(self, post_ids: List[str]) -> pl.DataFrame:
        """
        Collect all comments for the given post IDs.
        
        Args:
            post_ids: List of post IDs to collect comments for
        
        Returns:
            DataFrame containing all collected comments
        """
        self.logger.info(f"Collecting comments for {len(post_ids)} posts")
        
        all_comments = []
        total_count = 0
        
        # Process in batches to avoid hitting API limits
        batch_size = 10  # Process 10 posts at a time
        for i in range(0, len(post_ids), batch_size):
            batch_post_ids = post_ids[i:i+batch_size]
            self.logger.info(f"Processing posts {i+1}-{i+len(batch_post_ids)} of {len(post_ids)}")
            
            for post_id in batch_post_ids:
                comments = self._fetch_comments_for_post(post_id)
                if comments:
                    all_comments.extend(comments)
                    total_count += len(comments)
                    self.logger.info(f"Collected {len(comments)} comments for post {post_id}. Total: {total_count}")
        
        # Also collect general comments by pagination if date filters are specified
        if self.before or self.after:
            subreddit_comments = self._collect_paginated_comments()
            if subreddit_comments:
                # Filter out duplicates
                existing_ids = {c["id"] for c in all_comments}
                unique_comments = [c for c in subreddit_comments if c["id"] not in existing_ids]
                
                all_comments.extend(unique_comments)
                total_count += len(unique_comments)
                self.logger.info(f"Added {len(unique_comments)} additional comments from pagination. Total: {total_count}")
        
        if not all_comments:
            self.logger.warning("No comments collected.")
            return pl.DataFrame()
            
        # Process comments into DataFrame
        comments_df = self._process_comments(all_comments)
        
        # Save comments to parquet file
        self._save_to_parquet(comments_df, self.comments_file, "comment_id")
        
        return comments_df

    def _collect_paginated_comments(self) -> List[Dict]:
        """
        Collect all comments from the subreddit using pagination.
        
        Returns:
            List of comment dictionaries
        """
        all_comments = []
        next_before = self.before
        seen_comment_ids = set()
        total_count = 0
        
        self.logger.info("Collecting additional comments by date range")
        
        while True:
            comments = self._fetch_comments(before=next_before, after=self.after)
            if not comments:
                break
                
            # Filter out any comments we've already seen
            new_comments = [c for c in comments if c["id"] not in seen_comment_ids]
            if not new_comments:
                break
                
            # Add new comment IDs to our seen set
            seen_comment_ids.update(c["id"] for c in new_comments)
            
            all_comments.extend(new_comments)
            total_count += len(new_comments)
            self.logger.info(f"Collected {len(new_comments)} additional comments. Total: {total_count}")
            
            # Check if we got fewer comments than the batch size
            if len(comments) < self.batch_size:
                break
                
            # Update pagination marker for next batch
            try:
                oldest_timestamp = min(comment["created_utc"] for comment in comments)
                next_before = oldest_timestamp - 1
            except (ValueError, KeyError):
                break
        
        return all_comments

    def _fetch_comments(self, before: Optional[int] = None, after: Optional[int] = None) -> List[Dict]:
        """
        Fetch comments from PullPush API by date range.
        
        Args:
            before: UTC timestamp to fetch comments before
            after: UTC timestamp to fetch comments after
            
        Returns:
            List of comment dictionaries
        """
        params = {
            "subreddit": self.subreddit,
            "size": self.batch_size,
            "sort": "desc",
            "fields": "id,link_id,parent_id,body,created_utc"
        }
        
        # Add date filters if provided
        if before is not None:
            params["before"] = before
        if after is not None:
            params["after"] = after
        
        try:
            response = self.session.get(self.BASE_COMMENT_URL, params=params)
            response.raise_for_status()
            return response.json().get("data", [])
        except requests.RequestException as e:
            self.logger.error(f"Error fetching comments: {e}")
            return []

    def _fetch_comments_for_post(self, post_id: str) -> List[Dict]:
        """
        Fetch all comments for a specific post.
        
        Args:
            post_id: Reddit post ID
            
        Returns:
            List of comment dictionaries
        """
        params = {"link_id": f"t3_{post_id}", "size": self.batch_size}
        
        try:
            response = self.session.get(self.BASE_COMMENT_URL, params=params)
            response.raise_for_status()
            comments = response.json().get("data", [])
            
            # If we have more than batch_size comments, we need to paginate
            all_comments = comments.copy()
            seen_ids = {comment["id"] for comment in comments}
            
            # Continue paginating if we received a full batch
            while comments and len(comments) == self.batch_size:
                # Find oldest comment timestamp for pagination
                try:
                    oldest_timestamp = min(c["created_utc"] for c in comments)
                    
                    # Fetch next batch with updated 'before' parameter
                    params["before"] = oldest_timestamp - 1
                    response = self.session.get(self.BASE_COMMENT_URL, params=params)
                    response.raise_for_status()
                    comments = response.json().get("data", [])
                    
                    # Filter out duplicates and add new comments
                    new_comments = [c for c in comments if c["id"] not in seen_ids]
                    all_comments.extend(new_comments)
                    seen_ids.update(c["id"] for c in new_comments)
                    
                    if not new_comments:
                        break
                        
                except (ValueError, KeyError) as e:
                    self.logger.error(f"Error during comment pagination: {e}")
                    break
            
            return all_comments
            
        except requests.RequestException as e:
            self.logger.error(f"Error fetching comments for post {post_id}: {e}")
            return []

    def _process_posts(self, posts: List[Dict]) -> pl.DataFrame:
        """
        Process raw post data into a structured DataFrame.
        
        Args:
            posts: List of post dictionaries
            
        Returns:
            DataFrame with processed post data
        """
        processed_data = []
        
        for post in posts:
            # Extract image URL from post
            url = post.get("url", "")
            image_url = self._extract_image_url(url) if url else None
            
            # Download image if enabled
            image_path = None
            if self.save_images and image_url:
                image_path = self._download_image(image_url, f"post_{post['id']}")
                
            # Create record for this post
            processed_data.append({
                "post_id": post["id"],
                "title": post.get("title", ""),
                "text": post.get("selftext", ""),
                "created_utc": int(post["created_utc"]),
                "created_time": self._format_timestamp(post["created_utc"]),
                "image_url": image_url,
                "image_path": image_path
            })
        
        # Create DataFrame and apply schema
        return pl.DataFrame(processed_data, schema=self.POST_SCHEMA)

    def _process_comments(self, comments: List[Dict]) -> pl.DataFrame:
        """
        Process raw comment data into a structured DataFrame.
        
        Args:
            comments: List of comment dictionaries
            
        Returns:
            DataFrame with processed comment data
        """
        processed_data = []
        
        for comment in comments:
            # Extract potential image URL from comment text
            text = comment.get("body", "")
            image_url = self._extract_image_url(text)
            
            # Download image if enabled
            image_path = None
            if self.save_images and image_url:
                image_path = self._download_image(image_url, f"comment_{comment['id']}")
            
            # Extract post ID from link_id (removing prefix)
            post_id = comment.get("link_id", "").replace("t3_", "") if "link_id" in comment else ""
            parent_id = comment.get("parent_id", "").split("_")[-1] if "parent_id" in comment else ""
            
            # Create record for this comment
            processed_data.append({
                "comment_id": comment["id"],
                "post_id": post_id,
                "parent_id": parent_id,
                "text": text,
                #"created_utc": int(comment["created_utc"]),
                #"created_time": self._format_timestamp(comment["created_utc"]),
                "image_url": image_url,
                "image_path": image_path
            })
        
        # Create DataFrame and apply schema
        return pl.DataFrame(processed_data, schema=self.COMMENT_SCHEMA)
    
    def _extract_image_url(self, text: str) -> Optional[str]:
        """
        Extract the first valid image URL from text.
        
        Args:
            text: Text that might contain image URLs
            
        Returns:
            First valid image URL found, or None if none found
        """
        # Improved regex pattern to match common image URLs
        image_pattern = re.compile(
            r'(https?://[^\s)]+\.(?:jpg|jpeg|png|gif|webp)(?:\?[^\s)]*)?)|\
            (https?://i\.(?:imgur|redd)\.(?:it|com)/[^\s)]+)|\
            (https?://(?:preview|i)\.redd\.it/[^\s)]+)',
            re.IGNORECASE
        )
        
        # Find first match
        match = image_pattern.search(text)
        if match:
            # Return the group that matched
            return next((g for g in match.groups() if g), None)
        return None

    def _download_image(self, image_url: str, base_filename: str) -> Optional[str]:
        """
        Download and save an image from a URL.
        
        Args:
            image_url: URL of the image to download
            base_filename: Base name for the saved file
            
        Returns:
            Path to the saved image file, or None if download failed
        """
        image_path = self.image_dir / f"{base_filename}.webp"
        
        # Skip if already downloaded
        if image_path.exists():
            return str(image_path)
            
        try:
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                "Accept": "image/webp,image/jpeg,image/png,*/*"
            }
            
            response = self.session.get(image_url, headers=headers, stream=True, timeout=10)
            response.raise_for_status()
            
            # Try to open and save the image
            image = Image.open(BytesIO(response.content))
            image.save(image_path, "WEBP", quality=85)
            return str(image_path)
            
        except Exception as e:
            self.logger.warning(f"Failed to download image {image_url}: {e}")
            return None

    def _format_timestamp(self, timestamp: Union[int, float]) -> str:
        """
        Format a UTC timestamp to a human-readable datetime string.
        
        Args:
            timestamp: Unix timestamp
            
        Returns:
            Formatted datetime string
        """
        return datetime.fromtimestamp(float(timestamp), tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S')

    def _save_to_parquet(self, df: pl.DataFrame, file_path: Path, id_column: str) -> None:
        """
        Save DataFrame to Parquet file, merging with existing data if present.
        
        Args:
            df: DataFrame to save
            file_path: Path to save the file to
            id_column: Name of the ID column to use for deduplication
        """
        if df.is_empty():
            self.logger.info(f"No new data to save to {file_path}")
            return
        
        # If file exists, merge with existing data
        if file_path.exists():
            try:
                # Read existing data and concatenate with new data
                existing_df = pl.read_parquet(file_path)
                
                # Get existing IDs for faster filtering
                existing_ids = set(existing_df[id_column].to_list())
                
                # Filter out records that already exist
                new_records = df.filter(~pl.col(id_column).is_in(existing_ids))
                
                if new_records.is_empty():
                    self.logger.info(f"No new records to add to {file_path}")
                    return
                
                # Combine and save
                combined_df = pl.concat([existing_df, new_records])
                combined_df.write_parquet(file_path, compression="zstd")
                self.logger.info(f"Added {len(new_records)} new records to {file_path}")
                
            except Exception as e:
                self.logger.error(f"Error merging with existing data: {e}")
                # Save new data only as fallback
                df.write_parquet(file_path, compression="zstd")
                self.logger.warning(f"Saved {len(df)} records to {file_path} (could not merge)")
        else:
            # Save new data
            df.write_parquet(file_path, compression="zstd")
            self.logger.info(f"Saved {len(df)} records to {file_path}")


def to_utc_timestamp(date_str: Optional[str]) -> Optional[int]:
    """
    Convert a date string to a UTC timestamp.
    
    Args:
        date_str: Date string in format YYYY-MM-DD
        
    Returns:
        UTC timestamp as an integer, or None if date_str is None
    """
    if not date_str:
        return None
        
    try:
        dt = datetime.strptime(date_str, "%Y-%m-%d")
        return int(dt.replace(tzinfo=timezone.utc).timestamp())
    except ValueError as e:
        logging.error(f"Invalid date format: {e}")
        return None


def main():
    """Main entry point of the script."""
    parser = argparse.ArgumentParser(
        description="Collect Reddit data from a subreddit using the PullPush API.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument(
        "--subreddit", 
        type=str, 
        required=True, 
        help="Subreddit name to collect data from"
    )
    parser.add_argument(
        "--before", 
        type=str, 
        help="Collect data before this date (YYYY-MM-DD)"
    )
    parser.add_argument(
        "--after", 
        type=str, 
        help="Collect data after this date (YYYY-MM-DD)"
    )
    parser.add_argument(
        "--no-images", 
        action="store_true", 
        help="Skip downloading images"
    )
    parser.add_argument(
        "--batch-size", 
        type=int, 
        default=100, 
        help="Number of items to fetch per API request"
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Collect all posts and comments regardless of date filters"
    )
    
    args = parser.parse_args()
    
    # Convert date strings to timestamps
    before_timestamp = to_utc_timestamp(args.before)
    after_timestamp = to_utc_timestamp(args.after)
    
    # Create and run the collector
    collector = RedditDataCollector(
        subreddit=args.subreddit,
        before=before_timestamp,
        after=after_timestamp,
        save_images=not args.no_images,
        batch_size=args.batch_size,
        collect_all=args.all
    )
    
    collector.collect_all_data()


if __name__ == "__main__":
    main()