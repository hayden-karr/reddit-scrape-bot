"""
Core data models for Reddit Explorer.

This module defines Pydantic models that represent the application's domain entities,
providing validation, serialization, and clear type definitions.
"""

from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

from pydantic import BaseModel, Field, field_validator

from reddit_scraper.constants import ContentType


class RedditContent(BaseModel):
    """Base model for Reddit content (posts and comments)."""

    id: str = Field(..., description="Unique identifier")
    text: str = Field(..., description="Content text")
    created_utc: int = Field(..., description="Creation timestamp (UTC)")
    created_time: Optional[str] = Field(
        default=None, description="Human-readable creation time"
    )
    image_url: Optional[str] = Field(None, description="URL of associated image")
    image_path: Optional[str] = Field(None, description="Local path to saved image")
    
    @classmethod
    def model_validate(cls, data):
        if "created_time" not in data or data["created_time"] is None:
            created_utc = data.get("created_utc")
            if created_utc is not None:
                data["created_time"] = datetime.fromtimestamp(
                    created_utc
                ).strftime("%Y-%m-%d %H:%M:%S")
        return super().model_validate(data)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert model to a dictionary suitable for storage."""
        return self.model_dump()


class RedditPost(RedditContent):
    """Model representing a Reddit post."""

    title: str = Field(..., description="Post title")
    
    @property
    def post_id(self) -> str:
        """Alias for id to maintain compatibility with older code."""
        return self.id
    
    class Config:
        populate_by_name = True
        json_schema_extra = {
            "example": {
                "id": "abcd1234",
                "title": "Example post title",
                "text": "This is the content of the post.",
                "created_utc": 1615199000,
                "created_time": "2021-03-08 12:30:00",
                "image_url": "https://example.com/image.jpg",
                "image_path": "/data/reddit_data_example/images_example/abcd1234.webp",
            }
        }


class RedditComment(RedditContent):
    """Model representing a Reddit comment."""

    post_id: str = Field(..., description="ID of the parent post")
    parent_id: Optional[str] = Field(
        None, description="ID of the parent comment (if any)"
    )
    
    @property
    def comment_id(self) -> str:
        """Alias for id to maintain compatibility with older code."""
        return self.id
    
    class Config:
        populate_by_name = True
        json_schema_extra = {
            "example": {
                "id": "efgh5678",
                "post_id": "abcd1234",
                "parent_id": None,
                "text": "This is a comment on the post.",
                "created_utc": 1615199300,
                "created_time": "2021-03-08 12:35:00",
                "image_url": None,
                "image_path": None,
            }
        }


class CommentTree(BaseModel):
    """Model representing a hierarchical comment thread."""

    comment: RedditComment
    replies: List["CommentTree"] = Field(
        default_factory=list, description="Nested replies"
    )
    
    class Config:
        json_schema_extra = {
            "example": {
                "comment": {
                    "id": "efgh5678",
                    "post_id": "abcd1234",
                    "parent_id": None,
                    "text": "This is a comment on the post.",
                    "created_utc": 1615199300,
                    "created_time": "2021-03-08 12:35:00",
                },
                "replies": [
                    {
                        "comment": {
                            "id": "ijkl9012",
                            "post_id": "abcd1234",
                            "parent_id": "efgh5678",
                            "text": "This is a reply to the comment.",
                            "created_utc": 1615199600,
                            "created_time": "2021-03-08 12:40:00",
                        },
                        "replies": []
                    }
                ]
            }
        }


class PostWithComments(BaseModel):
    """Model representing a post with its comments."""

    post: RedditPost
    comments: List[CommentTree] = Field(
        default_factory=list, description="Top-level comments with their replies"
    )


class ScrapingResult(BaseModel):
    """Model representing the results of a scraping operation."""

    subreddit: str = Field(..., description="Name of the subreddit scraped")
    posts_count: int = Field(0, description="Number of posts scraped")
    comments_count: int = Field(0, description="Number of comments scraped")
    images_count: int = Field(0, description="Number of images downloaded")
    errors_count: int = Field(0, description="Number of errors encountered")
    start_time: datetime = Field(
        default_factory=datetime.now, description="When scraping started"
    )
    end_time: Optional[datetime] = Field(
        None, description="When scraping finished"
    )
    
    @property
    def duration_seconds(self) -> Optional[float]:
        """Calculate the duration of the scraping operation in seconds."""
        if self.end_time:
            return (self.end_time - self.start_time).total_seconds()
        return None
    
    def complete(self) -> "ScrapingResult":
        """Mark the scraping operation as complete."""
        self.end_time = datetime.now()
        return self
    
    def add_post(self) -> "ScrapingResult":
        """Increment the post count."""
        self.posts_count += 1
        return self
    
    def add_comment(self) -> "ScrapingResult":
        """Increment the comment count."""
        self.comments_count += 1
        return self
    
    def add_image(self) -> "ScrapingResult":
        """Increment the image count."""
        self.images_count += 1
        return self
    
    def add_error(self) -> "ScrapingResult":
        """Increment the error count."""
        self.errors_count += 1
        return self