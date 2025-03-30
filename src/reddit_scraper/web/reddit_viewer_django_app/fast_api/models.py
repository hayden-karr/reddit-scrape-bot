"""
Reddit Viewer API Models

This module defines the Pydantic models for the Reddit Viewer API,
providing strong typing and validation for API inputs and outputs.
"""

from typing import List, Optional
from pydantic import BaseModel, Field, ConfigDict


class CommentModel(BaseModel):
    """
    Pydantic model for a comment.
    
    Attributes:
        comment_id (str): Unique identifier for the comment
        text (str): Text content of the comment
        image (Optional[str]): Optional filename of an attached image
        replies (List[CommentModel]): Nested replies to this comment
    """
    model_config = ConfigDict(from_attributes=True)
    
    comment_id: str = Field(..., description="Unique identifier for the comment")
    text: str = Field(..., description="Text content of the comment")
    image: Optional[str] = Field(None, description="Optional filename of an attached image")
    replies: List["CommentModel"] = Field(default_factory=list, description="Nested replies to this comment")


class PostModel(BaseModel):
    """
    Pydantic model for a post.
    
    Attributes:
        id (str): Unique identifier for the post
        title (str): Title of the post
        image (Optional[str]): Optional filename of an attached image
        text (str): Text content of the post
        created_time (str): Timestamp when the post was created
        comments (List[CommentModel]): Comments on this post
        commentCount (int): Total number of comments
    """
    model_config = ConfigDict(from_attributes=True)
    
    id: str = Field(..., description="Unique identifier for the post")
    title: str = Field(..., description="Title of the post")
    image: Optional[str] = Field(None, description="Optional filename of an attached image")
    text: str = Field(..., description="Text content of the post")
    created_time: str = Field(..., description="Timestamp when the post was created")
    comments: List[CommentModel] = Field(default_factory=list, description="Comments on this post")
    commentCount: int = Field(..., description="Total number of comments")


class ChunkModel(BaseModel):
    """
    Pydantic model for a chunk of posts.
    
    Attributes:
        id (int): Chunk identifier
        posts (List[PostModel]): List of posts in this chunk
    """
    model_config = ConfigDict(from_attributes=True)
    
    id: int = Field(..., description="Chunk identifier")
    posts: List[PostModel] = Field(default_factory=list, description="List of posts in this chunk")


class ChunkCountModel(BaseModel):
    """
    Pydantic model for the total number of chunks.
    
    Attributes:
        count (int): Total number of chunks
    """
    model_config = ConfigDict(from_attributes=True)
    
    count: int = Field(..., description="Total number of chunks")


class CommentsResponseModel(BaseModel):
    """
    Pydantic model for a comments response.
    
    Attributes:
        comments (List[CommentModel]): List of comments
    """
    model_config = ConfigDict(from_attributes=True)
    
    comments: List[CommentModel] = Field(default_factory=list, description="List of comments")


class ErrorResponseModel(BaseModel):
    """
    Pydantic model for an error response.
    
    Attributes:
        error (str): Error message
        detail (Optional[str]): Optional detailed error information
    """
    model_config = ConfigDict(from_attributes=True)
    
    error: str = Field(..., description="Error message")
    detail: Optional[str] = Field(None, description="Optional detailed error information")