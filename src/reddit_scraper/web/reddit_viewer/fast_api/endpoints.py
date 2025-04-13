"""
Reddit Viewer API Endpoints

This module defines the FastAPI endpoints for the Reddit Viewer API.
"""

import logging
from typing import Annotated, Optional

from fastapi import APIRouter, HTTPException, Path, Query, status
from django.conf import settings

from reddit_scraper.web.reddit_viewer.fast_api.models import (
    ChunkModel,
    ChunkCountModel,
    CommentsResponseModel,
    ErrorResponseModel
)
from reddit_scraper.web.reddit_viewer.fast_api.dependencies import DataManagerDep
from reddit_scraper.web.reddit_viewer.services.data_manager import (
    DataManagerException,
    DataNotFoundException
)

# Set up logging
logger = logging.getLogger(__name__)

# Create the API router
router = APIRouter(tags=["reddit"])


@router.get(
    "/chunks/{chunk_id}",
    response_model=ChunkModel,
    responses={
        404: {"model": ErrorResponseModel, "description": "No posts found"},
        500: {"model": ErrorResponseModel, "description": "Server error"}
    },
    summary="Get a chunk of posts",
    description="Get a specific chunk of Reddit posts with their comments"
)
async def get_chunked_posts(
    chunk_id: Annotated[int, Path(..., title="Chunk ID", description="The ID of the chunk to retrieve", ge=1)],
    data_manager: DataManagerDep,
    chunk_size: Annotated[int, Query(title="Chunk size", description="Number of posts per chunk", ge=1)] = settings.CHUNK_SIZE
) -> ChunkModel:
    """
    Get a chunk of Reddit posts with their comments.
    
    Args:
        chunk_id: The ID of the chunk to retrieve (1-based)
        chunk_size: Number of posts per chunk
        data_manager: Injected RedditDataManager dependency
        
    Returns:
        A chunk of posts with their comments
        
    Raises:
        HTTPException: If there's an error retrieving the posts or no posts are found
    """
    try:
        chunk_data = data_manager.get_chunked_posts(chunk_id, chunk_size)
        
        # Do not raise 404 for empty posts; return the response as-is (allows for graceful infinite scroll)
            
        return ChunkModel(**chunk_data)
        
    except DataManagerException as e:
        logger.error(f"Error getting chunked posts: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error retrieving posts: {str(e)}"
        )


@router.get(
    "/chunks/count",
    response_model=ChunkCountModel,
    responses={
        404: {"model": ErrorResponseModel, "description": "No posts found"},
        500: {"model": ErrorResponseModel, "description": "Server error"}
    },
    summary="Get total chunks count",
    description="Get the total number of chunks based on the post count and chunk size"
)
async def get_total_chunks(
    data_manager: DataManagerDep,
    chunk_size: str = Query(None, title="Chunk size", description="Number of posts per chunk")
) -> ChunkCountModel:
    """
    Get the total number of chunks based on the post count and chunk size.
    
    Args:
        chunk_size: Number of posts per chunk
        data_manager: Injected RedditDataManager dependency
        
    Returns:
        The total number of chunks
        
    Raises:
        HTTPException: If there's an error calculating the total chunks or no posts are found
    """
    try:
        # Debug log for received chunk_size
        logger.debug(f"Received chunk_size param: {chunk_size!r}")
        # Robustly coerce chunk_size to int, fallback to default if invalid
        try:
            chunk_size_int = int(chunk_size)
            if chunk_size_int < 1:
                chunk_size_int = settings.CHUNK_SIZE
        except (TypeError, ValueError):
            chunk_size_int = settings.CHUNK_SIZE
        total_chunks = data_manager.get_total_chunks(chunk_size_int)
        # Always return a valid response, even if there are 0 posts
        if total_chunks == 0:
            return ChunkCountModel(count=0)
        
        if total_chunks == 0:
            logger.warning("No posts found when calculating total chunks")
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="No posts found"
            )
            
        return ChunkCountModel(count=total_chunks)
        
    except DataManagerException as e:
        logger.error(f"Error calculating total chunks: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error calculating total chunks: {str(e)}"
        )


@router.get(
    "/comments/{post_id}",
    response_model=CommentsResponseModel,
    responses={
        404: {"model": ErrorResponseModel, "description": "Post not found"},
        500: {"model": ErrorResponseModel, "description": "Server error"}
    },
    summary="Get comments for a post",
    description="Get all comments for a specific post"
)
async def get_comments(
    post_id: Annotated[str, Path(..., title="Post ID", description="The ID of the post to get comments for")],
    data_manager: DataManagerDep
) -> CommentsResponseModel:
    """
    Get all comments for a specific post.
    
    Args:
        post_id: The ID of the post to get comments for
        data_manager: Injected RedditDataManager dependency
        
    Returns:
        All comments for the specified post
        
    Raises:
        HTTPException: If there's an error retrieving the comments
    """
    try:
        comments = data_manager.get_comments_for_post(post_id)
        return CommentsResponseModel(comments=comments)
        
    except DataNotFoundException as e:
        logger.warning(f"Post not found: {post_id} : {e}")
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Post not found: {post_id}"
        )
        
    except DataManagerException as e:
        logger.error(f"Error getting comments for post {post_id}: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error retrieving comments: {str(e)}"
        )