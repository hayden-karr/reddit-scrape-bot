"""
Reddit Viewer API Router

This module configures the FastAPI router with all endpoints.
"""

from fastapi import APIRouter

from reddit_scraper.web.reddit_viewer.fast_api.endpoints import router as reddit_router

# Create the main API router
api_router = APIRouter()

# Include all endpoint routers
api_router.include_router(reddit_router)