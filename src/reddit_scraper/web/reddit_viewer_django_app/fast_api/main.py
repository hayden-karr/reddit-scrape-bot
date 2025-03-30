"""
Reddit Viewer FastAPI Application

This module initializes the FastAPI application with all routes and middleware.
"""

import logging
from fastapi import FastAPI, Request, status
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.openapi.docs import get_swagger_ui_html
from fastapi.openapi.utils import get_openapi

from django.conf import settings
from reddit_viewer_django_app.fast_api.router import api_router
from reddit_viewer_django_app.services.data_manager import DataManagerException

# Set up logging
logger = logging.getLogger(__name__)


def create_app() -> FastAPI:
    """
    Create and configure the FastAPI application.
    
    Returns:
        Configured FastAPI application
    """
    # Initialize FastAPI app with metadata
    app = FastAPI(
        title="Reddit Viewer API",
        description="API for viewing Reddit subreddit data",
        version="1.0.0",
        docs_url=None,  # We'll create a custom docs endpoint
        redoc_url=None,  # Disable redoc
        openapi_url="/api/openapi.json" if settings.DEBUG else None,  # Only expose in debug mode
    )
    
    # Add middleware
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"] if settings.DEBUG else [settings.ALLOWED_HOSTS],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    
    # Global exception handler
    @app.exception_handler(DataManagerException)
    async def data_manager_exception_handler(request: Request, exc: DataManagerException):
        logger.error(f"Data manager exception: {str(exc)}", exc_info=True)
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={"error": "Server error", "detail": str(exc)},
        )
    
    # Add custom docs endpoint (only in debug mode)
    if settings.DEBUG:
        @app.get("/api/docs", include_in_schema=False)
        async def custom_swagger_ui_html():
            return get_swagger_ui_html(
                openapi_url="/api/openapi.json",
                title="Reddit Viewer API Documentation",
                swagger_js_url="https://cdn.jsdelivr.net/npm/swagger-ui-dist@5/swagger-ui-bundle.js",
                swagger_css_url="https://cdn.jsdelivr.net/npm/swagger-ui-dist@5/swagger-ui.css",
            )
            
        @app.get("/api/openapi.json", include_in_schema=False)
        async def get_open_api_endpoint():
            return get_openapi(
                title="Reddit Viewer API",
                version="1.0.0",
                description="API for viewing Reddit subreddit data",
                routes=app.routes,
            )
    
    # Include API router
    app.include_router(api_router)
    
    # Mount static files if needed for images
    try:
        # This is for direct API access to images without going through Django
        app.mount(
            "/images", 
            StaticFiles(directory=settings.MEDIA_ROOT), 
            name="images"
        )
    except Exception as e:
        logger.warning(f"Could not mount images directory: {str(e)}")
    
    return app


# Create app instance for ASGI
app = create_app()