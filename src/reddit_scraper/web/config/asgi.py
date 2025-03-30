"""
ASGI Configuration for Django + FastAPI

This module sets up ASGI (Asynchronous Server Gateway Interface) configuration
for a hybrid application that combines Django for server-side rendering 
and FastAPI for REST API endpoints.
"""

import os
import logging
from pathlib import Path

from django.core.asgi import get_asgi_application
from django.conf import settings
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from starlette.middleware.cors import CORSMiddleware
from starlette.routing import Mount
from starlette.types import ASGIApp

# Configure Django settings module
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'config.settings')

# Set up logging
logger = logging.getLogger(__name__)

# Create logs directory if it doesn't exist
try:
    log_dir = Path(__file__).resolve().parent.parent.parent / 'logs'
    os.makedirs(log_dir, exist_ok=True)
except Exception as e:
    print(f"Warning: Could not create logs directory: {e}")


# Initialize the Django ASGI application
django_app = get_asgi_application()


# Import FastAPI app (must be after Django setup to access settings)
def get_fastapi_app() -> FastAPI:
    """
    Import and return the FastAPI application.
    
    Returns:
        The FastAPI application instance
    """
    try:
        from reddit_viewer.api.main import app as fastapi_app
        return fastapi_app
    except ImportError as e:
        logger.error(f"Error importing FastAPI app: {str(e)}", exc_info=True)
        # Create a minimal FastAPI app as a fallback
        app = FastAPI(title="API Error", docs_url=None)
        
        @app.get("/api/status")
        async def api_error():
            return {"error": "API initialization failed"}
        
        return app


# Combine Django and FastAPI applications
def create_hybrid_asgi_app() -> ASGIApp:
    """
    Create a hybrid ASGI application that combines Django and FastAPI.
    
    Returns:
        Combined ASGI application
    """
    try:
        # Get FastAPI app
        fastapi_app = get_fastapi_app()
        
        # Create a hybrid application that routes to Django or FastAPI based on the path
        # Routes starting with /api/ go to FastAPI, everything else goes to Django
        from starlette.routing import Router
        
        routes = [
            Mount("/api", app=fastapi_app, name="api"),
            Mount("/", app=django_app, name="django"),
        ]
        
        # Create the combined router
        return Router(routes=routes)
    except Exception as e:
        logger.error(f"Error creating hybrid ASGI app: {str(e)}", exc_info=True)
        # Return just Django as a fallback
        return django_app


# Create the hybrid application
application = create_hybrid_asgi_app()