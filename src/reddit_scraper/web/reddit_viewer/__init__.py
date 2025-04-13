"""
Web application runner for Reddit Scraper.

This module provides functions to run the Django + FastAPI web application
from the command line interface.
"""

import os
import sys
import logging
from pathlib import Path
from typing import Optional

from reddit_scraper.config import get_config
from reddit_scraper.utils.logging import get_logger

# Add src directory to Python path
src_path = Path(__file__).resolve().parent.parent.parent.parent
sys.path.insert(0, str(src_path))

logger = get_logger(__name__)


def run_app(host: Optional[str] = None, port: Optional[int] = None, debug: bool = False) -> None:
    """
    Run the Django + FastAPI web application.
    
    Args:
        host: Host to bind the server to
        port: Port to bind the server to
        debug: Whether to run in debug mode
    """
    try:
        # Get configuration
        config = get_config()
        
        # Use provided values or fall back to config
        host = host or config.web.host
        port = port or config.web.port
        
        # Check if uvicorn is available
        try:
            import uvicorn
        except ImportError:
            logger.error("Uvicorn is not installed. Please install it with 'pip install uvicorn[standard]'")
            sys.exit(1)
        
        # Set environment variables for Django
        os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'config.settings')
        os.environ.setdefault('DJANGO_SECRET_KEY', config.web.secret_key)
        os.environ.setdefault('DJANGO_DEBUG', str(debug).lower())
        os.environ.setdefault('DJANGO_ALLOWED_HOSTS', f"{host},localhost,127.0.0.1")
        
        # Get the Django project directory
        web_dir = Path(__file__).resolve().parent.parent
        
        # Add the Django project directory to the Python path
        sys.path.insert(0, str(web_dir))
        
        # Create the logs directory
        logs_dir = web_dir / 'logs'
        logs_dir.mkdir(parents=True, exist_ok=True)
        
        # Run the web application using the Django management command
        logger.info(f"Starting web server at http://{host}:{port}/")
        
        # Change to the web directory to ensure Django can find its files
        os.chdir(str(web_dir))
        
        # Build the command to run 
        cmd = [
            sys.executable,
            str(web_dir / 'manage.py'), 
            'run_server',
            f'--host={host}',
            f'--port={port}',
        ]
        
        if debug:
            cmd.append('--reload')
        
        logger.debug(f"Running command: {' '.join(cmd)}")
        
        # Use os.execvp to replace the current process with the Django process
        os.execvp(cmd[0], cmd)
        
    except Exception as e:
        logger.error(f"Error starting web application: {e}")
        sys.exit(1)


if __name__ == "__main__":
    # For testing
    run_app()