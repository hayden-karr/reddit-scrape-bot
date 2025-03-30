"""
WSGI Configuration for Django

This module contains the WSGI application used by Django's development server
and any production WSGI deployments. It should expose a module-level variable
named ``application``. Django's ``runserver`` and ``runfcgi`` commands discover
this application via the ``WSGI_APPLICATION`` setting.

Note that this WSGI application only runs the Django part of the hybrid application.
For the full hybrid application with FastAPI, use the ASGI configuration instead.
"""

import os
import logging
from pathlib import Path

from django.core.wsgi import get_wsgi_application

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

# Get the WSGI application
application = get_wsgi_application()

# Log startup information
logger.info("Django WSGI application initialized")