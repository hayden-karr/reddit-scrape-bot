"""
Django management command to run the hybrid Django + FastAPI application server.

This command starts the Uvicorn ASGI server to handle both Django and FastAPI.
"""

import os
import logging
from django.core.management.base import BaseCommand
from django.conf import settings

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    """
    Django management command to run the hybrid Django + FastAPI application server.
    """
    help = 'Run the hybrid Django + FastAPI application server with Uvicorn'
    
    def add_arguments(self, parser):
        """
        Add command line arguments.
        
        Args:
            parser: ArgumentParser instance
        """
        parser.add_argument(
            '--host',
            type=str,
            default='127.0.0.1',
            help='Host to run the server on (default: 127.0.0.1)'
        )
        parser.add_argument(
            '--port',
            type=int,
            default=8000,
            help='Port to run the server on (default: 8000)'
        )
        parser.add_argument(
            '--reload',
            action='store_true',
            help='Reload on code changes'
        )
        parser.add_argument(
            '--workers',
            type=int,
            default=1,
            help='Number of worker processes (default: 1)'
        )
    
    def handle(self, *args, **options):
        """
        Execute the command.
        
        Args:
            args: Positional arguments
            options: Named arguments
        """
        host = options['host']
        port = options['port']
        reload = options['reload']
        workers = options['workers']
        
        # Ensure log directory exists
        try:
            os.makedirs(settings.BASE_DIR.parent / 'logs', exist_ok=True)
        except Exception as e:
            self.stderr.write(self.style.ERROR(f"Could not create logs directory: {e}"))
        
        # Start Uvicorn
        self.stdout.write(self.style.SUCCESS(f"Starting hybrid Django + FastAPI server at http://{host}:{port}/"))
        self.stdout.write(self.style.SUCCESS(f"FastAPI docs available at http://{host}:{port}/api/docs/"))
        
        try:
            import uvicorn
            uvicorn.run(
                "config.asgi:application",
                host=host,
                port=port,
                reload=reload,
                workers=workers,
                log_level="info",
            )
        except ImportError:
            self.stderr.write(self.style.ERROR("Uvicorn not installed. Install it with 'pip install uvicorn[standard]'"))
        except Exception as e:
            self.stderr.write(self.style.ERROR(f"Error starting server: {e}"))