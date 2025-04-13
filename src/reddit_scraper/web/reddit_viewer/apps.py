"""
Reddit Viewer Django Application Configuration

This module defines the Django application configuration for the Reddit Viewer app.
"""

from django.apps import AppConfig


class RedditViewerConfig(AppConfig):
    """
    Django application configuration for the Reddit Viewer app.
    """
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'reddit_viewer'
    verbose_name = 'Reddit Viewer'
    
    def ready(self):
        """
        Perform application initialization when Django starts.
        """
        # Import signals or perform other setup if needed
        pass