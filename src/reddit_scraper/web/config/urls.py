"""
Django URL Configuration

This module defines the root URL patterns for the project.
"""

from django.contrib import admin
from django.urls import path, include
from django.conf import settings
#from django.conf.urls.static import static
from django.contrib.staticfiles.urls import staticfiles_urlpatterns # Import this

urlpatterns = [
    path('admin/', admin.site.urls),
    # Include Reddit viewer URLs
    path('', include('reddit_viewer.urls')),
]

# Include static and media URLs in development
if settings.DEBUG:
    urlpatterns += staticfiles_urlpatterns()