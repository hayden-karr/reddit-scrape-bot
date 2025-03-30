"""
Reddit Viewer Django URL Configuration

This module defines the URL patterns for the Reddit Viewer Django application.
"""

from django.urls import path

from reddit_viewer_django_app import views

# URL patterns specific to the reddit_viewer app
urlpatterns = [
    # Main page
    path('', views.index, name='index'),
    
    # Serve images
    path('images/<str:filename>', views.serve_image, name='serve_image'),
]