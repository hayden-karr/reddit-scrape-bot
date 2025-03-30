#!/usr/bin/env python
"""
Django's command-line utility for administrative tasks.

This file serves as the entry point for all Django management commands.
It sets up the Django environment, configures logging, and provides
access to all management commands including our custom ones.
"""
import os
import sys
from pathlib import Path


def main():
    """
    Main function to run management commands.
    
    This sets up the environment, configures Django, and runs the requested command.
    """
    # Set default Django settings module
    os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'config.settings')
    
    try:
        # Import Django and run management commands
        from django.core.management import execute_from_command_line
    except ImportError as exc:
        raise ImportError(
            "Couldn't import Django. Are you sure it's installed and "
            "available on your PYTHONPATH environment variable? Did you "
            "forget to activate a virtual environment?"
        ) from exc
    
    # Create logs directory if it doesn't exist
    try:
        log_dir = Path(__file__).resolve().parent / 'logs'
        os.makedirs(log_dir, exist_ok=True)
    except Exception as e:
        print(f"Warning: Could not create logs directory: {e}", file=sys.stderr)
    
    # Execute the command
    execute_from_command_line(sys.argv)


if __name__ == '__main__':
    # When this script is executed directly, call the main function
    main()