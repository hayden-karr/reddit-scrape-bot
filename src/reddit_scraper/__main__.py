"""
Main entry point for the Reddit Scraper package.

This module allows the package to be executed directly using:
python -m reddit_scraper [command] [options]
"""

import sys
from reddit_scraper.cli.main import app

if __name__ == "__main__":
    # Use the Typer app from cli/main.py as the entry point
    app()