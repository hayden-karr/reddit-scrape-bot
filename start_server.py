import os
import sys
from pathlib import Path

# Add the src directory to Python's path
project_root = Path(__file__).resolve().parent
src_path = project_root / "src"
sys.path.insert(0, str(src_path))

# Set Django environment variables
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'reddit_scraper.web.config.settings')
os.environ.setdefault('DJANGO_SECRET_KEY', 'test-secret-key-for-development')
os.environ.setdefault('DJANGO_DEBUG', 'true')
os.environ.setdefault('DJANGO_ALLOWED_HOSTS', 'localhost,127.0.0.1')

# Set a subreddit name if you've scraped one already
os.environ.setdefault('SUBREDDIT_NAME', 'python')  # Change to a subreddit you've scraped

# Try to import and run the server
try:
    print("Attempting to start Django server...")
    
    # Import Django and set it up
    import django
    django.setup()
    
    # Run the server
    from django.core.management import execute_from_command_line
    print("Starting server on http://127.0.0.1:8000/")
    execute_from_command_line(['manage.py', 'runserver'])
    
except ImportError as e:
    print(f"Error importing Django components: {e}")
    print("Make sure Django is installed: pip install django")
except Exception as e:
    print(f"Error starting server: {e}")