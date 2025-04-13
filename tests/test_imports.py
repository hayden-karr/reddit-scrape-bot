# test_imports.py
import sys
from pathlib import Path

# Add the src directory to Python's path
sys.path.insert(0, str(Path(__file__).parent / "src"))

try:
    import reddit_scraper
    print("Successfully imported reddit_scraper!")
    
    import reddit_scraper.cli
    print("Successfully imported reddit_scraper.cli!")
    
    import reddit_scraper.cli.main
    print("Successfully imported reddit_scraper.cli.main!")
    
    print("All imports successful!")
except ImportError as e:
    print(f"Import error: {e}")

    