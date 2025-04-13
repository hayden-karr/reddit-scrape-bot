"""
Command-line interface for Reddit Scraper.

This module provides a modern, user-friendly CLI for interacting with the
Reddit Scraper application, built with Typer and Rich for great UX.
"""

import sys
from datetime import datetime
from pathlib import Path
from typing import List, Optional
import os

import typer
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from reddit_scraper.config import get_config
from reddit_scraper.constants import ScraperMethod, RedditSort, TopTimeFilter
from reddit_scraper.scrapers import get_available_scrapers
from reddit_scraper.services.scraping_service import ScrapingService
from reddit_scraper.utils.logging import configure_logging, get_logger
from reddit_scraper.web.reddit_viewer import run_app
from reddit_scraper.utils.validators import validate_subreddit_name, sanitize_subreddit_name

# Initialize Typer app
app = typer.Typer(
    name="reddit-scraper",
    help="A professional Reddit scraper with web interface",
    add_completion=False,
)

# Initialize console for rich output
console = Console()
logger = get_logger(__name__)


def version_callback(value: bool) -> None:
    """Print version information and exit."""
    if value:
        console.print(
            Panel.fit(
                "[bold]Reddit Scraper[/bold] [cyan]v0.1.0[/cyan]",
                border_style="cyan",
                subtitle="Professional Reddit Scraping Tool",
            )
        )
        raise typer.Exit()


@app.callback()
def main(
    version: bool = typer.Option(
        False, "--version", "-v", help="Show version and exit", callback=version_callback
    ),
):
    """
    Reddit Scraper - Professional Reddit Scraping Tool
    
    A modular, modern tool for scraping and browsing Reddit content.
    """
    # Configure logging system
    configure_logging()


@app.command("scrape")
def scrape_command(
    subreddit: str = typer.Argument(..., help="Name of the subreddit to scrape"),
    method: ScraperMethod = typer.Option(
        ScraperMethod.PRAW, "--method", "-m", help="Scraping method to use",
    ),
    limit: int = typer.Option(
        100, "--limit", "-l", help="Maximum number of posts to scrape"
    ),
    comment_limit: int = typer.Option(
        100, "--comment-limit", "-c", help="Maximum number of comments per post"
    ),
    sort: RedditSort = typer.Option(
        RedditSort.NEW, "--sort", "-s", case_sensitive=False,
        help="Sort order for posts (new, hot, top)."
    ),
    time_filter: TopTimeFilter = typer.Option(
        TopTimeFilter.ALL, "--time-filter", "-t", case_sensitive=False,
        help="Time filter for 'top' sort (hour, day, week, month, year, all)."
    ),
    before: Optional[str] = typer.Option(
        None, "--before", "-b", help="Only fetch content before this date (YYYY-MM-DD)"
    ),
    after: Optional[str] = typer.Option(
        None, "--after", "-a", help="Only fetch content after this date (YYYY-MM-DD)"
    ),
    no_images: bool = typer.Option(
        False, "--no-images", help="Skip downloading images"
    ),
    quiet: bool = typer.Option(
        False, "--quiet", "-q", help="Suppress progress output"
    ),
) -> None:
    """
    Scrape content from a subreddit.
    
    This command fetches posts and comments from a subreddit using the
    specified scraping method and saves them to the local database.
    """
    if not validate_subreddit_name(subreddit):
        sanitized = sanitize_subreddit_name(subreddit)
        console.print(f"[yellow]Warning:[/yellow] Subreddit name '{subreddit}' was sanitized to '{sanitized}'")
        subreddit = sanitized

    # Validate time_filter usage
    if sort != RedditSort.TOP and time_filter != TopTimeFilter.ALL:
         console.print(f"[yellow]Warning:[/yellow] --time-filter '{time_filter.value}' is only applicable when --sort=top. Ignoring time filter.")

    try:
        # Convert date strings to datetime objects if provided
        before_date = None
        after_date = None
        
        if before:
            before_date = datetime.strptime(before, "%Y-%m-%d")
        if after:
            after_date = datetime.strptime(after, "%Y-%m-%d")
        
        # Create the scraping service
        service = ScrapingService(subreddit, method.value)
        
        # Get config to show data location
        config = get_config()
        storage_dir = config.storage.base_dir
        
        # Show scraping parameters
        if not quiet:
            console.print(
                Panel.fit(
                    f"[bold]Scraping[/bold] r/{subreddit} using [cyan]{method.value}[/cyan]",
                    border_style="green",
                )
            )
            console.print(f"Sort order: {sort.value}")
            if sort == RedditSort.TOP:
                console.print(f"Top time filter: {time_filter.value}")
            console.print(f"Posts limit: {limit}")
            console.print(f"Comments limit per post: {comment_limit}")
            console.print(f"Storage directory: {storage_dir}")
            if before_date:
                console.print(f"Before: {before}")
            if after_date:
                console.print(f"After: {after}")
            console.print(f"Download images: {not no_images}")
            
            console.print("\nStarting scrape operation...\n")
        
        # Perform the scraping
        result = service.scrape_and_store(
            post_limit=limit,
            comment_limit=comment_limit,
            sort_order=sort, 
            time_filter=time_filter, 
            before=before_date,
            after=after_date,
            download_images=not no_images,
            show_progress=not quiet,
        )
        
        # Show results
        if not quiet:
            duration = result.duration_seconds or 0
            
            console.print("\n[bold green]Scraping completed![/bold green]")
            
            table = Table(show_header=True, header_style="bold cyan")
            table.add_column("Metric")
            table.add_column("Value")
            
            table.add_row("Subreddit", f"r/{result.subreddit}")
            table.add_row("Posts scraped", str(result.posts_count))
            table.add_row("Comments scraped", str(result.comments_count))
            table.add_row("Images downloaded", str(result.images_count))
            table.add_row("Errors", str(result.errors_count))
            table.add_row("Duration", f"{duration:.2f} seconds")
            table.add_row("Storage location", str(storage_dir / f"reddit_data_{subreddit}"))
            
            console.print(table)
            
            if result.errors_count > 0:
                console.print(
                    f"[yellow]Warning:[/yellow] {result.errors_count} errors occurred during scraping. "
                    f"Check the logs for details."
                )
            
            console.print(
                "\n[bold]Explore the data:[/bold] "
                f"Run [cyan]reddit-scraper web --subreddit={subreddit}[/cyan] and navigate to http://localhost:8000/"
            )
    except Exception as e:
        console.print(f"[bold red]Error:[/bold red] {str(e)}")
        logger.error(f"Error in scrape command: {e}", exc_info=True)
        raise typer.Exit(code=1)


@app.command("web")
def web_command(
    subreddit: Optional[str] = typer.Option(
        None, "--subreddit", "-s", help="Subreddit to view (must be scraped first)"
    ),
    host: Optional[str] = typer.Option(
        None, "--host", "-h", help="Host to bind to"
    ),
    port: Optional[int] = typer.Option(
        None, "--port", "-p", help="Port to bind to"
    ),
    debug: bool = typer.Option(
        False, "--debug", "-d", help="Run in debug mode"
    ),
) -> None:
    """
    Start the web interface.
    
    This command launches a web server that provides a UI for browsing
    the scraped Reddit content.
    """
    try:
        # Get config for default values
        config = get_config()
        
        # Use provided values or fall back to config
        run_host = host or config.web.host
        run_port = port or config.web.port
        
        # Set subreddit environment variable if provided
        if subreddit:
            os.environ["SUBREDDIT_NAME"] = subreddit
        
        console.print(
            Panel.fit(
                f"[bold]Starting web server[/bold] on [cyan]{run_host}:{run_port}[/cyan]",
                border_style="green",
            )
        )
        
        if subreddit:
            console.print(f"Showing data for r/{subreddit}")
        else:
            console.print(
                "[yellow]No subreddit specified.[/yellow] "
                "Please provide one with the --subreddit option or set it in the web app."
            )
        
        storage_dir = config.storage.base_dir
        console.print(f"Looking for data in: {storage_dir}")
        
        console.print(
            "Press [bold]Ctrl+C[/bold] to stop the server\n"
        )
        
        # Run the web app
        run_app(host=run_host, port=run_port, debug=debug)
    except KeyboardInterrupt:
        console.print("\n[bold green]Server stopped.[/bold green]")
    except Exception as e:
        console.print(f"[bold red]Error:[/bold red] {str(e)}")
        logger.error(f"Error in web command: {e}", exc_info=True)
        raise typer.Exit(code=1)
    
@app.command("flask")
def flask_command(
    host: Optional[str] = typer.Option(
        None, "--host", "-h", help="Host to bind to"
    ),
    port: Optional[int] = typer.Option(
        None, "--port", "-p", help="Port to bind to"
    ),
    debug: bool = typer.Option(
        False, "--debug", "-d", help="Run in debug mode"
    ),
    subreddit: Optional[str] = typer.Option(
        None, "--subreddit", "-s", help="Subreddit to view (overrides config/env)"
    ),
    chunk_size: Optional[int] = typer.Option(
        None, "--chunk-size", "-c", help="Number of posts per chunk (overrides config/env)"
    ),
) -> None:
    """
    Start the Flask web interface.
    
    This command launches a lightweight Flask web server that provides
    a simpler UI for browsing the scraped Reddit content.
    """
    try:
        # Get config for default values
        config = get_config()
        
        # Use provided values or fall back to config
        run_host = host or config.web.host
        run_port = port or config.web.port
        
        console.print(
            Panel.fit(
                f"[bold]Starting Flask web server[/bold] on [cyan]{run_host}:{run_port}[/cyan]",
                border_style="green",
            )
        )
        
        console.print(
            "Press [bold]Ctrl+C[/bold] to stop the server\n"
        )
        
        # Run the Flask app
        from reddit_scraper.web.flask_app import run_flask_app
        run_flask_app(
            host=run_host,
            port=run_port,
            debug=debug,
            subreddit=subreddit,
            chunk_size=chunk_size
        )
    except KeyboardInterrupt:
        console.print("\n[bold green]Server stopped.[/bold green]")
    except Exception as e:
        console.print(f"[bold red]Error:[/bold red] {str(e)}")
        logger.error(f"Error in flask command: {e}", exc_info=True)
        raise typer.Exit(code=1)


@app.command("info")
def info_command(
    subreddit: Optional[str] = typer.Argument(
        None, help="Name of the subreddit to get info about"
    ),
) -> None:
    """
    Show information about available data.
    
    This command displays information about available subreddits,
    or detailed information about a specific subreddit if provided.
    """
    try:
        config = get_config()
        base_dir = config.storage.base_dir
        
        if subreddit:
            # Show detailed info about a specific subreddit
            service = ScrapingService(subreddit)
            data = service.get_available_data()
            
            if "error" in data:
                console.print(f"[bold red]Error:[/bold red] {data['error']}")
                return
            
            console.print(
                Panel.fit(
                    f"[bold]Information for r/{subreddit}[/bold]",
                    border_style="cyan",
                )
            )
            
            table = Table(show_header=True, header_style="bold cyan")
            table.add_column("Metric")
            table.add_column("Value")
            
            table.add_row("Posts", str(data["total_posts"]))
            table.add_row("Comments", str(data["total_comments"]))
            table.add_row("Data location", str(base_dir / f"reddit_data_{subreddit}"))
            
            console.print(table)
        else:
            # Show list of available subreddits
            console.print(
                Panel.fit(
                    "[bold]Available Subreddits[/bold]",
                    border_style="cyan",
                )
            )
            
            # Find all subreddit directories
            subreddits = []
            for path in base_dir.glob("reddit_data_*"):
                if path.is_dir():
                    subreddit_name = path.name.replace("reddit_data_", "")
                    
                    # Only include subreddits with data
                    posts_file = path / f"reddit_posts_{subreddit_name}.parquet"
                    if posts_file.exists():
                        # Get post count
                        service = ScrapingService(subreddit_name)
                        data = service.get_available_data()
                        
                        if "error" not in data:
                            subreddits.append({
                                "name": subreddit_name,
                                "posts": data["total_posts"],
                                "comments": data["total_comments"],
                            })
            
            if not subreddits:
                console.print(
                    f"[yellow]No subreddits found in {base_dir}.[/yellow] Try scraping some data first."
                )
                return
            
            table = Table(show_header=True, header_style="bold cyan")
            table.add_column("Subreddit")
            table.add_column("Posts")
            table.add_column("Comments")
            table.add_column("Location")
            
            for subreddit in sorted(subreddits, key=lambda s: s["name"]):
                table.add_row(
                    f"r/{subreddit['name']}",
                    str(subreddit["posts"]),
                    str(subreddit["comments"]),
                    str(base_dir / f"reddit_data_{subreddit['name']}")
                )
            
            console.print(table)
            
            console.print(
                "\n[bold]View data:[/bold] "
                "Run [cyan]reddit-scraper web --subreddit=SUBREDDIT[/cyan] to view a specific subreddit."
            )
    except Exception as e:
        console.print(f"[bold red]Error:[/bold red] {str(e)}")
        logger.error(f"Error in info command: {e}", exc_info=True)
        raise typer.Exit(code=1)


@app.command("methods")
def methods_command() -> None:
    """
    Show available scraping methods.
    
    This command displays information about the available scraping methods
    and their capabilities.
    """
    try:
        scrapers = get_available_scrapers()
        
        console.print(
            Panel.fit(
                "[bold]Available Scraping Methods[/bold]",
                border_style="cyan",
            )
        )
        
        table = Table(show_header=True, header_style="bold cyan")
        table.add_column("Method")
        table.add_column("Description")
        
        for method, name in scrapers.items():
            description = ""
            if method == ScraperMethod.PRAW:
                description = "Uses the official Reddit API via PRAW. Requires API credentials."
            elif method == ScraperMethod.PULLPUSH:
                description = "Uses the PullPush API. No credentials required, but has limitations."
            elif method == ScraperMethod.BROWSER:
                description = "Uses Selenium + Beautiful Soup to scrape. No credentials required."
            
            table.add_row(name, description)
        
        console.print(table)
        
        console.print(
            "\nUsage: [cyan]reddit-scraper scrape --method METHOD SUBREDDIT[/cyan]"
        )
    except Exception as e:
        console.print(f"[bold red]Error:[/bold red] {str(e)}")
        logger.error(f"Error in methods command: {e}", exc_info=True)
        raise typer.Exit(code=1)


if __name__ == "__main__":
    app()