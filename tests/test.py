import polars as pl
import os


# Ensure output directory exists
from reddit_scraper.config import get_config
output_dir = get_config().storage.base_dir
os.makedirs(output_dir, exist_ok=True)

# Sample posts data
posts_data = [
    {
        "post_id": 1,
        "title": "First Test Post",
        "text": "This is the text of the first test post.",
        "created_utc": 1700000000,
        "created_time": "2024-03-09 12:00:00",
        "image_url": r"https://example.com/image1.jpg",
        "image_path": r"reddit_data_TEST\images_TEST\1.webp"
    },
    {
        "post_id": 2,
        "title": "Second Test Post",
        "text": "This is the text of the second test post.",
        "created_utc": 1700000050,
        "created_time": "2024-03-09 12:05:00",
        "image_url": r"https://example.com/image2.jpg",
        "image_path": r"reddit_data_TEST\images_TEST\2.webp"
    }
]

# Convert to DataFrame and save as Parquet
posts_df = pl.DataFrame(posts_data)
posts_df.write_parquet(os.path.join(output_dir, "reddit_posts_TEST.parquet"))

# Sample comments data
comments_data = [
    # Comments for Post 1
    {"comment_id": 101, "post_id": 1, "parent_id": 1, "text": "Comment 1", "image_url": None, "image_path": r"reddit_data_TEST\images_TEST\comment_1.webp"},
    {"comment_id": 102, "post_id": 1, "parent_id": 101, "text": "Response to Comment 1", "image_url": None, "image_path": None},
    {"comment_id": 103, "post_id": 1, "parent_id": 102, "text": "Response to Response 1", "image_url": None, "image_path": None},
    {"comment_id": 104, "post_id": 1, "parent_id": 1, "text": "Comment 2", "image_url": None, "image_path": None},
    {"comment_id": 105, "post_id": 1, "parent_id": 104, "text": "Response to Comment 2", "image_url": None, "image_path": None},

    # Comments for Post 2
    {"comment_id": 201, "post_id": 2, "parent_id": 2, "text": "Comment A", "image_url": None, "image_path": None},
    {"comment_id": 202, "post_id": 2, "parent_id": 201, "text": "Response to Comment A", "image_url": None, "image_path": None},
    {"comment_id": 203, "post_id": 2, "parent_id": 202, "text": "Response to Response A", "image_url": None, "image_path": None},
    {"comment_id": 204, "post_id": 2, "parent_id": 2, "text": "Comment B", "image_url": None, "image_path": None},
    {"comment_id": 205, "post_id": 2, "parent_id": 204, "text": "Response to Comment B", "image_url": None, "image_path": None},
]

# Convert to DataFrame and save as Parquet
comments_df = pl.DataFrame(comments_data)
comments_df.write_parquet(os.path.join(output_dir, "reddit_comments_TEST.parquet"))

print("Test Parquet files created successfully!")
