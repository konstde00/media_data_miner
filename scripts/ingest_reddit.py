import os
import json
import time
import pandas as pd
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any, Optional
from app.config import SETTINGS
from app.utils import RequestHandler, extract_post_fields, validate_post_data, clean_text
from app.path_utils import get_safe_data_path, ensure_directory_exists


def within_days(created_utc: float, days: int) -> bool:
    """Check if the post is within the specified number of days"""
    dt = datetime.fromtimestamp(created_utc, tz=timezone.utc)
    return dt >= datetime.now(tz=timezone.utc) - timedelta(days=days)


def fetch_subreddit_posts(handler: RequestHandler, subreddit: str, 
                         sort_type: str = "new", limit_per_page: int = 100,
                         pages: int = 5) -> List[Dict[str, Any]]:
    """
    Fetch posts from a subreddit using Reddit's JSON API
    
    Args:
        handler: RequestHandler instance for making requests
        subreddit: Name of the subreddit (without r/)
        sort_type: Type of sorting (new, hot, top, rising)
        limit_per_page: Number of posts per page (max 100)
        pages: Number of pages to fetch
    
    Returns:
        List of post dictionaries
    """
    all_posts = []
    after = None
    
    print(f"Fetching r/{subreddit} ({sort_type}) - up to {pages} pages...")
    
    for page in range(pages):
        # Construct URL
        url = f"https://www.reddit.com/r/{subreddit}/{sort_type}.json"
        
        # Prepare parameters
        params = {"limit": limit_per_page}
        if after:
            params["after"] = after
            
        # Make request
        data = handler.make_request(url, params)
        if not data:
            print(f"Failed to fetch page {page + 1} for r/{subreddit}")
            break
            
        # Extract posts
        children = data.get("data", {}).get("children", [])
        if not children:
            print(f"No more posts found on page {page + 1}")
            break
            
        page_posts = []
        for item in children:
            if item.get("kind") != "t3":  # t3 = submission
                continue
                
            post_data = item.get("data", {})
            if not validate_post_data(post_data):
                continue
                
            # Apply filters
            created_utc = post_data.get("created_utc", 0)
            score = post_data.get("score", 0)
            
            if not within_days(created_utc, SETTINGS.ingest_days):
                continue
            if score < SETTINGS.min_score:
                continue
                
            # Extract all fields
            post_info = extract_post_fields(post_data)
            page_posts.append(post_info)
            
        all_posts.extend(page_posts)
        print(f"Page {page + 1}: {len(page_posts)} posts ({len(all_posts)} total)")
        
        # Get next page token
        after = data.get("data", {}).get("after")
        if not after:
            print("No more pages available")
            break
    
    return all_posts


def save_posts_parquet(posts: List[Dict[str, Any]], output_path: str):
    """Save posts to Parquet format"""
    if not posts:
        print("No posts to save")
        return
        
    df = pd.DataFrame(posts)
    df.to_parquet(output_path, index=False)
    print(f"Saved {len(posts)} posts to {output_path}")


def save_posts_jsonl(posts: List[Dict[str, Any]], output_path: str):
    """Save posts to JSONL format"""
    if not posts:
        print("No posts to save")
        return
        
    with open(output_path, 'w', encoding='utf-8') as f:
        for post in posts:
            f.write(json.dumps(post, ensure_ascii=False) + '\n')
    print(f"Saved {len(posts)} posts to {output_path}")


def run_reddit_ingestion():
    """Main function to run Reddit JSON ingestion"""
    print("🚀 Starting Reddit JSON ingestion (no API keys required)...")
    print(f"Target subreddits: {SETTINGS.subreddits}")
    print(f"Posts per subreddit: {SETTINGS.pages_per_subreddit} pages (~{SETTINGS.pages_per_subreddit * 100} posts)")
    print(f"Days filter: {SETTINGS.ingest_days} days")
    print(f"Min score: {SETTINGS.min_score}")
    print(f"Save format: {SETTINGS.save_format}")
    print(f"Request delay: {SETTINGS.request_delay} seconds")
    
    # Create output directory safely
    date_str = datetime.now().strftime("%Y-%m-%d")
    out_dir = ensure_directory_exists(f"/data/raw_parquet/{date_str}")
    
    # Initialize request handler
    handler = RequestHandler()
    
    # Collect all posts
    all_posts = []
    
    for subreddit in SETTINGS.subreddits:
        subreddit = subreddit.strip()
        try:
            posts = fetch_subreddit_posts(
                handler=handler,
                subreddit=subreddit,
                sort_type="new",  # Could make this configurable
                pages=SETTINGS.pages_per_subreddit
            )
            all_posts.extend(posts)
            
        except Exception as e:
            print(f"❌ Error processing r/{subreddit}: {e}")
            continue
    
    # Save posts
    if all_posts:
        timestamp = datetime.now().strftime("%H%M%S")
        
        if SETTINGS.save_format.lower() == "jsonl":
            output_path = get_safe_data_path(date_str, f"posts_{timestamp}.jsonl")
            save_posts_jsonl(all_posts, output_path)
        else:
            output_path = get_safe_data_path(date_str, f"posts_{timestamp}.parquet")
            save_posts_parquet(all_posts, output_path)
            
        print(f"\n✅ Ingestion completed successfully!")
        print(f"Total posts collected: {len(all_posts)}")
        print(f"Output saved to: {output_path}")
        
        # Print summary stats
        subreddit_counts = {}
        for post in all_posts:
            sub = post.get('subreddit', 'unknown')
            subreddit_counts[sub] = subreddit_counts.get(sub, 0) + 1
            
        print("\nPosts per subreddit:")
        for sub, count in sorted(subreddit_counts.items()):
            print(f"  r/{sub}: {count} posts")
            
    else:
        print("❌ No posts were collected.")


if __name__ == "__main__":
    try:
        run_reddit_ingestion()
    except KeyboardInterrupt:
        print("\n⚠️  Ingestion interrupted by user")
    except Exception as e:
        print(f"❌ Ingestion failed: {e}")
        import traceback
        traceback.print_exc()