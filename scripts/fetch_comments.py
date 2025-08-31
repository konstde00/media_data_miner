import os
import json
import time
import pandas as pd
from datetime import datetime
from typing import List, Dict, Any, Optional
from app.config import SETTINGS
from app.utils import RequestHandler, extract_comment_fields, safe_get, clean_text
from app.path_utils import validate_subreddit_name, validate_post_id, get_safe_data_path


def parse_comment_recursively(comment_obj: Dict[str, Any]) -> Dict[str, Any]:
    """
    Recursively parse a comment and all its replies
    
    Args:
        comment_obj: Comment object from Reddit JSON
        
    Returns:
        Dictionary with comment data and nested replies
    """
    if comment_obj.get("kind") != "t1":  # t1 = comment
        return None
        
    comment_data = comment_obj.get("data", {})
    
    # Extract base comment fields
    comment = extract_comment_fields(comment_data)
    
    # Initialize replies list
    comment["replies"] = []
    
    # Parse nested replies if they exist
    replies_data = comment_data.get("replies")
    if replies_data and isinstance(replies_data, dict):
        reply_children = replies_data.get("data", {}).get("children", [])
        
        for reply_obj in reply_children:
            if reply_obj.get("kind") == "t1":  # Only parse actual comments
                nested_comment = parse_comment_recursively(reply_obj)
                if nested_comment:
                    comment["replies"].append(nested_comment)
    
    return comment


def flatten_comments(comment: Dict[str, Any], depth: int = 0) -> List[Dict[str, Any]]:
    """
    Flatten a nested comment structure into a list of individual comments
    
    Args:
        comment: Comment dictionary with nested replies
        depth: Current nesting depth
        
    Returns:
        List of flattened comment dictionaries
    """
    flattened = []
    
    # Add current comment with depth info
    flat_comment = comment.copy()
    flat_comment["depth"] = depth
    flat_comment.pop("replies", None)  # Remove replies from flattened version
    flattened.append(flat_comment)
    
    # Recursively flatten replies
    for reply in comment.get("replies", []):
        flattened.extend(flatten_comments(reply, depth + 1))
    
    return flattened


def fetch_post_comments(handler: RequestHandler, subreddit: str, post_id: str,
                       limit: int = 500) -> tuple[Optional[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Fetch comments for a specific post
    
    Args:
        handler: RequestHandler instance
        subreddit: Subreddit name
        post_id: Reddit post ID
        limit: Maximum number of comments to fetch
        
    Returns:
        Tuple of (post_data, comments_list)
    """
    # Validate inputs to prevent injection
    if not validate_subreddit_name(subreddit):
        print(f"Invalid subreddit name: {subreddit}")
        return None, []
    
    if not validate_post_id(post_id):
        print(f"Invalid post ID: {post_id}")
        return None, []
    
    # Use old.reddit.com for better comment limits
    url = f"https://old.reddit.com/r/{subreddit}/comments/{post_id}.json"
    params = {"limit": limit, "raw_json": 1}  # raw_json=1 prevents HTML encoding
    
    data = handler.make_request(url, params)
    if not data or len(data) < 2:
        return None, []
    
    # Extract post data (first element)
    post_listing = data[0].get("data", {}).get("children", [])
    post_data = None
    if post_listing and post_listing[0].get("kind") == "t3":
        post_data = post_listing[0].get("data", {})
    
    # Extract comments (second element)
    comments_listing = data[1].get("data", {}).get("children", [])
    
    all_comments = []
    for comment_obj in comments_listing:
        if comment_obj.get("kind") == "t1":
            parsed_comment = parse_comment_recursively(comment_obj)
            if parsed_comment:
                all_comments.append(parsed_comment)
        elif comment_obj.get("kind") == "more":
            # Handle "more" placeholders - these require additional API calls
            more_data = comment_obj.get("data", {})
            children_ids = more_data.get("children", [])
            print(f"Found 'more' comments placeholder with {len(children_ids)} additional comments")
            # Note: Full implementation would require calling /api/morechildren
    
    return post_data, all_comments


def save_comments_parquet(comments: List[Dict[str, Any]], output_path: str):
    """Save comments to Parquet format"""
    if not comments:
        print("No comments to save")
        return
        
    df = pd.DataFrame(comments)
    df.to_parquet(output_path, index=False)
    print(f"Saved {len(comments)} comments to {output_path}")


def save_comments_jsonl(comments: List[Dict[str, Any]], output_path: str):
    """Save comments to JSONL format"""
    if not comments:
        print("No comments to save")
        return
        
    with open(output_path, 'w', encoding='utf-8') as f:
        for comment in comments:
            f.write(json.dumps(comment, ensure_ascii=False) + '\n')
    print(f"Saved {len(comments)} comments to {output_path}")


def load_posts_from_parquet(file_path: str) -> List[Dict[str, Any]]:
    """Load posts from a Parquet file"""
    try:
        df = pd.read_parquet(file_path)
        return df.to_dict('records')
    except Exception as e:
        print(f"Error loading posts from {file_path}: {e}")
        return []


def load_posts_from_jsonl(file_path: str) -> List[Dict[str, Any]]:
    """Load posts from a JSONL file"""
    posts = []
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            for line in f:
                posts.append(json.loads(line.strip()))
    except Exception as e:
        print(f"Error loading posts from {file_path}: {e}")
    return posts


def fetch_comments_for_posts(posts: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Fetch comments for a list of posts
    
    Args:
        posts: List of post dictionaries
        
    Returns:
        List of flattened comment dictionaries
    """
    handler = RequestHandler()
    all_comments = []
    
    print(f"Fetching comments for {len(posts)} posts...")
    
    for i, post in enumerate(posts):
        post_id = post.get('id')
        subreddit = post.get('subreddit')
        
        if not post_id or not subreddit:
            continue
            
        try:
            print(f"[{i+1}/{len(posts)}] Fetching comments for r/{subreddit}/comments/{post_id}")
            
            post_data, comments = fetch_post_comments(handler, subreddit, post_id, 
                                                     SETTINGS.max_comments_per_post)
            
            # Flatten nested comments
            flat_comments = []
            for comment in comments:
                flat_comments.extend(flatten_comments(comment))
            
            # Add post_id to each comment for linking
            for comment in flat_comments:
                comment['post_id'] = post_id
                comment['post_subreddit'] = subreddit
            
            all_comments.extend(flat_comments)
            print(f"  Collected {len(flat_comments)} comments")
            
        except Exception as e:
            print(f"  Error fetching comments for {post_id}: {e}")
            continue
    
    return all_comments


def run_comments_ingestion():
    """Main function to fetch comments for recently ingested posts"""
    print("Starting comments ingestion...")
    
    # Find the most recent posts file
    today = datetime.now().strftime("%Y-%m-%d")
    posts_dir = f"/data/raw_parquet/{today}"
    
    if not os.path.exists(posts_dir):
        print(f"No posts directory found for {today}")
        return
    
    # Look for the most recent posts file
    posts_files = []
    for filename in os.listdir(posts_dir):
        if filename.startswith("posts_") and (filename.endswith(".parquet") or filename.endswith(".jsonl")):
            posts_files.append(os.path.join(posts_dir, filename))
    
    if not posts_files:
        print("No posts files found to process")
        return
    
    # Use the most recent file
    posts_file = max(posts_files, key=os.path.getctime)
    print(f"Loading posts from: {posts_file}")
    
    # Load posts
    if posts_file.endswith(".parquet"):
        posts = load_posts_from_parquet(posts_file)
    else:
        posts = load_posts_from_jsonl(posts_file)
    
    if not posts:
        print("No posts loaded")
        return
    
    print(f"Loaded {len(posts)} posts")
    
    # Filter posts with comments
    posts_with_comments = [p for p in posts if p.get('num_comments', 0) > 0]
    print(f"Found {len(posts_with_comments)} posts with comments")
    
    if not posts_with_comments:
        print("No posts have comments to fetch")
        return
    
    # Fetch comments
    comments = fetch_comments_for_posts(posts_with_comments)
    
    if comments:
        # Save comments
        timestamp = datetime.now().strftime("%H%M%S")
        
        date_str = datetime.now().strftime("%Y-%m-%d")
        if SETTINGS.save_format.lower() == "jsonl":
            output_path = get_safe_data_path(date_str, f"comments_{timestamp}.jsonl")
            save_comments_jsonl(comments, output_path)
        else:
            output_path = get_safe_data_path(date_str, f"comments_{timestamp}.parquet")
            save_comments_parquet(comments, output_path)
        
        print(f"\nComments ingestion completed!")
        print(f"Total comments collected: {len(comments)}")
        print(f"Output saved to: {output_path}")
        
        # Print summary stats
        depth_counts = {}
        for comment in comments:
            depth = comment.get('depth', 0)
            depth_counts[depth] = depth_counts.get(depth, 0) + 1
        
        print("\nComments by depth:")
        for depth in sorted(depth_counts.keys()):
            print(f"  Depth {depth}: {depth_counts[depth]} comments")
    else:
        print("No comments were collected")


if __name__ == "__main__":
    try:
        run_comments_ingestion()
    except KeyboardInterrupt:
        print("\nComments ingestion interrupted by user")
    except Exception as e:
        print(f"Comments ingestion failed: {e}")