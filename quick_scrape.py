#!/usr/bin/env python3
"""
Quick Reddit scraper to get data flowing locally
"""
import requests
import json
import os
import pandas as pd
from datetime import datetime
from pathlib import Path

def scrape_subreddit(subreddit, limit=50):
    """Scrape posts from a subreddit"""
    print(f"🚀 Scraping r/{subreddit}...")
    
    url = f'https://www.reddit.com/r/{subreddit}/new.json'
    headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'}
    
    try:
        response = requests.get(url, headers=headers, params={'limit': limit}, timeout=10)
        if response.status_code == 200:
            data = response.json()
            posts = []
            
            for item in data['data']['children']:
                post = item['data']
                posts.append({
                    'id': post.get('id'),
                    'title': post.get('title'),
                    'selftext': post.get('selftext'),
                    'url': post.get('url'),
                    'score': post.get('score'),
                    'num_comments': post.get('num_comments'),
                    'created_utc': post.get('created_utc'),
                    'author': post.get('author'),
                    'subreddit': post.get('subreddit'),
                    'permalink': post.get('permalink')
                })
            
            print(f"✅ Got {len(posts)} posts from r/{subreddit}")
            return posts
        else:
            print(f"❌ Failed to fetch r/{subreddit}: HTTP {response.status_code}")
            return []
    
    except Exception as e:
        print(f"❌ Error scraping r/{subreddit}: {e}")
        return []

def save_data(posts, filename):
    """Save posts to parquet file"""
    if not posts:
        print("No posts to save")
        return
    
    # Create data directory
    data_dir = Path('./data/scraped')
    data_dir.mkdir(parents=True, exist_ok=True)
    
    # Convert to DataFrame and save
    df = pd.DataFrame(posts)
    filepath = data_dir / f"{filename}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet"
    df.to_parquet(filepath, index=False)
    
    print(f"💾 Saved {len(posts)} posts to {filepath}")
    print(f"📊 Data shape: {df.shape}")

def main():
    """Main scraping function"""
    subreddits = ['startups', 'entrepreneur', 'smallbusiness', 'SaaS']
    all_posts = []
    
    for subreddit in subreddits:
        posts = scrape_subreddit(subreddit, limit=25)
        all_posts.extend(posts)
        
        # Save individual subreddit data
        if posts:
            save_data(posts, f"reddit_{subreddit}")
    
    # Save combined data
    if all_posts:
        save_data(all_posts, "reddit_combined")
        print(f"\n🎉 Total posts scraped: {len(all_posts)}")
    else:
        print("\n❌ No data was scraped")

if __name__ == "__main__":
    main()