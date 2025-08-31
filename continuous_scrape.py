#!/usr/bin/env python3
"""
Continuous Reddit scraper
"""
import time
import requests
import pandas as pd
from datetime import datetime
from pathlib import Path

def scrape_and_save():
    """Scrape and save data"""
    subreddits = ['startups', 'entrepreneur', 'smallbusiness', 'SaaS']
    all_posts = []
    
    print(f"\n🚀 Scraping at {datetime.now().strftime('%H:%M:%S')}")
    
    for subreddit in subreddits:
        try:
            url = f'https://www.reddit.com/r/{subreddit}/new.json'
            headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'}
            
            response = requests.get(url, headers=headers, params={'limit': 25}, timeout=10)
            if response.status_code == 200:
                data = response.json()
                posts = []
                
                for item in data['data']['children']:
                    post = item['data']
                    posts.append({
                        'id': post.get('id'),
                        'title': post.get('title'),
                        'score': post.get('score'),
                        'num_comments': post.get('num_comments'),
                        'created_utc': post.get('created_utc'),
                        'subreddit': post.get('subreddit'),
                        'scraped_at': datetime.now().isoformat()
                    })
                
                all_posts.extend(posts)
                print(f"  ✅ r/{subreddit}: {len(posts)} posts")
            else:
                print(f"  ❌ r/{subreddit}: HTTP {response.status_code}")
        
        except Exception as e:
            print(f"  ❌ r/{subreddit}: {e}")
    
    if all_posts:
        # Save data
        data_dir = Path('./data/continuous')
        data_dir.mkdir(parents=True, exist_ok=True)
        
        df = pd.DataFrame(all_posts)
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filepath = data_dir / f"reddit_batch_{timestamp}.parquet"
        df.to_parquet(filepath, index=False)
        
        print(f"  💾 Saved {len(all_posts)} posts to {filepath}")
    
    return len(all_posts)

def main():
    """Main continuous scraping loop"""
    print("🔄 Starting continuous Reddit scraper...")
    print("   Press Ctrl+C to stop")
    
    total_scraped = 0
    batch_count = 0
    
    try:
        while True:
            batch_count += 1
            posts_count = scrape_and_save()
            total_scraped += posts_count
            
            print(f"📊 Batch {batch_count}: Total scraped = {total_scraped} posts")
            
            # Wait 5 minutes between batches
            print("⏰ Waiting 5 minutes until next scrape...")
            time.sleep(300)  # 5 minutes
            
    except KeyboardInterrupt:
        print(f"\n🛑 Scraping stopped. Total scraped: {total_scraped} posts in {batch_count} batches")

if __name__ == "__main__":
    main()