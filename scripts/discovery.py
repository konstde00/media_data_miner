"""
URL Discovery Phase for Parallel Reddit Scraping
Discovers all URLs to be scraped and populates job queue
"""
import os
import json
import time
from typing import List, Dict, Any, Set
from datetime import datetime, timedelta, timezone
from app.config import SETTINGS
from app.utils import RequestHandler
from .job_queue import JobQueue, JobType, estimate_memory_usage
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class URLDiscovery:
    """
    Discovers all URLs for parallel scraping
    Memory-optimized for 16GB RAM systems
    """
    
    def __init__(self, job_queue: JobQueue, max_discovery_urls: int = 100000):
        self.job_queue = job_queue
        self.max_discovery_urls = max_discovery_urls
        self.handler = RequestHandler()
        self.discovered_urls: Set[str] = set()
        
    def discover_subreddit_pages(self, subreddits: List[str], 
                               pages_per_subreddit: int = 5,
                               sort_types: List[str] = None) -> int:
        """
        Discover all subreddit page URLs to scrape
        Phase 1: Just the listing pages
        """
        if sort_types is None:
            sort_types = ["new", "hot", "top"]
        
        logger.info(f"🔍 Discovering URLs for {len(subreddits)} subreddits...")
        
        jobs_to_add = []
        
        for subreddit in subreddits:
            subreddit = subreddit.strip()
            
            for sort_type in sort_types:
                # Discover pagination URLs by making initial requests
                urls = self._discover_paginated_urls(
                    subreddit, sort_type, pages_per_subreddit
                )
                
                for i, url in enumerate(urls):
                    if len(self.discovered_urls) >= self.max_discovery_urls:
                        logger.warning(f"⚠️ Hit discovery limit of {self.max_discovery_urls}")
                        break
                        
                    if url not in self.discovered_urls:
                        self.discovered_urls.add(url)
                        jobs_to_add.append((
                            JobType.SUBREDDIT_PAGE,
                            url,
                            10 - i,  # Higher priority for earlier pages
                            {
                                "subreddit": subreddit,
                                "sort_type": sort_type,
                                "page": i + 1,
                                "phase": "discovery"
                            }
                        ))
        
        # Bulk insert jobs for efficiency
        added_count = self.job_queue.bulk_add_jobs(jobs_to_add)
        logger.info(f"✅ Added {added_count} subreddit page URLs to queue")
        
        return added_count
    
    def _discover_paginated_urls(self, subreddit: str, sort_type: str, 
                               max_pages: int) -> List[str]:
        """Discover paginated URLs for a subreddit"""
        urls = []
        after = None
        
        logger.info(f"  Discovering r/{subreddit}/{sort_type} pages...")
        
        for page in range(max_pages):
            # Build URL
            url = f"https://www.reddit.com/r/{subreddit}/{sort_type}.json"
            
            # Add pagination if needed
            if after:
                url += f"?after={after}&limit=100"
            else:
                url += "?limit=100"
            
            urls.append(url)
            
            # Make lightweight request to get next 'after' token
            # This is just for discovery - we won't save the data
            try:
                data = self.handler.make_request(url, {})
                if not data or not data.get("data", {}).get("children"):
                    logger.debug(f"    No more pages after page {page + 1}")
                    break
                    
                after = data.get("data", {}).get("after")
                if not after:
                    logger.debug(f"    No 'after' token, stopping at page {page + 1}")
                    break
                    
                # Small delay during discovery
                time.sleep(0.1)
                
            except Exception as e:
                logger.error(f"    Error discovering page {page + 1}: {e}")
                break
        
        logger.info(f"    Found {len(urls)} pages for r/{subreddit}/{sort_type}")
        return urls
    
    def discover_from_completed_pages(self, limit: int = 10000) -> int:
        """
        Phase 2: Discover individual post URLs from completed page scrapes
        """
        logger.info("🔍 Discovering post URLs from scraped pages...")
        
        # Get recently completed subreddit page jobs
        stats = self.job_queue.get_stats()
        logger.info(f"Queue stats: {stats['completed']} completed jobs")
        
        # This would scan saved page data and extract post URLs
        # For now, we'll implement a simpler approach
        post_jobs = []
        
        # Read from recent parquet files to find post URLs
        recent_files = self._find_recent_data_files()
        
        for file_path in recent_files[:10]:  # Limit for memory
            try:
                post_urls = self._extract_post_urls_from_file(file_path)
                
                for url, metadata in post_urls:
                    if len(self.discovered_urls) >= limit:
                        break
                        
                    if url not in self.discovered_urls:
                        self.discovered_urls.add(url)
                        post_jobs.append((
                            JobType.POST_DETAIL,
                            url,
                            5,  # Medium priority
                            metadata
                        ))
                        
            except Exception as e:
                logger.error(f"Error processing file {file_path}: {e}")
                continue
        
        added_count = self.job_queue.bulk_add_jobs(post_jobs)
        logger.info(f"✅ Added {added_count} post detail URLs to queue")
        
        return added_count
    
    def _find_recent_data_files(self) -> List[str]:
        """Find recent parquet files containing post data"""
        files = []
        data_dir = "/data/raw_parquet"
        
        if not os.path.exists(data_dir):
            return files
        
        # Look in recent date directories
        for days_back in range(7):  # Last week
            date_str = (datetime.now() - timedelta(days=days_back)).strftime("%Y-%m-%d")
            date_dir = os.path.join(data_dir, date_str)
            
            if os.path.exists(date_dir):
                for filename in os.listdir(date_dir):
                    if filename.startswith("posts_") and filename.endswith(".parquet"):
                        files.append(os.path.join(date_dir, filename))
        
        return sorted(files, reverse=True)  # Most recent first
    
    def _extract_post_urls_from_file(self, file_path: str) -> List[tuple]:
        """Extract individual post URLs from a parquet file"""
        try:
            import pandas as pd
            df = pd.read_parquet(file_path)
            
            urls = []
            for _, row in df.iterrows():
                subreddit = row.get('subreddit', '')
                post_id = row.get('id', '')
                
                if subreddit and post_id:
                    # Individual post URL
                    url = f"https://www.reddit.com/r/{subreddit}/comments/{post_id}.json"
                    metadata = {
                        "subreddit": subreddit,
                        "post_id": post_id,
                        "title": row.get('title', '')[:100],  # Truncate
                        "score": row.get('score', 0),
                        "phase": "post_detail"
                    }
                    urls.append((url, metadata))
            
            return urls
            
        except Exception as e:
            logger.error(f"Error reading {file_path}: {e}")
            return []
    
    def estimate_total_work(self, subreddits: List[str], 
                          pages_per_subreddit: int = 5) -> Dict[str, Any]:
        """
        Estimate total work and memory requirements
        """
        # Estimate URLs
        sort_types = ["new", "hot", "top"]
        subreddit_pages = len(subreddits) * len(sort_types) * pages_per_subreddit
        
        # Estimate posts (100 per page)
        estimated_posts = subreddit_pages * 100
        
        # Comments (if enabled)
        comments_per_post = SETTINGS.max_comments_per_post if SETTINGS.fetch_comments else 0
        estimated_comment_jobs = estimated_posts * (1 if comments_per_post > 0 else 0)
        
        total_urls = subreddit_pages + estimated_posts + estimated_comment_jobs
        
        memory_estimate = estimate_memory_usage(total_urls)
        
        # Time estimates
        request_delay = SETTINGS.request_delay
        
        # Sequential time (worst case)
        sequential_time_hours = (total_urls * request_delay) / 3600
        
        # Parallel time estimates
        worker_counts = [10, 25, 50, 100]
        parallel_estimates = {}
        
        for workers in worker_counts:
            # Account for setup overhead and coordination
            parallel_time_minutes = (total_urls / workers * request_delay / 60) * 1.2
            speedup = sequential_time_hours * 60 / parallel_time_minutes
            
            parallel_estimates[f"{workers}_workers"] = {
                "time_minutes": round(parallel_time_minutes, 1),
                "speedup": f"{speedup:.0f}x",
                "memory_per_worker_mb": round(memory_estimate["estimated_mb"] / workers + 50, 1)
            }
        
        return {
            "subreddit_pages": subreddit_pages,
            "estimated_posts": estimated_posts,
            "estimated_comments": estimated_comment_jobs,
            "total_urls": total_urls,
            "memory_estimate": memory_estimate,
            "sequential_time_hours": round(sequential_time_hours, 2),
            "parallel_estimates": parallel_estimates,
            "recommendation": self._get_worker_recommendation(total_urls)
        }
    
    def _get_worker_recommendation(self, total_urls: int) -> Dict[str, Any]:
        """Recommend optimal worker configuration for 16GB RAM"""
        if total_urls < 1000:
            return {"workers": 5, "reason": "Small job, low parallelism needed"}
        elif total_urls < 10000:
            return {"workers": 15, "reason": "Medium job, moderate parallelism"}
        elif total_urls < 50000:
            return {"workers": 30, "reason": "Large job, high parallelism with memory consideration"}
        else:
            return {"workers": 25, "reason": "Very large job, memory-limited parallelism"}
    
    def run_discovery_phase(self, subreddits: List[str]) -> Dict[str, Any]:
        """
        Run complete discovery phase
        """
        logger.info("🚀 Starting URL discovery phase...")
        
        start_time = time.time()
        
        # Phase 1: Discover subreddit listing pages
        page_count = self.discover_subreddit_pages(
            subreddits,
            SETTINGS.pages_per_subreddit
        )
        
        # Get stats after phase 1
        stats_after_pages = self.job_queue.get_stats()
        
        discovery_time = time.time() - start_time
        
        result = {
            "discovery_time_seconds": round(discovery_time, 2),
            "phase_1_pages": page_count,
            "total_discovered_urls": len(self.discovered_urls),
            "queue_stats": stats_after_pages,
            "memory_estimate": estimate_memory_usage(len(self.discovered_urls))
        }
        
        logger.info(f"✅ Discovery completed in {discovery_time:.1f}s")
        logger.info(f"   Pages discovered: {page_count}")
        logger.info(f"   Memory usage: {result['memory_estimate']['estimated_mb']:.1f} MB")
        
        return result


def run_discovery_cli():
    """CLI entry point for discovery phase"""
    logger.info("🔍 Starting Reddit URL Discovery...")
    
    # Initialize job queue
    queue = JobQueue()
    queue.clear_all()  # Fresh start
    
    # Initialize discovery
    discovery = URLDiscovery(queue)
    
    # Get estimate first
    estimate = discovery.estimate_total_work(SETTINGS.subreddits, SETTINGS.pages_per_subreddit)
    
    logger.info("📊 Work Estimate:")
    logger.info(f"  Total URLs: {estimate['total_urls']:,}")
    logger.info(f"  Memory needed: {estimate['memory_estimate']['estimated_mb']:.1f} MB")
    logger.info(f"  Sequential time: {estimate['sequential_time_hours']:.1f} hours")
    logger.info(f"  Recommended workers: {estimate['recommendation']['workers']}")
    
    # Ask for confirmation if huge job
    if estimate['total_urls'] > 50000:
        response = input(f"\n⚠️  Large job ({estimate['total_urls']:,} URLs). Continue? [y/N]: ")
        if response.lower() != 'y':
            logger.info("Discovery cancelled by user")
            return
    
    # Run discovery
    result = discovery.run_discovery_phase(SETTINGS.subreddits)
    
    logger.info("📊 Discovery Results:")
    for key, value in result.items():
        if isinstance(value, dict):
            logger.info(f"  {key}:")
            for k, v in value.items():
                logger.info(f"    {k}: {v}")
        else:
            logger.info(f"  {key}: {value}")


if __name__ == "__main__":
    run_discovery_cli()