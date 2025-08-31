"""
Parallel Worker Implementation for Reddit Scraping
Memory-optimized worker that processes jobs from the queue
"""
import os
import json
import time
import uuid
import pandas as pd
from typing import Dict, Any, Optional, List
from datetime import datetime
from multiprocessing import Process
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import signal
import sys
from app.config import SETTINGS
from app.utils import RequestHandler, extract_post_fields, validate_post_data, clean_text
from app.path_utils import get_safe_data_path
import logging
from app.logging_config import setup_logging, get_logger
from .job_queue import JobQueue, JobStatus, JobType

# Setup logging if not already configured
if not logging.getLogger().handlers:
    setup_logging()

logger = get_logger(__name__)


class MemoryOptimizedWorker:
    """
    Memory-optimized worker for parallel Reddit scraping
    Designed to fit within 16GB RAM system constraints
    """
    
    def __init__(self, worker_id: str = None, 
                 batch_size: int = 100,
                 memory_limit_mb: int = 512):
        self.worker_id = worker_id or f"worker-{uuid.uuid4().hex[:8]}"
        self.batch_size = batch_size
        self.memory_limit_mb = memory_limit_mb
        
        self.job_queue = JobQueue()
        self.handler = RequestHandler()
        
        self.stats = {
            "jobs_completed": 0,
            "jobs_failed": 0,
            "start_time": time.time(),
            "total_records": 0,
            "current_memory_mb": 0
        }
        
        self.running = True
        self.current_batch: List[Dict] = []
        
        # Setup graceful shutdown
        signal.signal(signal.SIGINT, self._handle_shutdown)
        signal.signal(signal.SIGTERM, self._handle_shutdown)
    
    def _handle_shutdown(self, signum, frame):
        """Handle graceful shutdown"""
        logger.info(f"Worker {self.worker_id} received shutdown signal")
        self.running = False
    
    def run(self, max_jobs: Optional[int] = None):
        """Main worker loop"""
        logger.info(f"🚀 Worker {self.worker_id} starting...")
        logger.info(f"   Memory limit: {self.memory_limit_mb} MB")
        logger.info(f"   Batch size: {self.batch_size}")
        
        jobs_processed = 0
        
        try:
            while self.running and (max_jobs is None or jobs_processed < max_jobs):
                try:
                    # Get next job
                    job = self.job_queue.get_next_job(self.worker_id)
                    
                    if not job:
                        logger.debug(f"Worker {self.worker_id} - no jobs available, waiting...")
                        time.sleep(5)
                        continue
                    
                    logger.debug(f"Worker {self.worker_id} processing job {job['id']}: {job['url'][:80]}...")
                    
                    # Process job based on type
                    success = self._process_job(job)
                    
                    if success:
                        self.stats["jobs_completed"] += 1
                        logger.debug(f"✅ Job {job['id']} completed")
                    else:
                        self.stats["jobs_failed"] += 1
                        logger.debug(f"❌ Job {job['id']} failed")
                    
                    jobs_processed += 1
                    
                    # Memory management - flush batch if getting full
                    if len(self.current_batch) >= self.batch_size:
                        self._flush_current_batch()
                    
                    # Adaptive delay based on memory usage
                    self._adaptive_delay()
                    
                except KeyboardInterrupt:
                    logger.info(f"Worker {self.worker_id} interrupted by user")
                    break
                except Exception as e:
                    logger.error(f"Worker {self.worker_id} error: {e}")
                    time.sleep(1)
        finally:
            # Always flush remaining data and log stats
            try:
                self._flush_current_batch()
            except Exception as e:
                logger.error(f"Failed to flush final batch: {e}")
            
            # Final stats
            self._log_final_stats()
            logger.info(f"Worker {self.worker_id} shutting down")
    
    def _process_job(self, job: Dict[str, Any]) -> bool:
        """Process a single job"""
        try:
            job_type = JobType(job['job_type'])
            url = job['url']
            metadata = json.loads(job['metadata']) if job['metadata'] else {}
            
            if job_type == JobType.SUBREDDIT_PAGE:
                return self._process_subreddit_page(job['id'], url, metadata)
            elif job_type == JobType.POST_DETAIL:
                return self._process_post_detail(job['id'], url, metadata)
            elif job_type == JobType.COMMENT_THREAD:
                return self._process_comment_thread(job['id'], url, metadata)
            else:
                logger.error(f"Unknown job type: {job_type}")
                self.job_queue.fail_job(job['id'], f"Unknown job type: {job_type}")
                return False
                
        except Exception as e:
            logger.error(f"Error processing job {job['id']}: {e}")
            self.job_queue.fail_job(job['id'], str(e))
            return False
    
    def _process_subreddit_page(self, job_id: int, url: str, metadata: Dict) -> bool:
        """Process a subreddit listing page"""
        try:
            # Make request
            data = self.handler.make_request(url)
            if not data:
                self.job_queue.fail_job(job_id, "Failed to fetch data")
                return False
            
            # Extract posts
            children = data.get("data", {}).get("children", [])
            posts = []
            
            for item in children:
                if item.get("kind") != "t3":
                    continue
                
                post_data = item.get("data", {})
                if not validate_post_data(post_data):
                    continue
                
                # Apply filters
                if not self._passes_filters(post_data):
                    continue
                
                # Extract post info
                post_info = extract_post_fields(post_data)
                post_info['discovered_by'] = self.worker_id
                post_info['batch_id'] = f"{metadata.get('subreddit', 'unknown')}_{int(time.time())}"
                
                posts.append(post_info)
            
            # Add to current batch
            self.current_batch.extend(posts)
            self.stats["total_records"] += len(posts)
            
            # Mark job complete
            self.job_queue.complete_job(job_id, record_count=len(posts))
            
            return True
            
        except Exception as e:
            logger.error(f"Error processing subreddit page {url}: {e}")
            self.job_queue.fail_job(job_id, str(e))
            return False
    
    def _process_post_detail(self, job_id: int, url: str, metadata: Dict) -> bool:
        """Process individual post detail"""
        try:
            data = self.handler.make_request(url)
            if not data or not isinstance(data, list) or len(data) < 1:
                self.job_queue.fail_job(job_id, "Invalid post data structure")
                return False
            
            # Extract post data (first element is post)
            post_item = data[0].get("data", {}).get("children", [])
            if not post_item:
                self.job_queue.fail_job(job_id, "No post data found")
                return False
            
            post_data = post_item[0].get("data", {})
            if not validate_post_data(post_data):
                self.job_queue.fail_job(job_id, "Post data validation failed")
                return False
            
            post_info = extract_post_fields(post_data)
            post_info['detail_fetch'] = True
            post_info['discovered_by'] = self.worker_id
            
            self.current_batch.append(post_info)
            self.stats["total_records"] += 1
            
            self.job_queue.complete_job(job_id, record_count=1)
            return True
            
        except Exception as e:
            logger.error(f"Error processing post detail {url}: {e}")
            self.job_queue.fail_job(job_id, str(e))
            return False
    
    def _process_comment_thread(self, job_id: int, url: str, metadata: Dict) -> bool:
        """Process comment thread (if comments enabled)"""
        if not SETTINGS.fetch_comments:
            self.job_queue.complete_job(job_id, record_count=0)
            return True
        
        try:
            data = self.handler.make_request(url)
            if not data or not isinstance(data, list) or len(data) < 2:
                self.job_queue.fail_job(job_id, "Invalid comment data structure")
                return False
            
            # Extract comments (second element is comments)
            comments_data = data[1].get("data", {}).get("children", [])
            comments = []
            
            self._extract_comment_tree(comments_data, comments, metadata.get('post_id', ''))
            
            # Limit comments per post
            if len(comments) > SETTINGS.max_comments_per_post:
                comments = comments[:SETTINGS.max_comments_per_post]
            
            self.current_batch.extend(comments)
            self.stats["total_records"] += len(comments)
            
            self.job_queue.complete_job(job_id, record_count=len(comments))
            return True
            
        except Exception as e:
            logger.error(f"Error processing comments {url}: {e}")
            self.job_queue.fail_job(job_id, str(e))
            return False
    
    def _extract_comment_tree(self, comments: List, result: List, post_id: str, parent_id: str = None, depth: int = 0):
        """Recursively extract comment tree"""
        for comment in comments:
            if comment.get("kind") != "t1":  # t1 = comment
                continue
            
            comment_data = comment.get("data", {})
            if not comment_data.get("body") or comment_data.get("body") in ["[deleted]", "[removed]"]:
                continue
            
            comment_info = {
                "id": comment_data.get("id"),
                "post_id": post_id,
                "parent_id": parent_id or post_id,
                "body": clean_text(comment_data.get("body", "")),
                "author": comment_data.get("author", "[deleted]"),
                "score": comment_data.get("score", 0),
                "created_utc": comment_data.get("created_utc", 0),
                "depth": depth,
                "discovered_by": self.worker_id
            }
            
            result.append(comment_info)
            
            # Recursively process replies
            replies = comment_data.get("replies")
            if isinstance(replies, dict) and replies.get("data", {}).get("children"):
                self._extract_comment_tree(
                    replies["data"]["children"],
                    result,
                    post_id,
                    comment_data["id"],
                    depth + 1
                )
    
    def _passes_filters(self, post_data: Dict) -> bool:
        """Check if post passes configured filters"""
        # Date filter
        created_utc = post_data.get("created_utc", 0)
        if not self._within_days(created_utc, SETTINGS.ingest_days):
            return False
        
        # Score filter
        score = post_data.get("score", 0)
        if score < SETTINGS.min_score:
            return False
        
        return True
    
    def _within_days(self, created_utc: float, days: int) -> bool:
        """Check if timestamp is within specified days"""
        from datetime import timezone, timedelta
        dt = datetime.fromtimestamp(created_utc, tz=timezone.utc)
        return dt >= datetime.now(tz=timezone.utc) - timedelta(days=days)
    
    def _flush_current_batch(self):
        """Save current batch to disk and clear memory"""
        if not self.current_batch:
            return
        
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            date_str = datetime.now().strftime('%Y-%m-%d')
            
            filename = f"parallel_{self.worker_id}_{timestamp}.parquet"
            output_path = get_safe_data_path(date_str, filename)
            
            df = pd.DataFrame(self.current_batch)
            df.to_parquet(output_path, index=False)
            
            logger.info(f"💾 Worker {self.worker_id} saved {len(self.current_batch)} records to {filename}")
            
            # Clear batch from memory
            self.current_batch.clear()
            
        except Exception as e:
            logger.error(f"Error saving batch: {e}")
    
    def _adaptive_delay(self):
        """Adaptive delay based on memory usage"""
        # Monitor memory usage (simplified)
        try:
            import psutil
            process = psutil.Process()
            memory_mb = process.memory_info().rss / 1024 / 1024
            self.stats["current_memory_mb"] = memory_mb
        except ImportError:
            # Fallback if psutil not available
            logger.debug("psutil not available, memory monitoring disabled")
            self.stats["current_memory_mb"] = 0
            memory_mb = 0
        except Exception as e:
            logger.debug(f"Could not get memory info: {e}")
            memory_mb = 0
            self.stats["current_memory_mb"] = 0
        
        if memory_mb > self.memory_limit_mb * 0.8:
            # Near memory limit, slow down
            delay = SETTINGS.request_delay * 2
            logger.debug(f"Worker {self.worker_id} near memory limit ({memory_mb:.1f}MB), slowing down")
        else:
            # Normal operation
            delay = SETTINGS.request_delay
        
        time.sleep(delay)
    
    def _log_final_stats(self):
        """Log final worker statistics"""
        runtime = time.time() - self.stats["start_time"]
        rate = self.stats["jobs_completed"] / max(runtime, 1) * 60  # jobs per minute
        
        logger.info(f"📊 Worker {self.worker_id} Final Stats:")
        logger.info(f"   Runtime: {runtime:.1f} seconds")
        logger.info(f"   Jobs completed: {self.stats['jobs_completed']}")
        logger.info(f"   Jobs failed: {self.stats['jobs_failed']}")
        logger.info(f"   Records processed: {self.stats['total_records']}")
        logger.info(f"   Rate: {rate:.1f} jobs/minute")
        logger.info(f"   Peak memory: {self.stats['current_memory_mb']:.1f} MB")


class WorkerPool:
    """
    Manages multiple parallel workers
    Memory-optimized for 16GB RAM systems
    """
    
    def __init__(self, num_workers: int = 10, memory_per_worker_mb: int = 512):
        self.num_workers = num_workers
        self.memory_per_worker_mb = memory_per_worker_mb
        self.workers: List[Process] = []
        self.running = False
        
        # Calculate total memory usage
        total_memory_mb = num_workers * memory_per_worker_mb
        if total_memory_mb > 12000:  # Leave 4GB for system
            logger.warning(f"⚠️ High memory usage: {total_memory_mb}MB for {num_workers} workers")
    
    def start(self, max_jobs_per_worker: Optional[int] = None):
        """Start all workers"""
        logger.info(f"🚀 Starting worker pool with {self.num_workers} workers...")
        
        self.running = True
        
        for i in range(self.num_workers):
            worker_id = f"worker-{i+1:02d}"
            
            process = Process(
                target=self._run_worker,
                args=(worker_id, max_jobs_per_worker)
            )
            process.start()
            self.workers.append(process)
            
            logger.info(f"   Started {worker_id} (PID: {process.pid})")
        
        logger.info(f"✅ All {self.num_workers} workers started")
    
    def _run_worker(self, worker_id: str, max_jobs: Optional[int]):
        """Run a single worker (called in separate process)"""
        try:
            worker = MemoryOptimizedWorker(
                worker_id=worker_id,
                memory_limit_mb=self.memory_per_worker_mb
            )
            worker.run(max_jobs)
        except Exception as e:
            logger.error(f"Worker {worker_id} crashed: {e}")
    
    def wait_completion(self, timeout: Optional[float] = None):
        """Wait for all workers to complete"""
        logger.info("⏳ Waiting for workers to complete...")
        
        for i, process in enumerate(self.workers):
            try:
                process.join(timeout)
                if process.is_alive():
                    logger.warning(f"Worker {i+1} still running after timeout")
                else:
                    logger.info(f"✅ Worker {i+1} completed")
            except Exception as e:
                logger.error(f"Error waiting for worker {i+1}: {e}")
        
        logger.info("🏁 All workers finished")
    
    def stop(self):
        """Stop all workers gracefully"""
        logger.info("🛑 Stopping worker pool...")
        
        for i, process in enumerate(self.workers):
            if process.is_alive():
                process.terminate()
                process.join(timeout=5)
                
                if process.is_alive():
                    logger.warning(f"Force killing worker {i+1}")
                    process.kill()
                    process.join()
        
        self.running = False
        logger.info("✅ Worker pool stopped")


def run_single_worker(worker_id: str = None, max_jobs: int = None):
    """CLI entry point for single worker"""
    worker = MemoryOptimizedWorker(worker_id=worker_id or "manual-worker")
    worker.run(max_jobs)


def run_worker_pool(num_workers: int = 10, max_jobs_per_worker: int = None):
    """CLI entry point for worker pool"""
    pool = WorkerPool(num_workers)
    
    try:
        pool.start(max_jobs_per_worker)
        pool.wait_completion()
    except KeyboardInterrupt:
        logger.info("Worker pool interrupted by user")
    finally:
        pool.stop()


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Reddit Parallel Worker")
    parser.add_argument("--workers", type=int, default=10, help="Number of parallel workers")
    parser.add_argument("--max-jobs", type=int, help="Maximum jobs per worker")
    parser.add_argument("--single", action="store_true", help="Run single worker")
    parser.add_argument("--worker-id", type=str, help="Worker ID for single worker")
    
    args = parser.parse_args()
    
    if args.single:
        run_single_worker(args.worker_id, args.max_jobs)
    else:
        run_worker_pool(args.workers, args.max_jobs)