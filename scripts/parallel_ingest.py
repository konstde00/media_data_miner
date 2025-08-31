"""
Main Orchestrator for Parallel Reddit Scraping
Coordinates discovery phase and parallel workers
"""
import os
import time
import json
import signal
import sys
from typing import Dict, Any, Optional
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed
import subprocess
from app.config import SETTINGS
from .job_queue import JobQueue, estimate_memory_usage
from .discovery import URLDiscovery
from .parallel_worker import WorkerPool, MemoryOptimizedWorker
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class ParallelIngestOrchestrator:
    """
    Orchestrates parallel Reddit scraping with memory optimization
    Designed for 16GB RAM systems with configurable worker pools
    """
    
    def __init__(self, 
                 num_workers: int = None,
                 memory_per_worker_mb: int = 400,
                 enable_discovery: bool = True):
        
        self.num_workers = self._calculate_optimal_workers(num_workers)
        self.memory_per_worker_mb = memory_per_worker_mb
        self.enable_discovery = enable_discovery
        
        self.job_queue = JobQueue()
        self.discovery = URLDiscovery(self.job_queue)
        self.worker_pool = None
        
        self.stats = {
            "start_time": None,
            "discovery_time": 0,
            "worker_time": 0,
            "total_jobs": 0,
            "total_records": 0,
            "success_rate": 0
        }
        
        # Setup signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._handle_shutdown)
        signal.signal(signal.SIGTERM, self._handle_shutdown)
        self.shutting_down = False
    
    def _calculate_optimal_workers(self, requested_workers: Optional[int]) -> int:
        """Calculate optimal number of workers for 16GB RAM system"""
        if requested_workers:
            return min(requested_workers, 50)  # Hard limit for safety
        
        # Conservative defaults based on available memory
        # Leave 4GB for system, 2GB for other services
        available_memory_gb = 10  # Conservative estimate for worker pool
        
        if available_memory_gb >= 8:
            return 20  # Good balance for large jobs
        elif available_memory_gb >= 4:
            return 10  # Moderate parallelism
        else:
            return 5   # Conservative for limited memory
    
    def _handle_shutdown(self, signum, frame):
        """Handle graceful shutdown"""
        logger.info("🛑 Received shutdown signal, gracefully stopping...")
        self.shutting_down = True
        
        if self.worker_pool:
            self.worker_pool.stop()
    
    def run_full_pipeline(self, 
                         subreddits: Optional[list] = None,
                         max_runtime_minutes: Optional[int] = None) -> Dict[str, Any]:
        """
        Run complete parallel scraping pipeline
        """
        logger.info("🚀 Starting Parallel Reddit Scraping Pipeline")
        logger.info(f"   Workers: {self.num_workers}")
        logger.info(f"   Memory per worker: {self.memory_per_worker_mb} MB")
        logger.info(f"   Total estimated memory: {self.num_workers * self.memory_per_worker_mb} MB")
        
        self.stats["start_time"] = time.time()
        
        if subreddits is None:
            subreddits = SETTINGS.subreddits
        
        try:
            # Phase 1: Discovery (if enabled)
            if self.enable_discovery:
                discovery_result = self._run_discovery_phase(subreddits)
                if not discovery_result["success"]:
                    return discovery_result
            else:
                logger.info("⏭️  Skipping discovery phase - using existing queue")
            
            # Phase 2: Parallel scraping
            scraping_result = self._run_parallel_scraping(max_runtime_minutes)
            
            # Phase 3: Cleanup and final stats
            final_result = self._finalize_pipeline(scraping_result)
            
            return final_result
            
        except KeyboardInterrupt:
            logger.info("Pipeline interrupted by user")
            return {"success": False, "error": "Interrupted by user"}
        except Exception as e:
            logger.error(f"Pipeline failed: {e}")
            return {"success": False, "error": str(e)}
    
    def _run_discovery_phase(self, subreddits: list) -> Dict[str, Any]:
        """Run URL discovery phase"""
        logger.info("🔍 Phase 1: URL Discovery")
        
        discovery_start = time.time()
        
        try:
            # Get work estimate first
            estimate = self.discovery.estimate_total_work(
                subreddits, 
                SETTINGS.pages_per_subreddit
            )
            
            logger.info("📊 Work Estimate:")
            logger.info(f"   Total URLs: {estimate['total_urls']:,}")
            logger.info(f"   Memory needed: {estimate['memory_estimate']['estimated_mb']:.1f} MB")
            logger.info(f"   Recommended workers: {estimate['recommendation']['workers']}")
            
            # Check if we have optimal worker count
            recommended = estimate['recommendation']['workers']
            if self.num_workers > recommended + 10:
                logger.warning(f"⚠️  Using {self.num_workers} workers, recommended: {recommended}")
            
            # Run discovery
            discovery_result = self.discovery.run_discovery_phase(subreddits)
            
            self.stats["discovery_time"] = time.time() - discovery_start
            self.stats["total_jobs"] = discovery_result.get("total_discovered_urls", 0)
            
            logger.info(f"✅ Discovery completed in {self.stats['discovery_time']:.1f}s")
            logger.info(f"   URLs discovered: {self.stats['total_jobs']:,}")
            
            if self.stats['total_jobs'] == 0:
                logger.warning("⚠️  No URLs discovered - check subreddit names and filters")
                return {"success": False, "error": "No URLs discovered"}
            
            return {
                "success": True,
                "discovery_result": discovery_result,
                "estimate": estimate
            }
            
        except Exception as e:
            logger.error(f"Discovery phase failed: {e}")
            return {"success": False, "error": f"Discovery failed: {e}"}
    
    def _run_parallel_scraping(self, max_runtime_minutes: Optional[int]) -> Dict[str, Any]:
        """Run parallel scraping phase"""
        logger.info("⚡ Phase 2: Parallel Scraping")
        
        scraping_start = time.time()
        
        try:
            # Initialize worker pool
            self.worker_pool = WorkerPool(
                num_workers=self.num_workers,
                memory_per_worker_mb=self.memory_per_worker_mb
            )
            
            # Calculate max jobs per worker if runtime limit set
            max_jobs_per_worker = None
            if max_runtime_minutes:
                # Conservative estimate: 1 job per second per worker
                max_jobs_per_worker = max_runtime_minutes * 60 // self.num_workers
                logger.info(f"   Runtime limit: {max_runtime_minutes} minutes")
                logger.info(f"   Max jobs per worker: {max_jobs_per_worker}")
            
            # Start workers
            self.worker_pool.start(max_jobs_per_worker)
            
            # Monitor progress
            self._monitor_progress(max_runtime_minutes)
            
            # Wait for completion
            timeout_seconds = max_runtime_minutes * 60 if max_runtime_minutes else None
            self.worker_pool.wait_completion(timeout_seconds)
            
            self.stats["worker_time"] = time.time() - scraping_start
            
            # Get final queue stats
            final_stats = self.job_queue.get_stats()
            
            logger.info(f"✅ Parallel scraping completed in {self.stats['worker_time']:.1f}s")
            logger.info(f"   Jobs completed: {final_stats['completed']:,}")
            logger.info(f"   Jobs failed: {final_stats['failed']:,}")
            
            success_rate = final_stats['completed'] / max(final_stats['total'], 1) * 100
            self.stats["success_rate"] = success_rate
            
            return {
                "success": True,
                "final_stats": final_stats,
                "success_rate": success_rate
            }
            
        except Exception as e:
            logger.error(f"Parallel scraping failed: {e}")
            return {"success": False, "error": f"Parallel scraping failed: {e}"}
    
    def _monitor_progress(self, max_runtime_minutes: Optional[int]):
        """Monitor scraping progress in background thread"""
        def monitor():
            start_time = time.time()
            last_completed = 0
            
            while not self.shutting_down:
                try:
                    stats = self.job_queue.get_stats()
                    
                    # Calculate progress
                    total = stats.get('total', 0)
                    completed = stats.get('completed', 0)
                    failed = stats.get('failed', 0)
                    in_progress = stats.get('in_progress', 0)
                    
                    progress_pct = (completed + failed) / max(total, 1) * 100
                    
                    # Calculate rate
                    current_time = time.time()
                    elapsed_minutes = (current_time - start_time) / 60
                    jobs_per_minute = (completed - last_completed) / max(1, 1)  # Per minute average
                    
                    logger.info(f"📊 Progress: {progress_pct:.1f}% | "
                              f"Completed: {completed:,} | "
                              f"Failed: {failed:,} | "
                              f"In Progress: {in_progress:,} | "
                              f"Rate: {jobs_per_minute:.0f}/min")
                    
                    last_completed = completed
                    
                    # Check runtime limit
                    if max_runtime_minutes and elapsed_minutes >= max_runtime_minutes:
                        logger.info(f"⏰ Runtime limit of {max_runtime_minutes} minutes reached")
                        self.shutting_down = True
                        break
                    
                    time.sleep(30)  # Monitor every 30 seconds
                    
                except Exception as e:
                    logger.error(f"Monitoring error: {e}")
                    time.sleep(30)
        
        # Start monitoring in background thread
        import threading
        monitor_thread = threading.Thread(target=monitor, daemon=True)
        monitor_thread.start()
    
    def _finalize_pipeline(self, scraping_result: Dict[str, Any]) -> Dict[str, Any]:
        """Finalize pipeline and generate summary"""
        logger.info("🏁 Phase 3: Finalizing Pipeline")
        
        total_time = time.time() - self.stats["start_time"]
        
        # Collect output files
        output_files = self._collect_output_files()
        
        # Generate summary
        summary = {
            "success": scraping_result.get("success", False),
            "total_runtime_seconds": round(total_time, 2),
            "total_runtime_minutes": round(total_time / 60, 2),
            "discovery_time_seconds": round(self.stats["discovery_time"], 2),
            "scraping_time_seconds": round(self.stats["worker_time"], 2),
            "num_workers": self.num_workers,
            "memory_per_worker_mb": self.memory_per_worker_mb,
            "final_stats": scraping_result.get("final_stats", {}),
            "success_rate_percent": round(self.stats["success_rate"], 2),
            "output_files": output_files,
            "speedup_estimate": self._calculate_speedup()
        }
        
        # Log final summary
        logger.info("📋 Final Summary:")
        logger.info(f"   ✅ Success: {summary['success']}")
        logger.info(f"   ⏱️  Total time: {summary['total_runtime_minutes']:.1f} minutes")
        logger.info(f"   📊 Success rate: {summary['success_rate_percent']:.1f}%")
        logger.info(f"   📁 Output files: {len(summary['output_files'])}")
        logger.info(f"   🚀 Estimated speedup: {summary['speedup_estimate']}x")
        
        # Cleanup
        self._cleanup()
        
        return summary
    
    def _collect_output_files(self) -> list:
        """Collect all output files generated"""
        files = []
        data_dir = f"/data/raw_parquet/{datetime.now().strftime('%Y-%m-%d')}"
        
        if os.path.exists(data_dir):
            for filename in os.listdir(data_dir):
                if filename.startswith("parallel_") and filename.endswith(".parquet"):
                    file_path = os.path.join(data_dir, filename)
                    file_size = os.path.getsize(file_path) / 1024 / 1024  # MB
                    
                    files.append({
                        "filename": filename,
                        "path": file_path,
                        "size_mb": round(file_size, 2)
                    })
        
        return sorted(files, key=lambda x: x["filename"])
    
    def _calculate_speedup(self) -> str:
        """Calculate estimated speedup vs sequential scraping"""
        if self.stats["worker_time"] == 0:
            return "N/A"
        
        # Estimate sequential time (rough approximation)
        final_stats = self.job_queue.get_stats()
        completed_jobs = final_stats.get('completed', 0)
        
        if completed_jobs == 0:
            return "N/A"
        
        # Sequential would be: jobs * request_delay
        sequential_time = completed_jobs * SETTINGS.request_delay
        actual_time = self.stats["worker_time"]
        
        speedup = sequential_time / max(actual_time, 1)
        return f"{speedup:.0f}"
    
    def _cleanup(self):
        """Cleanup resources"""
        try:
            # Reset stalled jobs
            self.job_queue.reset_stalled_jobs()
            
            # Cleanup old jobs (keep last 3 days)
            self.job_queue.cleanup_old_jobs(days=3)
            
            logger.info("🧹 Cleanup completed")
            
        except Exception as e:
            logger.error(f"Cleanup error: {e}")


def run_parallel_cli():
    """CLI entry point for parallel ingestion"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Parallel Reddit Scraping")
    parser.add_argument("--workers", type=int, help="Number of parallel workers")
    parser.add_argument("--memory-per-worker", type=int, default=400, 
                       help="Memory limit per worker (MB)")
    parser.add_argument("--max-runtime", type=int, 
                       help="Maximum runtime in minutes")
    parser.add_argument("--subreddits", type=str,
                       help="Comma-separated subreddit list")
    parser.add_argument("--no-discovery", action="store_true",
                       help="Skip discovery phase, use existing queue")
    parser.add_argument("--estimate-only", action="store_true",
                       help="Only show work estimate, don't run")
    
    args = parser.parse_args()
    
    # Parse subreddits
    subreddits = None
    if args.subreddits:
        subreddits = [s.strip() for s in args.subreddits.split(",")]
    
    # Show estimate only
    if args.estimate_only:
        job_queue = JobQueue()
        discovery = URLDiscovery(job_queue)
        
        estimate = discovery.estimate_total_work(
            subreddits or SETTINGS.subreddits,
            SETTINGS.pages_per_subreddit
        )
        
        print("\n📊 Work Estimate:")
        print(f"   Total URLs: {estimate['total_urls']:,}")
        print(f"   Memory needed: {estimate['memory_estimate']['estimated_mb']:.1f} MB")
        print(f"   Sequential time: {estimate['sequential_time_hours']:.1f} hours")
        print("\n   Parallel Estimates:")
        for key, value in estimate['parallel_estimates'].items():
            print(f"     {key}: {value}")
        print(f"\n   Recommendation: {estimate['recommendation']['workers']} workers")
        print(f"   Reason: {estimate['recommendation']['reason']}")
        return
    
    # Run orchestrator
    orchestrator = ParallelIngestOrchestrator(
        num_workers=args.workers,
        memory_per_worker_mb=args.memory_per_worker,
        enable_discovery=not args.no_discovery
    )
    
    result = orchestrator.run_full_pipeline(
        subreddits=subreddits,
        max_runtime_minutes=args.max_runtime
    )
    
    # Save result to file
    result_file = f"/data/parallel_result_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    with open(result_file, 'w') as f:
        json.dump(result, f, indent=2, default=str)
    
    print(f"\n📄 Full results saved to: {result_file}")
    
    # Exit with appropriate code
    sys.exit(0 if result["success"] else 1)


if __name__ == "__main__":
    run_parallel_cli()