"""
Job Queue Management for Parallel Reddit Scraping
Uses SQLite for lightweight job tracking without external dependencies
"""
import sqlite3
import json
import time
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime, timedelta
from enum import Enum
from contextlib import contextmanager
import threading
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class JobStatus(Enum):
    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    FAILED = "failed"
    RETRY = "retry"


class JobType(Enum):
    SUBREDDIT_PAGE = "subreddit_page"
    POST_DETAIL = "post_detail"
    COMMENT_THREAD = "comment_thread"


class JobQueue:
    """
    SQLite-based job queue for parallel scraping
    Designed to fit in memory for 16GB RAM systems
    """
    
    def __init__(self, db_path: str = "/data/job_queue.db", max_retries: int = 3):
        self.db_path = db_path
        self.max_retries = max_retries
        self._lock = threading.RLock()  # Use RLock for reentrant locking
        self._init_db()
        # Enable WAL mode for better concurrency
        with self._get_conn() as conn:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA busy_timeout=5000")  # 5 second timeout for locks
    
    @contextmanager
    def _get_conn(self):
        """Thread-safe connection context manager"""
        conn = sqlite3.connect(self.db_path, timeout=30.0, isolation_level='DEFERRED')
        conn.row_factory = sqlite3.Row
        # Enable optimizations for concurrent access
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA temp_store=MEMORY")
        try:
            yield conn
            conn.commit()
        except Exception as e:
            conn.rollback()
            raise e
        finally:
            conn.close()
    
    def _init_db(self):
        """Initialize database schema"""
        with self._get_conn() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS jobs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    job_type TEXT NOT NULL,
                    url TEXT NOT NULL UNIQUE,
                    status TEXT NOT NULL DEFAULT 'pending',
                    priority INTEGER DEFAULT 0,
                    retry_count INTEGER DEFAULT 0,
                    worker_id TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    started_at TIMESTAMP,
                    completed_at TIMESTAMP,
                    error_message TEXT,
                    metadata TEXT
                )
            """)
            
            # Create indexes for performance
            conn.execute("CREATE INDEX IF NOT EXISTS idx_status ON jobs(status)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_priority ON jobs(priority DESC)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_url ON jobs(url)")
            
            # Results table for storing scraped data references
            conn.execute("""
                CREATE TABLE IF NOT EXISTS results (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    job_id INTEGER NOT NULL,
                    data_path TEXT NOT NULL,
                    record_count INTEGER,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (job_id) REFERENCES jobs(id)
                )
            """)
            
            # Stats table for monitoring
            conn.execute("""
                CREATE TABLE IF NOT EXISTS stats (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    total_jobs INTEGER,
                    pending_jobs INTEGER,
                    completed_jobs INTEGER,
                    failed_jobs INTEGER,
                    avg_duration_seconds REAL
                )
            """)
    
    def add_job(self, job_type: JobType, url: str, priority: int = 0, 
                metadata: Optional[Dict] = None) -> Optional[int]:
        """Add a new job to the queue"""
        with self._lock:
            try:
                with self._get_conn() as conn:
                    cursor = conn.execute("""
                        INSERT OR IGNORE INTO jobs (job_type, url, priority, metadata)
                        VALUES (?, ?, ?, ?)
                    """, (job_type.value, url, priority, 
                          json.dumps(metadata) if metadata else None))
                    return cursor.lastrowid
            except sqlite3.IntegrityError:
                logger.debug(f"Job already exists: {url}")
                return None
    
    def bulk_add_jobs(self, jobs: List[Tuple[JobType, str, int, Optional[Dict]]]) -> int:
        """Bulk insert jobs for efficiency"""
        with self._lock:
            count = 0
            with self._get_conn() as conn:
                for job_type, url, priority, metadata in jobs:
                    try:
                        conn.execute("""
                            INSERT OR IGNORE INTO jobs (job_type, url, priority, metadata)
                            VALUES (?, ?, ?, ?)
                        """, (job_type.value, url, priority,
                              json.dumps(metadata) if metadata else None))
                        count += 1
                    except sqlite3.IntegrityError:
                        pass
            return count
    
    def get_next_job(self, worker_id: str) -> Optional[Dict[str, Any]]:
        """Get next available job for a worker"""
        with self._lock:
            with self._get_conn() as conn:
                # Find next pending job ordered by priority
                cursor = conn.execute("""
                    SELECT * FROM jobs
                    WHERE status = ? OR (status = ? AND retry_count < ?)
                    ORDER BY priority DESC, created_at ASC
                    LIMIT 1
                """, (JobStatus.PENDING.value, JobStatus.RETRY.value, self.max_retries))
                
                row = cursor.fetchone()
                if row:
                    # Mark job as in progress
                    conn.execute("""
                        UPDATE jobs
                        SET status = ?, worker_id = ?, started_at = CURRENT_TIMESTAMP
                        WHERE id = ?
                    """, (JobStatus.IN_PROGRESS.value, worker_id, row['id']))
                    
                    return dict(row)
        return None
    
    def complete_job(self, job_id: int, data_path: Optional[str] = None, 
                    record_count: Optional[int] = None):
        """Mark job as completed"""
        with self._lock:
            with self._get_conn() as conn:
                conn.execute("""
                    UPDATE jobs
                    SET status = ?, completed_at = CURRENT_TIMESTAMP
                    WHERE id = ?
                """, (JobStatus.COMPLETED.value, job_id))
                
                if data_path:
                    conn.execute("""
                        INSERT INTO results (job_id, data_path, record_count)
                        VALUES (?, ?, ?)
                    """, (job_id, data_path, record_count))
    
    def fail_job(self, job_id: int, error_message: str):
        """Mark job as failed and potentially retry"""
        with self._lock:
            with self._get_conn() as conn:
                cursor = conn.execute("SELECT retry_count FROM jobs WHERE id = ?", (job_id,))
                row = cursor.fetchone()
                
                if row and row['retry_count'] < self.max_retries:
                    # Mark for retry
                    conn.execute("""
                        UPDATE jobs
                        SET status = ?, retry_count = retry_count + 1, 
                            error_message = ?, worker_id = NULL
                        WHERE id = ?
                    """, (JobStatus.RETRY.value, error_message, job_id))
                else:
                    # Mark as permanently failed
                    conn.execute("""
                        UPDATE jobs
                        SET status = ?, error_message = ?, completed_at = CURRENT_TIMESTAMP
                        WHERE id = ?
                    """, (JobStatus.FAILED.value, error_message, job_id))
    
    def get_stats(self) -> Dict[str, Any]:
        """Get queue statistics"""
        with self._get_conn() as conn:
            # Count jobs by status
            cursor = conn.execute("""
                SELECT 
                    COUNT(*) as total,
                    SUM(CASE WHEN status = ? THEN 1 ELSE 0 END) as pending,
                    SUM(CASE WHEN status = ? THEN 1 ELSE 0 END) as in_progress,
                    SUM(CASE WHEN status = ? THEN 1 ELSE 0 END) as completed,
                    SUM(CASE WHEN status = ? THEN 1 ELSE 0 END) as failed,
                    SUM(CASE WHEN status = ? THEN 1 ELSE 0 END) as retry
                FROM jobs
            """, (JobStatus.PENDING.value, JobStatus.IN_PROGRESS.value,
                  JobStatus.COMPLETED.value, JobStatus.FAILED.value,
                  JobStatus.RETRY.value))
            
            stats = dict(cursor.fetchone())
            
            # Calculate average duration
            cursor = conn.execute("""
                SELECT AVG(
                    CAST((julianday(completed_at) - julianday(started_at)) * 86400 AS REAL)
                ) as avg_duration
                FROM jobs
                WHERE status = ? AND completed_at IS NOT NULL AND started_at IS NOT NULL
            """, (JobStatus.COMPLETED.value,))
            
            row = cursor.fetchone()
            stats['avg_duration_seconds'] = row['avg_duration'] if row else 0
            
            # Get rate
            cursor = conn.execute("""
                SELECT COUNT(*) as recent_completed
                FROM jobs
                WHERE status = ? AND completed_at > datetime('now', '-1 minute')
            """, (JobStatus.COMPLETED.value,))
            
            row = cursor.fetchone()
            stats['jobs_per_minute'] = row['recent_completed'] if row else 0
            
            return stats
    
    def cleanup_old_jobs(self, days: int = 7):
        """Remove old completed jobs to save space"""
        with self._lock:
            with self._get_conn() as conn:
                cutoff = datetime.now() - timedelta(days=days)
                conn.execute("""
                    DELETE FROM jobs
                    WHERE status IN (?, ?) AND completed_at < ?
                """, (JobStatus.COMPLETED.value, JobStatus.FAILED.value, cutoff))
    
    def reset_stalled_jobs(self, timeout_minutes: int = 30):
        """Reset jobs that have been in progress too long"""
        with self._lock:
            with self._get_conn() as conn:
                cutoff = datetime.now() - timedelta(minutes=timeout_minutes)
                conn.execute("""
                    UPDATE jobs
                    SET status = ?, worker_id = NULL, retry_count = retry_count + 1
                    WHERE status = ? AND started_at < ?
                """, (JobStatus.RETRY.value, JobStatus.IN_PROGRESS.value, cutoff))
    
    def get_pending_count(self) -> int:
        """Get count of pending jobs"""
        with self._get_conn() as conn:
            cursor = conn.execute("""
                SELECT COUNT(*) as count
                FROM jobs
                WHERE status IN (?, ?)
            """, (JobStatus.PENDING.value, JobStatus.RETRY.value))
            return cursor.fetchone()['count']
    
    def clear_all(self):
        """Clear all jobs (for testing/reset)"""
        with self._lock:
            with self._get_conn() as conn:
                conn.execute("DELETE FROM jobs")
                conn.execute("DELETE FROM results")
                conn.execute("DELETE FROM stats")


def estimate_memory_usage(total_urls: int) -> Dict[str, Any]:
    """
    Estimate memory usage for job queue
    Optimized for 16GB RAM system
    """
    # SQLite row overhead ~100 bytes per job
    job_size = 100  # bytes
    
    # URL average length ~80 chars
    url_size = 80  # bytes
    
    # Metadata JSON ~200 bytes
    metadata_size = 200  # bytes
    
    total_per_job = job_size + url_size + metadata_size  # ~380 bytes
    
    total_mb = (total_urls * total_per_job) / (1024 * 1024)
    
    return {
        "total_urls": total_urls,
        "bytes_per_job": total_per_job,
        "estimated_mb": round(total_mb, 2),
        "safe_for_16gb": total_mb < 1000,  # Keep under 1GB for queue
        "recommendation": "Safe" if total_mb < 1000 else "Consider batching"
    }


if __name__ == "__main__":
    # Test the job queue
    queue = JobQueue("/tmp/test_queue.db")
    
    # Add some test jobs
    print("Adding test jobs...")
    for i in range(10):
        queue.add_job(
            JobType.SUBREDDIT_PAGE,
            f"https://reddit.com/r/test/new.json?after={i}",
            priority=i,
            metadata={"subreddit": "test", "page": i}
        )
    
    # Get stats
    print("\nQueue stats:")
    stats = queue.get_stats()
    for key, value in stats.items():
        print(f"  {key}: {value}")
    
    # Simulate worker getting jobs
    print("\nSimulating worker...")
    worker_id = "worker-1"
    
    job = queue.get_next_job(worker_id)
    if job:
        print(f"Got job: {job['url']}")
        queue.complete_job(job['id'], "/tmp/data.parquet", 100)
    
    # Memory estimate
    print("\nMemory estimate for 100k URLs:")
    estimate = estimate_memory_usage(100000)
    for key, value in estimate.items():
        print(f"  {key}: {value}")