from pydantic import BaseModel, Field
from typing import Optional, List
import os

def get_subreddits() -> List[str]:
    """Get subreddits from environment variable"""
    return os.getenv("SUBREDDITS", "startups").split(",")

def get_proxy_pool() -> List[str]:
    """Get proxy pool from environment variable"""
    return os.getenv("PROXY_POOL", "").split(",") if os.getenv("PROXY_POOL") else []

def get_user_agents() -> List[str]:
    """Get default user agent strings"""
    return [
        "RedditScraper/1.0 (by /u/micro-saas-miner)",
        "Mozilla/5.0 (compatible; RedditDataMiner/1.0)",
        "RedditBot/1.0 (+http://example.com/bot)"
    ]

class Settings(BaseModel):
    # Proxy settings (optional)
    https_proxy: Optional[str] = os.getenv("HTTPS_PROXY")
    http_proxy: Optional[str] = os.getenv("HTTP_PROXY")

    # Basic ingestion settings
    subreddits: List[str] = Field(default_factory=get_subreddits)
    ingest_days: int = int(os.getenv("INGEST_DAYS", 7))
    min_score: int = int(os.getenv("MIN_SCORE", 3))

    # Vector database settings
    qdrant_url: str = os.getenv("QDRANT_URL", "http://localhost:6333")
    qdrant_collection: str = os.getenv("QDRANT_COLLECTION", "reddit_chunks")

    # Embedding and reranking models
    embedding_model: str = os.getenv("EMBEDDING_MODEL", "BAAI/bge-m3")
    reranker_model: str = os.getenv("RERANKER_MODEL", "BAAI/bge-reranker-v2-m3")

    # Text chunking settings
    chunk_size_tokens: int = int(os.getenv("CHUNK_SIZE_TOKENS", 600))
    chunk_overlap_tokens: int = int(os.getenv("CHUNK_OVERLAP_TOKENS", 80))

    # Retrieval settings
    topk_retrieve: int = int(os.getenv("TOPK_RETRIEVE", 200))
    topn_rerank: int = int(os.getenv("TOPN_RERANK", 25))

    # LLM settings
    ollama_url: str = os.getenv("OLLAMA_URL", "http://localhost:11434")
    ollama_model: str = os.getenv("OLLAMA_MODEL", "llama3.1:8b-instruct")
    max_idea_tokens: int = int(os.getenv("MAX_IDEA_TOKENS", 800))
    temp_ideas: float = float(os.getenv("TEMPERATURE_IDEAS", 0.7))
    temp_scoring: float = float(os.getenv("TEMPERATURE_SCORING", 0.3))

    # JSON ingestion settings (no API keys required)
    # Sequential scraping (original method)
    request_delay: float = float(os.getenv("REQUEST_DELAY", 1.0))  # seconds between requests
    max_comments_per_post: int = int(os.getenv("MAX_COMMENTS_PER_POST", 500))
    enable_proxy_rotation: bool = os.getenv("ENABLE_PROXY_ROTATION", "false").lower() == "true"
    proxy_pool: List[str] = Field(default_factory=get_proxy_pool)
    user_agents: List[str] = Field(default_factory=get_user_agents)
    pages_per_subreddit: int = int(os.getenv("PAGES_PER_SUBREDDIT", 5))  # 5 pages = ~500 posts
    posts_per_subreddit: int = int(os.getenv("POSTS_PER_SUBREDDIT", 500))  # Alternative to pages
    save_format: str = os.getenv("SAVE_FORMAT", "parquet")  # parquet or jsonl
    fetch_comments: bool = os.getenv("FETCH_COMMENTS", "true").lower() == "true"

    # Parallel scraping settings (optimized for 10GB system)
    parallel_workers: int = int(os.getenv("PARALLEL_WORKERS", 10))
    memory_per_worker_mb: int = int(os.getenv("MEMORY_PER_WORKER_MB", 200))
    parallel_batch_size: int = int(os.getenv("PARALLEL_BATCH_SIZE", 50))
    enable_parallel_discovery: bool = os.getenv("ENABLE_PARALLEL_DISCOVERY", "true").lower() == "true"
    max_parallel_runtime_minutes: int = int(os.getenv("MAX_PARALLEL_RUNTIME_MINUTES", 120))
    job_queue_cleanup_days: int = int(os.getenv("JOB_QUEUE_CLEANUP_DAYS", 3))

SETTINGS = Settings()


def get_parallel_config() -> dict:
    """Get optimized parallel configuration for current system"""
    try:
        import psutil
        # Get system memory
        total_memory_gb = psutil.virtual_memory().total / (1024**3)
    except ImportError:
        # Fallback if psutil not available - assume 16GB
        total_memory_gb = 16
    
    # Optimized for 10GB total system memory
    if total_memory_gb >= 14:  # 16GB+ system
        return {
            "recommended_workers": 15,
            "memory_per_worker_mb": 300,
            "max_concurrent_discovery": 5,
            "reason": "Optimized for 16GB+ RAM"
        }
    elif total_memory_gb >= 8:  # 10GB system (our target)
        return {
            "recommended_workers": 10,
            "memory_per_worker_mb": 200,
            "max_concurrent_discovery": 2,
            "reason": "Optimized for 10GB RAM"
        }
    elif total_memory_gb >= 6:  # 8GB system
        return {
            "recommended_workers": 6,
            "memory_per_worker_mb": 180,
            "max_concurrent_discovery": 2,
            "reason": "Optimized for 8GB RAM"
        }
    else:  # 4GB or less
        return {
            "recommended_workers": 3,
            "memory_per_worker_mb": 150,
            "max_concurrent_discovery": 1,
            "reason": "Optimized for limited RAM"
        }