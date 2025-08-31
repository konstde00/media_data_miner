# Reddit Micro-SaaS Idea Miner

🚀 End-to-end local pipeline that requires **NO Reddit API credentials**:
- Ingest Reddit via public .json endpoints -> Parquet/JSONL on SSD
- Index to Qdrant (BGE-M3 embeddings)
- Retrieve + rerank (BGE reranker)
- Generate ideas with local LLM (Ollama: llama3.1:8b-instruct by default)

## ✨ Key Features
- ✅ **No API keys required** - Uses Reddit's public JSON endpoints
- ✅ **Rich data** - 50+ metadata fields per post
- ✅ **Comment threads** - Full nested comment hierarchy
- ✅ **Proxy support** - For large-scale scraping
- ✅ **Rate limiting** - Respectful scraping with delays
- ✅ **Multiple formats** - Parquet or JSONL output

## 🚀 Quick Start (5 minutes to Reddit data!)

### Step 1: Prerequisites
- **Docker Desktop** installed and running ([Download here](https://www.docker.com/products/docker-desktop))
- **Git** installed
- **No Reddit account needed!**

### Step 2: Clone and Configure
```bash
# Clone the repository
git clone <your-repo-url>
cd reddit-data-miner

# Create configuration file (no API keys needed!)
cp .env.example .env

# Optional: Edit subreddits you want to mine
nano .env  # or use any text editor
# Change SUBREDDITS=startups,entrepreneur,smallbusiness,SaaS
```

### Step 3: Build and Launch All Services
```bash
# Build Docker images and start all services
make up

# This will start:
# - Qdrant (vector database)
# - Ollama (local LLM)
# - API server
# - File indexer
```

### Step 4: Download LLM Model (One-time setup)
```bash
# Download the language model (this may take 5-10 minutes)
make pull-model

# Wait for download to complete before proceeding
```

### Step 5: Fetch Reddit Data
```bash
# Fetch posts from your configured subreddits
make ingest

# This will:
# 1. Connect to Reddit's public JSON endpoints
# 2. Download posts from the last 14 days with score ≥5
# 3. Save data to data/raw_parquet/YYYY-MM-DD/
# 4. Automatically trigger indexing into Qdrant
```

### Step 6: Get Comment Threads (Optional)
```bash
# Fetch detailed comment threads for the posts
make fetch-comments

# Or do both posts + comments at once:
make ingest-full
```

### Step 7: Query for Ideas
```bash
# Test the idea generation
make ideas

# Or visit the web interface:
# http://localhost:8000/docs
```

## 📊 Detailed Instructions

### Understanding the Data Flow
1. **Reddit Ingestion** (`make ingest`):
   - Connects to `https://reddit.com/r/{subreddit}/new.json`
   - Fetches latest posts (5 pages = ~500 posts per subreddit)
   - Applies filters (date, minimum score)
   - Saves to `data/raw_parquet/YYYY-MM-DD/posts_HHMMSS.parquet`

2. **Comment Fetching** (`make fetch-comments`):
   - Reads recent post files
   - Fetches comment threads from `https://reddit.com/r/{subreddit}/comments/{post_id}.json`
   - Preserves nested comment hierarchy
   - Saves to `data/raw_parquet/YYYY-MM-DD/comments_HHMMSS.parquet`

3. **Automatic Indexing**:
   - `indexer` service watches for new files
   - Chunks text into 600-token segments
   - Generates embeddings using BGE-M3 model
   - Stores in Qdrant vector database

4. **Idea Generation**:
   - API searches Qdrant for relevant content
   - Reranks results using BGE reranker
   - Generates ideas using local Ollama LLM

### Manual Docker Commands (Alternative to Make)
```bash
# Start services individually
docker compose up -d qdrant      # Start vector database
docker compose up -d ollama      # Start LLM service
docker compose up -d api         # Start API server
docker compose up -d indexer     # Start file watcher

# Run data ingestion manually
docker compose run --rm ingest

# Fetch comments manually
docker compose run --rm fetch-comments

# View logs
docker compose logs -f ingest    # Ingestion logs
docker compose logs -f api       # API server logs
docker compose logs -f indexer   # Indexing logs
```

### Monitoring Your Data Collection

#### Check Service Status
```bash
# View all running services
docker compose ps

# Check specific service logs
make logs                           # All services
docker compose logs -f ingest      # Just ingestion
docker compose logs -f indexer     # Just indexing
```

#### Verify Data Collection
```bash
# Check collected data files
ls -la data/raw_parquet/           # List all dates
ls -la data/raw_parquet/$(date +%Y-%m-%d)/  # Today's files

# Check Qdrant database status
curl http://localhost:6333/collections  # List collections

# Check API health
curl http://localhost:8000/health   # Should return {"ok": true}
```

#### Monitor Progress During Ingestion
```bash
# Watch ingestion in real-time
docker compose logs -f ingest

# You should see output like:
# Fetching r/startups (new) - up to 5 pages...
# Page 1: 87 posts (87 total)
# Page 2: 73 posts (160 total)
# ✅ Ingestion completed successfully!
```

### Example API Usage
Once your system is running and has data:

```bash
# Get startup ideas about AI automation
curl -s "http://localhost:8000/ideas?query=ai+automation+for+small+business&n=3" | jq .

# Find pain points in marketing
curl -s "http://localhost:8000/pains?query=marketing+analytics+problems" | jq .

# Search for SaaS opportunities
curl -s "http://localhost:8000/ideas?query=saas+opportunities+remote+work&n=5" | jq .
```

Or visit the interactive web interface at: **http://localhost:8000/docs**

## 🔧 Troubleshooting Guide

### Common Issues and Solutions

#### ❌ "Docker is not running"
```bash
# Start Docker Desktop application
# Wait for it to fully start (whale icon in system tray)
# Then run: docker version
```

#### ❌ "make: command not found"
```bash
# On Windows: Install make or use Docker commands directly
# Alternative: Use docker compose commands instead
docker compose up -d --build
docker compose run --rm ingest
```

#### ❌ "No data collected" 
```bash
# Check subreddit names in .env
nano .env
# Verify subreddits exist and have recent posts

# Lower the minimum score filter
# Change MIN_SCORE=5 to MIN_SCORE=1

# Increase date range
# Change INGEST_DAYS=14 to INGEST_DAYS=30
```

#### ❌ "Rate limited by Reddit"
```bash
# Increase delay between requests in .env
# Change REQUEST_DELAY=1.0 to REQUEST_DELAY=2.0

# Or wait 10-15 minutes and try again
```

#### ❌ "LLM not responding"
```bash
# Ensure model is downloaded
make pull-model

# Check Ollama service
docker compose logs ollama

# Verify model exists
docker compose exec ollama ollama list
```

#### ❌ "Port already in use"
```bash
# Check what's using the ports
lsof -i :8000  # API port
lsof -i :6333  # Qdrant port
lsof -i :11434 # Ollama port

# Stop conflicting services or change ports in docker-compose.yml
```

### Performance Tips

#### Speed Up Data Collection
```bash
# Reduce request delay (be respectful)
REQUEST_DELAY=0.5

# Fetch fewer pages per subreddit
PAGES_PER_SUBREDDIT=3

# Skip comment fetching for faster runs
FETCH_COMMENTS=false
```

#### Optimize Storage
```bash
# Use JSONL format for smaller files
SAVE_FORMAT=jsonl

# Clean old data periodically
rm -rf data/raw_parquet/2024-*  # Remove old dates
```

#### Monitor Resource Usage
```bash
# Check Docker resource usage
docker stats

# Check disk usage
du -sh data/

# Check container logs
docker compose logs --tail=50
```

## 📁 File Structure After Setup

After running the system, your directory will look like:

```
reddit-data-miner/
├── app/                    # Application code
├── scripts/                # Data collection scripts
├── data/                   # Generated data
│   ├── raw_parquet/        # Daily folders with Reddit data
│   │   └── 2024-01-15/     # Example date folder
│   │       ├── posts_143052.parquet    # Posts data
│   │       └── comments_143801.parquet # Comments data
│   └── qdrant/             # Vector database storage
├── docker-compose.yml      # Service definitions  
├── Dockerfile              # Container build instructions
├── requirements.txt        # Python dependencies
├── .env                    # Your configuration
└── README.md              # This file
```

## 🚀 What Happens When You Run Commands

### `make up` (Start All Services)
1. Builds Docker images with Python dependencies
2. Starts Qdrant vector database (port 6333)
3. Starts Ollama LLM service (port 11434)
4. Starts FastAPI server (port 8000)
5. Starts file indexer (watches for new data)

### `make pull-model` (Download LLM)
1. Downloads llama3.1:8b-instruct model (~4.7GB)
2. Stores in Ollama for local inference
3. One-time setup, takes 5-10 minutes

### `make ingest` (Sequential - 30-60 min)
1. Reads subreddit list from .env file
2. Makes HTTP requests to reddit.com/r/{subreddit}/new.json
3. Filters posts by date and score
4. Saves raw data as Parquet files
5. Triggers automatic indexing

### `make parallel-ingest-fast` (Parallel - 2-3 min)
1. **Discovery Phase** (30s): Finds all URLs to scrape
2. **Queue Population**: Stores work in SQLite job database
3. **Worker Launch**: Starts 25 parallel workers
4. **Parallel Processing**: Workers pull jobs and scrape simultaneously
5. **Batch Saves**: Results saved in memory-optimized batches
6. **Coordination**: Job queue manages progress and retries

### `make fetch-comments` (Get Comment Threads)
1. Finds latest post files
2. Extracts post IDs
3. Fetches comment threads for each post
4. Preserves nested reply structure
5. Saves comment data separately

### `make ideas` (Test Idea Generation)
1. Searches Qdrant for relevant content
2. Reranks results using ML model
3. Sends context to local LLM
4. Generates startup ideas
5. Returns JSON response

## 5) Configuration Options

### Data Collection (JSON API)
The system uses Reddit's public `.json` endpoints:
- `https://reddit.com/r/subreddit/new.json` - Latest posts
- `https://reddit.com/r/subreddit/comments/postid.json` - Comments

**No authentication required** - just proper User-Agent headers and rate limiting.

### Data Pipeline
1. **Ingest**: `scripts/ingest_reddit.py` fetches posts via JSON API
2. **Comments**: `scripts/fetch_comments.py` fetches threaded comments
3. **Index**: `scripts/indexer.py` chunks text and embeds into Qdrant
4. **Query**: FastAPI app searches Qdrant and generates ideas via LLM

## 6) Configuration Options

### Basic Settings (.env)
```bash
SUBREDDITS=startups,entrepreneur,smallbusiness,SaaS  # Target subreddits
INGEST_DAYS=14                                       # How far back to look
MIN_SCORE=5                                          # Minimum post score
PAGES_PER_SUBREDDIT=5                               # Pages to fetch (~500 posts)
```

### Advanced Settings
```bash
SAVE_FORMAT=parquet           # or jsonl
FETCH_COMMENTS=true          # Include comment threads
MAX_COMMENTS_PER_POST=500    # Comment limit per post
REQUEST_DELAY=1.0            # Seconds between requests

# Optional proxy rotation for large-scale scraping
ENABLE_PROXY_ROTATION=false
PROXY_POOL=http://user:pass@proxy1:8080,http://user:pass@proxy2:8080
```

## 7) Available Commands
```bash
make up           # Start all services
make down         # Stop services
make logs         # View logs
make pull-model   # Download LLM model
make ingest       # Fetch Reddit posts
make fetch-comments # Fetch comment threads
make ingest-full  # Posts + comments
make ideas        # Test ideas endpoint
```

## 8) Docker Services
- `api` - FastAPI application (port 8000)
- `qdrant` - Vector database (port 6333)
- `ollama` - Local LLM (port 11434)
- `indexer` - Watches and indexes data files
- `ingest` - Reddit data collection
- `fetch-comments` - Comment thread collection

## 9) Data Output

### What You Get
- **Posts**: Title, content, author, scores, timestamps, 50+ metadata fields
- **Comments**: Nested comment threads with full hierarchy preserved
- **No rate limits**: Public endpoints vs API quotas
- **Rich metadata**: Media info, awards, flair, engagement metrics

### Storage Formats
- **Parquet** (default): Efficient columnar storage, great for analytics
- **JSONL**: Line-delimited JSON, great for streaming/processing

## 10) Tips & Best Practices

### Performance
- Keep data on fast SSD (`data/` folder is mounted)
- Tune `TOPK_RETRIEVE`/`TOPN_RERANK` for speed vs quality
- Use `REQUEST_DELAY=1.0` or higher for respectful scraping

### LLM Models
```bash
# Switch models by setting OLLAMA_MODEL:
OLLAMA_MODEL=gemma2:9b                    # Smaller, faster
OLLAMA_MODEL=llama3.1:8b-instruct        # Default balance
OLLAMA_MODEL=deepseek-r1:7b-distill-qwen # Alternative option
```

### Scaling Up
- Enable proxy rotation for large-scale collection
- Use multiple subreddit lists and run parallel jobs
- Monitor `data/` folder size and clean old files periodically

## 11) Data Quality & Safety

### Respectful Scraping
- ✅ 1 second delay between requests (configurable)
- ✅ Proper User-Agent headers
- ✅ Exponential backoff on rate limits
- ✅ Respects Reddit's robots.txt
- ✅ Uses public endpoints only

### Content Filtering
- Minimum score filtering (skip low-quality posts)
- Date range filtering (avoid stale content)
- Text cleaning and validation
- Optional NSFW filtering via post metadata

## 12) Troubleshooting

### Common Issues
- **No data collected**: Check subreddit names, adjust MIN_SCORE
- **Rate limited**: Increase REQUEST_DELAY, enable proxy rotation
- **Storage full**: Clean old files in `data/raw_parquet/`
- **LLM errors**: Ensure Ollama model is downloaded via `make pull-model`

### Logs
```bash
make logs  # View all service logs
docker compose logs ingest  # View ingestion logs only
```

---

**Ready to mine Reddit for your next startup idea?** 🚀

Just run `make up && make ingest-full && make ideas` and start exploring!

*No Reddit API registration required - uses public JSON endpoints responsibly.*