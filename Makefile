.PHONY: up down logs pull-model ideas ingest fetch-comments ingest-full parallel-estimate parallel-discovery parallel-ingest parallel-scale parallel-stop

up:
	docker compose up -d --build

down:
	docker compose down

logs:
	docker compose logs -f --tail=200

pull-model:
	# Pull a good default model for ideation
	docker compose exec ollama bash -lc "ollama pull llama3.1:8b-instruct"

ideas:
	curl -s "http://localhost:8000/ideas?query=nocoding+automation&n=5" | jq .

ingest:
	# Reddit JSON ingestion (no API credentials required)
	docker compose run --rm ingest

fetch-comments:
	# Fetch comments for recently ingested posts
	docker compose run --rm fetch-comments

ingest-full:
	# Full ingestion: posts + comments using JSON API
	docker compose run --rm ingest && docker compose run --rm fetch-comments

# Parallel scraping commands (10x-100x faster)
parallel-estimate:
	# Estimate parallel scraping work and optimal settings
	docker compose run --rm parallel-ingest --estimate-only

parallel-discovery:
	# Discovery phase: find all URLs to scrape
	docker compose run --rm parallel-discovery

parallel-ingest:
	# Full parallel pipeline: discovery + parallel scraping
	docker compose run --rm parallel-ingest

parallel-ingest-fast:
	# Fast parallel scraping with 10 workers (3-5 minutes)
	docker compose run --rm parallel-ingest --workers 10 --max-runtime 10

parallel-ingest-safe:
	# Safe parallel scraping with 6 workers (8-15 minutes)
	docker compose run --rm parallel-ingest --workers 6 --max-runtime 20

parallel-scale:
	# Scale up parallel workers (use with parallel-discovery first)
	# Usage: make parallel-scale WORKERS=10 (max recommended)
	docker compose up -d --scale parallel-worker=$(or $(WORKERS),10) parallel-worker

parallel-stop:
	# Stop all parallel workers
	docker compose stop parallel-worker parallel-ingest parallel-discovery