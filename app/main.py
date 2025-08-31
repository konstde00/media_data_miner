from fastapi import FastAPI, Query, HTTPException
from typing import Optional, Dict, Any
import requests
import time
from qdrant_client import QdrantClient
from qdrant_client.http.exceptions import UnexpectedResponse
from .config import SETTINGS
from . import prompts
from .retrieval import search_and_rerank, call_ollama

app = FastAPI(title="Reddit Idea Miner", version="0.1")

@app.get("/health")
def health():
    """Basic health check"""
    return {"ok": True, "service": "reddit-miner"}

@app.get("/health/qdrant")
def health_qdrant() -> Dict[str, Any]:
    """Check Qdrant vector database health"""
    try:
        start_time = time.time()
        client = QdrantClient(url=SETTINGS.qdrant_url)
        
        # Try to get collection info
        collections = client.get_collections()
        collection_exists = any(
            col.name == SETTINGS.qdrant_collection 
            for col in collections.collections
        )
        
        response_time = (time.time() - start_time) * 1000  # ms
        
        return {
            "service": "qdrant",
            "status": "healthy",
            "url": SETTINGS.qdrant_url,
            "collection_exists": collection_exists,
            "response_time_ms": round(response_time, 2)
        }
    except Exception as e:
        raise HTTPException(
            status_code=503,
            detail={
                "service": "qdrant",
                "status": "unhealthy",
                "error": str(e)
            }
        )

@app.get("/health/ollama")
def health_ollama() -> Dict[str, Any]:
    """Check Ollama LLM service health"""
    try:
        start_time = time.time()
        
        # Check if Ollama is accessible
        response = requests.get(
            f"{SETTINGS.ollama_url}/api/tags",
            timeout=5
        )
        response.raise_for_status()
        
        # Check if required model is available
        models = response.json().get("models", [])
        model_available = any(
            model.get("name", "").startswith(SETTINGS.ollama_model.split(":")[0])
            for model in models
        )
        
        response_time = (time.time() - start_time) * 1000  # ms
        
        return {
            "service": "ollama",
            "status": "healthy",
            "url": SETTINGS.ollama_url,
            "model": SETTINGS.ollama_model,
            "model_available": model_available,
            "response_time_ms": round(response_time, 2)
        }
    except requests.RequestException as e:
        raise HTTPException(
            status_code=503,
            detail={
                "service": "ollama",
                "status": "unhealthy",
                "error": str(e)
            }
        )

@app.get("/health/ready")
def health_ready() -> Dict[str, Any]:
    """Check if all services are ready"""
    health_checks = {}
    all_healthy = True
    
    # Check Qdrant
    try:
        qdrant_health = health_qdrant()
        health_checks["qdrant"] = qdrant_health
    except HTTPException as e:
        health_checks["qdrant"] = e.detail
        all_healthy = False
    
    # Check Ollama
    try:
        ollama_health = health_ollama()
        health_checks["ollama"] = ollama_health
    except HTTPException as e:
        health_checks["ollama"] = e.detail
        all_healthy = False
    
    if not all_healthy:
        raise HTTPException(
            status_code=503,
            detail={
                "ready": False,
                "services": health_checks
            }
        )
    
    return {
        "ready": True,
        "services": health_checks
    }

@app.get("/ideas")
def ideas(query: str = Query(..., description="topic or problem to search for"),
          n: int = 5):
    # 1) retrieve & rerank
    docs = search_and_rerank(query)
    # 2) build snippet block
    snippets = []
    for d in docs:
        link = d.get("permalink", "")
        text = d.get("chunk_text", "").replace("\n", " ")
        snippets.append(f"- {text} (link: {link})")
    snippets_block = "\n".join(snippets)

    # 3) ask LLM for ideas
    user_prompt = prompts.IDEA_GEN_USER.format(n=n, snippets=snippets_block)
    messages = [
        {"role": "system", "content": "You are a pragmatic product researcher."},
        {"role": "user", "content": user_prompt},
    ]
    ideas_text = call_ollama(messages, max_tokens=SETTINGS.max_idea_tokens, temperature=SETTINGS.temp_ideas)

    return {
        "query": query,
        "top_docs": docs,
        "ideas": ideas_text,
    }

@app.get("/pains")
def pains(query: str, period: str = "last 14 days", subs: Optional[str] = None):
    docs = search_and_rerank(query)
    snippets = []
    for d in docs:
        link = d.get("permalink", "")
        text = d.get("chunk_text", "").replace("\n", " ")
        snippets.append(f"- {text} (link: {link})")
    snippets_block = "\n".join(snippets)

    user_prompt = prompts.PAIN_MINING_USER.format(topic=query, subs=subs or ",".join(SETTINGS.subreddits), period=period, snippets=snippets_block)
    messages = [
        {"role": "system", "content": prompts.PAIN_MINING_SYSTEM},
        {"role": "user", "content": user_prompt},
    ]
    pains_text = call_ollama(messages, max_tokens=SETTINGS.max_idea_tokens, temperature=SETTINGS.temp_scoring)

    return {"pains": pains_text}