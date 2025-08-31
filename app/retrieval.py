from typing import List, Dict, Any, Optional
from qdrant_client import QdrantClient
from qdrant_client.http import models as qm
from sentence_transformers import SentenceTransformer, CrossEncoder
from .config import SETTINGS
import requests

# Embedding + reranker (loaded in API for query-time; indexer handles embeddings at write-time)
_embed_model = SentenceTransformer(SETTINGS.embedding_model)
_reranker = CrossEncoder(SETTINGS.reranker_model)

_qc = QdrantClient(url=SETTINGS.qdrant_url)


def embed_texts(texts: List[str]):
    return _embed_model.encode(texts, normalize_embeddings=True, convert_to_numpy=True, batch_size=32)


def search_and_rerank(query: str, filters: Optional[Dict[str, Any]] = None, topk: Optional[int] = None, topn: Optional[int] = None):
    topk = topk or SETTINGS.topk_retrieve
    topn = topn or SETTINGS.topn_rerank

    qvec = embed_texts([query])[0]

    # Optional payload filters
    qfilter = None
    if filters:
        # Example: {"subreddit": "startups", "min_score": 10}
        must = []
        for key, val in filters.items():
            if key == "min_score":
                must.append(qm.FieldCondition(key="score", range=qm.Range(gte=val)))
            else:
                must.append(qm.FieldCondition(key=key, match=qm.MatchValue(value=val)))
        qfilter = qm.Filter(must=must)

    hits = _qc.search(collection_name=SETTINGS.qdrant_collection,
                      query_vector=("dense", qvec.tolist()),
                      query_filter=qfilter,
                      limit=topk,
                      with_payload=True)

    candidates = [h.payload for h in hits]
    texts = [c["chunk_text"] for c in candidates]

    # Rerank using cross-encoder (query, text) pairs
    pair_inputs = [(query, t) for t in texts]
    scores = _reranker.predict(pair_inputs)

    scored = list(zip(candidates, scores))
    scored.sort(key=lambda x: float(x[1]), reverse=True)
    reranked = [c for c, s in scored[:topn]]

    return reranked


def call_ollama(messages: List[dict], max_tokens: Optional[int] = None, temperature: float = 0.7) -> str:
    url = f"{SETTINGS.ollama_url}/api/chat"
    payload = {
        "model": SETTINGS.ollama_model,
        "messages": messages,
        "options": {
            "temperature": temperature,
            **({"num_predict": max_tokens} if max_tokens else {}),
        },
        "stream": False,
    }
    r = requests.post(url, json=payload, timeout=600)
    r.raise_for_status()
    data = r.json()
    return data.get("message", {}).get("content", "")