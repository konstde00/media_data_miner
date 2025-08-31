import os
import time
import glob
import uuid
import pandas as pd
from sentence_transformers import SentenceTransformer
from qdrant_client import QdrantClient
from qdrant_client.http import models as qm
from app.config import SETTINGS

EMB = SentenceTransformer(SETTINGS.embedding_model)
QC = QdrantClient(url=SETTINGS.qdrant_url)

COL = SETTINGS.qdrant_collection

# Ensure collection exists with named vector 'dense'
def ensure_collection(dim: int):
    try:
      info = QC.get_collection(COL)
      return
    except Exception:
      pass
    QC.recreate_collection(
        collection_name=COL,
        vectors_config={
            "dense": qm.VectorParams(size=dim, distance=qm.Distance.COSINE)
        },
        optimizers_config=qm.OptimizersConfigDiff(indexing_threshold=20000),
    )

# very simple token-ish chunking by length in characters

def chunk_text(txt: str, size: int, overlap: int):
    if not txt:
        return []
    step = max(1, size - overlap)
    chunks = []
    for i in range(0, len(txt), step):
        chunk = txt[i:i+size]
        if chunk:
            chunks.append(chunk)
    return chunks


def index_parquet(path: str):
    print(f"Indexing {path}")
    df = pd.read_parquet(path)
    rows = []
    for _, r in df.iterrows():
        body = (r.get("title", "") + "\n" + r.get("selftext", "")).strip()
        chunks = chunk_text(body, size=SETTINGS.chunk_size_tokens*4, overlap=SETTINGS.chunk_overlap_tokens*4)
        for ch in chunks:
            rows.append({
                "id": str(uuid.uuid4()),
                "chunk_text": ch,
                "permalink": r.get("permalink", ""),
                "subreddit": r.get("subreddit", ""),
                "score": int(r.get("score", 0)),
                "created_utc": float(r.get("created_utc", 0.0)),
            })

    if not rows:
        print("No chunks to index.")
        return

    texts = [x["chunk_text"] for x in rows]
    vecs = EMB.encode(texts, normalize_embeddings=True, convert_to_numpy=True, batch_size=64)

    ensure_collection(vecs.shape[1])

    points = []
    for i, r in enumerate(rows):
        points.append(
            qm.PointStruct(
                id=r["id"],
                vector={"dense": vecs[i].tolist()},
                payload=r,
            )
        )

    QC.upsert(collection_name=COL, points=points)
    print(f"Upserted {len(points)} chunks.")


if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("--watch", action="store_true")
    args = p.parse_args()

    # One-shot index all parquet files, then optionally watch for new ones
    seen = set()
    while True:
        files = sorted(glob.glob("/data/raw_parquet/*/*.parquet"))
        for f in files:
            if f in seen:
                continue
            try:
                index_parquet(f)
                seen.add(f)
            except Exception as e:
                print(f"Failed {f}: {e}")
        if not args.watch:
            break
        time.sleep(10)