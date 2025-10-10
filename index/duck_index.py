# rag/pipeline/index_pipeline.py
import duckdb, time
from datetime import datetime
from typing import List, Dict
from rag.chunking import chunk_document
from rag.embed import GeminiEmbedder
from index.open_index import get_client, ensure_index, bulk_upsert_chunks

def fetch_ready_pages(db_path: str, file_limit: int = 1000) -> List[Dict]:
    con = duckdb.connect(db_path)
    rows = con.execute("""
        SELECT f.file_id, p.page_no, p.text
        FROM pages p
        JOIN files f ON f.file_id = p.file_id
        WHERE p.text IS NOT NULL AND TRIM(p.text) <> ''
    """).fetchall()
    # Map to dicts
    pages = [{"file_id": r[0], "page_no": r[1], "text": r[2]} for r in rows]
    return pages

def run_indexing(db_path: str, index_name="rag-chunks", embed_model="text-embedding-004", out_dim=768):
    # 1) Gather pages from DuckDB and build chunks
    pages = fetch_ready_pages(db_path)
    chunks = chunk_document(pages)

    # 2) Embed in mini-batches
    embedder = GeminiEmbedder(model=embed_model, output_dim=out_dim)
    B = 128
    for i in range(0, len(chunks), B):
        batch = chunks[i:i+B]
        vecs = embedder.embed([c["text"] for c in batch])
        for c, v in zip(batch, vecs):
            c["embedding"] = v
            c["created_at"] = datetime.utcnow().isoformat()

    # 3) Index to OpenSearch
    client = get_client()
    ensure_index(client, index_name=index_name, dim=out_dim)
    bulk_upsert_chunks(client, index_name, chunks)
    return len(chunks)
