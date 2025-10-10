from opensearchpy import OpenSearch, helpers
import time

def get_client():
    
    return OpenSearch(
        hosts=[{"host": "localhost", "port": 9200}],
        http_compress=True,
        http_auth=("admin", "admin"),
        use_ssl=False, verify_certs=False,
        ssl_assert_hostname=False, ssl_show_warn=False,
        timeout=60
    )

def ensure_index(client, index_name="rag-chunks", dim=768, ef_search=128, m=16, engine="nmslib"):
    body = {
        "settings": {
            "index": {
                "knn": True,
                "knn.algo_param.ef_search": ef_search
            }
        },
        "mappings": {
            "properties": {
                "chunk_id": {"type": "keyword"},
                "file_id": {"type": "keyword"},
                "page_no": {"type": "integer"},
                "ord": {"type": "integer"},
                "n_tokens": {"type": "integer"},
                "text": {"type": "text"},
                "created_at": {"type": "date"},
                "embedding": {
                    "type": "knn_vector",
                    "dimension": dim,
                    "method": {
                        "name": "hnsw",
                        "space_type": "cosinesimil",
                        "engine": engine,
                        "parameters": {"ef_construction": 128, "m": m}
                    }
                }
            }
        }
    }
    if not client.indices.exists(index=index_name):
        client.indices.create(index=index_name, body=body)

def bulk_upsert_chunks(client, index_name, chunks_with_vecs):
    actions = []
    for ch in chunks_with_vecs:
        actions.append({
            "_op_type": "index",
            "_index": index_name,
            "_id": ch["chunk_id"],
            "_source": ch
        })
    helpers.bulk(client, actions)

def knn_search(client, index_name, query_vec, k=8, min_score=None):
    q = {
        "size": k,
        "query": {
            "knn": {
                "embedding": {
                    "vector": query_vec,
                    "k": k
                }
            }
        }
    }
    if min_score is not None:
        # (Optional) radial search style thresholding
        q["min_score"] = min_score
    return client.search(index=index_name, body=q)