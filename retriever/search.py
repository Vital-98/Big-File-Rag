# rag/retriever/opensearch_retriever.py
from opensearchpy import OpenSearch

class OpenSearchRetriever:
    def __init__(self, client: OpenSearch, index_name="rag-chunks", k=6):
        self.client = client
        self.index_name = index_name
        self.k = k

    def retrieve(self, query_vec):
        body = {
            "size": self.k,
            "query": {
                "knn": {
                    "embedding": {
                        "vector": query_vec,
                        "k": self.k
                    }
                }
            }
        }
        resp = self.client.search(index=self.index_name, body=body)
        # Return list of text chunks
        hits = [hit["_source"]["text"] for hit in resp["hits"]["hits"]]
        return hits
