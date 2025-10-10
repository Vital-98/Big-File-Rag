# rag/retriever/query_embedder.py
from google import genai
from google.genai import types

class QueryEmbedder:
    def __init__(self, model="text-embedding-004", output_dim=768):
        self.client = genai.Client(api_key="AIzaSyBD4CEjduLEf-ZggPc66LiFSvkx6x_1SfY")
        self.model = model
        self.output_dim = output_dim


    def embed_query(self, query: str):
        resp = self.client.models.embed_content(
            model=self.model,
            contents=[query],
            config=types.EmbedContentConfig(output_dimensionality=self.output_dim)
        )
        return resp.embeddings[0].values