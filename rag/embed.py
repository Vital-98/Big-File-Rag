from typing import List, Dict
from google import genai
from google.genai import types

class GeminiEmbedder:
    def __init__(self, model: str = "text-embedding-004", output_dim: int = 768):
        self.client = genai.Client()  # api key/env handled by SDK
        self.model = model
        self.output_dim = output_dim

    def embed(self, texts: List[str]) -> List[List[float]]:
        # Batch with a single call; the SDK accepts list[str]
        resp = self.client.models.embed_content(
            model=self.model,
            contents=texts,
            config=types.EmbedContentConfig(output_dimensionality=self.output_dim)
        )
        # The response format is a list of embeddings in order
        return [vec.values for vec in resp.embeddings]
