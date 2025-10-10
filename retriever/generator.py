# rag/generator/gemini_generator.py
from google import genai

class RAGGenerator:
    def __init__(self, primary_model="models/gemini-2.5-flash", fallback_model="models/gemma-3-27b"):
        self.client = genai.Client(api_key="AIzaSyBD4CEjduLEf-ZggPc66LiFSvkx6x_1SfY") 
        self.primary_model = primary_model
        self.fallback_model = fallback_model

    def generate_answer(self, query: str, context_chunks: list):
        # Build system + user prompt
        context_text = "\n\n".join(context_chunks)
        prompt = f"""
You are a helpful assistant answering questions based on the following documents:

{context_text}

Question: {query}

Answer concisely and accurately. If unknown, say 'I don't know'.
"""
        try:
            # Primary generation
            response = self.client.models.generate_content(
                model=self.primary_model,
                contents=[{"role": "user", "text": prompt}],
                generation_config={"temperature": 0.0}
            )
            return response.text
        except Exception as e:
            # Fallback
            response = self.client.models.generate_content(
                model=self.fallback_model,
                contents=[{"role": "user", "text": prompt}],
                generation_config={"temperature": 0.0}
            )
            return response.text
