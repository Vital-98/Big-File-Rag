import streamlit as st
import uuid
from pathlib import Path

from parsers.registry import get_parser
from parsers.ocr import GeminiBatchOCR
from index.duck_index import run_indexing
from retriever.query import QueryEmbedder
from retriever.search import OpenSearchRetriever
from retriever.generator import RAGGenerator
from opensearchpy import OpenSearch

# Paths & DB
DB_PATH = "C:\\Projects\\Project RAG\\V3\\rag_demo.duckdb"
UPLOAD_DIR = Path("uploads")
UPLOAD_DIR.mkdir(exist_ok=True)

# Initialize RAG components
client = OpenSearch(hosts=[{"host":"localhost","port":9200}], http_compress=True)
embedder = QueryEmbedder()
retriever = OpenSearchRetriever(client)
generator = RAGGenerator()

st.set_page_config(page_title="RAG File Chat")
st.title("BIG File Parser")


st.header("Upload Files")
uploaded_files = st.file_uploader(
    "Choose files", accept_multiple_files=True, type=["pdf","docx","txt","csv","xls","xlsx","png","jpeg","jpg"]
)

if uploaded_files:
    for f in uploaded_files:
        file_path = UPLOAD_DIR / f"{uuid.uuid4()}_{f.name}"
        with open(file_path, "wb") as out:
            out.write(f.getbuffer())
        st.success(f"Saved: {f.name}")

        # Parse
        parser = get_parser(str(file_path), DB_PATH)
        parser.parse(file_id=str(uuid.uuid4()), file_path=str(file_path))
        st.info(f"Parsed: {f.name}")

    # Optional OCR for images/scanned PDFs
    ocr = GeminiBatchOCR(DB_PATH)
    ocr.process(batch_limit=16)  # batch limit for demo

    # Chunk + embed + index
    n_indexed = run_indexing(DB_PATH)
    st.success(f"Indexed {n_indexed} chunks into OpenSearch")


st.header("Ask a Question")
query = st.text_input("Type your question here:")

if query:
    
    q_vec = embedder.embed_query(query)
    chunks = retriever.retrieve(q_vec)
    answer = generator.generate_answer(query, chunks)
    st.subheader("Answer")
    st.write(answer)
