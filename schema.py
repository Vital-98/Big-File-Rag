import duckdb

DB_PATH = "C:\\Projects\\Project RAG\\V3\\rag_demo.duckdb"
con = duckdb.connect(DB_PATH)

con.execute("""
CREATE TABLE IF NOT EXISTS files (
    file_id VARCHAR PRIMARY KEY,
    file_name VARCHAR,
    path VARCHAR,
    uploaded_at TIMESTAMP DEFAULT (now())
);
""")

con.execute("""
CREATE TABLE IF NOT EXISTS pages (
    page_id VARCHAR PRIMARY KEY,
    file_id VARCHAR REFERENCES files(file_id),
    page_no INTEGER,
    text TEXT,
    n_tokens INTEGER,
    ocr_needed BOOLEAN DEFAULT FALSE,
    ocr_done BOOLEAN DEFAULT FALSE,
    embedding BLOB
);
""")
                               
con.execute("""
CREATE TABLE IF NOT EXISTS ingest_events (
    event_id VARCHAR PRIMARY KEY,
    file_id VARCHAR,
    stage VARCHAR,
    ok BOOLEAN,
    message TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
""")

print("DuckDB schema created")
