import os
import uuid
import hashlib
import time
import duckdb
from datetime import datetime
from pathlib import Path

# ================================================================
# 1️⃣ CONFIG
# ================================================================
DB_PATH = "C:\\Projects\\Project RAG\\V3\\rag_demo.duckdb"
print(f"📁 Using database: {os.path.abspath(DB_PATH)}")

# ================================================================
# 2️⃣ ENSURE SCHEMA EXISTS
# ================================================================
con = duckdb.connect(DB_PATH)

print("🧱 Ensuring tables exist...")

con.execute("""
CREATE TABLE IF NOT EXISTS files (
    file_id VARCHAR PRIMARY KEY,
    file_name VARCHAR,
    path VARCHAR,
    uploaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
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

print("✅ Schema ready!")


# ================================================================
# 3️⃣ PARSER CLASS (SELF-CONTAINED)
# ================================================================
class BaseParser:
    def __init__(self, db_path: str):
        self.conn = duckdb.connect(db_path)
        self.now = lambda: datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    def hash_content(self, data: str) -> str:
        return hashlib.sha256(data.encode()).hexdigest()

    def log_event(self, file_id: str, stage: str, ok: bool, message: str = ""):
        self.conn.execute(
            "INSERT INTO ingest_events VALUES (?, ?, ?, ?, ?, ?)",
            (
                hashlib.md5(f"{file_id}{time.time()}".encode()).hexdigest(),
                file_id,
                stage,
                ok,
                message,
                self.now()
            )
        )

    def ensure_file_record(self, file_id: str, file_path: str):
        file_name = Path(file_path).name
        self.conn.execute("""
            INSERT INTO files (file_id, file_name, path)
            VALUES (?, ?, ?)
            ON CONFLICT (file_id) DO NOTHING;
        """, (file_id, file_name, file_path))
        self.log_event(file_id, "file_registered", True, f"File registered: {file_name}")

    def insert_page(self, file_id, page_no, text, ocr_needed=False, ocr_done=False):
        self.conn.execute("""
            INSERT INTO pages VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            self.hash_content(f"{file_id}_{page_no}"),
            file_id,
            page_no,
            text,
            len(text.split()),
            ocr_needed,
            ocr_done,
            None
        ))
        self.log_event(file_id, f"page_{page_no}_parsed", True, "Inserted page record")

    def process_files(self, file_list):
        for file_id, file_path, pages in file_list:
            self.ensure_file_record(file_id, file_path)
            for page in pages:
                if len(page) == 2:
                    page_no, text = page
                    ocr_needed = False
                    ocr_done = False
                else:
                    page_no, text, ocr_needed, ocr_done = page
                self.insert_page(file_id, page_no, text, ocr_needed, ocr_done)


# ================================================================
# 4️⃣ RUN TEST
# ================================================================
print("🧠 Testing parser and inserts...")
parser = BaseParser(DB_PATH)

file_id = str(uuid.uuid4())
file_path = "demo_file.txt"

pages = [
    (1, "This is the first page of a demo file."),
    (2, "This is the second page for testing foreign key consistency.")
]

try:
    parser.process_files([(file_id, file_path, pages)])
    print(f"✅ Inserted file {file_id} and its pages successfully.")
except Exception as e:
    print("❌ Error:", e)

# ================================================================
# 5️⃣ VERIFY DATA
# ================================================================
print("\n📂 Files Table:")
print(con.execute("SELECT * FROM files").fetchdf())

print("\n📄 Pages Table:")
print(con.execute("SELECT * FROM pages").fetchdf())

print("\n🧾 Ingest Events (last 5):")
print(con.execute("SELECT * FROM ingest_events ORDER BY created_at DESC LIMIT 5").fetchdf())

print("\n✅ Test completed successfully.")
con.close()
