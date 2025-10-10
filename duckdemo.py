import os
import uuid
import duckdb
from parsers.base_parser import BaseParser  

DB_PATH = "C:\\Projects\\Project RAG\\V3\\rag_demo.duckdb"

# ✅ Check DB file exists
if not os.path.exists(DB_PATH):
    raise FileNotFoundError(f"{DB_PATH} not found! Run create_duckdb_schema.py first.")

# ✅ Create parser instance
parser = BaseParser(DB_PATH)

# ✅ Create a fake file + pages
file_id = str(uuid.uuid4())
file_path = "demo_file.txt"

pages = [
    (1, "This is the first test page."),
    (2, "This is the second test page for the same file.")
]

# ✅ Run processing
print(f"Processing file: {file_id}")
try:
    parser.process_files([(file_id, file_path, pages)])
    print("Inserted file and pages successfully")
except Exception as e:
    print("Error while inserting:", e)

con = duckdb.connect(DB_PATH)

print("\n📂 Files Table:")
print(con.execute("SELECT * FROM files").fetchdf())

print("\n📄 Pages Table:")
print(con.execute("SELECT * FROM pages").fetchdf())

print("\n🧾 Ingest Events:")
print(con.execute("SELECT * FROM ingest_events ORDER BY created_at DESC LIMIT 5").fetchdf())

con.close()
print("\n✅ Test completed successfully.")
