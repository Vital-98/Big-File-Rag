import duckdb
import hashlib
import time
from datetime import datetime
from pathlib import Path

class BaseParser:
    def __init__(self, db_path: str):
        self.conn = duckdb.connect(db_path, read_only=False)
        self.now = lambda: datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # -----------------------------
    # Utilities
    # -----------------------------
    def hash_content(self, data: str) -> str:
        return hashlib.sha256(data.encode()).hexdigest()

    def log_event(self, file_id: str, stage: str, ok: bool, message: str = ""):
        """Insert log event safely."""
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

    # -----------------------------
    # File-level operations
    # -----------------------------
    def ensure_file_record(self, file_id: str, file_path: str):
        """Insert file metadata if not already present."""
        file_name = Path(file_path).name

        # Check if file already exists
        res = self.conn.execute("SELECT 1 FROM files WHERE file_id = ?", (file_id,)).fetchone()
        if not res:
            self.conn.execute("""
                INSERT INTO files (file_id, file_name, path)
                VALUES (?, ?, ?)
            """, (file_id, file_name, file_path))

        self.log_event(file_id, "file_registered", True, f"File registered or already exists: {file_name}")

    # -----------------------------
    # Page-level operations
    # -----------------------------
    def insert_page(self, file_id, page_no, text, ocr_needed=False, ocr_done=False):
        """Insert a single page after confirming file exists."""
        # Confirm file exists
        res = self.conn.execute("SELECT 1 FROM files WHERE file_id = ?", (file_id,)).fetchone()
        if not res:
            raise ValueError(f"Cannot insert page: file_id {file_id} does not exist in files table")

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

    # -----------------------------
    # Batch processing
    # -----------------------------
    def process_files(self, file_list):
        """
        Process multiple files safely.
        file_list: List of tuples (file_id, file_path, pages)
        pages: List of tuples (page_no, text[, ocr_needed, ocr_done])
        """
        for file_id, file_path, pages in file_list:
            # 1️⃣ Ensure the file exists
            self.ensure_file_record(file_id, file_path)

            # 2️⃣ Insert pages safely
            for page in pages:
                if len(page) == 2:
                    page_no, text = page
                    ocr_needed = False
                    ocr_done = False
                else:
                    page_no, text, ocr_needed, ocr_done = page

                self.insert_page(file_id, page_no, text, ocr_needed, ocr_done)

    
    def process_files_transaction(self, file_list):
        """
        Batch processing inside a transaction. Good for bulk/million file ingestion.
        """
        with self.conn.transaction():
            self.process_files(file_list)
