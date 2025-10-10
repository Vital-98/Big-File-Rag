import duckdb
import tempfile
import os
import time
from datetime import datetime
from typing import List
from google import genai
from pdf2image import convert_from_path
from PIL import Image
import io

class GeminiBatchOCR:
    def __init__(self, db_path: str, model="models/gemini-2.0-flash", batch_size: int = 8):
        self.conn = duckdb.connect(db_path)
        self.client = genai.Client()
        self.model = model
        self.batch_size = batch_size
        self.now = datetime.utcnow

    def _get_pending_pages(self, limit: int = 100):
        """Fetch pages needing OCR from DuckDB."""
        query = """
        SELECT p.page_id, f.file_id, f.path, p.page_no
        FROM pages p
        JOIN files f ON p.file_id = f.file_id
        WHERE p.ocr_needed = TRUE AND (p.ocr_done = FALSE OR p.ocr_done IS NULL)
        LIMIT ?
        """
        return self.conn.execute(query, [limit]).fetchall()

    def _log_event(self, file_id, stage, ok, message=""):
        event_id = str(hash(file_id + str(time.time())))
        self.conn.execute(
            "INSERT INTO ingest_events VALUES (?, ?, ?, ?, ?, ?)",
            (event_id, file_id, stage, ok, message, self.now())
        )

    def _save_ocr_result(self, page_id, text):
        self.conn.execute(
            "UPDATE pages SET text = ?, ocr_done = TRUE WHERE page_id = ?",
            (text, page_id)
        )

    def _extract_page_images(self, pdf_path: str, page_numbers: List[int]) -> List[Image.Image]:
        """Convert specific PDF pages to PIL Images."""
        return convert_from_path(pdf_path, fmt="jpeg", first_page=min(page_numbers), last_page=max(page_numbers))

    def _call_gemini_batch(self, images: List[Image.Image]) -> List[str]:
        """Send multiple images to Gemini-2.0-Flash OCR."""
        files = []
        for img in images:
            buf = io.BytesIO()
            img.save(buf, format="JPEG")
            buf.seek(0)
            files.append({"mime_type": "image/jpeg", "data": buf.read()})

        # New GenAI SDK call
        response = self.client.models.generate_content(
            model=self.model,
            contents=[
                {
                    "role": "user",
                    "parts": [
                        {"text": "Transcribe text from these images. Preserve reading order, use '#' for headings and 'CSV:' for tables."},
                        *[{"inline_data": f} for f in files],
                    ],
                }
            ],
            generation_config={"temperature": 0.0}
        )

        # Gemini returns combined text — split heuristically by page marker
        text = response.text.strip()
        return text.split("\n\n---\n\n") if "---" in text else [text]

    def process(self, batch_limit: int = 100):
        pending = self._get_pending_pages(batch_limit)
        if not pending:
            print("✅ No OCR pending.")
            return

        grouped_by_file = {}
        for row in pending:
            page_id, file_id, path, page_no = row
            grouped_by_file.setdefault((file_id, path), []).append((page_id, page_no))

        for (file_id, path), pages in grouped_by_file.items():
            pages.sort(key=lambda x: x[1])
            page_batches = [pages[i:i+self.batch_size] for i in range(0, len(pages), self.batch_size)]

            for batch in page_batches:
                page_ids, page_nos = zip(*batch)
                try:
                    images = self._extract_page_images(path, list(page_nos))
                    texts = self._call_gemini_batch(images)
                    for pid, txt in zip(page_ids, texts):
                        self._save_ocr_result(pid, txt)
                    self._log_event(file_id, "ocr", True, f"OCR completed for pages {page_nos}")
                    print(f"OCR ✅ {file_id}: pages {page_nos}")
                except Exception as e:
                    self._log_event(file_id, "ocr", False, str(e))
                    print(f"OCR ❌ {file_id} pages {page_nos}: {e}")
