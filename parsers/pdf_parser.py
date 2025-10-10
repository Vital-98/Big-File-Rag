import pdfplumber
import os
from .base_parser import BaseParser

TEXT_DENSITY_THRESHOLD = 30  

class PDFParser(BaseParser):
    def parse(self, file_id: str, file_path: str):
        try:
            with pdfplumber.open(file_path) as pdf:
                for i, page in enumerate(pdf.pages, start=1):
                    text = page.extract_text() or ""
                    token_count = len(text.split())
                    needs_ocr = token_count < TEXT_DENSITY_THRESHOLD
                    self.insert_page(file_id, i, text, ocr_needed=needs_ocr, ocr_done=not needs_ocr)
            self.log_event(file_id, "parse", True, f"PDF parsed: {len(pdf.pages)} pages")
        except Exception as e:
            self.log_event(file_id, "parse", False, f"PDF parse error: {e}")
            raise
