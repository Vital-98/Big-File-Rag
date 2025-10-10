# rag/parsers/docx_parser.py
import docx
import os
from .base_parser import BaseParser

class DocxParser(BaseParser):
    def parse(self, file_id: str, file_path: str):
        try:
            doc = docx.Document(file_path)
            text = "\n".join([p.text for p in doc.paragraphs if p.text.strip()])
            self.insert_page(file_id, 1, text, ocr_needed=False, ocr_done=True)
            self.log_event(file_id, "parse", True, "DOCX parsed")
        except Exception as e:
            self.log_event(file_id, "parse", False, f"DOCX parse error: {e}")
            raise
