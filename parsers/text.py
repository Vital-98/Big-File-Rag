# rag/parsers/txt_parser.py
from .base_parser import BaseParser

class TxtParser(BaseParser):
    def parse(self, file_id: str, file_path: str):
        try:
            with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                text = f.read()
            self.insert_page(file_id, 1, text, ocr_needed=False, ocr_done=True)
            self.log_event(file_id, "parse", True, "TXT parsed")
        except Exception as e:
            self.log_event(file_id, "parse", False, f"TXT parse error: {e}")
            raise
