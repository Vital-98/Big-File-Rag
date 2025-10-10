# rag/parsers/csv_excel_parser.py
import pandas as pd
import os
from io import StringIO
from .base_parser import BaseParser

MAX_ROWS = 1000  # to avoid memory issues in demo

class CSVExcelParser(BaseParser):
    def parse(self, file_id: str, file_path: str):
        try:
            ext = os.path.splitext(file_path)[1].lower()
            if ext in [".csv"]:
                df = pd.read_csv(file_path, nrows=MAX_ROWS)
                text = df.to_csv(index=False)
            else:
                dfs = pd.read_excel(file_path, sheet_name=None)
                texts = []
                for sheet, df in dfs.items():
                    snippet = df.head(MAX_ROWS).to_csv(index=False)
                    texts.append(f"# Sheet: {sheet}\n{snippet}")
                text = "\n\n".join(texts)
            self.insert_page(file_id, 1, text, ocr_needed=False, ocr_done=True)
            self.log_event(file_id, "parse", True, f"CSV/Excel parsed")
        except Exception as e:
            self.log_event(file_id, "parse", False, f"CSV/Excel parse error: {e}")
            raise
