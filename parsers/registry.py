import mimetypes
from .pdf_parser import PDFParser
from .docx import DocxParser
from .text import TxtParser
from .csv import CSVExcelParser

def get_parser(file_path: str, db_path: str):
    mime, _ = mimetypes.guess_type(file_path)
    if not mime:
        ext = file_path.lower().split('.')[-1]
    else:
        ext = mime.split('/')[-1]

    if ext in ["pdf"]:
        return PDFParser(db_path)
    elif ext in ["docx", "doc"]:
        return DocxParser(db_path)
    elif ext in ["csv", "xls", "xlsx"]:
        return CSVExcelParser(db_path)
    elif ext in ["plain", "txt"]:
        return TxtParser(db_path)
    else:
        raise ValueError(f"Unsupported file type: {file_path}")