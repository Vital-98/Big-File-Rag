import re, hashlib
from typing import List, Dict, Iterable

MAX_TOKENS = 600          
MIN_TOKENS = 120
OVERLAP_TOKENS = 60

def _hash(s: str) -> str:
    return hashlib.sha256(s.encode()).hexdigest()

def _split_blocks(text: str) -> List[str]:
    # Respect table blocks and headings (from your OCR prompt conventions)
    blocks, buf = [], []
    lines = text.splitlines()
    def flush():
        if buf:
            blocks.append("\n".join(buf).strip()); buf.clear()
    for ln in lines:
        if ln.strip().startswith("CSV:"):
            flush(); blocks.append(ln.strip()); continue
        if re.match(r"^\s*#{1,6}\s+\S", ln):  # markdown-ish heading
            flush(); blocks.append(ln.strip()); continue
        if ln.strip() == "":
            flush(); continue
        buf.append(ln)
    flush()
    return [b for b in blocks if b]

def _len_tokens(s: str) -> int:
    # cheap token proxy
    return max(1, len(re.findall(r"\w+|\S", s)))

def _merge_to_token_windows(blocks: List[str]) -> List[str]:
    chunks, cur, cur_tokens = [], [], 0
    for b in blocks:
        bt = _len_tokens(b)
        if cur_tokens + bt > MAX_TOKENS and cur:
            chunks.append("\n".join(cur).strip())
            # overlap
            if OVERLAP_TOKENS > 0 and chunks[-1]:
                tail = " ".join(chunks[-1].split()[-OVERLAP_TOKENS:])
                cur, cur_tokens = [tail], _len_tokens(tail)
            else:
                cur, cur_tokens = [], 0
        cur.append(b); cur_tokens += bt
    if cur:
        chunks.append("\n".join(cur).strip())
    # pad small trailing chunks
    if chunks and _len_tokens(chunks[-1]) < MIN_TOKENS and len(chunks) > 1:
        chunks[-2] = (chunks[-2] + "\n" + chunks[-1]).strip()
        chunks.pop()
    return chunks

def chunk_page(file_id: str, page_no: int, text: str) -> List[Dict]:
    blocks = _split_blocks(text)
    chunks = _merge_to_token_windows(blocks)
    out = []
    for i, c in enumerate(chunks):
        out.append({
            "chunk_id": _hash(f"{file_id}:{page_no}:{i}:{_hash(c)[:12]}"),
            "file_id": file_id,
            "page_no": page_no,
            "ord": i,
            "text": c,
            "n_tokens": _len_tokens(c),
        })
    return out

def chunk_document(pages: Iterable[Dict]) -> List[Dict]:
    # pages: rows from DuckDB with file_id, page_no, text
    all_chunks = []
    for p in pages:
        all_chunks.extend(chunk_page(p["file_id"], p["page_no"], p["text"] or ""))
    return all_chunks
