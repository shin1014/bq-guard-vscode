from __future__ import annotations

import re
from typing import List


def normalize_sql(sql: str) -> str:
    return re.sub(r"\s+", " ", sql).strip().lower()


def split_statements(sql: str) -> List[str]:
    parts = [s.strip() for s in sql.split(";")]
    return [p for p in parts if p]


def extract_tables(sql: str) -> List[str]:
    pattern = re.compile(r"`?([\w-]+\.[\w-]+\.[\w-]+)`?", re.IGNORECASE)
    return list({match.group(1) for match in pattern.finditer(sql)})


def contains_word(sql: str, word: str) -> bool:
    return re.search(rf"\b{re.escape(word)}\b", sql, re.IGNORECASE) is not None
