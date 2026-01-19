from __future__ import annotations

import json
import os
import time
from typing import Any, Dict, List, Optional

from .config import get_cache_path


class TableMetaCache:
    def __init__(self, schema_version: int) -> None:
        self.schema_version = schema_version
        self.path = get_cache_path()
        self.tables: Dict[str, Dict[str, Any]] = {}
        self._load()

    def _load(self) -> None:
        try:
            with open(self.path, "r", encoding="utf-8") as handle:
                data = json.load(handle)
            if data.get("version") != self.schema_version:
                return
            self.tables = data.get("tables", {})
        except FileNotFoundError:
            return
        except Exception:
            return

    def save(self) -> None:
        try:
            os.makedirs(os.path.dirname(self.path), exist_ok=True)
            with open(self.path, "w", encoding="utf-8") as handle:
                json.dump({"version": self.schema_version, "tables": self.tables}, handle)
        except Exception:
            return

    def get(self, table: str) -> Optional[Dict[str, Any]]:
        return self.tables.get(table)

    def set(self, table: str, meta: Dict[str, Any]) -> None:
        meta = dict(meta)
        meta["last_seen_ts"] = int(time.time())
        self.tables[table] = meta

    def missing(self, tables: List[str]) -> List[str]:
        return [table for table in tables if table not in self.tables]
