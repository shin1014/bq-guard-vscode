from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Dict

from .config import get_history_path


def append_history(entry: Dict[str, Any]) -> None:
    path = get_history_path()
    entry = dict(entry)
    entry.setdefault("ts", datetime.now(timezone.utc).isoformat())
    import os

    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "a", encoding="utf-8") as handle:
        handle.write(json.dumps(entry, ensure_ascii=False) + "\n")
