from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


@dataclass
class Finding:
    severity: str
    code: str
    message: str
    evidence: Optional[str] = None
    table: Optional[str] = None
