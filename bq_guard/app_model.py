from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, List, Optional


@dataclass
class Finding:
    severity: str
    code: str
    message: str
    evidence: Optional[str] = None
    table: Optional[str] = None


@dataclass
class PartitionSummary:
    table: str
    partition_key: Optional[str]
    ok: bool
    required_keys: List[str] = field(default_factory=list)
    reason: Optional[str] = None


@dataclass
class EstimateResult:
    bytes_processed: int
    bytes_human: str
    referenced_tables: List[str]
    findings: List[Finding]
    partition_summary: List[PartitionSummary]


@dataclass
class ExecuteResult:
    job_id: str
    status: str


@dataclass
class FetchResult:
    columns: List[str]
    rows: List[List[Any]]
    page_token: Optional[str] = None
