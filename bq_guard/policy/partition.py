from __future__ import annotations

import re
from typing import Dict, List, Tuple

from .types import Finding


def enforce_partition_filters(
    sql: str,
    referenced_tables: List[str],
    table_meta: Dict[str, Dict[str, object]],
    exceptions: List[str],
    enforce: bool,
) -> Tuple[List[Finding], List[Dict[str, object]]]:
    findings: List[Finding] = []
    summary: List[Dict[str, object]] = []

    if not referenced_tables:
        findings.append(
            Finding(
                severity="WARN",
                code="TABLES_UNKNOWN",
                message="Referenced tables not detected; partition enforcement may be incomplete.",
            )
        )
        return findings, summary

    if not enforce:
        return findings, summary

    for table in referenced_tables:
        if table in exceptions:
            summary.append(
                {
                    "table": table,
                    "partition_key": None,
                    "ok": True,
                    "required_keys": [],
                    "reason": "exempt",
                }
            )
            continue
        meta = table_meta.get(table)
        if not meta:
            summary.append(
                {
                    "table": table,
                    "partition_key": None,
                    "ok": True,
                    "required_keys": [],
                    "reason": "metadata_missing",
                }
            )
            continue
        partition_key = meta.get("partition_key")
        ingestion_time = meta.get("ingestion_time")
        required_keys: List[str] = []
        ok = True
        reason = None
        if meta.get("partition_type") in {"time", "range"}:
            if ingestion_time:
                required_keys = ["_PARTITIONDATE", "_PARTITIONTIME"]
                if not re.search(r"\b(_PARTITIONDATE|_PARTITIONTIME)\b", sql, re.IGNORECASE):
                    ok = False
                    reason = "missing ingestion-time partition filter"
            else:
                required_keys = [str(partition_key)] if partition_key else []
                if partition_key and not re.search(rf"\b{re.escape(str(partition_key))}\b", sql, re.IGNORECASE):
                    ok = False
                    reason = "missing partition filter"
        summary.append(
            {
                "table": table,
                "partition_key": partition_key,
                "ok": ok,
                "required_keys": required_keys,
                "reason": reason,
            }
        )
        if required_keys and not ok:
            findings.append(
                Finding(
                    severity="ERROR",
                    code="PARTITION_MISSING",
                    message=f"Partition filter missing for {table}.",
                    evidence=", ".join(required_keys),
                    table=table,
                )
            )
    return findings, summary
