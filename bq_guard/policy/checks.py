from __future__ import annotations

import re
from typing import List

from .types import Finding
from .sql_sanitize import normalize_sql, split_statements


def check_bytes(bytes_processed: int, warn_bytes: int, block_bytes: int) -> List[Finding]:
    findings: List[Finding] = []
    if bytes_processed >= block_bytes:
        findings.append(
            Finding(
                severity="ERROR",
                code="BYTES_OVER_LIMIT",
                message=f"Estimated bytes {bytes_processed} exceeds block threshold {block_bytes}.",
                evidence=str(bytes_processed),
            )
        )
    elif bytes_processed >= warn_bytes:
        findings.append(
            Finding(
                severity="WARN",
                code="BYTES_OVER_WARN",
                message=f"Estimated bytes {bytes_processed} exceeds warning threshold {warn_bytes}.",
                evidence=str(bytes_processed),
            )
        )
    return findings


def check_select_star(sql: str, enabled: bool) -> List[Finding]:
    if not enabled:
        return []
    if re.search(r"select\s+\*", sql, re.IGNORECASE):
        return [Finding(severity="WARN", code="SELECT_STAR", message="SELECT * detected.")]
    return []


def check_cross_join(sql: str, enabled: bool) -> List[Finding]:
    if not enabled:
        return []
    if re.search(r"cross\s+join", sql, re.IGNORECASE):
        return [Finding(severity="WARN", code="CROSS_JOIN", message="CROSS JOIN detected.")]
    return []


def check_suspect_join(sql: str, enabled: bool) -> List[Finding]:
    if not enabled:
        return []
    normalized = normalize_sql(sql)
    if " join " in normalized and " on " not in normalized and " using " not in normalized:
        return [
            Finding(
                severity="WARN",
                code="SUSPECT_JOIN",
                message="JOIN detected without ON/USING clause.",
            )
        ]
    return []


def check_multi_statement(sql: str, block: bool) -> List[Finding]:
    statements = split_statements(sql)
    if len(statements) <= 1:
        return []
    severity = "ERROR" if block else "WARN"
    return [Finding(severity=severity, code="MULTI_STATEMENT", message="Multiple statements detected.")]


def check_ddl_dml(sql: str, enabled: bool) -> List[Finding]:
    if not enabled:
        return []
    if re.search(r"\b(delete|update|merge|create|drop|alter|truncate|insert)\b", sql, re.IGNORECASE):
        return [Finding(severity="WARN", code="DDL_DML", message="DDL/DML statement detected.")]
    return []


def run_policy_checks(sql: str, bytes_processed: int, policy: dict, limits: dict) -> List[Finding]:
    findings: List[Finding] = []
    findings.extend(check_bytes(bytes_processed, limits["warn_bytes"], limits["block_bytes"]))
    findings.extend(check_select_star(sql, policy.get("warn_select_star", True)))
    findings.extend(check_cross_join(sql, policy.get("warn_cross_join", True)))
    findings.extend(check_suspect_join(sql, policy.get("warn_suspect_join", True)))
    findings.extend(check_multi_statement(sql, policy.get("block_multi_statement", True)))
    findings.extend(check_ddl_dml(sql, policy.get("warn_ddl_dml", True)))
    return findings
