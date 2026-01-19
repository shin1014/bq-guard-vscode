from __future__ import annotations

import json
import os
import sys
from dataclasses import asdict
from typing import Any, Dict, List, Optional

from google.cloud import bigquery

from .app_model import EstimateResult, ExecuteResult, FetchResult
from .bq.client import get_client
from .bq.jobs import dry_run_query, execute_query, export_rows, fetch_page_rows, fetch_preview_rows
from .bq.metadata import fetch_table_metadata
from .cache import TableMetaCache
from .config import ConfigLoader, get_history_path, get_cache_path
from .gcloud import get_default_location, get_default_project
from .history import append_history
from .policy.checks import run_policy_checks
from .policy.partition import enforce_partition_filters
from .policy.sql_sanitize import extract_tables


def bytes_human(num: int) -> str:
    for unit in ["B", "KB", "MB", "GB", "TB", "PB"]:
        if num < 1024.0:
            return f"{num:3.1f}{unit}"
        num /= 1024.0
    return f"{num:.1f}EB"


def _referenced_tables_from_job(job: bigquery.QueryJob) -> List[str]:
    try:
        tables = job.referenced_tables or []
    except Exception:
        tables = []
    return [f"{t.project}.{t.dataset_id}.{t.table_id}" for t in tables]


def _resolve_project_location(config: Dict[str, Any]) -> Dict[str, Optional[str]]:
    project = config["app"].get("default_project") or get_default_project()
    location = config["app"].get("default_location") or get_default_location() or "asia-northeast1"
    return {"project": project, "location": location}


def _ensure_cache(cache: TableMetaCache, client: bigquery.Client, tables: List[str]) -> Dict[str, Dict[str, Any]]:
    missing = cache.missing(tables)
    for table in missing:
        meta = fetch_table_metadata(client, table)
        if meta:
            cache.set(table, meta)
    cache.save()
    return {table: cache.get(table) for table in tables if cache.get(table)}


def _run_estimate(sql: str, config: Dict[str, Any]) -> Dict[str, Any]:
    resolved = _resolve_project_location(config)
    project = resolved["project"]
    location = resolved["location"]
    client = get_client(project)
    try:
        job = dry_run_query(
            client,
            sql,
            location,
            config["app"]["bq"]["use_query_cache"],
            config["app"]["bq"]["labels"],
        )
    except Exception as exc:
        append_history(
            {
                "status": "DRYRUN_FAILED",
                "project": project,
                "location": location,
                "sql": sql,
                "error": str(exc),
            }
        )
        raise

    bytes_processed = int(job.total_bytes_processed or 0)
    referenced = _referenced_tables_from_job(job)
    if not referenced:
        referenced = extract_tables(sql)

    cache = TableMetaCache(config["app"]["cache"]["schema_version"])
    table_meta = _ensure_cache(cache, client, referenced)

    findings = run_policy_checks(sql, bytes_processed, config["app"]["policy"], config["app"]["limits"])
    partition_findings, partition_summary = enforce_partition_filters(
        sql,
        referenced,
        table_meta,
        config["app"]["exceptions"]["partition_exempt_tables"],
        config["app"]["policy"]["enforce_partition_filter"],
    )
    findings.extend(partition_findings)

    result = EstimateResult(
        bytes_processed=bytes_processed,
        bytes_human=bytes_human(bytes_processed),
        referenced_tables=referenced,
        findings=findings,
        partition_summary=[
            dict(item)
            for item in partition_summary
        ],
    )
    append_history(
        {
            "status": "ESTIMATED",
            "project": project,
            "location": location,
            "sql": sql,
            "dry_run_bytes": bytes_processed,
            "referenced_tables": referenced,
            "findings": [asdict(f) for f in findings],
        }
    )
    return {
        "project": project,
        "location": location,
        "result": result,
    }


def handle_request(payload: Dict[str, Any]) -> Dict[str, Any]:
    config = ConfigLoader().load()
    op = payload.get("op")
    sql = payload.get("sql")

    if op in {"estimate", "review"}:
        if not sql:
            return {"ok": False, "error": {"message": "SQL is required."}}
        try:
            estimate_data = _run_estimate(sql, config)
        except Exception as exc:
            return {"ok": False, "error": {"message": "Dry run failed.", "detail": str(exc)}}
        result: EstimateResult = estimate_data["result"]
        if op == "review":
            has_error = any(finding.severity == "ERROR" for finding in result.findings)
            if has_error:
                append_history(
                    {
                        "status": "BLOCKED",
                        "project": estimate_data["project"],
                        "location": estimate_data["location"],
                        "sql": sql,
                        "dry_run_bytes": result.bytes_processed,
                        "referenced_tables": result.referenced_tables,
                        "findings": [asdict(f) for f in result.findings],
                    }
                )
        return {
            "ok": True,
            "project": estimate_data["project"],
            "location": estimate_data["location"],
            "estimate": {
                "bytes_processed": result.bytes_processed,
                "bytes_human": result.bytes_human,
                "referenced_tables": result.referenced_tables,
                "findings": [asdict(f) for f in result.findings],
                "partition_summary": result.partition_summary,
            },
        }

    if op == "execute":
        if not sql:
            return {"ok": False, "error": {"message": "SQL is required."}}
        resolved = _resolve_project_location(config)
        client = get_client(resolved["project"])
        try:
            job = execute_query(
                client,
                sql,
                resolved["location"],
                config["app"]["bq"]["use_query_cache"],
                config["app"]["bq"]["labels"],
            )
            job_id = job.job_id
            result = ExecuteResult(job_id=job_id, status="EXECUTED")
            append_history(
                {
                    "status": "EXECUTED",
                    "project": resolved["project"],
                    "location": resolved["location"],
                    "sql": sql,
                    "job_id": job_id,
                }
            )
            return {"ok": True, "execute": asdict(result)}
        except Exception as exc:
            append_history(
                {
                    "status": "EXEC_FAILED",
                    "project": resolved["project"],
                    "location": resolved["location"],
                    "sql": sql,
                    "error": str(exc),
                }
            )
            return {"ok": False, "error": {"message": "Execute failed.", "detail": str(exc)}}

    if op == "fetch_preview":
        job_id = payload.get("job_id")
        if not job_id:
            return {"ok": False, "error": {"message": "job_id is required."}}
        resolved = _resolve_project_location(config)
        client = get_client(resolved["project"])
        try:
            data = fetch_preview_rows(
                client, job_id, resolved["location"], config["app"]["preview_rows"]
            )
            result = FetchResult(**data)
            return {"ok": True, "preview": asdict(result)}
        except Exception as exc:
            return {"ok": False, "error": {"message": "Preview failed.", "detail": str(exc)}}

    if op == "fetch_page":
        job_id = payload.get("job_id")
        if not job_id:
            return {"ok": False, "error": {"message": "job_id is required."}}
        resolved = _resolve_project_location(config)
        client = get_client(resolved["project"])
        try:
            data = fetch_page_rows(
                client,
                job_id,
                resolved["location"],
                config["app"]["page_size"],
                payload.get("page_token"),
            )
            result = FetchResult(**data)
            return {"ok": True, "page": asdict(result)}
        except Exception as exc:
            return {"ok": False, "error": {"message": "Page fetch failed.", "detail": str(exc)}}

    if op == "export":
        job_id = payload.get("job_id")
        mode = payload.get("mode")
        out_path = payload.get("out_path")
        if not job_id or not mode or not out_path:
            return {"ok": False, "error": {"message": "job_id, mode, out_path required."}}
        resolved = _resolve_project_location(config)
        client = get_client(resolved["project"])
        try:
            total_rows = export_rows(
                client,
                job_id,
                resolved["location"],
                mode,
                out_path,
                config["app"]["page_size"],
            )
            append_history(
                {
                    "status": "EXPORTED",
                    "project": resolved["project"],
                    "location": resolved["location"],
                    "job_id": job_id,
                    "exported_files": [out_path],
                }
            )
            return {"ok": True, "export": {"rows": total_rows, "path": out_path}}
        except Exception as exc:
            append_history(
                {
                    "status": "EXPORT_FAILED",
                    "project": resolved["project"],
                    "location": resolved["location"],
                    "job_id": job_id,
                    "error": str(exc),
                }
            )
            return {"ok": False, "error": {"message": "Export failed.", "detail": str(exc)}}

    if op == "refresh_metadata":
        tables = payload.get("tables") or []
        resolved = _resolve_project_location(config)
        client = get_client(resolved["project"])
        cache = TableMetaCache(config["app"]["cache"]["schema_version"])
        refreshed = []
        for table in tables:
            meta = fetch_table_metadata(client, table)
            if meta:
                cache.set(table, meta)
                refreshed.append(table)
        cache.save()
        return {"ok": True, "refreshed": refreshed}

    if op == "get_effective_config":
        return {
            "ok": True,
            "config": config,
            "paths": {
                "config": ConfigLoader().config_path,
                "history": get_history_path(),
                "cache": get_cache_path(),
            },
        }

    return {"ok": False, "error": {"message": f"Unknown op {op}."}}


def main() -> None:
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        try:
            payload = json.loads(line)
            response = handle_request(payload)
        except Exception as exc:
            response = {"ok": False, "error": {"message": "Unhandled error", "detail": str(exc)}}
        sys.stdout.write(json.dumps(response, ensure_ascii=False) + "\n")
        sys.stdout.flush()


if __name__ == "__main__":
    main()
