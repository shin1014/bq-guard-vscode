from __future__ import annotations

import csv
from typing import Any, Dict, List, Optional

from google.cloud import bigquery

from .client import build_job_config


def dry_run_query(
    client: bigquery.Client,
    sql: str,
    location: Optional[str],
    use_query_cache: bool,
    labels: Dict[str, Any],
) -> bigquery.QueryJob:
    job_config = build_job_config(use_query_cache=use_query_cache, labels=labels, dry_run=True)
    return client.query(sql, job_config=job_config, location=location)


def execute_query(
    client: bigquery.Client,
    sql: str,
    location: Optional[str],
    use_query_cache: bool,
    labels: Dict[str, Any],
) -> bigquery.QueryJob:
    job_config = build_job_config(use_query_cache=use_query_cache, labels=labels, dry_run=False)
    return client.query(sql, job_config=job_config, location=location)


def fetch_preview_rows(
    client: bigquery.Client,
    job_id: str,
    location: Optional[str],
    max_rows: int,
) -> Dict[str, Any]:
    job = client.get_job(job_id, location=location)
    result_iter = job.result(max_results=max_rows)
    rows = list(result_iter)
    columns = [field.name for field in result_iter.schema]
    data = [[row.get(col) for col in columns] for row in rows]
    return {"columns": columns, "rows": data}


def fetch_page_rows(
    client: bigquery.Client,
    job_id: str,
    location: Optional[str],
    page_size: int,
    page_token: Optional[str],
) -> Dict[str, Any]:
    job = client.get_job(job_id, location=location)
    result_iter = job.result(page_size=page_size, page_token=page_token)
    page = next(result_iter.pages)
    rows = list(page)
    columns = [field.name for field in result_iter.schema]
    data = [[row.get(col) for col in columns] for row in rows]
    return {"columns": columns, "rows": data, "page_token": result_iter.next_page_token}


def export_rows(
    client: bigquery.Client,
    job_id: str,
    location: Optional[str],
    mode: str,
    out_path: str,
    page_size: int,
) -> int:
    job = client.get_job(job_id, location=location)
    total_rows = 0
    with open(out_path, "w", encoding="utf-8", newline="") as handle:
        writer = None
        if mode == "preview":
            result_iter = job.result(max_results=page_size)
            rows = list(result_iter)
            columns = [field.name for field in result_iter.schema]
            writer = csv.writer(handle)
            writer.writerow(columns)
            for row in rows:
                writer.writerow([row.get(col) for col in columns])
                total_rows += 1
        else:
            result_iter = job.result(page_size=page_size)
            columns = [field.name for field in result_iter.schema]
            writer = csv.writer(handle)
            writer.writerow(columns)
            for page in result_iter.pages:
                for row in page:
                    writer.writerow([row.get(col) for col in columns])
                    total_rows += 1
    return total_rows
