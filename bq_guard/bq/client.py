from __future__ import annotations

from typing import Any, Dict, Optional

from google.cloud import bigquery


def get_client(project: Optional[str]) -> bigquery.Client:
    return bigquery.Client(project=project)


def build_job_config(use_query_cache: bool, labels: Dict[str, Any], dry_run: bool) -> bigquery.QueryJobConfig:
    config = bigquery.QueryJobConfig()
    config.dry_run = dry_run
    config.use_query_cache = use_query_cache
    config.labels = labels
    return config
