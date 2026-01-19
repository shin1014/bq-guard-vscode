from __future__ import annotations

from typing import Dict, Optional

from google.cloud import bigquery


def fetch_table_metadata(client: bigquery.Client, table_id: str) -> Optional[Dict[str, str]]:
    try:
        table = client.get_table(table_id)
    except Exception:
        return None

    partition_type = "none"
    partition_key = None
    ingestion_time = False
    if table.time_partitioning:
        partition_type = "time"
        partition_key = table.time_partitioning.field
        if partition_key is None:
            ingestion_time = True
    if table.range_partitioning:
        partition_type = "range"
        partition_key = table.range_partitioning.field
    return {
        "partition_type": partition_type,
        "partition_key": partition_key,
        "ingestion_time": ingestion_time,
    }
