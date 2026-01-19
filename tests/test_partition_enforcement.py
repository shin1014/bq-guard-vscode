from bq_guard.policy.partition import enforce_partition_filters


def test_ingestion_time_partition_ok():
    findings, summary = enforce_partition_filters(
        "SELECT * FROM t WHERE _PARTITIONDATE = '2024-01-01'",
        ["p.d.t"],
        {"p.d.t": {"partition_type": "time", "partition_key": None, "ingestion_time": True}},
        [],
        True,
    )
    assert not findings
    assert summary[0]["ok"] is True


def test_ingestion_time_partition_missing():
    findings, summary = enforce_partition_filters(
        "SELECT * FROM t",
        ["p.d.t"],
        {"p.d.t": {"partition_type": "time", "partition_key": None, "ingestion_time": True}},
        [],
        True,
    )
    assert findings and findings[0].code == "PARTITION_MISSING"
    assert summary[0]["ok"] is False


def test_column_partition_missing():
    findings, summary = enforce_partition_filters(
        "SELECT * FROM t",
        ["p.d.t"],
        {"p.d.t": {"partition_type": "time", "partition_key": "event_date", "ingestion_time": False}},
        [],
        True,
    )
    assert findings and findings[0].code == "PARTITION_MISSING"
    assert summary[0]["ok"] is False


def test_partition_exemption():
    findings, summary = enforce_partition_filters(
        "SELECT * FROM t",
        ["p.d.t"],
        {"p.d.t": {"partition_type": "time", "partition_key": "event_date", "ingestion_time": False}},
        ["p.d.t"],
        True,
    )
    assert not findings
    assert summary[0]["reason"] == "exempt"


def test_unknown_tables_warn():
    findings, summary = enforce_partition_filters(
        "SELECT * FROM t",
        [],
        {},
        [],
        True,
    )
    assert findings and findings[0].code == "TABLES_UNKNOWN"
    assert summary == []
