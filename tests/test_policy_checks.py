from bq_guard.policy.checks import (
    check_cross_join,
    check_ddl_dml,
    check_multi_statement,
    check_select_star,
    check_bytes,
)


def test_select_star_detected():
    findings = check_select_star("SELECT * FROM table", enabled=True)
    assert findings and findings[0].code == "SELECT_STAR"


def test_cross_join_detected():
    findings = check_cross_join("SELECT 1 FROM a CROSS JOIN b", enabled=True)
    assert findings and findings[0].code == "CROSS_JOIN"


def test_multi_statement_detected():
    findings = check_multi_statement("SELECT 1; SELECT 2;", block=True)
    assert findings and findings[0].code == "MULTI_STATEMENT" and findings[0].severity == "ERROR"


def test_multi_statement_allows_trailing_semicolon():
    findings = check_multi_statement("SELECT 1;", block=True)
    assert not findings


def test_ddl_dml_detected():
    findings = check_ddl_dml("DELETE FROM table WHERE id=1", enabled=True)
    assert findings and findings[0].code == "DDL_DML"


def test_bytes_thresholds():
    warn_findings = check_bytes(200, warn_bytes=100, block_bytes=500)
    assert warn_findings and warn_findings[0].severity == "WARN"
    error_findings = check_bytes(600, warn_bytes=100, block_bytes=500)
    assert error_findings and error_findings[0].severity == "ERROR"
