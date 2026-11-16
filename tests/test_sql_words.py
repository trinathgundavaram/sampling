import re
from pathlib import Path

RESERVED = {"SEL", "DEL", "INS", "UPD", "ABS", "ALL", "AND", "ANY", "CASE", "CAST", "CHAR", "COUNT", "DATE", "DAY",
            "DEFAULT", "DESC", "END", "FROM", "GROUP", "HOUR", "IN", "INDEX", "KEY", "LEFT", "MAX", "MIN", "MINUTE",
            "MONTH", "NULL", "ORDER", "RIGHT", "SAMPLE", "SECOND", "SELECT", "SUM", "TIME", "TITLE", "TOP", "USER",
            "VALUE", "WHERE", "YEAR"}
SOURCES = sorted((Path(__file__).parent.parent / "airflow" / "sampling").glob("*.py"))
SOURCES.append(Path(__file__).parent.parent / "airflow_resources" / "docs" / "tableau" / "ODAG1_dashboard_views.sql")


def test_no_alias_is_a_teradata_reserved_word():
    found = []
    for path in SOURCES:
        for alias in re.findall(r"\bAS ([A-Za-z_][A-Za-z_0-9]*)\b(?!\s*[()])", path.read_text()):
            if alias.upper() in RESERVED:
                found.append(f"{path.name}: AS {alias}")
    assert not found, found
