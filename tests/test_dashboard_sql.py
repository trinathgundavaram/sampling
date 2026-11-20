import re
import xml.etree.ElementTree as ET
from pathlib import Path

DOCS = Path(__file__).parent.parent / "airflow_resources" / "docs" / "tableau"
TWB = DOCS / "ODAG1_Sampling_Dashboard.twb"
COPY = DOCS / "ODAG1_dashboard_custom_sql.sql"


def _tables():
    root = ET.parse(TWB).getroot()
    ds = next(d for d in root.iter("datasource") if d.get("name") == "federated.odag1_sampling")
    graph = next(e for e in ds if e.tag.endswith("object-graph"))
    return {o.get("caption"): o.find("properties/relation").text for o in graph.iter("object")}, ds


def test_custom_sql_has_no_angle_brackets():
    tables, _ = _tables()
    found = {name: sorted(set(re.findall(r"[<>]", sql))) for name, sql in tables.items() if re.search(r"[<>]", sql)}
    assert not found, found


def test_every_copy_of_a_table_sql_is_the_same():
    tables, ds = _tables()
    for rel in ds.iter():
        if rel.tag.endswith("relation") and rel.get("type") == "text":
            assert rel.text == tables[rel.get("name")], rel.get("name")


def test_the_readable_copy_matches_the_workbook():
    tables, _ = _tables()
    text = COPY.read_text()
    for name, sql in tables.items():
        assert f"-- {name}: " in text and sql + ";" in text, name


def test_each_table_file_matches_the_workbook():
    tables, _ = _tables()
    files = sorted((DOCS / "custom_sql").glob("*.sql"))
    assert [f.stem.split("_", 1)[1].replace("_", " ") for f in files] == list(tables)
    for f in files:
        assert f.read_text() == tables[f.stem.split("_", 1)[1].replace("_", " ")] + "\n", f.name
