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
    files = sorted(f for f in (DOCS / "custom_sql").glob("*.sql") if int(f.name[:2]) <= 10)
    assert [f.stem.split("_", 1)[1].replace("_", " ") for f in files] == list(tables)
    for f in files:
        assert f.read_text() == tables[f.stem.split("_", 1)[1].replace("_", " ")] + "\n", f.name


def test_every_field_is_mapped_to_its_table():
    _, ds = _tables()
    conn = ds.find("connection")
    maps = {m.get("key"): m.get("value") for m in conn.find("cols")}
    records = [m for m in conn.find("metadata-records") if m.get("class") == "column"]
    assert len(maps) == len(records)
    for m in records:
        assert maps[m.findtext("local-name")] == f"{m.findtext('parent-name')}.[{m.findtext('remote-name')}]"


def test_gre_files_have_unique_columns_and_no_angle_brackets():
    names = []
    for f in sorted((DOCS / "custom_sql").glob("*_GRE_*.sql")):
        text = f.read_text()
        assert not re.search(r"[<>]", text), f.name
        names += re.findall(r" AS (G[A-Z]*_\w+)\b", text.split("\nFROM ")[0])
    assert len(names) == 112
    assert len(names) == len(set(n.upper() for n in names))


def test_odag1_data_file_has_the_record_key_and_no_angle_brackets():
    text = (DOCS / "custom_sql" / "16_ODAG1_data.sql").read_text()
    assert not re.search(r"[<>]", text)
    assert "AS Src_Data_Key_Txt" in text and "HSLABBOSS.SDO_GOVT_RGLTRY_CLNCL_AUD_UM_AUTHZN_ODAG1 o" in text


def test_gre_files_read_hsabc_v():
    for f in sorted((DOCS / "custom_sql").glob("*_GRE_*.sql")):
        dbs = set(re.findall(r"\b(HS\w+)\.", f.read_text()))
        assert dbs == {"HSABC_V"}, (f.name, dbs)
