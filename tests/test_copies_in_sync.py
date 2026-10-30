"""The Airflow and the local version run the same engine: their copies must not drift apart."""
from conftest import ROOT

SHARED = ("__init__.py", "db.py", "engine.py", "settings.py")


def test_engine_copies_are_identical():
    for name in SHARED:
        airflow = (ROOT / "airflow" / "sampling" / name).read_bytes()
        local = (ROOT / "local" / "sampling" / name).read_bytes()
        assert airflow == local, f"airflow/sampling/{name} and local/sampling/{name} differ: copy the change across"


def test_engine_never_imports_airflow():
    assert not (ROOT / "local" / "sampling" / "dags.py").exists()
    for name in SHARED:
        text = (ROOT / "local" / "sampling" / name).read_text()
        assert "import airflow" not in text and "from airflow" not in text, name
