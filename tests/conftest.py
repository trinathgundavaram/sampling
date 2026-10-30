"""Test setup: the engine runs on DuckDB standing in for teradatasql (same DB-API shape, ? parameters).

The stand-in keeps one DuckDB connection for every cursor (so transactions span statements, as on Teradata)
and maps the `autocommit` switch to BEGIN / COMMIT. Teradata-only syntax is avoided: Settings(access_lock=False).
"""
import os
import random
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "airflow"))

duckdb = pytest.importorskip("duckdb")

from sampling.db import Connection  # noqa: E402
from sampling.settings import Settings  # noqa: E402

META_DB = "main"


class _Cursor:
    def __init__(self, raw):
        self._raw = raw
        self.description = None
        self.rowcount = -1

    def execute(self, sql, params=None):
        import re
        top = re.match(r"\s*SELECT TOP (\d+) (.*)", sql, re.S)   # Teradata's TOP n -> LIMIT n
        if top:
            sql = f"SELECT {top.group(2)} LIMIT {top.group(1)}"
        self._raw.execute(sql, params) if params else self._raw.execute(sql)
        self.description = self._raw.description
        if self.description and self.description[0][0] == "Count" and not sql.lstrip().upper().startswith("SELECT"):
            self.rowcount = int(self._raw.fetchone()[0])
            self.description = None

    def executemany(self, sql, rows):
        self._raw.executemany(sql, rows)

    def fetchall(self):
        return self._raw.fetchall()

    def fetchmany(self, n):
        return self._raw.fetchmany(n)

    def close(self):
        pass


class DuckRaw:
    """teradatasql-like connection over one DuckDB connection."""

    def __init__(self):
        self.db = duckdb.connect(":memory:")
        self._auto = True
        self.fail_on = None   # a SQL fragment: execute() raises when a statement contains it

    @property
    def autocommit(self):
        return self._auto

    @autocommit.setter
    def autocommit(self, on):
        if not on and self._auto:
            self.db.execute("BEGIN TRANSACTION")
        self._auto = on

    def cursor(self):
        raw = self

        class C(_Cursor):
            def execute(self, sql, params=None):
                if raw.fail_on and raw.fail_on in sql:
                    raise RuntimeError(f"[Teradata Database] [Error 9999] injected failure on {raw.fail_on}")
                super().execute(sql, params)

            def executemany(self, sql, rows):
                if raw.fail_on and raw.fail_on in sql:
                    raise RuntimeError(f"[Teradata Database] [Error 9999] injected failure on {raw.fail_on}")
                super().executemany(sql, rows)
        return C(self.db)

    def commit(self):
        self.db.commit()

    def rollback(self):
        self.db.rollback()

    def close(self):
        self.db.close()


def load_cli():
    """local/run_sampling.py as a module (its `sampling` package is the same code as airflow/sampling)."""
    import importlib.util
    spec = importlib.util.spec_from_file_location("run_sampling", ROOT / "local" / "run_sampling.py")
    module = importlib.util.module_from_spec(spec)
    sys.modules["run_sampling"] = module
    spec.loader.exec_module(module)
    return module


def create_tables(db):
    """sql/schema.sql with Teradata-only clauses translated for DuckDB."""
    import re
    text = (ROOT / "sql" / "schema.sql").read_text().replace("{{META_DB}}", META_DB)
    for stmt in load_cli().split_statements(text):
        if stmt.upper().startswith("CREATE INDEX"):
            continue
        stmt = re.sub(r"CREATE MULTISET TABLE", "CREATE TABLE", stmt)
        stmt = re.sub(r"\)\s*(UNIQUE\s+)?PRIMARY INDEX \([^)]*\)\s*$", ")", stmt, flags=re.S)
        stmt = re.sub(r"BIGINT GENERATED ALWAYS AS IDENTITY \([^)]*\)", "BIGINT", stmt)
        stmt = (stmt.replace("BYTEINT", "TINYINT").replace("CLOB", "VARCHAR")
                .replace("CURRENT_TIMESTAMP(6)", "CURRENT_TIMESTAMP").replace("TIMESTAMP(6)", "TIMESTAMP"))
        stmt = re.sub(r"\bFLOAT\b", "DOUBLE", stmt)
        stmt = re.sub(r"--[^\n]*", "", stmt)
        db.execute(stmt)


@pytest.fixture
def raw():
    r = DuckRaw()
    create_tables(r.db)
    return r


@pytest.fixture
def conn(raw):
    return Connection(raw, host="duckdb", access_lock=False)


@pytest.fixture
def settings():
    return Settings(meta_db=META_DB, environment="DEV", access_lock=False, chunk_rows=7)


def insert_config(db, config_id=1, sampling_method="RANKED", priority_rank_sql="revision DESC", target_volume=150,
                  rounding_mode="FLOOR", universe_table="case_universe", key_columns="case_id",
                  exclusion_sql="auto_closed = 1", scope_sql="pull_date = '{batch_id}'", project_name="UMCM",
                  process_name="WEEKLY", sample_name="weekly_sample", act_ind=1, **more):
    db.execute("""
        INSERT INTO gre_sampling_config (config_id, act_ind, project_name, process_name, sample_name,
            universe_table, key_columns, scope_sql, exclusion_sql, target_volume, sampling_method,
            priority_rank_sql, rounding_mode)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
               [config_id, act_ind, project_name, process_name, sample_name, universe_table, key_columns,
                scope_sql, exclusion_sql, target_volume, sampling_method, priority_rank_sql, rounding_mode])
    set_config(db, config_id, **more)


def insert_rule(db, rule_class, rule_id, rule_name, config_id=1, seq_no=100, act_ind=1, **cols):
    """One gre_sampling_rule row; cols = any of its other columns."""
    row = {"rule_id": rule_id, "config_id": config_id, "act_ind": act_ind, "rule_class": rule_class,
           "seq_no": seq_no, "rule_name": rule_name, **cols}
    db.execute(f"INSERT INTO gre_sampling_rule ({', '.join(row)}) VALUES ({', '.join('?' * len(row))})",
               list(row.values()))


def insert_strata(db, strata_id, config_id, level_order, name, expr, mix: dict, applies_to="*",
                  unlisted_policy="SHARE"):
    """One LEVEL rule (rule_id 1000 * strata_id) and its BUCKET rules; mix = {bucket_name: fraction}
    (FRACTION buckets) or {bucket_name: dict of bucket columns}."""
    insert_rule(db, "LEVEL", 1000 * strata_id, name, config_id, seq_no=1, rule_sql=expr, level_order=level_order,
                parent_path=applies_to, unlisted_policy=unlisted_policy)
    for i, (bucket, spec) in enumerate(mix.items()):
        insert_bucket(db, 1000 * strata_id + i + 1, level_order, bucket, config_id=config_id, seq_no=i + 1,
                      **(spec if isinstance(spec, dict) else {"target_fraction": spec}))


def insert_bucket(db, rule_id, level_order, bucket_name, config_id=1, parent_path="*", seq_no=100, rule_sql=None,
                  quota_mode="FRACTION", target_fraction=None, max_count=None, act_ind=1):
    insert_rule(db, "BUCKET", rule_id, bucket_name, config_id, seq_no, act_ind, rule_sql=rule_sql,
                level_order=level_order, parent_path=parent_path, quota_mode=quota_mode,
                target_fraction=target_fraction, max_count=max_count)


def set_config(db, config_id=1, **cols):
    for name, value in cols.items():
        db.execute(f"UPDATE gre_sampling_config SET {name} = ? WHERE config_id = ?", [value, config_id])


def build_universe(db, n=1000, table="case_universe"):
    """The fixture universe of the DQ repo's frozen UM regression test (random.seed(42))."""
    db.execute(f"CREATE TABLE {table} (case_id VARCHAR, category VARCHAR, area VARCHAR, revision INTEGER, "
               f"pull_date DATE, auto_closed INTEGER)")
    categories = ["Denied"] * 80 + ["Withdrawn"] * 10 + ["Dismissed"] * 2 + ["Approved"] * 8
    areas = ["Area A"] * 13 + ["Area B"] * 8 + ["Area C"] * 79
    random.seed(42)
    rows = [(f"C{i}", random.choice(categories), random.choice(areas), random.choice([0, 1]), "2026-08-01",
             1 if i % 50 == 0 else 0) for i in range(n)]
    db.executemany(f"INSERT INTO {table} VALUES (?, ?, ?, ?, ?, ?)", rows)


def um_config(db, **kw):
    insert_config(db, **kw)
    insert_strata(db, 1, kw.get("config_id", 1), 0, "category", "category",
                  {"Denied": 0.80, "Withdrawn": 0.10, "Dismissed": 0.02, "Approved": 0.08})
    insert_strata(db, 2, kw.get("config_id", 1), 1, "area", "area", {"Area A": 0.13, "Area B": 0.08})
