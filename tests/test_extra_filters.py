"""extra_filters: {column: value} given at run time, ANDed to the scope (as the GRE's extra_filters), for tables
that hold several loads, batches or SCD2 versions of a case."""
import json
from datetime import date

import pytest

from conftest import insert_config, insert_strata, load_cli
from sampling.db import extra_filters_clause
from sampling.engine import run_sampling, validate

AS_OF = date(2026, 8, 7)


def _scd2(db):
    """Cases c00..c09 in batches B1 and B2; in B2 each case has an old version (N) and a current one (Y)."""
    db.execute("CREATE TABLE h (case_id VARCHAR, disp VARCHAR, btch_id VARCHAR, curr_ind VARCHAR)")
    rows = [(f"c{i:02d}", "Denied", "B1", "Y") for i in range(10)]
    rows += [(f"c{i:02d}", "Denied", "B2", ind) for i in range(10) for ind in ("N", "Y")]
    db.executemany("INSERT INTO h VALUES (?, ?, ?, ?)", rows)
    insert_config(db, universe_table="h", scope_sql=None, exclusion_sql=None, target_volume=4,
                  priority_rank_sql="case_id")
    insert_strata(db, 1, 1, 0, "disp", "disp", {"Denied": 1.0})


def _rows(raw, sql):
    return raw.db.execute(sql).fetchall()


def test_the_clause():
    assert extra_filters_clause(None) == "" and extra_filters_clause({}) == ""
    assert extra_filters_clause({"ETL_IS_CURR_IND": "Y", "btch_id": "O'B", "END_DT": None}) == \
        "btch_id = 'O''B' AND END_DT IS NULL AND ETL_IS_CURR_IND = 'Y'"
    for bad in ({"a; DROP": 1}, {"a b": 1}, {"1a": 1}, {"t.col": 1}):
        with pytest.raises(ValueError, match="plain column names"):
            extra_filters_clause(bad)
    with pytest.raises(ValueError, match="one value each"):
        extra_filters_clause({"a": [1, 2]})


def test_filters_pick_the_load_and_the_current_version(raw, conn, settings):
    out = run_sampling(conn, settings, 1, run_key="B2", as_of=AS_OF,
                       extra_filters={"btch_id": "B2", "curr_ind": "Y"})
    assert out["status"] == "COMPLETED" and out["in_scope"] == 10 and out["selected"] == 4
    assert out["params"]["extra_filters"] == {"btch_id": "B2", "curr_ind": "Y"}
    # kept with the run's parameters: a case replacement rebuilds its SQL from them
    assert json.loads(_rows(raw, "SELECT run_params_txt FROM gre_sampling_run")[0][0])["extra_filters"] == \
        {"btch_id": "B2", "curr_ind": "Y"}


def test_without_filters_the_duplicate_versions_are_refused(raw, conn, settings):
    out = run_sampling(conn, settings, 1, run_key="ALL", as_of=AS_OF)
    assert out["status"] == "ERROR" and "duplicate keys" in out["error"]


def test_filters_add_to_scope_sql(raw, conn, settings):
    raw.db.execute("UPDATE gre_sampling_config SET scope_sql = 'case_id < ''c05''' WHERE config_id = 1")
    out = run_sampling(conn, settings, 1, run_key="B1", as_of=AS_OF, extra_filters={"btch_id": "B1"})
    assert out["in_scope"] == 5 and out["selected"] == 4


def test_a_bad_filter_column_is_a_config_error(raw, conn, settings):
    out = run_sampling(conn, settings, 1, run_key="B1", as_of=AS_OF, extra_filters={"btch_id = 'B1' OR 1": "1"})
    assert out["error_type"] == "CONFIG_ERROR" and "plain column names" in out["error"]
    assert _rows(raw, "SELECT COUNT(*) FROM gre_sample_selections") == [(0,)]


def test_validate_checks_the_filters(raw, conn, settings):
    ok = validate(conn, settings, config_id=1, check_sql=True, extra_filters={"btch_id": "B1"})
    assert ok["result"] == "VALID"
    bad = validate(conn, settings, config_id=1, check_sql=True, extra_filters={"no_such_column": "x"})
    assert "SQL check failed" in bad["configs"][0]["problems"][0]


def test_cli_filter_argument():
    cli = load_cli()
    args = cli.build_parser().parse_args(["run", "--config-id", "1", "--filter", "BTCH_ID=B=2", "--filter", "X=Y"])
    assert cli._filters(args) == {"BTCH_ID": "B=2", "X": "Y"}


@pytest.fixture(autouse=True)
def _data(raw):
    _scd2(raw.db)
