"""extra_filters: a SQL condition given at run time, ANDed to the scope, for tables
that hold several loads, batches or SCD2 versions of a case."""
import json
from datetime import date

import pytest

from conftest import insert_config, insert_rule, load_cli
from sampling.engine import run_sampling, validate

AS_OF = date(2026, 8, 7)
CURRENT_B2 = "btch_id = 'B2' AND curr_ind = 'Y'"


@pytest.fixture(autouse=True)
def _data(raw):
    """Cases c00..c09 in batches B1 and B2; in B2 each case has an old version (N) and a current one (Y)."""
    db = raw.db
    db.execute("CREATE TABLE h (case_id VARCHAR, disp VARCHAR, btch_id VARCHAR, curr_ind VARCHAR)")
    rows = [(f"c{i:02d}", "Denied", "B1", "Y") for i in range(10)]
    rows += [(f"c{i:02d}", "Denied", "B2", ind) for i in range(10) for ind in ("N", "Y")]
    db.executemany("INSERT INTO h VALUES (?, ?, ?, ?)", rows)
    insert_config(db, universe_table="h", scope_sql=None, exclusion_sql=None, target_volume=4,
                  priority_rank_sql="case_id")
    # the level's SELECT reads one version of each case of the run's batch
    insert_rule(db, "LEVEL", 1000, "disp", seq_no=1, level_order=0, parent_path="*",
                rule_sql="SELECT case_id, disp FROM h WHERE btch_id = '{batch}' AND curr_ind = 'Y'")
    db.execute("INSERT INTO GOVT_RGLTRY_SAMPLING_RULE (rule_id, config_id, act_ind, rule_class, seq_no, rule_name, "
               "level_order, parent_path, quota_mode, target_fraction) "
               "VALUES (1001, 1, 1, 'BUCKET', 1, 'Denied', 0, '*', 'FRACTION', 1.0)")


def _rows(raw, sql):
    return raw.db.execute(sql).fetchall()


def test_a_filter_picks_the_load_and_the_current_version(raw, conn, settings):
    out = run_sampling(conn, settings, 1, run_key="B2", as_of=AS_OF, run_params={"batch": "B2"},
                       extra_filters=CURRENT_B2)
    assert out["status"] == "COMPLETED", out.get("error")
    assert out["in_scope"] == 10 and out["selected"] == 4
    assert out["params"]["extra_filters"] == CURRENT_B2
    # kept with the run's parameters: a case replacement rebuilds its SQL from them
    assert json.loads(_rows(raw, "SELECT run_params_txt FROM GOVT_RGLTRY_SAMPLING_RUN")[0][0])["extra_filters"] == CURRENT_B2


def test_a_filter_may_use_run_parameters(raw, conn, settings):
    out = run_sampling(conn, settings, 1, run_key="B1", as_of=AS_OF, run_params={"batch": "B1"},
                       extra_filters="btch_id = '{batch}'")
    assert out["status"] == "COMPLETED" and out["in_scope"] == 10


def test_without_filters_the_duplicate_versions_are_refused(raw, conn, settings):
    out = run_sampling(conn, settings, 1, run_key="ALL", as_of=AS_OF, run_params={"batch": "B2"})
    assert out["status"] == "ERROR" and "duplicate keys" in out["error"]


def test_filters_add_to_scope_sql(raw, conn, settings):
    raw.db.execute("UPDATE GOVT_RGLTRY_SAMPLING_CONFIG SET scope_sql = 'case_id < ''c05''' WHERE config_id = 1")
    out = run_sampling(conn, settings, 1, run_key="B1", as_of=AS_OF, run_params={"batch": "B1"},
                       extra_filters="btch_id = 'B1'")
    assert out["in_scope"] == 5 and out["selected"] == 4


@pytest.mark.parametrize("bad, found", [
    ("btch_id = 'B1'; DELETE FROM GOVT_RGLTRY_SAMPLING_RUN", "';' is not allowed"),
    ("btch_id = 'B1' -- the rest", "'--' comments"),
    ("btch_id IN (SELECT 1", "parentheses"),
])
def test_a_filter_must_only_read(raw, conn, settings, bad, found):
    out = run_sampling(conn, settings, 1, run_key="B1", as_of=AS_OF, run_params={"batch": "B1"}, extra_filters=bad)
    assert out["error_type"] == "CONFIG_ERROR" and found in out["error"]
    assert _rows(raw, "SELECT COUNT(*) FROM GOVT_RGLTRY_SAMPLE_SELECTIONS") == [(0,)]


def test_validate_checks_the_filters(raw, conn, settings):
    ok = validate(conn, settings, config_id=1, check_sql=True, run_params={"batch": "B1"},
                  extra_filters="btch_id = 'B1'")
    assert ok["result"] == "VALID", ok
    bad = validate(conn, settings, config_id=1, check_sql=True, run_params={"batch": "B1"},
                   extra_filters="no_such_column = 'x'")
    assert "SQL check failed" in bad["configs"][0]["problems"][0]


def test_cli_filter_argument():
    cli = load_cli()
    args = cli.build_parser().parse_args(["run", "--config-id", "1", "--filter", "BTCH_ID = 'B=2'",
                                          "--filter", "X = 'Y' OR Z = 1"])
    assert cli._filters(args) == "(BTCH_ID = 'B=2') AND (X = 'Y' OR Z = 1)"
    assert cli._filters(cli.build_parser().parse_args(["run", "--filter", "A = 1"])) == "A = 1"
