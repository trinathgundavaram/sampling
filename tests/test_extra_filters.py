import json
from datetime import date

import pytest

from conftest import insert_config, insert_rule, key, load_cli
from sampling.engine import run_sampling, validate

AS_OF = date(2026, 8, 7)
CURRENT_B2 = "btch_id = 'B2' AND curr_ind = 'Y'"


@pytest.fixture(autouse=True)
def _data(raw):
    db = raw.db
    db.execute("CREATE TABLE h (case_id VARCHAR, disp VARCHAR, btch_id VARCHAR, curr_ind VARCHAR)")
    rows = [(f"c{i:02d}", "Denied", "B1", "Y") for i in range(10)]
    rows += [(f"c{i:02d}", "Denied", "B2", ind) for i in range(10) for ind in ("N", "Y")]
    db.executemany("INSERT INTO h VALUES (?, ?, ?, ?)", rows)
    insert_config(db, src_table_nm="h", scope_sql_txt=None, exclusion_sql=None, target_cnt=4, order_by=None)
    insert_rule(db, "PRIORITY", 900, "by_id", rule_ty="EXPRESSION",
                rule_sql_txt="SELECT DISTINCT case_id, case_id AS v FROM h")
    insert_rule(db, "LEVEL", 1000, "disp", seq_nbr=1, level_nbr=0, parent_path_txt="*",
                rule_sql_txt="SELECT case_id, disp FROM h WHERE btch_id = '{batch}' AND curr_ind = 'Y'")
    db.execute("INSERT INTO GOVT_RGLTRY_SamplingRule (Rule_ID, Project_Cd, Table_Nm, Run_Ty, Active_Ind, Rule_Class_Cd, "
               "Seq_Nbr, Rule_Nm, Level_Nbr, Parent_Path_Txt, Quota_Cd, Quota_Val) "
               "VALUES (1001, 'UMCM', 'T1', 'WKLY', 1, 'BUCKET', 1, 'Denied', 0, '*', 'PCT', 100)")


def _rows(raw, sql):
    return raw.db.execute(sql).fetchall()


def test_a_filter_picks_the_load_and_the_current_version(raw, conn, settings):
    out = run_sampling(conn, settings, key(1), run_key="B2", as_of=AS_OF, run_params={"batch": "B2"},
                       extra_filters=CURRENT_B2)
    assert out["status"] == "COMPLETED", out.get("error")
    assert out["in_scope"] == 10 and out["selected"] == 4
    assert out["params"]["extra_filters"] == CURRENT_B2
    assert json.loads(_rows(raw, "SELECT Run_Parm_Txt FROM GOVT_RGLTRY_SamplingRun")[0][0])["extra_filters"] == CURRENT_B2


def test_a_filter_may_use_run_parameters(raw, conn, settings):
    out = run_sampling(conn, settings, key(1), run_key="B1", as_of=AS_OF, run_params={"batch": "B1"},
                       extra_filters="btch_id = '{batch}'")
    assert out["status"] == "COMPLETED" and out["in_scope"] == 10


def test_without_filters_the_duplicate_versions_are_refused(raw, conn, settings):
    out = run_sampling(conn, settings, key(1), run_key="ALL", as_of=AS_OF, run_params={"batch": "B2"})
    assert out["status"] == "ERROR" and "duplicate keys" in out["error"]


def test_filters_add_to_scope_sql(raw, conn, settings):
    raw.db.execute("UPDATE GOVT_RGLTRY_SamplingConfig SET scope_sql_txt = 'SELECT * FROM main.h WHERE case_id < ''c05''' WHERE Table_Nm = 'T1'")
    out = run_sampling(conn, settings, key(1), run_key="B1", as_of=AS_OF, run_params={"batch": "B1"},
                       extra_filters="btch_id = 'B1'")
    assert out["in_scope"] == 5 and out["selected"] == 4


@pytest.mark.parametrize("bad, found", [
    ("btch_id = 'B1'; DELETE FROM GOVT_RGLTRY_SamplingRun", "';' is not allowed"),
    ("btch_id = 'B1' -- the rest", "'--' comments"),
    ("btch_id IN (SELECT 1", "parentheses"),
])
def test_a_filter_must_only_read(raw, conn, settings, bad, found):
    out = run_sampling(conn, settings, key(1), run_key="B1", as_of=AS_OF, run_params={"batch": "B1"}, extra_filters=bad)
    assert out["error_type"] == "CONFIG_ERROR" and found in out["error"]
    assert _rows(raw, "SELECT COUNT(*) FROM GOVT_RGLTRY_SampleSelection") == [(0,)]


def test_validate_checks_the_filters(raw, conn, settings):
    ok = validate(conn, settings, project="UMCM", check_sql=True, run_params={"batch": "B1"},
                  extra_filters="btch_id = 'B1'")
    assert ok["result"] == "VALID", ok
    bad = validate(conn, settings, project="UMCM", check_sql=True, run_params={"batch": "B1"},
                   extra_filters="no_such_column = 'x'")
    assert "SQL check failed" in bad["configs"][0]["problems"][0]


def test_cli_filter_argument():
    cli = load_cli()
    args = cli.build_parser().parse_args(["run", "--project", "UMCM", "--filter", "BTCH_ID = 'B=2'",
                                          "--filter", "X = 'Y' OR Z = 1"])
    assert cli._filters(args) == "(BTCH_ID = 'B=2') AND (X = 'Y' OR Z = 1)"
    assert cli._filters(cli.build_parser().parse_args(["run", "--filter", "A = 1"])) == "A = 1"
