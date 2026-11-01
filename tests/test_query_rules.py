"""rule_type QUERY: a whole SELECT (joins and all, as a GRE rule) returning the key_columns of the matching cases,
for EXCLUSION, PRIORITY and BUCKET rules whose test needs another table or several of them."""
from datetime import date

import pytest

from conftest import insert_bucket, insert_config, insert_rule
from sampling.engine import load_config, config_problems, run_sampling, validate

AS_OF = date(2026, 8, 7)
# cases whose procedure code is on the BH code list and whose provider is a BH provider type
BH_QUERY = ("SELECT c.case_id FROM cases c JOIN bh_codes b ON b.proc_cd = c.proc_cd "
            "JOIN providers p ON p.prov_id = c.prov_id WHERE p.prov_ty IN ('PSY', 'LCSW') AND b.act = 'Y'")


@pytest.fixture(autouse=True)
def _data(raw):
    """200 cases; every 5th one is BH (code on the list and a BH provider): 40 BH cases."""
    db = raw.db
    db.execute("CREATE TABLE cases (case_id VARCHAR, proc_cd VARCHAR, prov_id INTEGER, disp VARCHAR)")
    db.execute("CREATE TABLE bh_codes (proc_cd VARCHAR, act VARCHAR)")
    db.execute("CREATE TABLE providers (prov_id INTEGER, prov_ty VARCHAR)")
    db.executemany("INSERT INTO cases VALUES (?, ?, ?, ?)",
                   [(f"c{i:03d}", "H0031" if i % 5 == 0 else "99213", 1 if i % 5 == 0 else 2,
                     "Denied" if i % 2 else "Approved") for i in range(200)])
    db.executemany("INSERT INTO bh_codes VALUES (?, ?)", [("H0031", "Y"), ("99213", "N")])
    db.executemany("INSERT INTO providers VALUES (?, ?)", [(1, "PSY"), (2, "MD")])
    insert_config(db, universe_table="cases", scope_sql=None, exclusion_sql=None, target_volume=100,
                  priority_rank_sql="case_id")


def _bh_level(db, bh_sql=BH_QUERY, fraction=0.21):
    insert_rule(db, "LEVEL", 10, "program", seq_no=1, level_order=0, parent_path="*", unlisted_policy="SKIP")
    insert_bucket(db, 11, 0, "Behavioral Health", seq_no=1, rule_sql=bh_sql, target_fraction=fraction)
    db.execute("UPDATE gre_sampling_rule SET rule_type = 'QUERY' WHERE rule_id = 11")
    insert_bucket(db, 12, 0, "Other", seq_no=2, rule_sql="1=1", quota_mode="REMAINDER")


def _bh_count(raw, run_id=None):
    return raw.db.execute("SELECT COUNT(*) FROM gre_sample_selections WHERE selected_flag = 1 "
                          "AND stratum_path = 'Behavioral Health'").fetchone()[0]


def test_a_21_percent_bucket_from_a_join(raw, conn, settings):
    _bh_level(raw.db)
    out = run_sampling(conn, settings, 1, run_key="K1", as_of=AS_OF)
    assert out["status"] == "COMPLETED", out.get("error")
    assert out["selected"] == 100 and _bh_count(raw) == 21
    assert out["rule_queries"] == {"Behavioral Health": 40}
    # the BH cases picked are BH ones
    picked = {r[0] for r in raw.db.execute("SELECT case_key FROM gre_sample_selections WHERE selected_flag = 1 "
                                           "AND stratum_path = 'Behavioral Health'").fetchall()}
    assert all(int(k.split("=c")[1]) % 5 == 0 for k in picked)


def test_only_the_pulled_cases_count(raw, conn, settings):
    """A QUERY returning cases outside the scope adds nothing: the scope stays the config's."""
    raw.db.execute("UPDATE gre_sampling_config SET scope_sql = 'case_id < ''c050''' WHERE config_id = 1")
    _bh_level(raw.db, fraction=0.5)
    out = run_sampling(conn, settings, 1, run_key="K1", as_of=AS_OF)
    assert out["in_scope"] == 50 and _bh_count(raw) == 10          # 10 BH cases among c000..c049


def test_exclusion_and_priority_queries(raw, conn, settings):
    insert_rule(raw.db, "EXCLUSION", 20, "BH_EXCLUDED", seq_no=1, rule_type="QUERY", rule_sql=BH_QUERY)
    insert_rule(raw.db, "PRIORITY", 30, "DENIED_FIRST", seq_no=1, rule_type="QUERY",
                rule_sql="SELECT case_id FROM cases WHERE disp = 'Denied'")
    out = run_sampling(conn, settings, 1, run_key="K1", as_of=AS_OF)
    assert out["status"] == "COMPLETED" and out["exclusions"] == {"BH_EXCLUDED": 40} and out["candidates"] == 160
    disp = dict(raw.db.execute("SELECT case_id, disp FROM cases").fetchall())
    chosen = [r[0].split("=")[1] for r in raw.db.execute(
        "SELECT case_key FROM gre_sample_selections WHERE selected_flag = 1 ORDER BY selection_order").fetchall()]
    # the 80 non-BH denials first, then 20 approvals
    assert [disp[c] for c in chosen] == ["Denied"] * 80 + ["Approved"] * 20


def test_a_query_without_the_key_columns_is_a_config_error(raw, conn, settings):
    _bh_level(raw.db, bh_sql="SELECT proc_cd FROM bh_codes")
    out = run_sampling(conn, settings, 1, run_key="K1", as_of=AS_OF)
    assert out["error_type"] == "CONFIG_ERROR" and "must return the key_columns case_id" in out["error"]


def test_a_query_must_be_a_select_that_only_reads(raw, conn, settings):
    _bh_level(raw.db)
    insert_rule(raw.db, "BUCKET", 13, "Cond", seq_no=0, level_order=0, parent_path="*", target_fraction=0.1,
                rule_sql="proc_cd = 'H0031'", raw_sql=True)                    # a condition, not a SELECT
    insert_rule(raw.db, "EXCLUSION", 20, "BAD", rule_type="QUERY", rule_sql="SELECT case_id FROM cases; DELETE FROM x")
    insert_rule(raw.db, "PRIORITY", 30, "OLD_STYLE", rule_type="CONDITION", rule_sql="disp = 'Denied'", raw_sql=True)
    problems = config_problems(load_config(conn, settings, 1))
    assert any("BUCKET rule Cond: must be a SELECT" in p for p in problems)
    assert any("BAD" in p and "only reading SQL" in p for p in problems)
    assert any("OLD_STYLE: rule_type CONDITION: a condition is not a rule" in p for p in problems)


def test_the_rest_bucket_must_be_last(raw, conn, settings):
    _bh_level(raw.db)
    raw.db.execute("UPDATE gre_sampling_rule SET seq_no = 0 WHERE rule_id = 12")    # 'Other' (no rule_sql) first
    problems = config_problems(load_config(conn, settings, 1))
    assert any("a bucket without rule_sql takes every case left" in p for p in problems)


def test_a_value_select_needs_one_value_per_case(raw, conn, settings):
    insert_rule(raw.db, "LEVEL", 10, "disp", seq_no=1, level_order=0, parent_path="*",
                rule_sql="SELECT case_id, disp, proc_cd FROM cases")
    out = run_sampling(conn, settings, 1, run_key="K1", as_of=AS_OF)
    assert out["error_type"] == "CONFIG_ERROR" and "needs one: the value" in out["error"]
    raw.db.execute("UPDATE gre_sampling_rule SET rule_sql = 'SELECT c.case_id, c.disp FROM cases c "
                   "JOIN (SELECT 1 AS n UNION ALL SELECT 2) t ON 1=1' WHERE rule_id = 10")
    out = run_sampling(conn, settings, 1, run_key="K2", as_of=AS_OF)
    assert out["error_type"] == "CONFIG_ERROR" and "more than once" in out["error"]


def test_query_only_on_condition_levels(raw, conn, settings):
    insert_rule(raw.db, "LEVEL", 10, "disp", seq_no=1, rule_sql="disp", level_order=0, parent_path="*")
    insert_bucket(raw.db, 11, 0, "Denied", target_fraction=0.5)
    raw.db.execute("UPDATE gre_sampling_rule SET rule_type = 'QUERY' WHERE rule_id = 11")
    insert_rule(raw.db, "PRIORITY", 30, "P", rule_type="NOPE", rule_sql="x")
    problems = config_problems(load_config(conn, settings, 1))
    assert any("only used on a level without its own SELECT" in p for p in problems)
    assert any("PRIORITY rule P: rule_type must be one of" in p and "QUERY" in p for p in problems)


def test_validate_runs_the_queries(raw, conn, settings):
    _bh_level(raw.db)
    assert validate(conn, settings, config_id=1, check_sql=True)["result"] == "VALID"
    raw.db.execute("UPDATE gre_sampling_rule SET rule_sql = 'SELECT case_id FROM no_such_table' WHERE rule_id = 11")
    bad = validate(conn, settings, config_id=1, check_sql=True)
    assert "SQL check failed" in bad["configs"][0]["problems"][0]
