"""The configurable rules (gaps A-G of the Section 4 review): every behaviour is rows, none is a field name or value."""
import json
from datetime import date, timedelta

from conftest import insert_bucket, insert_config, insert_rule, insert_strata, set_config
from sampling.engine import config_problems, load_config, run_sampling, validate

AS_OF = date(2026, 8, 7)


def _universe(db, rows, table="u"):
    """rows: (case_id, disp, area, code, rev, batch, flag)."""
    db.execute(f"CREATE TABLE {table} (case_id VARCHAR, disp VARCHAR, area VARCHAR, code VARCHAR, rev INTEGER, "
               f"batch VARCHAR, flag VARCHAR)")
    db.executemany(f"INSERT INTO {table} VALUES (?, ?, ?, ?, ?, ?, ?)", rows)


def _cases(prefix, n, disp, area="A", code="X", rev=0, batch="B1", flag="N"):
    return [(f"{prefix}{i:03d}", disp, area, code, rev, batch, flag) for i in range(n)]


def _config(db, target, **kw):
    kw.setdefault("priority_rank_sql", "case_id")
    insert_config(db, universe_table="u", exclusion_sql=None, scope_sql=None, target_volume=target, **kw)


def _run(conn, s, **kw):
    kw.setdefault("as_of", AS_OF)
    return run_sampling(conn, s, kw.pop("config_id", 1), **kw)


def _selected(out):
    return {k: v["selected"] for k, v in out["by_stratum"].items()}


def _rows(raw, sql):
    return raw.db.execute(sql).fetchall()


# ── A: a level only under some parents, a mix per parent ───────────────────

def test_a_level_applies_only_under_its_parent(raw, conn, settings):
    _universe(raw.db, _cases("d", 40, "Denied", "Part B") + _cases("e", 40, "Denied", "Other")
              + _cases("w", 40, "Withdrawn", "Part B"))
    _config(raw.db, 20)
    insert_strata(raw.db, 1, 1, 0, "disp", "disp", {"Denied": 0.5, "Withdrawn": 0.5})
    insert_strata(raw.db, 2, 1, 1, "area", "area", {"Part B": {"target_fraction": 0.2, "parent_path": "Denied"}},
                  applies_to="Denied")
    out = _run(conn, settings)
    assert _selected(out) == {"Denied/Part B": 2, "Denied/Other": 8, "Withdrawn": 10}


def test_a_most_specific_parent_path_wins(raw, conn, settings):
    _universe(raw.db, _cases("d", 40, "Denied", "P") + _cases("e", 40, "Denied", "Q")
              + _cases("w", 40, "Withdrawn", "P") + _cases("x", 40, "Withdrawn", "Q"))
    _config(raw.db, 20)
    insert_strata(raw.db, 1, 1, 0, "disp", "disp", {"Denied": 0.5, "Withdrawn": 0.5})
    insert_strata(raw.db, 2, 1, 1, "area", "area", {"P": {"target_fraction": 0.2, "parent_path": "*"}})
    insert_bucket(raw.db, 300, 1, "P", parent_path="Denied", target_fraction=0.8)
    out = _run(conn, settings)
    assert _selected(out) == {"Denied/P": 8, "Denied/Q": 2, "Withdrawn/P": 2, "Withdrawn/Q": 8}


# ── B: everything there is, and the remainder ──────────────────────────────

def test_b_all_and_remainder(raw, conn, settings):
    _universe(raw.db, _cases("w", 5, "Withdrawn") + _cases("m", 3, "Dismissed") + _cases("a", 2, "Approved")
              + _cases("d", 100, "Denied"))
    _config(raw.db, 30)
    insert_strata(raw.db, 1, 1, 0, "disp", "disp", {
        "Withdrawn": {"quota_mode": "ALL"}, "Dismissed": {"quota_mode": "ALL", "max_count": 2},
        "Approved": {"target_fraction": 0.2}, "Denied": {"quota_mode": "REMAINDER"}})
    out = _run(conn, settings)
    # Approved is short (2 of 6): its unused quota flows to the remainder
    assert _selected(out) == {"Withdrawn": 5, "Dismissed": 2, "Approved": 2, "Denied": 21}
    assert out["buckets"]["Approved"]["target"] == 6 and out["shortfall"] == 0


def test_b_skip_policy_leaves_unlisted_values_out(raw, conn, settings):
    _universe(raw.db, _cases("d", 50, "Denied") + _cases("z", 50, "Unknown"))
    _config(raw.db, 10, shortfall_policy="NONE")
    insert_strata(raw.db, 1, 1, 0, "disp", "disp", {"Denied": 0.5}, unlisted_policy="SKIP")
    out = _run(conn, settings)
    assert _selected(out) == {"Denied": 5, "Unknown": 0} and out["shortfall"] == 5


# ── C: equal portions, water-filled ────────────────────────────────────────

def test_c_condition_buckets_share_equally_and_refill(raw, conn, settings):
    _universe(raw.db, _cases("a", 2, "D", code="IP") + _cases("b", 50, "D", code="OP")
              + _cases("c", 50, "D", code="BH") + _cases("z", 50, "D", code="ZZ"))
    _config(raw.db, 20)
    insert_rule(raw.db, "LEVEL", 10, "area", level_order=0, unlisted_policy="SKIP")   # no rule_sql: conditions
    for i, (name, sql) in enumerate((("Inpatient", "code = 'IP'"), ("Outpatient", "code = 'OP'"),
                                     ("Behavioral", "code IN ('BH', 'OP')")), 1):
        insert_bucket(raw.db, 10 + i, 0, name, seq_no=i, rule_sql=sql, quota_mode="REMAINDER")
    out = _run(conn, settings)
    # the first true condition wins (OP rows are Outpatient), IP has 2: the other two share the rest
    assert _selected(out) == {"Inpatient": 2, "Outpatient": 9, "Behavioral": 9, "OTHER": 0}
    paths = _rows(raw, "SELECT stratum_path, COUNT(*) FROM GOVT_RGLTRY_SAMPLE_SELECTIONS GROUP BY 1 ORDER BY 1")
    assert paths == [("Behavioral", 50), ("Inpatient", 2), ("OTHER", 50), ("Outpatient", 50)]


# ── D: variety across codes ───────────────────────────────────────────────

def test_d_diversity_rotates_and_cap_holds_through_top_up(raw, conn, settings):
    _universe(raw.db, _cases("a", 20, "D", code="X") + _cases("b", 20, "D", code="Y") + _cases("c", 3, "D", code="Z"))
    _config(raw.db, 9, diversity_expr="code")
    out = _run(conn, settings)
    codes = _rows(raw, "SELECT SUBSTR(case_key, 9, 1), COUNT(*) FROM GOVT_RGLTRY_SAMPLE_SELECTIONS WHERE selected_flag = 1 "
                       "GROUP BY 1 ORDER BY 1")
    assert out["selected"] == 9 and codes == [("a", 3), ("b", 3), ("c", 3)]
    set_config(raw.db, diversity_cap=2)
    out = _run(conn, settings, run_key="capped")
    assert out["selected"] == 6 and out["shortfall"] == 3


# ── E: the sample table: the selected cases' source rows ──────────────────

SAMPLE_COLS = ("etl_is_curr_ind VARCHAR, etl_is_del_ind VARCHAR, etl_load_dt_tm TIMESTAMP, "
               "etl_last_updt_dt_tm TIMESTAMP, smpl_run_id VARCHAR, smpl_run_key VARCHAR, smpl_cycle_dt DATE, "
               "smpl_order_nbr INTEGER, smpl_stratum_path VARCHAR")


def _source(db, rows):
    """A source carrying the framework's audit columns: rows (case_id, code, etl_is_curr_ind)."""
    db.execute("CREATE TABLE src (case_id VARCHAR, code VARCHAR, etl_is_curr_ind VARCHAR, etl_is_del_ind VARCHAR, "
               "etl_load_dt_tm TIMESTAMP, etl_last_updt_dt_tm TIMESTAMP)")
    db.executemany("INSERT INTO src VALUES (?, ?, ?, 'N', TIMESTAMP '2020-01-01 00:00:00', NULL)", rows)
    insert_config(db, universe_table="src", exclusion_sql=None, scope_sql="etl_is_curr_ind = 'Y'", target_volume=4,
                  priority_rank_sql="case_id", output_db="main", output_table="smpl_t")


def test_e_the_sample_table_gets_every_source_column(raw, conn, settings):
    _source(raw.db, [(f"d{i:03d}", f"K{i}", "Y") for i in range(10)])
    raw.db.execute(f"CREATE TABLE smpl_t (case_id VARCHAR, code VARCHAR, {SAMPLE_COLS})")
    first = _run(conn, settings, run_key="W1")
    assert first["status"] == "COMPLETED" and first["output_table"] == "main.smpl_t", first
    rows = _rows(raw, "SELECT smpl_order_nbr, case_id, code, smpl_run_key, smpl_stratum_path, etl_is_curr_ind, "
                      "etl_is_del_ind, YEAR(etl_load_dt_tm) > 2020, smpl_run_id = (SELECT run_id FROM "
                      "GOVT_RGLTRY_SAMPLING_RUN) FROM smpl_t ORDER BY 1")
    # the source's audit values (2020, its own flags) are replaced by the process's
    assert rows == [(n + 1, f"d{n:03d}", f"K{n}", "W1", "ALL", "Y", "N", True, True) for n in range(4)]
    _run(conn, settings, run_key="W1")                                        # a rerun retires the earlier rows
    assert _rows(raw, "SELECT etl_is_curr_ind, etl_is_del_ind, COUNT(*), COUNT(etl_last_updt_dt_tm) FROM smpl_t "
                      "GROUP BY 1, 2 ORDER BY 1") == [("N", "Y", 4, 4), ("Y", "N", 4, 4)]
    _run(conn, settings, run_key="W2")                                        # another cycle leaves W1 alone
    assert _rows(raw, "SELECT smpl_run_key, COUNT(*) FROM smpl_t WHERE etl_is_curr_ind = 'Y' GROUP BY 1 "
                      "ORDER BY 1") == [("W1", 4), ("W2", 4)]


def test_e_the_sample_table_columns_are_checked(raw, conn, settings):
    _source(raw.db, [("d000", "K", "Y")])
    raw.db.execute("CREATE TABLE smpl_t (code VARCHAR, notes VARCHAR, etl_is_curr_ind VARCHAR)")
    out = _run(conn, settings)
    assert out["error_type"] == "CONFIG_ERROR"
    assert "needs the column SMPL_RUN_KEY" in out["error"] and "needs the key column case_id" in out["error"]
    assert "neither in the source nor set by sampling: notes" in out["error"]
    bad = validate(conn, settings, config_id=1, check_sql=True)
    assert "needs the column SMPL_RUN_KEY" in bad["configs"][0]["problems"][0]


def test_e_one_source_row_per_selected_case(raw, conn, settings):
    _source(raw.db, [("d000", "K", "Y"), ("d000", "K2", "Y")])                 # two current versions of one case
    raw.db.execute(f"CREATE TABLE smpl_t (case_id VARCHAR, code VARCHAR, {SAMPLE_COLS})")
    out = _run(conn, settings)
    assert out["error_type"] == "CONFIG_ERROR" and "do not identify a case uniquely" in out["error"]


def test_e_sample_table_names_and_old_output_rules(raw, conn, settings):
    _universe(raw.db, _cases("d", 3, "D"))
    _config(raw.db, 2, output_table="smpl_t")
    insert_rule(raw.db, "OUTPUT", 1, "case_id", rule_sql="case_id")
    problems = config_problems(load_config(conn, settings, 1))
    assert "output_db and output_table go together: set both (the sample table) or neither" in problems
    assert any("OUTPUT rules are no longer used" in p for p in problems)
    set_config(raw.db, output_db="main.x")
    assert any("output_db 'main.x' must be a plain name" in p for p in config_problems(load_config(conn, settings, 1)))


# ── F: history: dedupe and carry-over ─────────────────────────────────────

def test_f_prior_selection_excluded_and_unselected_carried_over(raw, conn, settings):
    _universe(raw.db, _cases("c", 10, "D"))
    _config(raw.db, 3)
    first = _run(conn, settings, run_key="W1", as_of=AS_OF)
    assert first["selected"] == 3
    insert_rule(raw.db, "EXCLUSION", 1, "ALREADY_AUDITED", seq_no=1, rule_type="PRIOR_SELECTION", lookback_days=30,
                history_scope="CONFIG")
    # priority: highest case_id first, but last cycle's unselected cases before anything else
    set_config(raw.db, priority_rank_sql="case_id DESC")
    second = _run(conn, settings, run_key="W2", as_of=AS_OF + timedelta(days=7))
    assert second["exclusions"] == {"ALREADY_AUDITED": 3} and second["candidates"] == 7
    assert _rows(raw, "SELECT exclusion_reason, COUNT(*) FROM GOVT_RGLTRY_SAMPLE_SELECTIONS WHERE run_key = 'W2' "
                      "AND excluded_flag = 1 GROUP BY 1") == [("ALREADY_AUDITED", 3)]
    picked = {r[0] for r in _rows(raw, "SELECT case_key FROM GOVT_RGLTRY_SAMPLE_SELECTIONS WHERE run_key = 'W2' "
                                       "AND selected_flag = 1")}
    assert picked == {"case_id=c009", "case_id=c008", "case_id=c007"}

    insert_rule(raw.db, "PRIORITY", 2, "CARRY_OVER", seq_no=1, rule_type="PRIOR_CANDIDATE", lookback_days=30,
                history_scope="CONFIG")
    raw.db.execute("UPDATE GOVT_RGLTRY_SAMPLING_RULE SET act_ind = 0 WHERE rule_class = 'EXCLUSION'")
    third = _run(conn, settings, run_key="W3", as_of=AS_OF + timedelta(days=14))
    # c003..c006 were considered (W1, W2) but never selected: they come first
    picked = {r[0] for r in _rows(raw, "SELECT case_key FROM GOVT_RGLTRY_SAMPLE_SELECTIONS WHERE run_key = 'W3' "
                                       "AND selected_flag = 1")}
    assert picked == {"case_id=c006", "case_id=c005", "case_id=c004"}, third


def test_f_sql_exclusions_first_match_and_stats(raw, conn, settings):
    _universe(raw.db, _cases("a", 5, "D", flag="Y") + _cases("b", 5, "D", code="Q") + _cases("c", 5, "D"))
    _config(raw.db, 3, keep_excluded=1)
    for i, (name, sql) in enumerate((("FLAGGED", "flag = 'Y'"), ("CODE_Q", "code = 'Q' OR flag = 'Y'")), 1):
        insert_rule(raw.db, "EXCLUSION", i, name, seq_no=i, rule_type="SQL", rule_sql=sql)
    out = _run(conn, settings)
    assert out["exclusions"] == {"FLAGGED": 5, "CODE_Q": 5} and out["candidates"] == 5 and out["excluded"] == 10
    report = json.loads(_rows(raw, "SELECT run_report FROM GOVT_RGLTRY_SAMPLING_RUN")[0][0])
    assert report["exclusions"] == {"FLAGGED": 5, "CODE_Q": 5} and report["buckets"]["ALL"]["selected"] == 3
    reasons = _rows(raw, "SELECT exclusion_reason, COUNT(*) FROM GOVT_RGLTRY_SAMPLE_SELECTIONS WHERE excluded_flag = 1 "
                         "GROUP BY 1 ORDER BY 1")
    assert reasons == [("CODE_Q", 5), ("FLAGGED", 5)]


def test_priority_conditions_and_expressions_in_order(raw, conn, settings):
    _universe(raw.db, [("p1", "D", "A", "X", 0, "B1", "N"), ("p2", "D", "A", "Y", 1, "B1", "N"),
                       ("p3", "D", "A", "X", 1, "B1", "N"), ("p4", "D", "A", "Y", None, "B1", "N")])
    _config(raw.db, 4, priority_rank_sql=None)
    insert_rule(raw.db, "PRIORITY", 1, "REV", seq_no=1, rule_type="CONDITION", rule_sql="rev = 1")
    insert_rule(raw.db, "PRIORITY", 2, "CODE", seq_no=2, rule_type="EXPRESSION", rule_sql="code", sort_dir="DESC")
    _run(conn, settings)
    order = _rows(raw, "SELECT case_key FROM GOVT_RGLTRY_SAMPLE_SELECTIONS ORDER BY selection_order")
    assert [r[0] for r in order] == ["case_id=p2", "case_id=p3", "case_id=p4", "case_id=p1"]


# ── G: parameters looked up by SQL, run_key templates ─────────────────────

def test_g_param_lookup_scopes_the_run_and_names_it(raw, conn, settings):
    _universe(raw.db, _cases("o", 10, "D", batch="B1") + _cases("n", 6, "D", batch="B2"))
    _config(raw.db, 4)
    set_config(raw.db, scope_sql="batch = '{batch_id}'")
    insert_rule(raw.db, "PARAM", 1, "batch_id", seq_no=1, rule_sql="SELECT MAX(batch) FROM u")
    out = _run(conn, settings, run_key="{batch_id}")
    assert out["status"] == "COMPLETED" and out["run_key"] == "B2" and out["in_scope"] == 6
    assert json.loads(_rows(raw, "SELECT run_params_txt FROM GOVT_RGLTRY_SAMPLING_RUN")[0][0])["batch_id"] == "B2"
    given = _run(conn, settings, run_key="{batch_id}", run_params={"batch_id": "B1"})
    assert given["run_key"] == "B1" and given["in_scope"] == 10


def test_g_param_without_a_value_is_a_technical_failure(raw, conn, settings):
    _universe(raw.db, _cases("o", 3, "D"))
    _config(raw.db, 2)
    insert_rule(raw.db, "PARAM", 1, "batch_id", seq_no=1, rule_sql="SELECT MAX(batch) FROM u WHERE 1 = 0")
    out = _run(conn, settings)
    assert out["error_kind"] == "technical" and out["error_type"] == "PARAM_FAILURE"
    assert _rows(raw, "SELECT status, error_type FROM GOVT_RGLTRY_SAMPLING_RUN") == [("ERROR", "PARAM_FAILURE")]


# ── validation of the rule rows ───────────────────────────────────────────

def test_rule_rows_are_validated(raw, conn, settings):
    _universe(raw.db, _cases("o", 3, "D"))
    _config(raw.db, 2, diversity_cap=2, sampling_method="SYSTEMATIC", diversity_expr="code")
    insert_rule(raw.db, "EXCLUSION", 1, "H", seq_no=1, rule_type="PRIOR_SELECTION", lookback_days=0)
    insert_rule(raw.db, "PRIORITY", 2, "P", seq_no=1, rule_type="BOGUS")
    insert_rule(raw.db, "PARAM", 3, "p", rule_sql="SELECT 1", quota_mode="ALL")      # a column PARAM does not use
    insert_rule(raw.db, "TYPO", 4, "x")
    insert_bucket(raw.db, 5, 7, "orphan")                                               # no LEVEL 7
    insert_strata(raw.db, 1, 1, 0, "disp", "disp", {"A/B": 0.7, "C": 0.6, "D": {"quota_mode": "ALL",
                                                                          "target_fraction": 0.1}})
    problems = " | ".join(validate(conn, settings, config_id=1)["configs"][0]["problems"])
    for text in ("SYSTEMATIC", "lookback_days must be >= 1", "rule_type must be one of", "must not contain '/'",
                 "add up to 1.3000", "not used with quota_mode ALL", "PARAM rule p: quota_mode is not used",
                 "rule_class 'TYPO'", "level_order 7, which has no LEVEL"):
        assert text in problems, (text, problems)


# ── gates: wait until the data is ready ────────────────────────────────────

def test_gate_waits_until_its_select_returns_a_row(raw, conn, settings):
    _universe(raw.db, _cases("o", 5, "D", batch="B1"))
    raw.db.execute("CREATE TABLE batch_control (batch VARCHAR, status VARCHAR)")
    raw.db.execute("INSERT INTO batch_control VALUES ('B1', 'OPEN')")
    _config(raw.db, 2)
    set_config(raw.db, scope_sql="batch = '{batch_id}'")
    insert_rule(raw.db, "PARAM", 1, "batch_id", seq_no=1, rule_sql="SELECT MAX(batch) FROM u")
    insert_rule(raw.db, "GATE", 2, "BATCH_CLOSED", rule_sql="SELECT 1 FROM batch_control WHERE batch = '{batch_id}' "
                                                            "AND status = 'CLOSED'", description="batch closed")
    out = _run(conn, settings, run_key="{batch_id}")
    assert out["error_type"] == "GATE_NOT_MET" and out["error_kind"] == "technical"   # Airflow retries it
    assert "BATCH_CLOSED (batch closed)" in out["error"]
    assert _rows(raw, "SELECT COUNT(*) FROM GOVT_RGLTRY_SAMPLE_SELECTIONS")[0][0] == 0
    raw.db.execute("UPDATE batch_control SET status = 'CLOSED'")
    again = _run(conn, settings, run_key="{batch_id}")
    assert again["status"] == "COMPLETED" and again["run_key"] == "B1" and again["selected"] == 2
    assert _rows(raw, "SELECT status FROM GOVT_RGLTRY_SAMPLING_RUN ORDER BY started_at") == [("ERROR",), ("COMPLETED",)]


def test_a_broken_gate_is_reported(raw, conn, settings):
    _universe(raw.db, _cases("o", 3, "D"))
    _config(raw.db, 2)
    insert_rule(raw.db, "GATE", 1, "BROKEN", rule_sql="SELECT 1 FROM no_such_table")
    assert _run(conn, settings)["error_type"] == "GATE_FAILURE"
    raw.db.execute("UPDATE GOVT_RGLTRY_SAMPLING_RULE SET rule_sql = NULL")
    assert _run(conn, settings)["error_type"] == "CONFIG_ERROR"
