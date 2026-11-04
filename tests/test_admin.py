"""Safety and operations: the read-only check on configured SQL, retention."""
from datetime import date, timedelta

import pytest

from conftest import insert_config, insert_rule, insert_strata, key, load_cli
from sampling.admin import history_days, purge_selections
from sampling.engine import finalize_run, run_sampling, sql_problems
from sampling.settings import ConfigError, Settings

AS_OF = date(2026, 8, 7)


def _sample(db, target=4, **kw):
    """Cases d00..d09 Denied and w00..w04 Withdrawn, half and half."""
    db.execute("CREATE TABLE u (case_id VARCHAR, disp VARCHAR, code VARCHAR)")
    db.executemany("INSERT INTO u VALUES (?, ?, ?)", [(f"d{i:02d}", "Denied", "X") for i in range(10)]
                   + [(f"w{i:02d}", "Withdrawn", "Y") for i in range(5)])
    kw.setdefault("exclusion_sql", None)
    insert_config(db, src_table_nm="u", scope_sql_txt=None, target_cnt=target, order_by="case_id DESC", **kw)
    insert_strata(db, 1, 1, 0, "disp", "disp", {"Denied": 50, "Withdrawn": 50})


def _rows(raw, sql):
    return raw.db.execute(sql).fetchall()


# ── configured SQL only reads ──────────────────────────────────────────────

@pytest.mark.parametrize("text, found", [
    ("disp = 'Denied'; DELETE FROM x", "';'"),
    ("1=1) OR (1=1", "parentheses"),
    ("disp = 'x' -- the rest", "'--'"),
    ("code IN (SELECT code FROM t) OR UPDATE_DT IS NULL", None),             # a column named UPDATE_DT is fine
    ("disp = 'DROP TABLE x; --'", None),                                       # inside quotes: a value
    ("CASE WHEN disp = 'A' THEN 1 ELSE 0 END = 1 /* end; drop */", None),
    ("disp = 'unclosed", "not closed"),
    ("MERGE INTO t USING s", "found MERGE"),
    ("COLLECT STATISTICS ON t", "found COLLECT"),
])
def test_configured_sql_must_only_read(text, found):
    problems = sql_problems(text, "x")
    assert (not problems) if found is None else any(found in p for p in problems), problems


def test_param_and_gate_sql_must_be_a_select():
    assert sql_problems("SELECT MAX(b) FROM t", "p", select=True) == []
    assert sql_problems("LOCKING ROW FOR ACCESS SELECT MAX(b) FROM t", "p", select=True) == []
    assert sql_problems("WITH x AS (SELECT 1 AS a) SELECT a FROM x", "p", select=True) == []
    assert any("must be a SELECT" in p for p in sql_problems("CALL db.proc()", "p", select=True))


def test_a_run_refuses_writing_sql(raw, conn, settings):
    _sample(raw.db)
    insert_rule(raw.db, "EXCLUSION", 11, "bad", rule_sql_txt="disp = 'x'; DELETE FROM GOVT_RGLTRY_SamplingRun")
    out = run_sampling(conn, settings, key(1), run_key="K1", as_of=AS_OF)
    assert out["error_type"] == "CONFIG_ERROR" and "';' is not allowed" in out["error"]
    assert _rows(raw, "SELECT COUNT(*) FROM GOVT_RGLTRY_SampleSelection") == [(0,)]


# A batch lookup as PARAM and GATE rules hold it: the sample's own project, table and run type are placeholders,
# so one pair of rows (Table_Nm / Run_Ty '*') serves every sample of a project. The control table is configuration
# (an example name here), never code.
BATCH_SQL = (
    "SELECT MAX(Btch_ID) FROM (SELECT Btch_ID FROM {db}.Batch_Control_Example WHERE Project_Cd = '{{project_cd}}' "
    "AND Table_Nm = '{{table_nm}}' AND Run_Ty = '{{run_ty}}' GROUP BY Btch_ID HAVING MIN(Batch_Close_Ind) = 1) "
    "closed_batches",
    "SELECT 1 FROM {db}.Batch_Control_Example WHERE Btch_ID = '{{batch_id}}' AND Project_Cd = '{{project_cd}}' "
    "AND Table_Nm = '{{table_nm}}' AND Run_Ty = '{{run_ty}}' HAVING COUNT(*) > 0 AND MIN(Batch_Close_Ind) = 1",
)


def test_a_batch_lookup_reads_and_follows_the_environment():
    """A control table in HSABC_$ENV: HSABC_DEV in DEV, HSABC in UAT / PROD."""
    for text in (t.format(db="HSABC_$ENV") for t in BATCH_SQL):
        assert sql_problems(text, "batch", select=True) == []
        for env, db in (("DEV", "HSABC_DEV"), ("QA", "HSABC_QA"), ("UAT", "HSABC"), ("PROD", "HSABC")):
            resolved = Settings(meta_db="M", environment=env).resolve_env(text, identifier=False)
            assert f" {db}.Batch_Control_Example " in resolved and "$" not in resolved


def test_shared_batch_rules_use_each_samples_project_table_and_run_type(raw, conn, settings):
    """One PARAM and one GATE row for the whole project (Table_Nm / Run_Ty '*'): each sample finds its own batch."""
    _sample(raw.db)
    insert_config(raw.db, config_id=2, table_nm="T1", run_ty="MNT", src_table_nm="u", exclusion_sql=None,
                  scope_sql_txt=None, target_cnt=2, order_by="case_id")   # T1 monthly
    raw.db.execute("CREATE TABLE Batch_Control_Example (Project_Cd VARCHAR, Table_Nm VARCHAR, Run_Ty VARCHAR, "
                   "Btch_ID VARCHAR, Batch_Close_Ind INTEGER)")
    raw.db.executemany("INSERT INTO Batch_Control_Example VALUES ('UMCM', 'T1', ?, ?, ?)",
                       [("WKLY", "W1", 1), ("WKLY", "W2", 1), ("WKLY", "W3", 0), ("MNT", "M1", 1)])
    insert_rule(raw.db, "PARAM", 31, "batch_id", table_nm="*", run_ty="*", rule_sql_txt=BATCH_SQL[0].format(db="main"))
    insert_rule(raw.db, "GATE", 32, "closed", table_nm="*", run_ty="*", rule_sql_txt=BATCH_SQL[1].format(db="main"))
    weekly = run_sampling(conn, settings, key(1), run_key="{batch_id}", as_of=AS_OF)
    assert (weekly["status"], weekly["run_key"]) == ("COMPLETED", "W2")
    monthly = run_sampling(conn, settings, key(1, run_ty="MNT"), run_key="{batch_id}", as_of=AS_OF)
    assert (monthly["status"], monthly["run_key"]) == ("COMPLETED", "M1")
    assert monthly["params"]["project_cd"] == "UMCM" and monthly["params"]["run_ty"] == "MNT"


# ── retention ──────────────────────────────────────────────────────────────

def _counts(raw):
    return dict(_rows(raw, "SELECT Run_Key || ':' || Curr_Ind, COUNT(*) FROM GOVT_RGLTRY_SampleSelection "
                           "GROUP BY 1"))


def test_purge_keeps_what_is_still_needed(raw, conn, settings):
    _sample(raw.db, keep_excluded_ind=1)
    insert_rule(raw.db, "EXCLUSION", 11, "not_w04", rule_sql_txt="case_id = 'w04'")
    run_sampling(conn, settings, key(1), run_key="OLD", as_of=AS_OF - timedelta(days=100))
    run_sampling(conn, settings, key(1), run_key="OLD", as_of=AS_OF - timedelta(days=100))      # retires the first
    run_sampling(conn, settings, key(1), run_key="FIN", as_of=AS_OF - timedelta(days=100))
    finalize_run(conn, settings, key(1), "FIN", "auditor")
    run_sampling(conn, settings, key(1), run_key="NEW", as_of=AS_OF)
    assert _counts(raw) == {"OLD:0": 15, "OLD:1": 15, "FIN:1": 15, "NEW:1": 15}

    shown = purge_selections(conn, settings, 30, project="UMCM", as_of=AS_OF)
    assert shown["configs"][0]["rows"] == {"retired_runs": 15, "unselected_of_current_runs": 11}
    assert _counts(raw)["OLD:0"] == 15                                                       # counted only
    purge_selections(conn, settings, 30, project="UMCM", table="T1", as_of=AS_OF, apply=True)
    assert _counts(raw) == {"OLD:1": 4, "FIN:1": 15, "NEW:1": 15}           # selected kept, FINAL and new untouched
    assert _rows(raw, "SELECT COUNT(*) FROM GOVT_RGLTRY_SamplingRun") == [(4,)]                     # run rows never go


def test_purge_respects_the_history_rules(raw, conn, settings):
    _sample(raw.db)
    insert_rule(raw.db, "PRIORITY", 21, "carry_over", rule_ty="PRIOR_CANDIDATE", lookback_days=90)
    assert history_days(conn, settings, key(1)) == 90
    insert_config(raw.db, config_id=2, src_table_nm="u")
    insert_rule(raw.db, "EXCLUSION", 211, "no_repeat", config_id=2, rule_ty="PRIOR_SELECTION", lookback_days=150,
                history_scope_cd="TABLE")
    assert history_days(conn, settings, key(1)) == 90               # TABLE: T2's rule reads T2's rows only
    insert_rule(raw.db, "EXCLUSION", 212, "no_repeat", config_id=2, table_nm="T1", run_ty="MNT",
                rule_ty="PRIOR_SELECTION", lookback_days=160, history_scope_cd="TABLE")
    assert history_days(conn, settings, key(1)) == 160              # T1's monthly rule reads T1's weekly rows
    insert_rule(raw.db, "EXCLUSION", 213, "no_repeat_p", config_id=2, rule_ty="PRIOR_SELECTION", lookback_days=180,
                history_scope_cd="PROJECT")
    assert history_days(conn, settings, key(1)) == 180              # config 2 reads config 1's rows (PROJECT)
    out = purge_selections(conn, settings, 120, project="UMCM", table="T1", as_of=AS_OF, apply=True)
    assert "keep at least" in out["configs"][0]["skipped"]
    with pytest.raises(ConfigError):
        purge_selections(conn, settings, 0, project="UMCM")


def test_cli_purge(raw, monkeypatch, tmp_path, capsys):
    from sampling.db import Connection
    cli = load_cli()
    monkeypatch.setenv("SAMPLING_META_DB", "main")
    monkeypatch.setenv("SAMPLING_ACCESS_LOCK", "false")
    monkeypatch.setenv("SAMPLING_ENV_FILE", "does-not-exist.env")
    monkeypatch.setenv("SAMPLING_LOG_DIR", str(tmp_path))
    monkeypatch.setattr(cli, "connect_teradata", lambda db, lock: Connection(raw, host="duckdb", access_lock=False))
    monkeypatch.setattr(raw, "close", lambda: None)
    _sample(raw.db)
    assert cli.main(["purge", "--project", "UMCM", "--older-than-days", "30"]) == 0
