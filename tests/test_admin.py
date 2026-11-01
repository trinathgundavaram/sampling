"""Safety and operations: the read-only check on configured SQL, retention."""
from datetime import date, timedelta

import pytest

from conftest import ROOT, insert_config, insert_rule, insert_strata, load_cli
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
    insert_config(db, universe_table="u", scope_sql=None, target_volume=target, priority_rank_sql="case_id DESC", **kw)
    insert_strata(db, 1, 1, 0, "disp", "disp", {"Denied": 0.5, "Withdrawn": 0.5})


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
    insert_rule(raw.db, "EXCLUSION", 11, "bad", rule_sql="disp = 'x'; DELETE FROM gre_sampling_run")
    out = run_sampling(conn, settings, 1, run_key="K1", as_of=AS_OF)
    assert out["error_type"] == "CONFIG_ERROR" and "';' is not allowed" in out["error"]
    assert _rows(raw, "SELECT COUNT(*) FROM gre_sample_selections") == [(0,)]


def test_the_seeds_pass_the_sql_check(raw, conn, settings):
    """umcm_sample_config.sql is validated in test_local_cli; the ODAG1 template's SQL (but its TODOs) as well."""
    cli = load_cli()
    for stmt in cli.split_statements(cli.render_sql(ROOT / "sql" / "seed" / "odag1_config_template.sql", "main")):
        raw.db.execute(stmt)
    for r in conn.query("SELECT rule_name, rule_class, rule_sql FROM gre_sampling_rule WHERE config_id = 2"):
        if "TODO" not in str(r["rule_sql"] or ""):
            assert sql_problems(r["rule_sql"], r["rule_name"], select=r["rule_class"] in ("PARAM", "GATE")) == []


def test_the_odag1_batch_lookup_follows_the_environment(raw, conn):
    """The framework's ComplianceRequestControl is in HSABC_$ENV: HSABC_DEV in DEV, HSABC in UAT / PROD."""
    cli = load_cli()
    for stmt in cli.split_statements(cli.render_sql(ROOT / "sql" / "seed" / "odag1_config_template.sql", "main")):
        raw.db.execute(stmt)
    rules = conn.query("SELECT rule_sql FROM gre_sampling_rule WHERE rule_class IN ('PARAM', 'GATE') AND config_id = 2")
    assert len(rules) == 2
    for env, db in (("DEV", "HSABC_DEV"), ("QA", "HSABC_QA"), ("UAT", "HSABC"), ("PROD", "HSABC")):
        for r in rules:
            text = Settings(meta_db="M", environment=env).resolve_env(r["rule_sql"], identifier=False)
            assert f" {db}.ComplianceRequestControl " in text and "$" not in text


# ── retention ──────────────────────────────────────────────────────────────

def _counts(raw):
    return dict(_rows(raw, "SELECT run_key || ':' || etl_is_curr_ind, COUNT(*) FROM gre_sample_selections "
                           "GROUP BY 1"))


def test_purge_keeps_what_is_still_needed(raw, conn, settings):
    _sample(raw.db, keep_excluded=1)
    insert_rule(raw.db, "EXCLUSION", 11, "not_w04", rule_sql="case_id = 'w04'")
    run_sampling(conn, settings, 1, run_key="OLD", as_of=AS_OF - timedelta(days=100))
    run_sampling(conn, settings, 1, run_key="OLD", as_of=AS_OF - timedelta(days=100))      # retires the first
    run_sampling(conn, settings, 1, run_key="FIN", as_of=AS_OF - timedelta(days=100))
    finalize_run(conn, settings, 1, "FIN", "auditor")
    run_sampling(conn, settings, 1, run_key="NEW", as_of=AS_OF)
    assert _counts(raw) == {"OLD:N": 15, "OLD:Y": 15, "FIN:Y": 15, "NEW:Y": 15}

    shown = purge_selections(conn, settings, 30, config_id=1, as_of=AS_OF)
    assert shown["configs"][0]["rows"] == {"retired_runs": 15, "unselected_of_current_runs": 11}
    assert _counts(raw)["OLD:N"] == 15                                                       # counted only
    purge_selections(conn, settings, 30, config_id=1, as_of=AS_OF, apply=True)
    assert _counts(raw) == {"OLD:Y": 4, "FIN:Y": 15, "NEW:Y": 15}           # selected kept, FINAL and new untouched
    assert _rows(raw, "SELECT COUNT(*) FROM gre_sampling_run") == [(4,)]                     # run rows never go


def test_purge_respects_the_history_rules(raw, conn, settings):
    _sample(raw.db)
    insert_rule(raw.db, "PRIORITY", 21, "carry_over", rule_type="PRIOR_CANDIDATE", lookback_days=90)
    insert_config(raw.db, config_id=2, universe_table="u", project_name="UMCM")
    insert_rule(raw.db, "EXCLUSION", 211, "no_repeat", config_id=2, rule_type="PRIOR_SELECTION", lookback_days=180,
                history_scope="PROJECT")
    assert history_days(conn, settings, 1) == 180                  # config 2 reads config 1's rows (PROJECT)
    out = purge_selections(conn, settings, 120, config_id=1, as_of=AS_OF, apply=True)
    assert "keep at least" in out["configs"][0]["skipped"]
    with pytest.raises(ConfigError):
        purge_selections(conn, settings, 0, config_id=1)


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
    assert cli.main(["purge", "--config-id", "1", "--older-than-days", "30"]) == 0
