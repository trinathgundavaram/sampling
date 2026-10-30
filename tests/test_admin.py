"""Safety and operations: the read-only check on configured SQL, the configuration file, retention."""
import json
import re
from datetime import date, timedelta

import pytest

from conftest import ROOT, insert_config, insert_rule, insert_strata, load_cli
from sampling.admin import (CONFIG_COLUMNS, RULE_COLUMNS, export_config, history_days, load_config_file,
                            purge_selections)
from sampling.engine import finalize_run, load_config, run_sampling, sql_problems
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


# ── configuration from a file ──────────────────────────────────────────────

def test_columns_match_the_schema():
    schema = (ROOT / "sql" / "schema.sql").read_text()

    def columns(table):
        body = schema.split(f"{{{{META_DB}}}}.{table} (")[1].split("\n)")[0]
        return [m.group(1) for m in re.finditer(r"^\s{4}(\w+)\s", body, re.M)]
    audit = {"created_by", "last_updated_by", "load_datetime", "last_updated_datetime"}
    assert [c for c in columns("gre_sampling_config") if c not in audit] == list(CONFIG_COLUMNS)
    assert [c for c in columns("gre_sampling_rule") if c not in audit] == list(RULE_COLUMNS)


def test_export_then_load_changes_nothing(raw, conn, settings):
    _sample(raw.db)
    data = export_config(conn, settings, 1)
    assert data["config"]["universe_table"] == "u" and len(data["rules"]) == 3
    out = load_config_file(conn, settings, json.dumps(data, default=str), "me", apply=True)
    assert out["config"] == "unchanged" and not out["rules_added"] and not out["rules_changed"]
    assert not out["rules_switched_off"] and out["applied"]


def test_load_shows_the_changes_then_applies_them(raw, conn, settings):
    _sample(raw.db)
    before = load_config(conn, settings, 1).config_hash
    data = export_config(conn, settings, 1)
    data["config"]["target_volume"] = 6
    data["rules"] = [r for r in data["rules"] if r["rule_name"] != "Withdrawn"]     # leaves the file: switched off
    data["rules"][1]["target_fraction"] = 1.0
    data["rules"].append({"rule_id": 11, "rule_class": "EXCLUSION", "rule_name": "no_y", "rule_sql": "code = 'Y'"})
    text = json.dumps(data, default=str)

    shown = load_config_file(conn, settings, text, "me")
    assert not shown["applied"] and shown["config"] == {"target_volume": {"from": 4, "to": 6}}
    assert shown["rules_added"] == ["11 EXCLUSION no_y"] and shown["rules_switched_off"] == [1002]
    assert shown["rules_changed"] == {1001: {"target_fraction": {"from": 0.5, "to": 1.0}}}
    assert load_config(conn, settings, 1).config_hash == before                      # nothing written yet

    assert load_config_file(conn, settings, text, "me", apply=True)["applied"]
    assert _rows(raw, "SELECT act_ind FROM gre_sampling_rule WHERE rule_id = 1002") == [(0,)]   # never deleted
    assert _rows(raw, "SELECT created_by FROM gre_sampling_rule WHERE rule_id = 11") == [("me",)]
    assert _rows(raw, "SELECT target_volume, last_updated_by FROM gre_sampling_config") == [(6, "me")]
    out = run_sampling(conn, settings, 1, run_key="K1", as_of=AS_OF)
    assert out["status"] == "COMPLETED" and out["selected"] == 6 and out["exclusions"] == {"no_y": 5}


def test_load_refuses_a_bad_file(raw, conn, settings):
    _sample(raw.db)
    insert_rule(raw.db, "PARAM", 900, "other", config_id=2, rule_sql="SELECT 1")
    data = export_config(conn, settings, 1)
    data["rules"].append({"rule_id": 900, "rule_class": "EXCLUSION", "rule_name": "x", "rule_sql": "disp = 'A'; x"})
    out = load_config_file(conn, settings, json.dumps(data, default=str), "me", apply=True)
    assert not out["applied"] and any("belongs to config_id 2" in p for p in out["problems"])
    assert any("';'" in p for p in out["problems"])
    with pytest.raises(ConfigError, match="unknown column"):
        load_config_file(conn, settings, json.dumps({"config": {"config_id": 1, "nope": 1}, "rules": []}), "me")
    with pytest.raises(ConfigError, match="rule_id repeated"):
        load_config_file(conn, settings, json.dumps({"config": {"config_id": 1}, "rules": [
            {"rule_id": 5, "rule_class": "PARAM"}, {"rule_id": 5, "rule_class": "PARAM"}]}), "me")


def test_load_a_new_config(raw, conn, settings):
    _sample(raw.db)
    data = export_config(conn, settings, 1)
    data["config"].update(config_id=3, sample_name="copy")
    data["rules"] = [{**r, "rule_id": r["rule_id"] + 3000, "config_id": 3} for r in data["rules"]]
    out = load_config_file(conn, settings, json.dumps(data, default=str), "me", apply=True)
    assert out["config"] == "new" and len(out["rules_added"]) == 3
    assert run_sampling(conn, settings, 3, run_key="K1", as_of=AS_OF)["selected"] == 4


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


def test_cli_config_file_and_purge(raw, monkeypatch, tmp_path, capsys):
    from sampling.db import Connection
    cli = load_cli()
    monkeypatch.setenv("SAMPLING_META_DB", "main")
    monkeypatch.setenv("SAMPLING_ACCESS_LOCK", "false")
    monkeypatch.setenv("SAMPLING_ENV_FILE", "does-not-exist.env")
    monkeypatch.setenv("SAMPLING_LOG_DIR", str(tmp_path))
    monkeypatch.setattr(cli, "connect_teradata", lambda db, lock: Connection(raw, host="duckdb", access_lock=False))
    monkeypatch.setattr(raw, "close", lambda: None)
    _sample(raw.db)
    path = tmp_path / "c.json"
    assert cli.main(["export-config", "--config-id", "1", "--file", str(path)]) == 0
    data = json.loads(path.read_text())
    data["config"]["target_volume"] = 2
    path.write_text(json.dumps(data))
    capsys.readouterr()
    assert cli.main(["load-config", "--file", str(path)]) == 0
    assert not json.loads(capsys.readouterr().out)["applied"]
    assert cli.main(["load-config", "--file", str(path), "--apply", "--by", "jdoe"]) == 0
    assert json.loads(capsys.readouterr().out)["applied"]
    assert cli.main(["load-config", "--file", str(tmp_path / "missing.json")]) == 2
    assert cli.main(["purge", "--config-id", "1", "--older-than-days", "30"]) == 0
