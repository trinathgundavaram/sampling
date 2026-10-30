"""Audit defensibility: the rules snapshot of every run, finalising a sample, replacing a case."""
import json
from datetime import date, timedelta

import pytest

from conftest import insert_config, insert_rule, insert_strata, load_cli
from sampling.engine import finalize_run, replace_case, run_sampling
from sampling.settings import ConfigError

AS_OF = date(2026, 8, 7)


def _universe(db, rows):
    """rows: (case_id, disp, code)."""
    db.execute("CREATE TABLE u (case_id VARCHAR, disp VARCHAR, code VARCHAR)")
    db.executemany("INSERT INTO u VALUES (?, ?, ?)", rows)


def _sample(db, target=4, **kw):
    """Cases d00..d09 Denied and w00..w04 Withdrawn; half and half, highest case_id first."""
    _universe(db, [(f"d{i:02d}", "Denied", "X") for i in range(10)] + [(f"w{i:02d}", "Withdrawn", "Y") for i in range(5)])
    insert_config(db, universe_table="u", exclusion_sql=None, scope_sql=None, target_volume=target,
                  priority_rank_sql="case_id DESC", **kw)
    insert_strata(db, 1, 1, 0, "disp", "disp", {"Denied": 0.5, "Withdrawn": 0.5})


def _run(conn, s, **kw):
    kw.setdefault("as_of", AS_OF)
    kw.setdefault("run_key", "K1")
    return run_sampling(conn, s, 1, **kw)


def _rows(raw, sql):
    return raw.db.execute(sql).fetchall()


def _selected(raw, run_key="K1"):
    return [r[0] for r in _rows(raw, "SELECT case_key FROM gre_sample_selections WHERE etl_is_curr_ind = 'Y' AND "
                                     f"run_key = '{run_key}' AND selected_flag = 1 ORDER BY selection_order")]


# ── rules snapshot ─────────────────────────────────────────────────────────

def test_every_run_stores_the_rules_it_used_and_their_hash(raw, conn, settings):
    _sample(raw.db)
    first = _run(conn, settings)
    run = _rows(raw, "SELECT config_hash, run_report FROM gre_sampling_run")[0]
    report = json.loads(run[1])
    assert run[0] == first["config_hash"] == report["config_hash"] and len(run[0]) == 64
    assert report["config"]["config"]["universe_table"] == "u"
    assert [(r["rule_class"], r["rule_name"]) for r in report["config"]["rules"]] == \
        [("LEVEL", "disp"), ("BUCKET", "Denied"), ("BUCKET", "Withdrawn")]

    raw.db.execute("UPDATE gre_sampling_rule SET created_by = 'someone', load_datetime = CURRENT_TIMESTAMP")
    assert _run(conn, settings, dry_run=True)["config_hash"] == first["config_hash"]   # who / when do not count
    raw.db.execute("UPDATE gre_sampling_rule SET target_fraction = 0.75 WHERE rule_name = 'Denied'")
    raw.db.execute("UPDATE gre_sampling_rule SET target_fraction = 0.25 WHERE rule_name = 'Withdrawn'")
    assert _run(conn, settings, run_key="K2")["config_hash"] != first["config_hash"]


def test_a_failed_run_keeps_its_snapshot_too(raw, conn, settings):
    _sample(raw.db, sampling_method="BOGUS")
    out = _run(conn, settings)
    report = json.loads(_rows(raw, "SELECT run_report FROM gre_sampling_run")[0][0])
    assert out["error_type"] == "CONFIG_ERROR" and report["config"]["config"]["sampling_method"] == "BOGUS"


# ── finalise ──────────────────────────────────────────────────────────────

def test_a_final_sample_needs_a_force_reason_to_be_replaced(raw, conn, settings):
    _sample(raw.db)
    first = _run(conn, settings)
    assert finalize_run(conn, settings, 1, "K1", "auditor")["status"] == "FINAL"
    with pytest.raises(ConfigError):
        finalize_run(conn, settings, 1, "K1", "auditor")                  # already final
    refused = _run(conn, settings)
    assert refused["status"] == "ERROR" and refused["error_type"] == "FINALIZED" and refused["error_kind"] == "config"
    current = _rows(raw, "SELECT run_id, status, finalized_by FROM gre_sampling_run WHERE etl_is_curr_ind = 'Y'")
    assert current == [(first["sample_run_id"], "FINAL", "auditor")]       # the final sample is untouched
    assert _run(conn, settings, dry_run=True)["finalized_run"] == first["sample_run_id"]   # a dry run may look

    forced = _run(conn, settings, force_reason="rules corrected, approved by J. Doe")
    assert forced["status"] == "COMPLETED" and forced["retired_runs"] == 1
    assert _rows(raw, f"SELECT override_reason, etl_is_curr_ind FROM gre_sampling_run "
                      f"WHERE run_id = '{forced['sample_run_id']}'") == [("rules corrected, approved by J. Doe", "Y")]
    assert _rows(raw, f"SELECT status, etl_is_curr_ind FROM gre_sampling_run "
                      f"WHERE run_id = '{first['sample_run_id']}'") == [("FINAL", "N")]


def test_finalize_needs_a_current_sample(raw, conn, settings):
    _sample(raw.db)
    with pytest.raises(ConfigError):
        finalize_run(conn, settings, 1, "K1", "auditor")


# ── replace a case ────────────────────────────────────────────────────────

def test_replace_takes_the_next_candidate_of_the_same_bucket(raw, conn, settings):
    _sample(raw.db)
    _run(conn, settings)
    assert _selected(raw) == ["case_id=w04", "case_id=w03", "case_id=d09", "case_id=d08"]
    finalize_run(conn, settings, 1, "K1", "auditor")                       # allowed on a final sample
    out = replace_case(conn, settings, 1, "K1", "case_id=d09", "no medical record", "auditor")
    assert (out["replacement"], out["selection_order"], out["stratum_path"]) == ("case_id=d07", 3, "Denied")
    assert _selected(raw) == ["case_id=w04", "case_id=w03", "case_id=d07", "case_id=d08"]
    rows = _rows(raw, "SELECT case_key, selection_status, linked_case_key, status_reason, status_by "
                      "FROM gre_sample_selections WHERE selection_status IN ('REPLACED', 'REPLACEMENT') ORDER BY 2")
    assert rows == [("case_id=d09", "REPLACED", "case_id=d07", "no medical record", "auditor"),
                    ("case_id=d07", "REPLACEMENT", "case_id=d09", "no medical record", "auditor")]

    with pytest.raises(ConfigError):                                         # no longer in the sample
        replace_case(conn, settings, 1, "K1", "case_id=d09", "again", "auditor")
    with pytest.raises(ConfigError):
        replace_case(conn, settings, 1, "K1", "case_id=d08", "", "auditor")  # a reason is required
    replace_case(conn, settings, 1, "K1", "case_id=d07", "also no record", "auditor")
    assert "case_id=d06" in _selected(raw) and "case_id=d09" not in _selected(raw)


def test_replace_when_the_bucket_is_used_up(raw, conn, settings):
    _sample(raw.db, target=10)                                               # all 5 Withdrawn are in
    _run(conn, settings)
    with pytest.raises(ConfigError, match="allow any bucket"):
        replace_case(conn, settings, 1, "K1", "case_id=w04", "duplicate", "auditor")
    out = replace_case(conn, settings, 1, "K1", "case_id=w04", "duplicate", "auditor", any_bucket=True)
    assert out["replacement"] == "case_id=d04" and out["stratum_path"] == "Denied"


def test_replace_updates_the_output_snapshot(raw, conn, settings):
    _sample(raw.db, output_table="out_t")
    raw.db.execute("CREATE TABLE out_t (sample_run_id VARCHAR, config_id INTEGER, run_key VARCHAR, sample_cycle DATE, "
                   "case_key VARCHAR, selection_order INTEGER, priority_rank INTEGER, stratum_path VARCHAR, "
                   "etl_is_curr_ind VARCHAR, load_datetime TIMESTAMP, last_updated_datetime TIMESTAMP, "
                   "case_id VARCHAR, code VARCHAR)")
    insert_rule(raw.db, "OUTPUT", 1, "case_id", rule_sql="case_id")
    insert_rule(raw.db, "OUTPUT", 2, "code", rule_sql="code")
    _run(conn, settings)
    raw.db.execute("UPDATE gre_sampling_rule SET rule_sql = 'LOWER(code)' WHERE rule_id = 2")   # changed later
    replace_case(conn, settings, 1, "K1", "case_id=w04", "no record", "auditor")
    assert _rows(raw, "SELECT case_id, code, selection_order FROM out_t WHERE etl_is_curr_ind = 'Y' "
                      "ORDER BY selection_order") == [("w02", "Y", 1), ("w03", "Y", 2), ("d09", "X", 3), ("d08", "X", 4)]
    assert _rows(raw, "SELECT case_id FROM out_t WHERE etl_is_curr_ind = 'N'") == [("w04",)]


def test_a_replaced_case_is_not_carried_over(raw, conn, settings):
    _sample(raw.db, target=2)
    insert_rule(raw.db, "PRIORITY", 1, "CARRY_OVER", rule_type="PRIOR_CANDIDATE", lookback_days=30)
    _run(conn, settings)                                                     # w04, d09
    replace_case(conn, settings, 1, "K1", "case_id=d09", "no record", "auditor")   # -> d08
    _run(conn, settings, run_key="K2", as_of=AS_OF + timedelta(days=7))
    # carry-over: never selected in K1 come first (w03, d07 ...) but not d09, which was taken out on purpose
    assert _selected(raw, "K2") == ["case_id=w03", "case_id=d07"]


# ── command line ──────────────────────────────────────────────────────────

def test_cli_finalize_replace_and_force(raw, conn, monkeypatch, tmp_path, capsys):
    from sampling.db import Connection
    cli = load_cli()
    monkeypatch.setenv("SAMPLING_META_DB", "main")
    monkeypatch.setenv("SAMPLING_ACCESS_LOCK", "false")
    monkeypatch.setenv("SAMPLING_ENV_FILE", "does-not-exist.env")
    monkeypatch.setenv("SAMPLING_LOG_DIR", str(tmp_path))
    monkeypatch.setattr(cli, "connect_teradata", lambda db, lock: Connection(raw, host="duckdb", access_lock=False))
    monkeypatch.setattr(raw, "close", lambda: None)
    _sample(raw.db)
    base = ["--config-id", "1", "--run-key", "K1"]
    assert cli.main(["run", *base, "--as-of", "2026-08-07"]) == 0
    assert cli.main(["finalize", *base, "--by", "auditor"]) == 0
    assert cli.main(["replace", *base, "--case-key", "case_id=d09", "--reason", "no record"]) == 0
    assert cli.main(["replace", *base, "--case-key", "case_id=zzz", "--reason", "x"]) == 2
    assert cli.main(["run", *base, "--as-of", "2026-08-07"]) == 1                  # FINALIZED
    assert cli.main(["run", *base, "--as-of", "2026-08-07", "--force-reason", "approved"]) == 0
    capsys.readouterr()
    assert cli.main(["runs", "--config-id", "1", "--status", "FINAL"]) == 0
    runs = json.loads(capsys.readouterr().out)["runs"]
    assert len(runs) == 1 and runs[0]["finalized_by"] == "auditor" and runs[0]["etl_is_curr_ind"] == "N"
