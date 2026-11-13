from datetime import date, datetime, timedelta

import pytest

from conftest import insert_config, insert_rule, insert_strata, key, load_cli
from sampling.engine import (current_rows, revalidate_run, revalidate_since, run_sampling,
                             signoff_revalidation)
from sampling.settings import ConfigError

AS_OF = date(2026, 8, 7)
LOADED = datetime(2026, 1, 1)
CURRENT = "SELECT * FROM src WHERE {current_rows}"


def _source(db, n=60):
    db.execute("CREATE TABLE src (case_id VARCHAR, area VARCHAR, ETL_IS_CURR_IND VARCHAR, ETL_IS_DEL_IND VARCHAR, "
               "ETL_LOAD_DT_TM TIMESTAMP, ETL_LAST_UPDT_DT_TM TIMESTAMP)")
    db.executemany("INSERT INTO src VALUES (?, ?, 'Y', 'N', ?, ?)",
                   [(f"c{i:03d}", "A" if i % 2 else "B", LOADED, LOADED) for i in range(n)])


def _setup(raw, method="RANDOM", target=10):
    _source(raw.db)
    insert_config(raw.db, src_table_nm="src", exclusion_sql=None, scope_sql_txt=CURRENT, target_cnt=target,
                  smpl_method_cd=method, order_by=None if method == "RANDOM" else "case_id")
    insert_strata(raw.db, 1, 1, 0, "area", "SELECT case_id, area FROM src WHERE {current_rows}", {"A": 50, "B": 50})


def _run(conn, s, **kw):
    kw.setdefault("as_of", AS_OF)
    return run_sampling(conn, s, key(1), **kw)


def _picks(raw, run_id):
    return [r[0] for r in raw.db.execute("SELECT Case_Key FROM GOVT_RGLTRY_SampleSelection WHERE Smpl_Run_ID = ? "
                                         "AND Selected_Ind = 1 ORDER BY Selection_Order_Nbr", [run_id]).fetchall()]


def _reval_rows(raw):
    return raw.db.execute("SELECT Reval_Stat, Orig_Selected_Cnt, Replay_Selected_Cnt, Same_Cnt, Only_Orig_Cnt, "
                          "Only_Replay_Cnt, Missing_Cnt, Diff_Txt, Error_Txt, Engine_Version_Txt "
                          "FROM GOVT_RGLTRY_SamplingRevalidation ORDER BY Reval_Dtts").fetchall()


def test_current_rows_expands_to_the_current_filter_or_the_version_at_a_time():
    assert current_rows() == "ETL_IS_CURR_IND = 'Y' AND ETL_IS_DEL_IND = 'N'"
    at = current_rows(datetime(2026, 8, 7, 9, 30))
    assert "ETL_LOAD_DT_TM <= TIMESTAMP '2026-08-07 09:30:00.000000'" in at
    assert "ETL_LAST_UPDT_DT_TM > TIMESTAMP '2026-08-07 09:30:00.000000'" in at
    assert current_rows(alias="s") == "s.ETL_IS_CURR_IND = 'Y' AND s.ETL_IS_DEL_IND = 'N'"
    assert current_rows(datetime(2026, 8, 7), "s").count("s.ETL_") == 5


def test_the_placeholder_takes_an_alias_and_any_case():
    from sampling.engine import _expand
    assert _expand("WHERE {CURRENT_ROWS:o} AND {current_rows}", {}) == \
        "WHERE (o.ETL_IS_CURR_IND = 'Y' AND o.ETL_IS_DEL_IND = 'N') AND (ETL_IS_CURR_IND = 'Y' AND ETL_IS_DEL_IND = 'N')"


def test_a_normal_run_reads_only_current_rows(raw, conn, settings):
    _setup(raw)
    raw.db.execute("UPDATE src SET ETL_IS_CURR_IND = 'N' WHERE case_id < 'c010'")
    raw.db.execute("UPDATE src SET ETL_IS_DEL_IND = 'Y' WHERE case_id BETWEEN 'c010' AND 'c019'")
    out = _run(conn, settings)
    assert out["status"] == "COMPLETED" and out["in_scope"] == 40


def test_a_dry_run_returns_its_picks(raw, conn, settings):
    _setup(raw)
    out = _run(conn, settings, dry_run=True, seed=7)
    assert len(out["picked"]) == 10 and [p["order"] for p in out["picked"]] == list(range(1, 11))
    assert {p["path"] for p in out["picked"]} == {"A", "B"}


def test_replay_matches_and_is_recorded(raw, conn, settings):
    _setup(raw)
    first = _run(conn, settings)
    out = revalidate_run(conn, settings, first["smpl_run_id"], "tester")
    assert out["status"] == "MATCH" and out["same"] == 10 and out["diff_txt"] is None
    assert _reval_rows(raw) == [("MATCH", 10, 10, 10, 0, 0, 0, None, None, "0.1.0")]
    assert raw.db.execute("SELECT COUNT(*) FROM GOVT_RGLTRY_SamplingRun").fetchone()[0] == 1


def test_replay_reads_the_source_as_it_was_after_rows_changed(raw, conn, settings):
    _setup(raw)
    first = _run(conn, settings)
    later = datetime.now() + timedelta(hours=1)
    picked = [k.split("=")[1] for k in _picks(raw, first["smpl_run_id"])]
    raw.db.execute(f"UPDATE src SET ETL_IS_CURR_IND = 'N', ETL_LAST_UPDT_DT_TM = ? "
                   f"WHERE case_id IN ({', '.join('?' * len(picked))})", [later, *picked])
    raw.db.executemany("INSERT INTO src VALUES (?, 'B', 'Y', 'N', ?, ?)", [(c, later, later) for c in picked])
    raw.db.execute("UPDATE src SET ETL_IS_DEL_IND = 'Y', ETL_LAST_UPDT_DT_TM = ? WHERE case_id = 'c059'", [later])
    raw.db.executemany("INSERT INTO src VALUES (?, 'A', 'Y', 'N', ?, ?)",
                       [(f"n{i:03d}", later, later) for i in range(30)])
    assert revalidate_run(conn, settings, first["smpl_run_id"])["status"] == "MATCH"


def test_replay_uses_the_runs_own_rules_not_rules_edited_since(raw, conn, settings):
    _setup(raw)
    first = _run(conn, settings)
    raw.db.execute("UPDATE GOVT_RGLTRY_SamplingRule SET Quota_Val = 90 WHERE Rule_Nm = 'A'")
    raw.db.execute("UPDATE GOVT_RGLTRY_SamplingConfig SET Target_Val = 30")
    assert revalidate_run(conn, settings, first["smpl_run_id"])["status"] == "MATCH"


def test_replay_reports_different_picks(raw, conn, settings):
    _setup(raw)
    first = _run(conn, settings)
    raw.db.execute("UPDATE GOVT_RGLTRY_SamplingRun SET Random_Seed_Nbr = Random_Seed_Nbr + 1")
    out = revalidate_run(conn, settings, first["smpl_run_id"])
    assert out["status"] == "DIFFERENT" and out["only_orig"] and len(out["only_orig"]) == len(out["only_replay"])
    assert "picked then, not now" in out["diff_txt"] and "picked now, not then" in out["diff_txt"]


def test_replay_reports_a_case_gone_from_the_source(raw, conn, settings):
    _setup(raw)
    first = _run(conn, settings)
    gone = _picks(raw, first["smpl_run_id"])[0]
    raw.db.execute("DELETE FROM src WHERE case_id = ?", [gone.split("=")[1]])
    out = revalidate_run(conn, settings, first["smpl_run_id"])
    assert out["status"] == "SOURCE_CHANGED" and out["missing"] == [gone]
    assert out["diff_txt"].startswith(f"not in the source as it was (1): {gone}")


def test_replaced_cases_count_as_the_original_picks(raw, conn, settings):
    from sampling.engine import replace_case
    _setup(raw)
    first = _run(conn, settings)
    replace_case(conn, settings, key(1), first["run_key"], _picks(raw, first["smpl_run_id"])[0], "unreadable", "me")
    assert revalidate_run(conn, settings, first["smpl_run_id"])["status"] == "MATCH"


def test_a_replay_that_cannot_run_is_recorded_as_failed(raw, conn, settings):
    _setup(raw)
    first = _run(conn, settings)
    raw.db.execute("UPDATE GOVT_RGLTRY_SamplingRun SET Run_Parm_Txt = NULL")
    out = revalidate_run(conn, settings, first["smpl_run_id"])
    assert out["status"] == "FAILED" and "stored no run parameters" in out["error"]
    row = _reval_rows(raw)[0]
    assert row[0] == "FAILED" and row[2:7] == (None, None, None, None, None) and "stored no run" in row[8]


def test_only_completed_runs_can_be_replayed(raw, conn, settings):
    _setup(raw)
    insert_rule(raw.db, "PARAM", 77, "batch_id", rule_sql_txt="SELECT NULL")
    failed = _run(conn, settings)
    assert failed["status"] == "ERROR"
    with pytest.raises(ConfigError, match="only a COMPLETED or FINAL run"):
        revalidate_run(conn, settings, failed["smpl_run_id"])
    with pytest.raises(ConfigError, match="no run"):
        revalidate_run(conn, settings, "nope")


def test_revalidate_since_takes_the_current_runs_in_the_window(raw, conn, settings):
    _setup(raw)
    _run(conn, settings, as_of=date(2026, 1, 9), run_key="old")
    _run(conn, settings, as_of=date(2026, 7, 3), run_key="w27")
    _run(conn, settings, as_of=date(2026, 7, 3), run_key="w27", rerun_reason="redo")
    _run(conn, settings, as_of=date(2026, 7, 10), run_key="w28")
    out = revalidate_since(conn, settings, date(2026, 6, 1))
    assert out["runs"] == 2 and out["counts"]["MATCH"] == 2
    assert sorted(r["run_key"] for r in out["results"]) == ["w27", "w28"]
    assert revalidate_since(conn, settings, date(2026, 6, 1), project="OTHER")["runs"] == 0


def test_signoff_once_with_a_note(raw, conn, settings):
    _setup(raw)
    first = _run(conn, settings)
    reval = revalidate_run(conn, settings, first["smpl_run_id"])["reval_id"]
    with pytest.raises(ConfigError, match="sign-off note"):
        signoff_revalidation(conn, settings, reval, "auditor", " ")
    out = signoff_revalidation(conn, settings, reval, "auditor", "reviewed, matches")
    assert out["signoff_by"] == "auditor"
    with pytest.raises(ConfigError, match="already signed off by auditor"):
        signoff_revalidation(conn, settings, reval, "someone", "again")
    with pytest.raises(ConfigError, match="no revalidation"):
        signoff_revalidation(conn, settings, "nope", "auditor", "x")


def test_cli_revalidate_and_signoff(raw, conn, settings):
    cli = load_cli()
    _setup(raw)
    first = _run(conn, settings)
    args = cli.build_parser().parse_args(["revalidate", "--run-id", first["smpl_run_id"]])
    out, code = cli.cmd_revalidate(conn, settings, args)
    assert code == 0 and out["status"] == "MATCH"
    args = cli.build_parser().parse_args(["revalidate", "--since", "2026-01-01", "--project", "UMCM"])
    out, code = cli.cmd_revalidate(conn, settings, args)
    assert code == 0 and out["runs"] == 1
    with pytest.raises(ConfigError, match="--run-id or --since"):
        cli.cmd_revalidate(conn, settings, cli.build_parser().parse_args(["revalidate"]))
    args = cli.build_parser().parse_args(["signoff", "--reval-id", out["results"][0]["reval_id"], "--note", "ok",
                                          "--by", "me"])
    assert cli.cmd_signoff(conn, settings, args)[0]["signoff_by"] == "me"


def _history_weeks(raw, conn, settings):
    from conftest import build_source, um_config
    build_source(raw.db, n=400)
    um_config(raw.db, run_ty="WKLY")
    insert_rule(raw.db, "EXCLUSION", 210, "SELECTED_IN_LAST_90_DAYS", seq_nbr=0, rule_ty="PRIOR_SELECTION",
                lookback_days=90, history_scope_cd="SAMPLE")
    batch = {"batch_id": "2026-08-01"}
    w1 = run_sampling(conn, settings, key(1), run_key="W1", as_of=AS_OF - timedelta(days=7), run_params=batch)
    w2 = run_sampling(conn, settings, key(1), run_key="W2", as_of=AS_OF, run_params=batch)
    assert w1["status"] == w2["status"] == "COMPLETED", (w1.get("error"), w2.get("error"))
    return w2, batch


def test_replay_reads_history_as_it_was_after_later_replacements(raw, conn, settings):
    from sampling.engine import replace_case
    w2, _ = _history_weeks(raw, conn, settings)
    picks = [r[0] for r in raw.db.execute("SELECT Case_Key FROM GOVT_RGLTRY_SampleSelection WHERE Run_Key = 'W1' "
                                          "AND Selected_Ind = 1").fetchall()]
    for case in picks[:20]:
        replace_case(conn, settings, key(1), "W1", case, "unreadable", "me", any_bucket=True)
    out = revalidate_run(conn, settings, w2["smpl_run_id"])
    assert out["status"] == "MATCH", out["diff_txt"]


def test_replay_reads_history_as_it_was_after_a_later_rerun(raw, conn, settings):
    w2, batch = _history_weeks(raw, conn, settings)
    first = raw.db.execute("SELECT Smpl_Run_ID FROM GOVT_RGLTRY_SamplingRun WHERE Run_Key = 'W1'").fetchone()[0]
    before = set(_picks(raw, first))
    raw.db.execute("UPDATE GOVT_RGLTRY_SamplingConfig SET Smpl_Method_Cd = 'RANDOM'")
    rerun = run_sampling(conn, settings, key(1), run_key="W1", as_of=AS_OF - timedelta(days=7), run_params=batch,
                         seed=999, rerun_reason="redo")
    assert rerun["status"] == "COMPLETED" and set(_picks(raw, rerun["smpl_run_id"])) != before
    out = revalidate_run(conn, settings, w2["smpl_run_id"])
    assert out["status"] == "MATCH", out["diff_txt"]
