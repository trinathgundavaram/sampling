from datetime import date

import pytest

from conftest import insert_bucket, insert_config, insert_rule, key
from sampling.engine import refinement_report, replace_case, review_refinement, run_sampling
from sampling.settings import ConfigError

AS_OF = date(2026, 8, 7)


def _setup(db):
    db.execute("CREATE TABLE u (case_id VARCHAR, disp VARCHAR, cpt VARCHAR, flag VARCHAR)")
    rows = [(f"a{i:03d}", "A", "C1" if i < 6 else f"A{i}", "N") for i in range(10)]
    rows += [(f"b{i:03d}", "B", f"B{i}", "Y" if i < 2 else "N") for i in range(3)]
    rows += [(f"z{i:03d}", "Z", f"Z{i}", "N") for i in range(2)]
    db.executemany("INSERT INTO u VALUES (?, ?, ?, ?)", rows)
    insert_config(db, src_table_nm="u", exclusion_sql=None, scope_sql_txt=None, target_cnt=10, order_by="case_id")
    insert_rule(db, "EXCLUSION", 5, "FLAGGED", seq_nbr=1, rule_sql_txt="flag = 'Y'")
    insert_rule(db, "EXCLUSION", 6, "NEVER", seq_nbr=2, rule_sql_txt="flag = 'Q'")
    insert_rule(db, "LEVEL", 1000, "disp", seq_nbr=1, level_nbr=0, rule_sql_txt="disp", unlisted_cd="SKIP")
    insert_bucket(db, 1001, 0, "A", seq_nbr=1, quota_val=50)
    insert_bucket(db, 1002, 0, "B", seq_nbr=2, quota_val=50)
    insert_rule(db, "CAP", 50, "CPT_VARIETY", seq_nbr=50, rule_ty="PER_VALUE", rule_sql_txt="SELECT case_id, cpt FROM main.u",
                max_ty="CNT", max_val=3, raw_sql=True)


def _stats(raw, run_id):
    cur = raw.db.execute("SELECT * FROM GOVT_RGLTRY_SamplingRunStat WHERE Smpl_Run_ID = ? ORDER BY Stat_Seq_Nbr",
                         [run_id])
    names = [d[0].lower() for d in cur.description]
    return [dict(zip(names, r)) for r in cur.fetchall()]


def test_a_run_writes_its_stats(raw, conn, settings):
    _setup(raw.db)
    out = run_sampling(conn, settings, key(1), run_key="K", as_of=AS_OF)
    assert out["status"] == "COMPLETED", out.get("error")
    stats = _stats(raw, out["smpl_run_id"])
    assert [s["stat_seq_nbr"] for s in stats] == list(range(1, len(stats) + 1))
    assert {(s["project_cd"], s["table_nm"], s["run_ty"], s["run_key"], s["as_of_dt_key"]) for s in stats} == \
        {("UMCM", "T1", "WKLY", "K", AS_OF)}
    by = {}
    for s in stats:
        by.setdefault(s["stat_ty"], {})[s["stat_nm"]] = s
    assert {p: (b["leaf_ind"], b["candidate_cnt"], b["target_cnt"], b["selected_cnt"]) for p, b in by["BUCKET"].items()} \
        == {p: (1, n["candidates"], n["target"], n["selected"]) for p, n in out["by_stratum"].items()}
    cap = by["CAP"]["CPT_VARIETY"]
    assert (cap["rule_id"], cap["limit_cnt"], cap["over_cnt"], cap["replaced_cnt"]) == \
        (50, *[out["caps"]["CPT_VARIETY"][k] for k in ("limit", "over", "replaced")])
    assert {n: (s["rule_id"], s["case_cnt"]) for n, s in by["EXCLUSION"].items()} == {"FLAGGED": (5, 2), "NEVER": (6, 0)}
    assert by["RULE"]["EXCLUSION NEVER"]["case_cnt"] == 0 and by["RULE"]["EXCLUSION FLAGGED"]["case_cnt"] == 2
    assert by["RULE"]["CAP CPT_VARIETY"]["case_cnt"] == 15
    assert {k: s["case_cnt"] for k, s in by["NOT_PICKED"].items()} == out["not_picked"]
    assert {p: (b["candidate_cnt"], b["target_cnt"], b["selected_cnt"]) for p, b in by["BUCKET"].items()} == \
        {"A": (10, 5, 7), "B": (1, 5, 1), "Z": (2, 0, 0)}
    assert (cap["over_cnt"], cap["replaced_cnt"]) == (2, 2)
    assert by["TOPUP"]["top-up"]["case_cnt"] == 2 and out["selected"] == 8
    assert {k: s["case_cnt"] for k, s in by["NOT_PICKED"].items()} == {"cap CPT_VARIETY": 3, "bucket not sampled": 2}
    assert [s["stat_txt"] for s in stats if s["stat_ty"] == "NOTE"] == out["notes"]


def test_the_stats_follow_the_run(raw, conn, settings):
    _setup(raw.db)
    first = run_sampling(conn, settings, key(1), run_key="K", as_of=AS_OF)
    assert run_sampling(conn, settings, key(1), run_key="K", as_of=AS_OF, dry_run=True,
                        rerun_reason="check")["status"] == "COMPLETED"
    again = run_sampling(conn, settings, key(1), run_key="K", as_of=AS_OF)
    assert again["error_type"] == "RERUN_REASON"
    runs = raw.db.execute("SELECT Smpl_Run_ID, COUNT(*) FROM GOVT_RGLTRY_SamplingRunStat GROUP BY 1").fetchall()
    assert [r[0] for r in runs] == [first["smpl_run_id"]]


def _refinement_rows(raw):
    cur = raw.db.execute("SELECT * FROM GOVT_RGLTRY_SamplingRefinement ORDER BY Refine_ID, Finding_Seq_Nbr")
    names = [d[0].lower() for d in cur.description]
    return [dict(zip(names, r)) for r in cur.fetchall()]


def _three_weeks(raw, conn, settings):
    _setup(raw.db)
    days = [date(2026, 7, 3), date(2026, 7, 10), date(2026, 7, 17)]
    for i, day in enumerate(days):
        if i == 2:
            raw.db.execute("UPDATE u SET flag = 'Y' WHERE case_id IN ('a008', 'a009')")
        out = run_sampling(conn, settings, key(1), run_key=f"W{i}", as_of=day)
        assert out["status"] == "COMPLETED", out.get("error")
    run_sampling(conn, settings, key(1), run_key="OLD", as_of=date(2026, 1, 2))
    picked = raw.db.execute("SELECT Case_Key FROM GOVT_RGLTRY_SampleSelection WHERE Run_Key = 'W2' "
                            "AND Selected_Ind = 1 ORDER BY Selection_Order_Nbr").fetchone()[0]
    replace_case(conn, settings, key(1), "W2", picked, "unreadable", "me")
    return days


def test_the_refinement_report_lists_what_to_look_at(raw, conn, settings):
    days = _three_weeks(raw, conn, settings)
    report = refinement_report(conn, settings, date(2026, 6, 1), date(2026, 7, 31), project="umcm", record=False,
                               swing_runs=2, swing_min=1)
    sample = report["samples"][0]
    found = {(f["kind"], f["name"]): f for f in sample["findings"]}
    assert set(found) == {("SHORT_BUCKET", "B"), ("CAP_BLOCKS", "CPT_VARIETY"), ("DEAD_RULE", "EXCLUSION NEVER"),
                          ("EXCLUSION_SWING", "FLAGGED")}
    assert found["SHORT_BUCKET", "B"]["short_runs"] == 3 and found["SHORT_BUCKET", "B"]["missing_cnt"] == 12
    assert found["CAP_BLOCKS", "CPT_VARIETY"]["blocked_cnt"] == 6 and found["CAP_BLOCKS", "CPT_VARIETY"]["lost_cnt"] == 0
    swing = found["EXCLUSION_SWING", "FLAGGED"]
    assert (swing["case_cnt"], swing["prior_avg"], swing["last"], swing["rule_id"]) == (4, 2, days[2], 5)
    assert report["counts"] == {"SHORT_BUCKET": 1, "CAP_BLOCKS": 1, "DEAD_RULE": 1, "EXCLUSION_SWING": 1}
    assert (sample["sample"], sample["runs"], sample["refine_id"]) == ("UMCM.T1.WKLY", 3, None)
    totals = {(f["kind"], f["name"]): f["case_cnt"] for f in sample["totals"]}
    assert totals[("REPLACED", "unreadable")] == 1 and totals[("TOPUP", "top-up")] > 0
    assert {k for k, _ in totals} == {"NOT_PICKED", "TOPUP", "REPLACED"}
    assert _refinement_rows(raw) == []
    assert refinement_report(conn, settings, date(2026, 6, 1), date(2026, 7, 31), short_runs=4,
                             record=False)["counts"] == {"SHORT_BUCKET": 0, "CAP_BLOCKS": 1, "DEAD_RULE": 0,
                                                         "EXCLUSION_SWING": 0}
    defaults = refinement_report(conn, settings, date(2026, 6, 1), date(2026, 7, 31), record=False)
    assert defaults["counts"]["EXCLUSION_SWING"] == 0
    assert refinement_report(conn, settings, date(2026, 6, 1), date(2026, 7, 31), record=False, swing_runs=2,
                             swing_min=5)["counts"]["EXCLUSION_SWING"] == 0
    assert refinement_report(conn, settings, date(2026, 6, 1), table="T9")["samples"] == []
    with pytest.raises(ConfigError, match="above 0"):
        refinement_report(conn, settings, date(2026, 6, 1), swing=0)


def test_the_refinement_report_is_recorded_and_reviewed(raw, conn, settings):
    _three_weeks(raw, conn, settings)
    report = refinement_report(conn, settings, date(2026, 6, 1), date(2026, 7, 31), triggered_by="tester",
                               swing_runs=2, swing_min=1)
    sample = report["samples"][0]
    rows = _refinement_rows(raw)
    assert [r["finding_seq_nbr"] for r in rows] == list(range(1, len(rows) + 1))
    assert len(rows) == len(sample["findings"]) + len(sample["totals"])
    assert {(r["refine_id"], r["project_cd"], r["table_nm"], r["run_ty"], r["window_start_dt"], r["window_end_dt"],
             r["run_cnt"], r["triggered_by"], r["engine_version_txt"], r["threshold_txt"]) for r in rows} == \
        {(sample["refine_id"], "UMCM", "T1", "WKLY", date(2026, 6, 1), date(2026, 7, 31), 3, "tester", "0.1.0",
          "short runs 3, swing 0.5, swing runs 2, swing min 1")}
    by = {(r["finding_ty"], r["finding_nm"]): r for r in rows}
    short = by["SHORT_BUCKET", "B"]
    assert (short["short_run_cnt"], short["missing_cnt"], short["finding_txt"]) == \
        (3, 12, "short of its target in 3 of 3 run(s), 12 case(s) in all")
    assert by["CAP_BLOCKS", "CPT_VARIETY"]["blocked_cnt"] == 6 and by["EXCLUSION_SWING", "FLAGGED"]["prior_avg_val"] == 2
    assert by["REPLACED", "unreadable"]["case_cnt"] == 1 and by["DEAD_RULE", "EXCLUSION NEVER"]["rule_id"] == 6
    rid = sample["refine_id"]
    with pytest.raises(ConfigError, match="review note"):
        review_refinement(conn, settings, rid, "lead", " ")
    one = review_refinement(conn, settings, rid, "lead", "lower B's quota", seq=short["finding_seq_nbr"])
    assert one["reviewed"] == [short["finding_seq_nbr"]]
    with pytest.raises(ConfigError, match="already reviewed"):
        review_refinement(conn, settings, rid, "lead", "again", seq=short["finding_seq_nbr"])
    rest = review_refinement(conn, settings, rid, "lead", "rest seen, no change")
    assert len(rest["reviewed"]) == len(rows) - 1
    reviews = {r["finding_seq_nbr"]: (r["review_by"], r["review_txt"]) for r in _refinement_rows(raw)}
    assert reviews[short["finding_seq_nbr"]] == ("lead", "lower B's quota")
    assert sum(1 for v in reviews.values() if v == ("lead", "rest seen, no change")) == len(rows) - 1
    with pytest.raises(ConfigError, match="already reviewed"):
        review_refinement(conn, settings, rid, "lead", "again")
    with pytest.raises(ConfigError, match="no refinement"):
        review_refinement(conn, settings, "nope", "lead", "x")
    later = refinement_report(conn, settings, date(2026, 6, 1), date(2026, 8, 31), swing_runs=2, swing_min=1)
    assert later["samples"][0]["swings_after"] == date(2026, 7, 31)
    assert later["counts"] == {"SHORT_BUCKET": 1, "CAP_BLOCKS": 1, "DEAD_RULE": 1, "EXCLUSION_SWING": 0}


def test_cli_refine_and_review(raw, conn, settings):
    from conftest import load_cli
    cli = load_cli()
    _setup(raw.db)
    run_sampling(conn, settings, key(1), run_key="K", as_of=AS_OF)
    parse = cli.build_parser().parse_args
    out, code = cli.cmd_refine(conn, settings, parse(["refine", "--since", "2026-08-01", "--until", "2026-08-31",
                                                     "--short-runs", "1", "--dry-run"]))
    assert code == 0 and out["counts"]["SHORT_BUCKET"] == 1 and out["counts"]["DEAD_RULE"] == 1
    assert _refinement_rows(raw) == []
    out, _ = cli.cmd_refine(conn, settings, parse(["refine", "--since", "2026-08-01", "--until", "2026-08-31"]))
    rows = _refinement_rows(raw)
    assert rows and rows[0]["triggered_by"].startswith("local-")
    done, _ = cli.cmd_review(conn, settings, parse(["review", "--refine-id", out["samples"][0]["refine_id"],
                                                    "--note", "ok", "--by", "me"]))
    assert done["review_by"] == "me" and len(done["reviewed"]) == len(rows)
    with pytest.raises(ConfigError, match="--since"):
        cli.cmd_refine(conn, settings, parse(["refine"]))
