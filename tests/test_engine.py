import random
from datetime import date

import pytest

from conftest import build_source, insert_config, insert_strata, key, um_config
from sampling.db import generate_run_id, substitute_params
from sampling.engine import (Sample, Stratifier, build_queries, discover_configs, round_target, run_sampling,
                             run_scope, select, validate)
from sampling.settings import ConfigError, Settings

AS_OF = date(2026, 8, 1)
PARAMS = {"batch_id": "2026-08-01"}


def _run(conn, s, config_id=1, **kw):
    kw.setdefault("run_params", PARAMS)
    kw.setdefault("as_of", AS_OF)
    return run_sampling(conn, s, key(config_id), **kw)


def _rows(raw, sql):
    return raw.db.execute(sql).fetchall()


def test_round_target_rounding_and_float_artefacts():
    assert [round_target(3.5, m) for m in ("FLOOR", "CEIL", "ROUND")] == [3, 4, 4]
    assert round_target(100 * 0.29, "FLOOR") == 29
    assert round_target(100 * 0.07, "CEIL") == 7


def test_select_methods():
    rows = [{"i": i} for i in range(100)]
    assert select(rows, 5, "RANKED", random.Random(1)) == rows[:5]
    assert select(rows, 5, "RANDOM", random.Random(7)) == select(rows, 5, "RANDOM", random.Random(7))
    sys_pick = select(rows, 10, "SYSTEMATIC", random.Random(3))
    assert len(sys_pick) == 10 and len({b["i"] - a["i"] for a, b in zip(sys_pick, sys_pick[1:])}) == 1
    assert select(rows, 0, "RANKED", random.Random(1)) == []
    with pytest.raises(ConfigError):
        select(rows, 1, "BOGUS", random.Random(1))


def test_stratify_any_number_of_levels_shares_equally():
    rows = [{"smp_r_1": a, "smp_r_2": b, "smp_r_3": c, "_order": 0}
            for a in ("X", "Y") for b in ("P", "Q") for c in ("M", "N") for _ in range(10)]
    levels = [{"rule_id": i, "level_nbr": i, "buckets": [], "value_level": True} for i in (1, 2, 3)]
    strat = Stratifier(Sample(config={"smpl_method_cd": "RANKED"}, levels=levels), None, "FLOOR")
    chosen = strat.run(rows, 40)
    assert len(chosen) == 40 and len(strat.leaves) == 8 and all(v["selected"] == 5 for v in strat.leaves.values())


FROZEN_BY_STRATUM = {
    "Approved/Area A": (6, 1), "Approved/Area B": (7, 0), "Approved/Area C": (59, 9),
    "Denied/Area A": (96, 15), "Denied/Area B": (71, 9), "Denied/Area C": (610, 94),
    "Dismissed/Area A": (3, 0), "Dismissed/Area B": (4, 0), "Dismissed/Area C": (14, 2),
    "Withdrawn/Area A": (7, 1), "Withdrawn/Area B": (9, 1), "Withdrawn/Area C": (94, 11),
}


def test_um_regression_matches_frozen_output(raw, conn, settings):
    build_source(raw.db)
    um_config(raw.db)
    out = _run(conn, settings)
    assert out["status"] == "COMPLETED", out
    assert out["in_scope"] == 1000 and out["excluded"] == 20 and out["candidates"] == 980
    assert {k: (v["candidates"], v["selected"]) for k, v in out["by_stratum"].items()} == FROZEN_BY_STRATUM
    assert out["selected"] == 150 and out["shortfall"] == 0


def test_run_writes_every_candidate_and_the_run_row(raw, conn, settings):
    build_source(raw.db)
    um_config(raw.db)
    out = _run(conn, settings, triggered_by="tester")
    sel = _rows(raw, "SELECT COUNT(*), SUM(Selected_Ind), MIN(Run_Key), MIN(Smpl_Cycle_Dt_Key) FROM GOVT_RGLTRY_SampleSelection")
    assert sel[0] == (1000, 150, "2026-08-01", AS_OF)
    assert _rows(raw, "SELECT Exclusion_Rsn_Txt, COUNT(*) FROM GOVT_RGLTRY_SampleSelection WHERE Excluded_Ind = 1 "
                      "GROUP BY 1") == [("EXCLUDED", 20)]
    run = _rows(raw, "SELECT Run_Stat, In_Scope_Cnt, Excluded_Cnt, Candidate_Cnt, Selected_Cnt, Shortfall_Cnt, "
                     "Triggered_By, Smpl_Run_ID, Curr_Ind, Error_Ty, Project_Cd, Table_Nm, Run_Ty, Src_Table_Nm, "
                     "Target_Cnt, LoadUser_ID FROM GOVT_RGLTRY_SamplingRun")
    assert run == [("COMPLETED", 1000, 20, 980, 150, 0, "tester", out["smpl_run_id"], 1, None, "UMCM", "T1", "WKLY",
                    "case_source", 150, "tester")]
    rules = _rows(raw, "SELECT Rule_ID, Rule_Class_Cd, Rule_Nm FROM GOVT_RGLTRY_SamplingRunRule ORDER BY Rule_ID")
    assert len(rules) == 10 and rules[0] == (1000, "LEVEL", "category") and rules[-1] == (9001, "EXCLUSION", "EXCLUDED")
    assert (8001, "PRIORITY", "ORDER") in rules
    report = out
    assert report["buckets"]["Denied"] == {"candidates": 777, "target": 120, "selected": 118}
    assert {k: (v["candidates"], v["selected"]) for k, v in report["buckets"].items() if "/" in k} == FROZEN_BY_STRATUM
    assert out["smpl_run_id"].startswith("UMCM.T1.WKLY::2026-08-01::attempt-1::tester::")
    paths = _rows(raw, "SELECT DISTINCT Stratum_Path_Txt FROM GOVT_RGLTRY_SampleSelection WHERE Selected_Ind = 1")
    assert {p[0] for p in paths} <= set(FROZEN_BY_STRATUM)


def test_rerun_retires_the_earlier_run_of_the_same_run_key(raw, conn, settings):
    build_source(raw.db)
    um_config(raw.db)
    first = _run(conn, settings)
    second = _run(conn, settings)
    assert "::attempt-2::" in second["smpl_run_id"] and second["retired_runs"] == 1
    curr = _rows(raw, "SELECT Smpl_Run_ID, Curr_Ind, COUNT(*) FROM GOVT_RGLTRY_SampleSelection GROUP BY 1, 2")
    assert {(r[0], r[1]) for r in curr} == {(first["smpl_run_id"], 0), (second["smpl_run_id"], 1)}
    assert _rows(raw, "SELECT Smpl_Run_ID, Curr_Ind FROM GOVT_RGLTRY_SamplingRun ORDER BY Start_Dtts") == \
        [(first["smpl_run_id"], 0), (second["smpl_run_id"], 1)]
    other = _run(conn, settings, run_key="OTHER")
    assert other["retired_runs"] == 0


def test_random_reproducible_with_stored_seed(raw, conn, settings):
    build_source(raw.db)
    um_config(raw.db, smpl_method_cd="RANDOM", order_by=None)
    first = _run(conn, settings)
    assert first["seed"] is not None
    stored = _rows(raw, "SELECT Random_Seed_Nbr FROM GOVT_RGLTRY_SamplingRun")[0][0]
    again = _run(conn, settings, seed=stored, run_key="replay")

    def picked(run_id):
        return {r[0] for r in _rows(raw, f"SELECT Case_Key FROM GOVT_RGLTRY_SampleSelection WHERE Selected_Ind = 1 AND "
                                         f"Smpl_Run_ID = '{run_id}'")}
    assert picked(first["smpl_run_id"]) == picked(again["smpl_run_id"])


def test_systematic_needs_priority_and_records_config_error(raw, conn, settings):
    build_source(raw.db)
    um_config(raw.db, smpl_method_cd="SYSTEMATIC", order_by=None)
    out = _run(conn, settings)
    assert out["status"] == "ERROR" and out["error_kind"] == "config" and "needs PRIORITY rules" in out["error"]
    assert _rows(raw, "SELECT Run_Stat, Error_Ty, Curr_Ind FROM GOVT_RGLTRY_SamplingRun") == \
        [("ERROR", "CONFIG_ERROR", 0)]


def test_null_exclusion_column_keeps_the_row(raw, conn, settings):
    raw.db.execute("CREATE TABLE u (case_id VARCHAR, flag VARCHAR)")
    raw.db.execute("INSERT INTO u VALUES ('a', 'Y'), ('b', NULL), ('c', 'N')")
    insert_config(raw.db, src_table_nm="u", exclusion_sql="flag = 'Y'", scope_sql_txt=None, order_by="case_id",
                  target_cnt=10)
    out = _run(conn, settings)
    assert out["candidates"] == 2 and out["excluded"] == 1 and out["shortfall"] == 8


def test_bucket_values_match_without_case_or_trailing_blanks(raw, conn, settings):
    raw.db.execute("CREATE TABLE u (case_id VARCHAR, cat VARCHAR)")
    raw.db.executemany("INSERT INTO u VALUES (?, ?)", [(f"d{i}", "DENIED  ") for i in range(50)]
                       + [(f"a{i}", "approved") for i in range(50)])
    insert_config(raw.db, src_table_nm="u", exclusion_sql=None, scope_sql_txt=None, order_by="case_id",
                  target_cnt=10)
    insert_strata(raw.db, 1, 1, 0, "cat", "cat", {"Denied": 80, "Approved": 20})
    out = _run(conn, settings)
    assert {k: v["selected"] for k, v in out["by_stratum"].items()} == {"Approved": 2, "Denied": 8}


def test_pull_failure_rolls_nothing_in_and_logs(raw, conn, settings):
    build_source(raw.db)
    um_config(raw.db)
    raw.fail_on = "SELECT case_id FROM (SELECT * FROM main.case_source WHERE pull_date = '2026-08-01')"
    out = _run(conn, settings)
    assert out["status"] == "ERROR" and out["error_type"] == "PULL_FAILURE" and out["error_kind"] == "technical"
    assert _rows(raw, "SELECT COUNT(*) FROM GOVT_RGLTRY_SampleSelection")[0][0] == 0


def test_persist_failure_leaves_no_partial_rows(raw, conn, settings):
    build_source(raw.db)
    um_config(raw.db)
    raw.fail_on = "UPDATE main.GOVT_RGLTRY_SampleSelection"
    out = _run(conn, settings)
    assert out["status"] == "ERROR" and out["error_type"] == "PERSIST_FAILURE"
    assert _rows(raw, "SELECT COUNT(*) FROM GOVT_RGLTRY_SampleSelection")[0][0] == 0
    assert _rows(raw, "SELECT Run_Stat, Error_Ty FROM GOVT_RGLTRY_SamplingRun") == [("ERROR", "PERSIST_FAILURE")]
    stored = _rows(raw, "SELECT Error_Txt FROM GOVT_RGLTRY_SamplingRun")[0][0]
    assert stored.startswith("RuntimeError: ") and "\n" not in stored and "Traceback" not in stored


def test_a_later_error_retires_the_earlier_one(raw, conn, settings):
    build_source(raw.db)
    um_config(raw.db, smpl_method_cd="SYSTEMATIC", order_by=None)
    _run(conn, settings)
    _run(conn, settings)
    assert _rows(raw, "SELECT COUNT(*), MIN(Run_Stat), MAX(Run_Stat) FROM GOVT_RGLTRY_SamplingRun")[0] == (2, "ERROR", "ERROR")


def test_unresolved_token_is_a_config_error(raw, conn, settings):
    build_source(raw.db)
    um_config(raw.db)
    out = _run(conn, settings, run_params={})
    assert out["error_kind"] == "config" and "batch_id" in out["error"]


def test_builtin_as_of_date_and_run_key_tokens(raw, conn, settings):
    build_source(raw.db)
    um_config(raw.db, scope_sql_txt="pull_date = CAST('{as_of_date}' AS DATE) AND '{run_key}' = '{run_key}'")
    out = _run(conn, settings, run_params={})
    assert out["status"] == "COMPLETED" and out["candidates"] == 980 and out["run_key"] == "2026-08-01"


def test_duplicate_keys_are_a_config_error(raw, conn, settings):
    build_source(raw.db)
    um_config(raw.db, key_columns="category")
    out = _run(conn, settings)
    assert out["error_kind"] == "config" and "uniquely" in out["error"]


def test_dry_run_writes_nothing(raw, conn, settings):
    build_source(raw.db)
    um_config(raw.db)
    out = _run(conn, settings, dry_run=True)
    assert out["selected"] == 150 and out["smpl_run_id"] is None
    for table in ("GOVT_RGLTRY_SamplingRun", "GOVT_RGLTRY_SampleSelection"):
        assert _rows(raw, f"SELECT COUNT(*) FROM {table}")[0][0] == 0


def test_run_scope_by_project_table_run_type_and_errors(raw, conn, settings):
    build_source(raw.db)
    um_config(raw.db, config_id=1)
    insert_config(raw.db, config_id=2, smpl_method_cd="BOGUS")
    insert_config(raw.db, config_id=3, run_ty="MNT")
    insert_config(raw.db, config_id=4, project_cd="OTHER")
    out = run_scope(conn, settings, project="umcm", run_type="wkly", run_params=PARAMS, as_of=AS_OF)
    assert [r["table_nm"] for r in out["runs"]] == ["T1", "T2"] and out["failed"] == 1
    assert out["result"] == "1 OF 2 SAMPLE(S) FAILED"
    assert discover_configs(conn, settings, "UMCM") == [key(1), key(2), key(3, run_ty="MNT")]
    assert discover_configs(conn, settings, "Umcm", table="t1") == [key(1)]
    with pytest.raises(ConfigError):
        run_scope(conn, settings, project="NOPE")
    with pytest.raises(ConfigError):
        discover_configs(conn, settings)


def test_validate_with_sql_check(raw, conn, settings):
    build_source(raw.db)
    um_config(raw.db)
    insert_config(raw.db, config_id=2, src_table_nm="missing_table")
    out = validate(conn, settings, project="UMCM", run_params=PARAMS, check_sql=True)
    problems = {c["table_nm"]: c["problems"] for c in out["configs"]}
    assert problems["T1"] == [] and "SQL check failed" in problems["T2"][0]


def test_queries_resolve_env_and_read_the_scope_select():
    s = Settings(meta_db="M", environment="PROD")
    config = {"src_schema_nm": "UMCM_$ENV_T", "src_table_nm": "u", "key_columns_txt": "a, b",
              "scope_sql_txt": "SELECT u.* FROM UMCM_$ENV_T.u JOIN UMCM_$ENV_T.b ON b.a = u.a WHERE db = '$env'",
              "smpl_method_cd": "RANKED", "tgt_schema_nm": "OUT", "tgt_table_nm": "s"}
    q = build_queries(Sample(config=config), s, {"extra_filters": "b = 1"})
    assert q["pull"] == "SELECT a, b FROM (SELECT u.* FROM UMCM_T.u JOIN UMCM_T.b ON b.a = u.a WHERE db = '') " \
                        "smp_scope WHERE (b = 1)"
    assert q["output"] == "SELECT * FROM (SELECT u.* FROM UMCM_T.u JOIN UMCM_T.b ON b.a = u.a WHERE db = '') " \
                          "smp_scope WHERE (b = 1)"
    q = build_queries(Sample(config={**config, "scope_sql_txt": None}), s, {})
    assert q["pull"] == "SELECT a, b FROM (SELECT * FROM UMCM_T.u) smp_scope WHERE (1=1)"
    assert Settings(meta_db="M", environment="DEV").resolve_env("UMCM_$ENV_STG") == "UMCM_DEV_STG"


def test_params_and_ids():
    assert substitute_params("a = '{x}' AND b = '$Y'", {"x": "it's", "y": 2}) == "a = 'it''s' AND b = '2'"
    with pytest.raises(ValueError):
        substitute_params("a = '{missing}'", {})
    assert generate_run_id("P.S", "rk", None, "").startswith("P.S::rk::")


def test_settings_validation():
    with pytest.raises(ConfigError):
        Settings.from_mapping({})
    with pytest.raises(ConfigError):
        Settings(meta_db="bad name")
    s = Settings.from_mapping({"META_DB": "HSABC_DEV", "load_env": "qa", "access_lock": "false"})
    assert (s.environment, s.access_lock) == ("QA", False)
    assert [Settings.from_mapping({"meta_db": "HSABC_$ENV", "environment": e}).meta_db
            for e in ("DEV", "QA", "UAT", "PROD")] == ["HSABC_DEV", "HSABC_QA", "HSABC", "HSABC"]


def test_cron_match_without_croniter():
    from datetime import datetime
    from sampling.settings import cron_match
    friday_8 = datetime(2026, 8, 7, 8, 0)
    assert cron_match("0 8 * * 5", friday_8) and cron_match("0 8 * * FRI", friday_8)
    assert not cron_match("0 8 * * 1-4", friday_8)
    assert cron_match("*/15 6-9 * * *", datetime(2026, 8, 7, 9, 45))
    assert cron_match("0 9 1 * *", datetime(2026, 9, 1, 9)) and not cron_match("0 9 1 * *", datetime(2026, 9, 2, 9))
    assert cron_match("0 0 * * 7", datetime(2026, 8, 9)) and cron_match("@weekly", datetime(2026, 8, 9))
    assert cron_match("0 6 1 * 1", datetime(2026, 8, 3, 6))
    with pytest.raises(ConfigError, match="5 fields"):
        cron_match("0 8 * *", friday_8)
    with pytest.raises(ConfigError, match="outside"):
        cron_match("0 25 * * *", friday_8)
