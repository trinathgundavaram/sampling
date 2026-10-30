"""The sampling engine (sampling/engine.py) end to end on the DuckDB stand-in."""
import json
import random
from datetime import date

import pytest

from conftest import build_universe, insert_config, insert_strata, um_config
from sampling.db import build_run_key, generate_run_id, substitute_params
from sampling.engine import (Sample, Stratifier, build_queries, discover_configs, round_target, run_sampling,
                             run_scope, select, validate)
from sampling.settings import ConfigError, Settings

AS_OF = date(2026, 8, 1)
PARAMS = {"batch_id": "2026-08-01"}


def _run(conn, s, config_id=1, **kw):
    kw.setdefault("run_params", PARAMS)
    kw.setdefault("as_of", AS_OF)
    return run_sampling(conn, s, config_id, **kw)


def _rows(raw, sql):
    return raw.db.execute(sql).fetchall()


# ── quotas and selection ────────────────────────────────────────────────────

def test_round_target_rounding_and_float_artefacts():
    assert [round_target(3.5, m) for m in ("FLOOR", "CEIL", "ROUND")] == [3, 4, 4]
    assert round_target(100 * 0.29, "FLOOR") == 29   # 28.999999999999996
    assert round_target(100 * 0.07, "CEIL") == 7     # 7.000000000000001


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
    levels = [{"rule_id": i, "level_order": i, "buckets": [], "value_level": True} for i in (1, 2, 3)]
    strat = Stratifier(Sample(config={"sampling_method": "RANKED"}, levels=levels), None, "FLOOR")
    chosen = strat.run(rows, 40)
    assert len(chosen) == 40 and len(strat.leaves) == 8 and all(v["selected"] == 5 for v in strat.leaves.values())


# ── the frozen UM regression of the DQ repo ─────────────────────────────────

FROZEN_BY_STRATUM = {
    "Approved/Area A": (6, 1), "Approved/Area B": (7, 0), "Approved/Area C": (59, 9),
    "Denied/Area A": (96, 15), "Denied/Area B": (71, 9), "Denied/Area C": (610, 94),
    "Dismissed/Area A": (3, 0), "Dismissed/Area B": (4, 0), "Dismissed/Area C": (14, 2),
    "Withdrawn/Area A": (7, 1), "Withdrawn/Area B": (9, 1), "Withdrawn/Area C": (94, 11),
}


def test_um_regression_matches_frozen_output(raw, conn, settings):
    build_universe(raw.db)
    um_config(raw.db)
    out = _run(conn, settings)
    assert out["status"] == "COMPLETED", out
    assert out["in_scope"] == 1000 and out["excluded"] == 20 and out["candidates"] == 980
    assert {k: (v["candidates"], v["selected"]) for k, v in out["by_stratum"].items()} == FROZEN_BY_STRATUM
    assert out["selected"] == 150 and out["shortfall"] == 0


def test_run_writes_every_candidate_and_the_run_row(raw, conn, settings):
    build_universe(raw.db)
    um_config(raw.db)
    out = _run(conn, settings, triggered_by="tester")
    sel = _rows(raw, "SELECT COUNT(*), SUM(selected_flag), MIN(run_key), MIN(sample_cycle) FROM gre_sample_selections")
    assert sel[0] == (980, 150, "2026-08-01", AS_OF)
    run = _rows(raw, "SELECT status, total_in_scope, total_excluded, total_candidates, total_selected, shortfall, "
                     "triggered_by, run_id, etl_is_curr_ind, error_type FROM gre_sampling_run")
    assert run == [("COMPLETED", 1000, 20, 980, 150, 0, "tester", out["sample_run_id"], "Y", None)]
    report = json.loads(_rows(raw, "SELECT run_report FROM gre_sampling_run")[0][0])
    assert report["buckets"]["Denied"] == {"candidates": 777, "target": 120, "selected": 118}
    assert {k: (v["candidates"], v["selected"]) for k, v in report["buckets"].items() if "/" in k} == FROZEN_BY_STRATUM
    assert out["sample_run_id"].startswith("UMCM.weekly_sample::2026-08-01::attempt-1::tester::")
    paths = _rows(raw, "SELECT DISTINCT stratum_path FROM gre_sample_selections WHERE selected_flag = 1")
    assert {p[0] for p in paths} <= set(FROZEN_BY_STRATUM)


def test_rerun_retires_the_earlier_run_of_the_same_run_key(raw, conn, settings):
    build_universe(raw.db)
    um_config(raw.db)
    first = _run(conn, settings)
    second = _run(conn, settings)
    assert "::attempt-2::" in second["sample_run_id"] and second["retired_runs"] == 1
    curr = _rows(raw, "SELECT sample_run_id, etl_is_curr_ind, COUNT(*) FROM gre_sample_selections GROUP BY 1, 2")
    assert {(r[0], r[1]) for r in curr} == {(first["sample_run_id"], "N"), (second["sample_run_id"], "Y")}
    assert _rows(raw, "SELECT run_id, etl_is_curr_ind FROM gre_sampling_run ORDER BY started_at") == \
        [(first["sample_run_id"], "N"), (second["sample_run_id"], "Y")]
    other = _run(conn, settings, run_key="OTHER")
    assert other["retired_runs"] == 0


def test_random_reproducible_with_stored_seed(raw, conn, settings):
    build_universe(raw.db)
    um_config(raw.db, sampling_method="RANDOM", priority_rank_sql=None)
    first = _run(conn, settings)
    assert first["seed"] is not None
    stored = _rows(raw, "SELECT random_seed FROM gre_sampling_run")[0][0]
    again = _run(conn, settings, seed=stored, run_key="replay")

    def picked(run_id):
        return {r[0] for r in _rows(raw, f"SELECT case_key FROM gre_sample_selections WHERE selected_flag = 1 AND "
                                         f"sample_run_id = '{run_id}'")}
    assert picked(first["sample_run_id"]) == picked(again["sample_run_id"])


def test_systematic_needs_priority_and_records_config_error(raw, conn, settings):
    build_universe(raw.db)
    um_config(raw.db, sampling_method="SYSTEMATIC", priority_rank_sql=None)
    out = _run(conn, settings)
    assert out["status"] == "ERROR" and out["error_kind"] == "config" and "priority_rank_sql" in out["error"]
    assert _rows(raw, "SELECT status, error_type, etl_is_curr_ind FROM gre_sampling_run") == \
        [("ERROR", "CONFIG_ERROR", "N")]


def test_null_exclusion_column_keeps_the_row(raw, conn, settings):
    raw.db.execute("CREATE TABLE u (case_id VARCHAR, flag VARCHAR)")
    raw.db.execute("INSERT INTO u VALUES ('a', 'Y'), ('b', NULL), ('c', 'N')")
    insert_config(raw.db, universe_table="u", exclusion_sql="flag = 'Y'", scope_sql=None, priority_rank_sql="case_id",
                  target_volume=10)
    out = _run(conn, settings)
    assert out["candidates"] == 2 and out["excluded"] == 1 and out["shortfall"] == 8


def test_bucket_values_match_without_case_or_trailing_blanks(raw, conn, settings):
    raw.db.execute("CREATE TABLE u (case_id VARCHAR, cat VARCHAR)")
    raw.db.executemany("INSERT INTO u VALUES (?, ?)", [(f"d{i}", "DENIED  ") for i in range(50)]
                       + [(f"a{i}", "approved") for i in range(50)])
    insert_config(raw.db, universe_table="u", exclusion_sql=None, scope_sql=None, priority_rank_sql="case_id",
                  target_volume=10)
    insert_strata(raw.db, 1, 1, 0, "cat", "cat", {"Denied": 0.8, "Approved": 0.2})
    out = _run(conn, settings)
    assert {k: v["selected"] for k, v in out["by_stratum"].items()} == {"Approved": 2, "Denied": 8}


def test_pull_failure_rolls_nothing_in_and_logs(raw, conn, settings):
    build_universe(raw.db)
    um_config(raw.db)
    raw.fail_on = "FROM case_universe WHERE (pull_date = '2026-08-01') AND"
    out = _run(conn, settings)
    assert out["status"] == "ERROR" and out["error_type"] == "PULL_FAILURE" and out["error_kind"] == "technical"
    assert _rows(raw, "SELECT COUNT(*) FROM gre_sample_selections")[0][0] == 0


def test_persist_failure_leaves_no_partial_rows(raw, conn, settings):
    build_universe(raw.db)
    um_config(raw.db)
    raw.fail_on = "UPDATE main.gre_sample_selections"
    out = _run(conn, settings)
    assert out["status"] == "ERROR" and out["error_type"] == "PERSIST_FAILURE"
    assert _rows(raw, "SELECT COUNT(*) FROM gre_sample_selections")[0][0] == 0   # rolled back
    assert _rows(raw, "SELECT status, error_type FROM gre_sampling_run") == [("ERROR", "PERSIST_FAILURE")]
    assert "Traceback" in _rows(raw, "SELECT error_detail FROM gre_sampling_run")[0][0]


def test_a_later_error_retires_the_earlier_one(raw, conn, settings):
    build_universe(raw.db)
    um_config(raw.db, sampling_method="SYSTEMATIC", priority_rank_sql=None)
    _run(conn, settings)
    _run(conn, settings)
    assert _rows(raw, "SELECT COUNT(*), MIN(status), MAX(status) FROM gre_sampling_run")[0] == (2, "ERROR", "ERROR")


def test_unresolved_token_is_a_config_error(raw, conn, settings):
    build_universe(raw.db)
    um_config(raw.db)
    out = _run(conn, settings, run_params={})
    assert out["error_kind"] == "config" and "batch_id" in out["error"]


def test_builtin_as_of_date_and_run_key_tokens(raw, conn, settings):
    build_universe(raw.db)
    um_config(raw.db, scope_sql="pull_date = CAST('{as_of_date}' AS DATE) AND '{run_key}' = '{run_key}'")
    out = _run(conn, settings, run_params={})
    assert out["status"] == "COMPLETED" and out["candidates"] == 980 and out["run_key"] == "2026-08-01"


def test_duplicate_keys_are_a_config_error(raw, conn, settings):
    build_universe(raw.db)
    um_config(raw.db, key_columns="category")
    out = _run(conn, settings)
    assert out["error_kind"] == "config" and "uniquely" in out["error"]


def test_dry_run_writes_nothing(raw, conn, settings):
    build_universe(raw.db)
    um_config(raw.db)
    out = _run(conn, settings, dry_run=True)
    assert out["selected"] == 150 and out["sample_run_id"] is None
    for table in ("gre_sampling_run", "gre_sample_selections"):
        assert _rows(raw, f"SELECT COUNT(*) FROM {table}")[0][0] == 0


def test_run_scope_by_process_and_errors(raw, conn, settings):
    build_universe(raw.db)
    um_config(raw.db, config_id=1)
    insert_config(raw.db, config_id=2, sampling_method="BOGUS")
    insert_config(raw.db, config_id=3, process_name="OTHER")
    out = run_scope(conn, settings, process_name="WEEKLY", run_params=PARAMS, as_of=AS_OF)
    assert [r["config_id"] for r in out["runs"]] == [1, 2] and out["failed"] == 1
    assert out["result"] == "1 OF 2 SAMPLE(S) FAILED"
    with pytest.raises(ConfigError):
        run_scope(conn, settings, process_name="NOPE")
    with pytest.raises(ConfigError):
        discover_configs(conn, settings)


def test_validate_with_sql_check(raw, conn, settings):
    build_universe(raw.db)
    um_config(raw.db)
    insert_config(raw.db, config_id=2, universe_table="missing_table", process_name="WEEKLY")
    out = validate(conn, settings, process_name="WEEKLY", run_params=PARAMS, check_sql=True)
    problems = {c["config_id"]: c["problems"] for c in out["configs"]}
    assert problems[1] == [] and "SQL check failed" in problems[2][0]


def test_queries_resolve_env_and_tie_break_on_keys():
    s = Settings(meta_db="M", environment="PROD")
    sample = Sample(config={"universe_table": "UMCM_$ENV_T.u", "key_columns": "a, b", "scope_sql": "db = '$env'",
                            "exclusion_sql": "x = 1", "sampling_method": "RANKED", "priority_rank_sql": "x DESC"})
    q = build_queries(sample, s, {})
    assert "FROM UMCM_T.u" in q["pull"] and "db = ''" in q["count"]
    assert "ROW_NUMBER() OVER (ORDER BY x DESC, a, b) AS smp_base_rank" in q["pull"]
    assert "CASE WHEN (x = 1) THEN 1 ELSE 0 END = 0" in q["pull"]
    assert Settings(meta_db="M", environment="DEV").resolve_env("CMS_$ENV_STG") == "CMS_DEV_STG"


def test_params_and_ids():
    assert substitute_params("a = '{x}' AND b = '$Y'", {"x": "it's", "y": 2}) == "a = 'it''s' AND b = '2'"
    with pytest.raises(ValueError):
        substitute_params("a = '{missing}'", {})
    assert build_run_key(2026, 8) == "2026_8"
    assert generate_run_id("P.S", "rk", None, "") .startswith("P.S::rk::")


def test_settings_validation():
    with pytest.raises(ConfigError):
        Settings.from_mapping({})
    with pytest.raises(ConfigError):
        Settings(meta_db="bad name")
    s = Settings.from_mapping({"META_DB": "HSABC_DEV", "load_env": "qa", "access_lock": "false"})
    assert (s.environment, s.access_lock) == ("QA", False)
    assert [Settings.from_mapping({"meta_db": "HSABC_$ENV", "environment": e}).meta_db
            for e in ("DEV", "QA", "UAT", "PROD")] == ["HSABC_DEV", "HSABC_QA", "HSABC", "HSABC"]
