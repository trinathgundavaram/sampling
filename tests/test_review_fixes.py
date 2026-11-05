from datetime import date, datetime

import pytest

from conftest import insert_config, insert_rule, key, set_config
from sampling import engine
from sampling.db import generate_run_id, substitute_params
from sampling.admin import purge_selections
from sampling.engine import (DIVERSITY, Caps, Sample, Stratifier, _typed_keys, apply_caps, config_problems,
                             finalize_run, load_config, order_candidates, replace_case, run_sampling, run_scope,
                             sql_problems)
from sampling.settings import ConfigError

AS_OF = date(2026, 8, 7)


@pytest.fixture(autouse=True)
def _data(raw):
    db = raw.db
    db.execute("CREATE TABLE u (case_id VARCHAR, n INTEGER, d DATE)")
    db.executemany("INSERT INTO u VALUES (?, ?, ?)", [(f"c{i:02d}", i, date(2026, 8, 1 + i % 5)) for i in range(30)])
    insert_config(db, src_table_nm="u", exclusion_sql=None, scope_sql_txt=None, target_cnt=5, order_by="case_id")


def _run(conn, settings, **kw):
    return run_sampling(conn, settings, key(1), as_of=AS_OF, **{"run_key": "K", **kw})


def test_a_value_outside_quotes_may_only_be_a_plain_word():
    assert substitute_params("n = {n}", {"n": "12"}) == "n = 12"
    assert substitute_params("d = DATE '{d}'", {"d": "x' OR '1'='1"}) == "d = DATE 'x'' OR ''1''=''1'"
    with pytest.raises(ValueError, match="outside quotes"):
        substitute_params("n = {n}", {"n": "1 OR 1=1"})
    with pytest.raises(ValueError, match="outside quotes"):
        substitute_params("n = $n", {"n": "1; DELETE FROM x"})
    assert substitute_params("a = '{a}'", {"a": "{b}"}) == "a = '{b}'"


def test_an_unquoted_parameter_cannot_change_the_sql(raw, conn, settings):
    set_config(raw.db, scope_sql_txt="SELECT * FROM main.u WHERE n < {top}")
    assert _run(conn, settings, run_params={"top": "20"})["status"] == "COMPLETED"
    out = _run(conn, settings, run_key="K2", run_params={"top": "20 OR 1=1"})
    assert out["error_type"] == "CONFIG_ERROR" and "outside quotes" in out["error"]


def test_built_in_parameters_cannot_be_passed_or_named(raw, conn, settings):
    out = _run(conn, settings, run_params={"Project_Cd": "OTHER"})
    assert out["error_type"] == "CONFIG_ERROR" and "set by the engine" in out["error"]
    insert_rule(raw.db, "PARAM", 50, "run_key", seq_nbr=1, rule_sql_txt="SELECT 'X' AS v")
    assert any("set by the engine" in p for p in config_problems(load_config(conn, settings, key(1))))
    out = _run(conn, settings, run_key="K3")
    assert out["error_type"] == "CONFIG_ERROR" and "set by the engine" in out["error"]


def test_one_sample_failing_unexpectedly_does_not_stop_the_others(raw, conn, settings, monkeypatch):
    insert_config(raw.db, config_id=2, src_table_nm="u", exclusion_sql=None, scope_sql_txt=None, target_cnt=5,
                  order_by="case_id")
    real = engine.load_config

    def broken(c, s, k):
        if tuple(k)[1] == "T1":
            raise RuntimeError("[Teradata Database] [Error 2631] Transaction ABORTed due to deadlock")
        return real(c, s, k)

    monkeypatch.setattr(engine, "load_config", broken)
    out = run_scope(conn, settings, project="UMCM", run_key="K", as_of=AS_OF)
    by_table = {r["table_nm"]: r for r in out["runs"]}
    assert by_table["T1"]["error_type"] == "START_FAILURE" and by_table["T1"]["error_kind"] == "technical"
    assert "Error 2631" in by_table["T1"]["error"] and by_table["T2"]["status"] == "COMPLETED"


def test_a_failure_writing_the_run_row_is_recorded_as_a_start_failure(conn, settings, monkeypatch):
    def broken(*a, **k):
        raise RuntimeError("[Teradata Database] [Error 2801] Duplicate unique prime key error")

    monkeypatch.setattr(engine, "_start_run", broken)
    out = _run(conn, settings)
    assert out["error_type"] == "START_FAILURE" and out["error_kind"] == "technical"


def test_a_sample_finalised_during_a_rerun_is_not_replaced(raw, conn, settings, monkeypatch):
    first = _run(conn, settings)
    real = engine._log_selection

    def finalise_meanwhile(*a, **k):
        finalize_run(conn, settings, key(1), "K", by="someone")
        return real(*a, **k)

    monkeypatch.setattr(engine, "_log_selection", finalise_meanwhile)
    out = _run(conn, settings)
    assert out["error_type"] == "FINALIZED" and "finalised while this run was selecting" in out["error"]
    current = raw.db.execute("SELECT Smpl_Run_ID, Run_Stat FROM GOVT_RGLTRY_SamplingRun WHERE Curr_Ind = 1").fetchall()
    assert current == [(first["smpl_run_id"], "FINAL")]


def test_finalise_refuses_a_run_a_rerun_replaced_meanwhile(raw, conn, settings, monkeypatch):
    first = _run(conn, settings)
    _run(conn, settings)
    monkeypatch.setattr(engine, "current_run", lambda *a: {"smpl_run_id": first["smpl_run_id"],
                                                           "run_stat": "COMPLETED"})
    with pytest.raises(ConfigError, match="no longer the current"):
        finalize_run(conn, settings, key(1), "K", by="someone")


def test_run_parameters_too_long_for_the_run_row_are_a_config_error(raw, conn, settings):
    out = _run(conn, settings, run_params={"big": "x" * 5000})
    assert out["error_type"] == "CONFIG_ERROR" and "Run_Parm_Txt" in out["error"]
    stored = raw.db.execute("SELECT Run_Stat, Run_Parm_Txt FROM GOVT_RGLTRY_SamplingRun").fetchall()
    assert stored == [("ERROR", None)]


def test_long_run_ids_and_run_keys_fit_their_columns(raw, conn, settings):
    run_id = generate_run_id("P" * 200, "K" * 200, "attempt-1", timestamp=datetime(2026, 8, 7))
    assert len(run_id) == 250 and run_id.split("::")[-2] == "20260807T000000.000000"
    out = _run(conn, settings, run_key="R" * 300, triggered_by="U" * 300)
    assert out["error_type"] == "CONFIG_ERROR" and "over the 250" in out["error"]
    row = raw.db.execute("SELECT LENGTH(Run_Key), LENGTH(Smpl_Run_ID), LENGTH(Triggered_By) "
                         "FROM GOVT_RGLTRY_SamplingRun").fetchall()
    assert row == [(250, 250, 100)]


def test_date_key_values_are_compared_as_dates(conn):
    sample = Sample(config={"key_columns_txt": "case_id, d"})
    queries = {"scope": "FROM (SELECT * FROM main.u) smp_scope WHERE (1=1)"}
    rows = _typed_keys(conn, queries, sample, [{"case_id": "c01", "d": "2026-08-02"}])
    assert rows == [{"case_id": "c01", "d": date(2026, 8, 2)}]


def test_stored_errors_do_not_name_cases(raw, conn, settings):
    insert_rule(raw.db, "LEVEL", 10, "n", seq_nbr=1, level_nbr=0,
                rule_sql_txt="SELECT u.case_id, u.n FROM main.u JOIN (SELECT 1 AS x UNION ALL SELECT 2) t ON 1=1")
    out = _run(conn, settings)
    assert "more than once" in out["error"] and "c0" not in out["error"]


@pytest.mark.parametrize("sql", ["DEL FROM t", "INS INTO t VALUES (1)", "UPD t SET a = 1",
                                 "LOCKING TABLE t FOR WRITE SELECT * FROM t", "LOCK ROW FOR EXCLUSIVE SELECT 1"])
def test_teradata_short_forms_and_write_locks_are_refused(sql):
    assert any("only reading SQL" in p for p in sql_problems(sql, "x"))


def test_access_locks_and_plain_selects_still_pass():
    assert sql_problems("LOCKING ROW FOR ACCESS SELECT del_ind, upd_dt FROM t", "x", select=True) == []


def test_a_failed_run_is_recorded_under_the_run_key_it_was_given(raw, conn, settings):
    insert_rule(raw.db, "PARAM", 50, "batch_id", seq_nbr=1, rule_sql_txt="SELECT n FROM main.u WHERE n > 99")
    out = _run(conn, settings, run_key="{run_ty}-{as_of_date}")
    assert out["error_type"] == "PARAM_FAILURE" and out["run_key"] == "WKLY-2026-08-07"
    out = _run(conn, settings, run_key="B-{batch_id}")
    assert out["run_key"] == "B-{batch_id}"


def test_replace_says_when_the_candidates_were_purged(raw, conn, settings):
    run = _run(conn, settings)
    picked = raw.db.execute("SELECT MIN(Case_Key) FROM GOVT_RGLTRY_SampleSelection WHERE Selected_Ind = 1").fetchone()[0]
    purge_selections(conn, settings, 30, project="UMCM", apply=True, as_of=date(2027, 1, 1))
    with pytest.raises(ConfigError, match="were purged"):
        replace_case(conn, settings, key(1), "K", picked, "no record", "A" * 300)
    assert run["status"] == "COMPLETED"


def test_replace_keeps_the_who_within_its_column(raw, conn, settings):
    _run(conn, settings)
    picked = raw.db.execute("SELECT MIN(Case_Key) FROM GOVT_RGLTRY_SampleSelection WHERE Selected_Ind = 1").fetchone()[0]
    replace_case(conn, settings, key(1), "K", picked, "no record", "A" * 300)
    assert raw.db.execute("SELECT MAX(LENGTH(Stat_By)) FROM GOVT_RGLTRY_SampleSelection").fetchone()[0] == 100


def test_a_cap_replacement_keeps_the_diversity_cap():
    sample = Sample(config={"diversity_sql_txt": "x", "diversity_cap_ty": "CNT", "diversity_cap_val": 1,
                            "smpl_method_cd": "RANKED"},
                    caps=[{"rule_id": 1, "rule_nm": "C", "rule_ty": "QUERY", "max_ty": "CNT", "max_val": 0}])
    rows = [{"_key": k, "_order": i, "_path": "ALL", DIVERSITY: g, "smp_r_1": cap}
            for i, (k, g, cap) in enumerate([("a", "G1", 1), ("b", "G2", 0), ("c", "G2", 0), ("d", "G3", 0)])]
    strat = Stratifier(sample, None, "FLOOR")
    chosen = strat.run(rows, 2)
    assert [r["_key"] for r in chosen] == ["a", "b"]
    kept = apply_caps(Caps(sample, 2, "FLOOR"), strat, rows, chosen)
    assert [r["_key"] for r in kept] == ["b", "d"]


def test_mixed_value_types_sort_instead_of_failing():
    sample = Sample(config={"key_columns_txt": "case_id"},
                    priorities=[{"rule_id": 9, "rule_ty": "EXPRESSION", "sort_dir_cd": "DESC"}])
    rows = [{"case_id": c, "smp_r_9": v} for c, v in (("a", 3), ("b", "x"), ("c", None), ("d", date(2026, 1, 1)),
                                                        ("e", datetime(2026, 1, 2)), ("f", 1.5))]
    ordered = [r["case_id"] for r in order_candidates(rows, sample, {})]
    assert ordered == ["b", "e", "d", "a", "f", "c"]


def test_a_dollar_inside_a_name_is_not_a_parameter():
    assert substitute_params("SELECT * FROM DB.TAB$X WHERE a = '$v'", {"v": "1"}) == "SELECT * FROM DB.TAB$X WHERE a = '1'"


def test_key_columns_must_be_plain_names(raw, conn, settings):
    set_config(raw.db, key_columns_txt="case_id, n; DROP TABLE u")
    assert any("must be plain column names" in p for p in config_problems(load_config(conn, settings, key(1))))


def test_water_filling_reallocates_only_buckets_whose_share_changed(raw, conn, settings, monkeypatch):
    from conftest import insert_bucket
    raw.db.execute("CREATE TABLE w (case_id VARCHAR, g VARCHAR)")
    raw.db.executemany("INSERT INTO w VALUES (?, ?)", [(f"{g}{i:02d}", g) for g, n in (("A", 2), ("B", 30), ("C", 30))
                                                       for i in range(n)])
    insert_config(raw.db, config_id=2, src_table_nm="w", exclusion_sql=None, scope_sql_txt=None, target_cnt=30,
                  order_by="case_id")
    insert_rule(raw.db, "LEVEL", 2000, "g", config_id=2, seq_nbr=1, level_nbr=0, rule_sql_txt="g")
    for i, g in enumerate("ABC"):
        insert_bucket(raw.db, 2001 + i, 0, g, config_id=2, seq_nbr=i + 1, quota_cd="ALL")
    calls = []
    real = Stratifier._allocate

    def counted(self, rows, target, index, path):
        calls.append(path)
        return real(self, rows, target, index, path)

    monkeypatch.setattr(Stratifier, "_allocate", counted)
    out = run_sampling(conn, settings, key(2), run_key="K", as_of=AS_OF)
    assert out["selected"] == 30 and out["by_stratum"]["A"]["selected"] == 2
    assert calls.count("A") == 1 and calls.count("B") == 2
