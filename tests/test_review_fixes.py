from datetime import date, datetime

import pytest

from conftest import insert_config, insert_rule, key, set_config
from sampling import engine
from sampling.db import generate_run_id, substitute_params
from sampling.engine import (Sample, _typed_keys, config_problems, finalize_run, load_config, run_sampling,
                             run_scope, sql_problems)
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
