import logging
from datetime import date

from conftest import ROOT, build_source, insert_config, insert_rule, key, um_config
from sampling.db import error_text
from sampling.engine import run_sampling

PARAMS = {"batch_id": "2026-08-01"}


def _run(conn, s):
    return run_sampling(conn, s, key(1), run_params=PARAMS, as_of=date(2026, 8, 1))


def _messages(caplog):
    return [r.getMessage() for r in caplog.records if r.name.startswith("sampling")]


def test_every_stage_is_logged_as_step_sample_what(raw, conn, settings, caplog):
    build_source(raw.db)
    um_config(raw.db)
    with caplog.at_level(logging.INFO, logger="sampling"):
        out = _run(conn, settings)
    assert out["status"] == "COMPLETED"
    lines = _messages(caplog)
    for start in ("SAMPLING | UMCM.T1.WKLY | starting | run_key 2026-08-01",
                  "SAMPLING | UMCM.T1.WKLY | pull\n    SQL:", "SAMPLING | UMCM.T1.WKLY | 1000 row(s) in scope",
                  "SAMPLING | UMCM.T1.WKLY | EXCLUSION rule EXCLUDED returned",
                  "SAMPLING | UMCM.T1.WKLY | excluded 20 (EXCLUDED 20) | 980 candidate(s) | target 150",
                  "SAMPLING | UMCM.T1.WKLY | bucket Denied/Area A | selected",
                  "SAMPLING | UMCM.T1.WKLY | complete | run "):
        assert any(line.startswith(start) for line in lines), (start, lines)
    pull = next(line for line in lines if line.startswith("SAMPLING | UMCM.T1.WKLY | pull"))
    assert "parameters:  batch_id = '2026-08-01'" in pull and "with values:" in pull


def test_config_problems_are_one_line_in_the_table_and_all_in_the_log(raw, conn, settings, caplog):
    build_source(raw.db)
    insert_config(raw.db, smpl_method_cd="BOGUS", order_by=None)
    insert_rule(raw.db, "CAP", 60, "no_limit", rule_sql_txt="SELECT case_id FROM main.case_source", raw_sql=True)
    with caplog.at_level(logging.INFO, logger="sampling"):
        out = _run(conn, settings)
    problems = out["error"].split("; ")
    assert out["error_type"] == "CONFIG_ERROR" and len(problems) >= 2
    stored = raw.db.execute("SELECT Error_Txt FROM GOVT_RGLTRY_SamplingRun").fetchone()[0]
    assert stored == f"{problems[0]} (+{len(problems) - 1} more: see the task log)"
    logged = next(m for m in _messages(caplog) if "CONFIG_ERROR" in m)
    assert all(p in logged for p in problems)


def test_an_unexpected_failure_logs_its_traceback_but_stores_one_line(raw, conn, settings, caplog):
    build_source(raw.db)
    um_config(raw.db)
    raw.fail_on = "UPDATE main.GOVT_RGLTRY_SampleSelection"
    with caplog.at_level(logging.INFO, logger="sampling"):
        out = _run(conn, settings)
    assert out["error_type"] == "PERSIST_FAILURE"
    record = next(r for r in caplog.records if "PERSIST_FAILURE" in r.getMessage())
    assert record.levelno == logging.ERROR and record.exc_info is not None
    stored = raw.db.execute("SELECT Error_Txt FROM GOVT_RGLTRY_SamplingRun").fetchone()[0]
    assert "\n" not in stored and len(stored) <= 1000


def test_error_text_keeps_the_database_message():
    driver = RuntimeError("[Version 20.0.0.1] [Session 123] [Teradata SQL Driver] Failure\n"
                          "[Teradata Database] [Error 3807] Object 'X' does not exist.")
    assert error_text(driver) == "RuntimeError: [Teradata Database] [Error 3807] Object 'X' does not exist."
    assert error_text(ValueError("x" * 3000)) == ("ValueError: " + "x" * 3000)[:1000]


def test_the_run_table_has_no_traceback_or_report_column():
    schema = (ROOT / "sql" / "schema.sql").read_text()
    assert "Error_Detail_Txt" not in schema and "Run_Rpt_Txt" not in schema
