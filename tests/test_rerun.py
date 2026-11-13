import pytest

from conftest import build_source, key, um_config
from sampling.db import Connection
from sampling.engine import finalize_run, run_sampling
from datetime import date

AS_OF = date(2026, 8, 1)
PARAMS = {"batch_id": "2026-08-01"}


@pytest.fixture
def ready(raw):
    build_source(raw.db)
    um_config(raw.db)
    return raw


def _run(conn, s, **kw):
    return run_sampling(conn, s, key(1), run_key="K", run_params=PARAMS, as_of=AS_OF, **kw)


def _runs(raw):
    return raw.db.execute("SELECT Run_Stat, Curr_Ind, Rerun_Rsn_Txt, Error_Ty FROM GOVT_RGLTRY_SamplingRun "
                          "ORDER BY Start_Dtts").fetchall()


def test_a_second_run_of_a_run_key_needs_a_reason(ready, conn, settings):
    first = _run(conn, settings)
    again = _run(conn, settings)
    assert again["status"] == "ERROR" and again["error_type"] == "RERUN_REASON"
    assert "already has a completed sample" in again["error"] and "set rerun_reason" in again["error"]
    redo = _run(conn, settings, rerun_reason="  source rows corrected  ")
    assert redo["status"] == "COMPLETED" and redo["rerun_reason"] == "source rows corrected"
    assert _runs(ready) == [("COMPLETED", 0, None, None), ("ERROR", 0, None, "RERUN_REASON"),
                            ("COMPLETED", 1, "source rows corrected", None)]
    assert first["smpl_run_id"] != redo["smpl_run_id"]


@pytest.mark.parametrize("blank", ["", "   ", "\t"])
def test_a_blank_reason_is_refused(ready, conn, settings, blank):
    _run(conn, settings)
    out = _run(conn, settings, rerun_reason=blank)
    assert out["error_type"] == "RERUN_REASON" and "blank" in out["error"]
    assert run_sampling(conn, settings, key(1), run_key="NEW", run_params=PARAMS, as_of=AS_OF,
                        rerun_reason=blank)["error_type"] == "RERUN_REASON"


def test_a_reason_left_for_a_regular_run_is_refused(ready, conn, settings):
    out = _run(conn, settings, rerun_reason="left over from last week")
    assert out["status"] == "ERROR" and out["error_type"] == "RERUN_REASON" and "has no sample yet" in out["error"]
    assert _run(conn, settings)["status"] == "COMPLETED"


def test_a_failed_run_does_not_make_the_next_one_a_rerun(ready, conn, settings):
    assert _run(conn, settings, rerun_reason="x")["status"] == "ERROR"
    assert _run(conn, settings)["status"] == "COMPLETED"


def test_a_dry_run_follows_the_same_rule(ready, conn, settings):
    _run(conn, settings)
    assert _run(conn, settings, dry_run=True)["error_type"] == "RERUN_REASON"
    assert _run(conn, settings, dry_run=True, rerun_reason="check")["status"] == "COMPLETED"


def test_a_final_sample_still_needs_the_force_reason(ready, conn, settings, raw):
    _run(conn, settings)
    finalize_run(Connection(raw, access_lock=False), settings, key(1), "K", "auditor")
    assert _run(conn, settings, rerun_reason="redo")["error_type"] == "FINALIZED"
    assert _run(conn, settings, force_reason="approved by audit")["status"] == "COMPLETED"


def test_a_retry_keeps_the_samples_its_airflow_run_already_made(ready, conn, settings, raw):
    from conftest import insert_config
    from sampling.engine import run_scope
    insert_config(raw.db, 2, src_table_nm="later_src", exclusion_sql=None, order_by="case_id")
    kw = dict(run_key="K", run_params=PARAMS, as_of=AS_OF)
    since = settings.now()
    first = run_scope(conn, settings, project="UMCM", **kw)
    assert [(r["table_nm"], r["status"]) for r in first["runs"]] == [("T1", "COMPLETED"), ("T2", "ERROR")]
    build_source(raw.db, table="later_src")
    retry = run_scope(conn, settings, project="UMCM", resume_since=since, **kw)
    assert [(r["table_nm"], r["status"]) for r in retry["runs"]] == [("T1", "COMPLETED"), ("T2", "COMPLETED")]
    kept = retry["runs"][0]
    assert kept["resumed"] and kept["smpl_run_id"] == first["runs"][0]["smpl_run_id"]
    assert kept["selected"] == first["runs"][0]["selected"]
    assert ready.db.execute("SELECT COUNT(*) FROM GOVT_RGLTRY_SamplingRun WHERE Table_Nm = 'T1'").fetchone() == (1,)


def test_a_sample_from_before_the_airflow_run_still_needs_a_reason(ready, conn, settings):
    _run(conn, settings)
    later = settings.now()
    out = _run(conn, settings, resume_since=later)
    assert out["error_type"] == "RERUN_REASON"
    redo = _run(conn, settings, resume_since=later, rerun_reason="source corrected")
    assert redo["status"] == "COMPLETED" and not redo.get("resumed")
