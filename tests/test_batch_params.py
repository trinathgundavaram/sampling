import json
from datetime import date

from conftest import insert_config, insert_rule, key
from sampling.engine import run_sampling

AS_OF = date(2026, 8, 7)
PARAM_SQL = "SELECT batch, run_ty AS batch_run_ty FROM ctl ORDER BY batch DESC LIMIT 1"


def _source(db):
    db.execute("CREATE TABLE u (case_id VARCHAR, batch VARCHAR, run_ty VARCHAR)")
    rows = [(f"a{i:02d}", "B1", "WKLY") for i in range(10)] + [(f"b{i:02d}", "B2", "WKLY") for i in range(6)]
    rows += [(f"c{i:02d}", "B2", "MNT") for i in range(3)]
    db.executemany("INSERT INTO u VALUES (?, ?, ?)", rows)


def _control(db, rows):
    db.execute("CREATE TABLE ctl (batch VARCHAR, run_ty VARCHAR)")
    db.executemany("INSERT INTO ctl VALUES (?, ?)", rows)


def _config(db):
    insert_config(db, src_table_nm="u", exclusion_sql=None, target_cnt=4, order_by="case_id",
                  scope_sql_txt="batch = '{batch_id}' AND run_ty = '{batch_run_ty}'")
    insert_rule(db, "PARAM", 1, "batch_id", seq_nbr=1, rule_sql_txt=PARAM_SQL)


def _run(conn, settings, **kw):
    return run_sampling(conn, settings, key(1), as_of=AS_OF, run_key="{batch_id}", **kw)


def test_the_batch_and_its_run_type_come_from_one_param_rule(raw, conn, settings):
    _source(raw.db)
    _control(raw.db, [("B1", "WKLY"), ("B2", "WKLY")])
    _config(raw.db)
    out = _run(conn, settings)
    assert out["status"] == "COMPLETED", out.get("error")
    assert out["run_key"] == "B2" and out["in_scope"] == 6
    stored = json.loads(raw.db.execute("SELECT Run_Parm_Txt FROM GOVT_RGLTRY_SamplingRun").fetchone()[0])
    assert stored["batch_id"] == "B2" and stored["batch_run_ty"] == "WKLY"


def test_a_passed_batch_and_run_type_are_used_without_reading_the_control_table(raw, conn, settings):
    _source(raw.db)
    _config(raw.db)
    out = _run(conn, settings, run_params={"batch_id": "B2", "batch_run_ty": "MNT"})
    assert out["status"] == "COMPLETED", out.get("error")
    assert out["in_scope"] == 3


def test_a_passed_batch_needs_its_run_type_too(raw, conn, settings):
    _source(raw.db)
    _config(raw.db)
    out = _run(conn, settings, run_params={"batch_id": "B2"})
    assert out["status"] == "ERROR" and "batch_run_ty" in out["error"]


def test_a_batch_the_source_does_not_have_fails_the_run(raw, conn, settings):
    _source(raw.db)
    _control(raw.db, [("B3", "WKLY")])
    _config(raw.db)
    out = _run(conn, settings)
    assert out["error_kind"] == "config" and out["error_type"] == "EMPTY_SCOPE"
    assert "batch_id=B3" in out["error"] and "batch_run_ty=WKLY" in out["error"]
    assert raw.db.execute("SELECT Run_Stat, Error_Ty FROM GOVT_RGLTRY_SamplingRun").fetchall() == [
        ("ERROR", "EMPTY_SCOPE")]


def test_a_passed_run_type_the_source_does_not_have_fails_the_run(raw, conn, settings):
    _source(raw.db)
    _config(raw.db)
    out = _run(conn, settings, run_params={"batch_id": "B1", "batch_run_ty": "MNT"})
    assert out["error_type"] == "EMPTY_SCOPE" and "batch_run_ty=MNT" in out["error"]


def test_a_param_column_cannot_take_an_engine_name(raw, conn, settings):
    _source(raw.db)
    _control(raw.db, [("B1", "WKLY")])
    insert_config(raw.db, src_table_nm="u", exclusion_sql=None, target_cnt=4, order_by="case_id",
                  scope_sql_txt="batch = '{batch_id}'")
    insert_rule(raw.db, "PARAM", 1, "batch_id", seq_nbr=1, rule_sql_txt="SELECT batch, run_ty FROM ctl")
    out = _run(conn, settings)
    assert out["status"] == "ERROR" and "column run_ty is set by the engine" in out["error"]


def test_the_run_stores_the_sql_it_executed(raw, conn, settings):
    _source(raw.db)
    _control(raw.db, [("B2", "WKLY")])
    _config(raw.db)
    insert_rule(raw.db, "EXCLUSION", 2, "skip_a", seq_nbr=1, rule_ty="QUERY",
                rule_sql_txt="SELECT case_id FROM main.u WHERE batch = '{batch_id}' AND run_ty = '{batch_run_ty}' "
                             "AND case_id = 'b00'", raw_sql=True)
    out = _run(conn, settings)
    assert out["status"] == "COMPLETED", out.get("error")
    scope, configured = raw.db.execute(
        "SELECT Exec_Scope_Sql_Txt, Scope_Sql_Txt FROM GOVT_RGLTRY_SamplingRun").fetchone()
    assert "batch = 'B2' AND run_ty = 'WKLY'" in scope and "{" not in scope
    assert configured.endswith("batch = '{batch_id}' AND run_ty = '{batch_run_ty}'")
    rules = dict(raw.db.execute("SELECT Rule_ID, Exec_Rule_Sql_Txt FROM GOVT_RGLTRY_SamplingRunRule").fetchall())
    assert rules[1] == PARAM_SQL
    assert rules[2] == "SELECT case_id FROM main.u WHERE batch = 'B2' AND run_ty = 'WKLY' AND case_id = 'b00'"


def test_the_config_run_key_ties_the_run_to_its_batch(raw, conn, settings):
    _source(raw.db)
    _control(raw.db, [("B1", "WKLY"), ("B2", "WKLY")])
    _config(raw.db)
    raw.db.execute("UPDATE GOVT_RGLTRY_SamplingConfig SET Run_Key_Txt = '{batch_id}'")
    out = run_sampling(conn, settings, key(1), as_of=AS_OF)
    assert out["status"] == "COMPLETED", out.get("error")
    assert raw.db.execute("SELECT Run_Key, Run_Key_Txt FROM GOVT_RGLTRY_SamplingRun").fetchall() == [
        ("B2", "{batch_id}")]
    again = run_sampling(conn, settings, key(1), as_of=date(2026, 8, 14))
    assert again["error_type"] == "RERUN_REASON" and "run_key B2 already has a completed sample" in again["error"]


def test_a_passed_run_key_cannot_replace_the_config_one(raw, conn, settings):
    _source(raw.db)
    _control(raw.db, [("B2", "WKLY")])
    _config(raw.db)
    raw.db.execute("UPDATE GOVT_RGLTRY_SamplingConfig SET Run_Key_Txt = '{batch_id}'")
    out = run_sampling(conn, settings, key(1), as_of=AS_OF, run_key="2026-08-07")
    assert out["error_type"] == "CONFIG_ERROR" and "Run_Key_Txt '{batch_id}'" in out["error"]
    assert _run(conn, settings)["status"] == "COMPLETED"
