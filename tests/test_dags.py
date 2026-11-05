import importlib.util
import json
from datetime import date

import pytest

pytest.importorskip("airflow.sdk")

from conftest import ROOT, build_source, insert_config, key, um_config  # noqa: E402
from sampling import dags  # noqa: E402
from sampling.settings import ConfigError  # noqa: E402

VAR_KEY = "govt_rgltry_umcm_sampling_config_var"
BASE = {"td_conn_var": "DEV", "meta_db": "main", "load_env": "DEV", "project": "UMCM", "run_type": "WKLY",
        "run_params": {"batch_id": "2026-08-01"}, "as_of": "2026-08-01", "access_lock": False}


@pytest.fixture
def variable(monkeypatch):
    def set_(value):
        monkeypatch.setenv(f"AIRFLOW_VAR_{VAR_KEY.upper()}", json.dumps(value))
    set_(BASE)
    monkeypatch.setenv("AIRFLOW_CONN_DEV", json.dumps({"conn_type": "teradata", "host": "td.example", "login": "svc",
                                                       "password": "secret", "extra": {"logmech": "TD2"}}))
    return set_


def _load_dag_file():
    spec = importlib.util.spec_from_file_location("umcm_dag", ROOT / "airflow" / "govt_rgltry_umcm_sampling.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.dag


def test_dag_parses_with_the_example_variable(variable):
    example = json.loads((ROOT / "airflow_resources" / "examples" / "umcm_dev" / f"{VAR_KEY}.json").read_text())
    variable({**example, "schedule_interval": "0 8 * * 5", "trigger_dags": ["NEXT_DAG"]})
    dag = _load_dag_file()
    assert dag.dag_id == "GOVT_RGLTRY_UMCM_SAMPLING" and dag.max_active_runs == 1
    assert set(dag.task_dict) == {"run_sampling", "trigger_next_dag", "success_email", "fail_email"}
    assert set(dag.params) >= {"project", "table", "run_type", "run_key", "run_params", "seed", "as_of"}
    assert "run" not in dag.params and type(dag.timetable).__name__ == "CronTriggerTimetable"


def test_dag_parses_without_a_variable(monkeypatch):
    monkeypatch.delenv(f"AIRFLOW_VAR_{VAR_KEY.upper()}", raising=False)
    dag = _load_dag_file()
    assert list(dag.task_dict) == ["run_sampling"]


def test_runs_entries_become_tasks(variable):
    variable({**BASE, "runs": [{"name": "weekly", "run_type": "WKLY"}, {"table": "ODAG1", "run_type": "MNT"}]})
    dag = _load_dag_file()
    assert sorted(dag.task_dict) == ["run_sampling_odag1_mnt", "run_sampling_weekly_wkly"]


TIMED = {**BASE, "runs": [{"name": "daily", "run_type": "DLY", "schedule_interval": "0 6 * * *"},
                          {"name": "weekly", "run_type": "WKLY", "schedule_interval": "0 8 * * 5"},
                          {"name": "monthly", "run_type": "MNT", "schedule_interval": "0 9 1 * *"}]}


class _DagRun:
    def __init__(self, run_type="scheduled", when=None, conf=None):
        import pendulum
        self.run_type, self.conf = run_type, conf or {}
        self.run_after = self.logical_date = when or pendulum.datetime(2026, 8, 7, 8, tz="America/Chicago")


def test_timed_runs_one_task_scheduled_at_every_time(variable):
    variable(TIMED)
    dag = _load_dag_file()
    assert list(dag.task_dict) == ["run_sampling"] and "run" in dag.params
    assert type(dag.timetable).__name__ == "MultipleCronTriggerTimetable"
    import pendulum
    friday_8 = pendulum.datetime(2026, 8, 7, 8, tz="America/Chicago")
    assert dags.chosen_runs(TIMED, _DagRun(when=friday_8)) == [1]
    first_6 = pendulum.datetime(2026, 9, 1, 6, tz="America/Chicago")
    assert dags.chosen_runs(TIMED, _DagRun(when=first_6)) == [0]
    assert dags.chosen_runs(TIMED, _DagRun("manual", conf={"run": "Monthly, daily"})) == [2, 0]
    assert dags.chosen_runs(TIMED, _DagRun("manual")) == [None]
    with pytest.raises(ConfigError, match="not a name in runs"):
        dags.chosen_runs(TIMED, _DagRun("manual", conf={"run": "yearly"}))


def test_a_timed_run_samples_the_entries_due(monkeypatch, variable, raw):
    from sampling.db import Connection
    build_source(raw.db)
    um_config(raw.db, run_ty="WKLY")
    insert_config(raw.db, config_id=2, run_ty="MNT", table_nm="T1", exclusion_sql=None)
    variable(TIMED)
    monkeypatch.setattr(dags, "connect_teradata", lambda db, lock: Connection(raw, host="duckdb", access_lock=False))
    monkeypatch.setattr(raw, "close", lambda: None)
    report = dags.run_timed_task(VAR_KEY, {}, _DagRun())
    assert [(r["table_nm"], r["run_ty"]) for r in report["runs"]] == [("T1", "WKLY")]
    report = dags.run_timed_task(VAR_KEY, {"run": "weekly,monthly"}, _DagRun("manual", conf={"run": "weekly,monthly"}))
    assert [(r["table_nm"], r["run_ty"], r["status"]) for r in report["runs"]] == \
        [("T1", "WKLY", "COMPLETED"), ("T1", "MNT", "COMPLETED")]
    with pytest.raises(dags.AirflowFailException, match="1 of 1 run"):
        dags.run_timed_task(VAR_KEY, {}, _DagRun("manual", conf={"run": "daily"}))


def test_trigger_keys_limit_what_an_upstream_dag_can_change(variable):
    variable({**BASE, "trigger_keys": ["run_key", "as_of"]})
    config = dags.resolve_config(VAR_KEY, {"project": "FRAMEWORK_PROJECT", "run_key": "B42", "as_of": "2026-08-08"})
    assert config["project"] == "UMCM" and config["run_key"] == "B42" and config["as_of"] == "2026-08-08"
    assert "trigger_keys" not in config
    variable({**BASE, "trigger_keys": "run_key, nope"})
    with pytest.raises(ConfigError):
        dags.resolve_config(VAR_KEY, {})


FRAMEWORK_CONF = {"project": "UMCM_FRAMEWORK", "table_group": "UM", "table": "ODAG1", "run_type": "WEEKLY",
                  "source": "", "as_of": "2026-08-08"}


def test_triggered_by_the_framework(variable):
    variable({**BASE, "trigger_keys": ["as_of"], "trigger_run_params": ["run_type", "table"]})
    config = dags.resolve_config(VAR_KEY, FRAMEWORK_CONF)
    assert config["project"] == "UMCM" and config["as_of"] == "2026-08-08"
    assert config["run_params"] == {"batch_id": "2026-08-01", "run_type": "WEEKLY", "table": "ODAG1"}
    assert "trigger_run_params" not in config


def test_sample_ready_email_and_its_summary(variable):
    variable({**BASE, "notify_recipients": "auditors@example.com", "retry_delay_minutes": 30})
    dag = _load_dag_file()
    ready = dag.task_dict["sample_ready_email"]
    assert ready.to == ["auditors@example.com"] and ready.upstream_task_ids == {"run_sampling"}
    assert dag.task_dict["run_sampling"].retry_delay.total_seconds() == 1800

    class TI:
        def xcom_pull(self, task_ids):
            return {"runs": [{"project_cd": "UMCM", "table_nm": "S<1>", "run_ty": "WKLY", "run_key": "B42", "status": "COMPLETED", "selected": 150,
                              "target_cnt": 150, "shortfall": 0, "tgt_table_nm": "DB.OUT",
                              "config_hash": "abcdef0123456789"}]}
    text = dags.sample_summary(TI(), ["run_sampling"])
    assert "UMCM.S&lt;1&gt;.WKLY" in text and "<td>B42</td>" in text and "abcdef012345<" in text and "DB.OUT" in text
    assert dags.sample_summary(type("E", (), {"xcom_pull": lambda self, task_ids: None})(), ["x"]) == \
        "<p>No sample was made.</p>"
    rendered = dag.get_template_env().from_string(ready.html_content).render(run_id="manual__1", ti=TI())
    assert rendered.startswith("<p>DAG GOVT_RGLTRY_UMCM_SAMPLING run manual__1:</p><table") and "B42" in rendered


def test_retention_task(monkeypatch, variable, raw):
    from sampling.db import Connection
    variable({**BASE, "retention_days": 400, "email_recipient": "support@example.com"})
    dag = _load_dag_file()
    purge = dag.task_dict["purge_selections"]
    assert purge.upstream_task_ids == {"run_sampling"} and "purge_selections" in dag.task_dict["fail_email"].upstream_task_ids
    build_source(raw.db)
    um_config(raw.db)
    monkeypatch.setattr(dags, "connect_teradata", lambda db, lock: Connection(raw, host="duckdb", access_lock=False))
    monkeypatch.setattr(raw, "close", lambda: None)
    out = dags.purge_task(VAR_KEY)
    assert out["scopes"][0]["configs"][0]["rows"] == {"retired_runs": 0, "unselected_of_current_runs": 0}
    variable({**BASE, "retention_days": "soon"})
    with pytest.raises(dags.AirflowFailException):
        dags.purge_task(VAR_KEY)


def test_layering_and_blocked_trigger_keys(variable):
    variable({**BASE, "runs": [{"table": "ODAG1", "name": "x", "schedule_interval": "0 1 * * *"}]})
    config = dags.resolve_config(VAR_KEY, {"RUN_KEY": "rk", "meta_db": "PROD_DB", "td_conn_var": "PROD"}, 0)
    assert (config["project"], config["table"], config["run_type"]) == ("UMCM", "ODAG1", "WKLY")
    assert "schedule_interval" not in config and "name" not in config
    assert config["run_key"] == "rk" and config["meta_db"] == "main" and config["td_conn_var"] == "DEV"


def _run_task(monkeypatch, raw, overrides=None):
    from sampling.db import Connection
    monkeypatch.setattr(dags, "connect_teradata", lambda db, lock: Connection(raw, host="duckdb", access_lock=False))
    return dags.run_sampling_task(VAR_KEY, overrides=overrides or {})


def test_task_runs_the_scope_and_exports_the_connection(monkeypatch, variable, raw):
    build_source(raw.db)
    um_config(raw.db)
    report = _run_task(monkeypatch, raw)
    assert report["result"] == "ALL SAMPLES COMPLETED" and report["runs"][0]["selected"] == 150
    assert report["runs"][0]["as_of_date"] == "2026-08-01"
    import os
    assert (os.environ["TERADATA_HOST"], os.environ["TERADATA_USER"], os.environ["TERADATA_LOGMECH"]) == \
        ("td.example", "svc", "TD2")


def test_config_problem_fails_without_retry(monkeypatch, variable, raw):
    from airflow.sdk.exceptions import AirflowFailException
    build_source(raw.db)
    insert_config(raw.db, smpl_method_cd="BOGUS")
    with pytest.raises(AirflowFailException):
        _run_task(monkeypatch, raw)


def test_a_final_sample_is_not_replaced_by_the_dag(monkeypatch, variable, raw, settings):
    from airflow.sdk.exceptions import AirflowFailException
    from sampling.db import Connection
    from sampling.engine import finalize_run
    monkeypatch.setattr(raw, "close", lambda: None)
    build_source(raw.db)
    um_config(raw.db)
    _run_task(monkeypatch, raw)
    finalize_run(Connection(raw, access_lock=False), settings, key(1), "2026-08-01", "auditor")
    with pytest.raises(AirflowFailException):
        _run_task(monkeypatch, raw)


def test_technical_problem_is_retried(monkeypatch, variable, raw):
    build_source(raw.db)
    um_config(raw.db)
    raw.fail_on = "SELECT case_id FROM (SELECT * FROM main.case_source WHERE pull_date"
    with pytest.raises(RuntimeError):
        _run_task(monkeypatch, raw)


def test_unknown_scope_and_missing_meta_db_fail_without_retry(monkeypatch, variable, raw):
    from airflow.sdk.exceptions import AirflowFailException
    with pytest.raises(AirflowFailException):
        _run_task(monkeypatch, raw, {"project": "NOPE"})
    variable({**BASE, "meta_db": ""})
    with pytest.raises(AirflowFailException):
        _run_task(monkeypatch, raw)


def test_as_of_and_run_params_from_a_trigger(monkeypatch, variable, raw):
    build_source(raw.db)
    um_config(raw.db)
    variable({**BASE, "trigger_keys": ["as_of", "run_params", "dry_run"]})
    report = _run_task(monkeypatch, raw, {"as_of": "2026-09-01", "run_params": '{"batch_id": "2026-08-01"}',
                                          "dry_run": "true"})
    run = report["runs"][0]
    assert run["as_of_date"] == "2026-09-01" and run["dry_run"] is True and run["smpl_run_id"] is None
    with pytest.raises(ConfigError):
        dags._as_of({"as_of": "09/01/2026"}, None, "America/Chicago")
    assert dags._as_of({}, None, "America/Chicago") <= date.today()


def test_extra_filters_add_up_across_the_layers(variable):
    variable({**BASE, "extra_filters": "ETL_IS_CURR_IND = 'Y'", "trigger_keys": ["extra_filters"],
              "runs": [{"table": "T5", "extra_filters": ["SRC = 'A'", "X = 1 OR Y = 2"]}]})
    config = dags.resolve_config(VAR_KEY, {"extra_filters": "BTCH_ID = 'B42'"}, 0)
    assert config["extra_filters"] == "(ETL_IS_CURR_IND = 'Y') AND (SRC = 'A') AND (X = 1 OR Y = 2) AND (BTCH_ID = 'B42')"
    variable({**BASE, "extra_filters": "A = 1"})
    assert dags.resolve_config(VAR_KEY, {})["extra_filters"] == "A = 1"
    with pytest.raises(ConfigError, match="extra_filters must be SQL text"):
        variable({**BASE, "extra_filters": {"BTCH_ID": "B42"}})
        dags.resolve_config(VAR_KEY, {})


def test_the_connection_id_falls_back():
    assert dags._connection_id({}) == "govt_rgltry_teradata"
    assert dags._connection_id({"td_conn_var": "DEV"}) == "DEV"
    assert dags._connection_id({"td_conn_var": "DEV", "connection_id": "govt_rgltry_teradata_dev"}) == "govt_rgltry_teradata_dev"
    assert dags._connection_id({"connection_id": "a", "connection_type": "Teradata"}, "b") == "b"
    with pytest.raises(ConfigError, match="connection_type must be 'teradata'"):
        dags._connection_id({"connection_type": "postgres"})


def test_an_unknown_log_level_fails_without_retry(monkeypatch, variable, raw):
    from airflow.sdk.exceptions import AirflowFailException
    with pytest.raises(AirflowFailException, match="log_level"):
        _run_task(monkeypatch, raw, {"log_level": "LOUD"})


def test_a_trigger_sets_sql_text_only_when_the_variable_allows_it(variable):
    variable({**BASE, "extra_filters": "A = 1"})
    config = dags.resolve_config(VAR_KEY, {"extra_filters": "1=1 OR B = 2", "run_params": '{"batch_id": "X"}',
                                           "run_key": "rk"})
    assert config["extra_filters"] == "A = 1" and config["run_params"] == {"batch_id": "2026-08-01"}
    assert config["run_key"] == "rk"
    variable({**BASE, "trigger_keys": ["run_params"]})
    assert dags.resolve_config(VAR_KEY, {"run_params": '{"batch_id": "X"}'})["run_params"] == '{"batch_id": "X"}'
