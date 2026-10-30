"""The Airflow version: DAG parsing, Variable / trigger layering, the task's outcome (needs apache-airflow 3)."""
import importlib.util
import json
from datetime import date

import pytest

pytest.importorskip("airflow.sdk")

from conftest import ROOT, build_universe, insert_config, um_config  # noqa: E402
from sampling import dags  # noqa: E402
from sampling.settings import ConfigError  # noqa: E402

VAR_KEY = "cms_compliance_umcm_sampling_config_var"
BASE = {"td_conn_var": "DEV", "meta_db": "main", "load_env": "DEV", "project": "UMCM", "process_name": "WEEKLY",
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
    spec = importlib.util.spec_from_file_location("umcm_dag", ROOT / "airflow" / "cms_compliance_umcm_sampling.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.dag


def test_dag_parses_with_the_example_variable(variable):
    example = json.loads((ROOT / "airflow_resources" / "examples" / "umcm_dev" / f"{VAR_KEY}.json").read_text())
    variable({**example, "schedule_interval": "0 8 * * 5", "trigger_dags": ["NEXT_DAG"]})
    dag = _load_dag_file()
    assert dag.dag_id == "CMS_COMPLIANCE_UMCM_SAMPLING" and dag.max_active_runs == 1
    assert set(dag.task_dict) == {"run_sampling", "trigger_next_dag", "success_email", "fail_email"}
    assert set(dag.params) >= {"config_id", "project", "process_name", "run_key", "run_params", "seed", "as_of"}


def test_dag_parses_without_a_variable(monkeypatch):
    monkeypatch.delenv(f"AIRFLOW_VAR_{VAR_KEY.upper()}", raising=False)
    dag = _load_dag_file()
    assert list(dag.task_dict) == ["run_sampling"]


def test_runs_entries_become_tasks(variable):
    variable({**BASE, "runs": [{"name": "weekly", "process_name": "WEEKLY"}, {"config_id": 7}]})
    dag = _load_dag_file()
    assert sorted(dag.task_dict) == ["run_sampling_7", "run_sampling_weekly_weekly"]


def test_trigger_keys_limit_what_an_upstream_dag_can_change(variable):
    variable({**BASE, "trigger_keys": ["run_key", "as_of"]})
    config = dags.resolve_config(VAR_KEY, {"project": "FRAMEWORK_PROJECT", "run_key": "B42", "as_of": "2026-08-08"})
    assert config["project"] == "UMCM" and config["run_key"] == "B42" and config["as_of"] == "2026-08-08"
    assert "trigger_keys" not in config
    variable({**BASE, "trigger_keys": "run_key, nope"})
    with pytest.raises(ConfigError):
        dags.resolve_config(VAR_KEY, {})


def test_layering_and_blocked_trigger_keys(variable):
    variable({**BASE, "runs": [{"config_id": 5}]})
    config = dags.resolve_config(VAR_KEY, {"RUN_KEY": "rk", "meta_db": "PROD_DB", "td_conn_var": "PROD"}, 0)
    assert config["config_id"] == 5 and "process_name" not in config      # the entry's scope replaces the Variable's
    assert config["run_key"] == "rk" and config["meta_db"] == "main" and config["td_conn_var"] == "DEV"


def _run_task(monkeypatch, raw, overrides=None):
    from sampling.db import Connection
    monkeypatch.setattr(dags, "connect_teradata", lambda db, lock: Connection(raw, host="duckdb", access_lock=False))
    return dags.run_sampling_task(VAR_KEY, overrides=overrides or {})


def test_task_runs_the_scope_and_exports_the_connection(monkeypatch, variable, raw):
    build_universe(raw.db)
    um_config(raw.db)
    report = _run_task(monkeypatch, raw)
    assert report["result"] == "ALL SAMPLES COMPLETED" and report["runs"][0]["selected"] == 150
    assert report["runs"][0]["as_of_date"] == "2026-08-01"
    import os
    assert (os.environ["TERADATA_HOST"], os.environ["TERADATA_USER"], os.environ["TERADATA_LOGMECH"]) == \
        ("td.example", "svc", "TD2")


def test_config_problem_fails_without_retry(monkeypatch, variable, raw):
    from airflow.sdk.exceptions import AirflowFailException
    build_universe(raw.db)
    insert_config(raw.db, sampling_method="BOGUS")
    with pytest.raises(AirflowFailException):
        _run_task(monkeypatch, raw)


def test_technical_problem_is_retried(monkeypatch, variable, raw):
    build_universe(raw.db)
    um_config(raw.db)
    raw.fail_on = "FROM case_universe WHERE (pull_date"
    with pytest.raises(RuntimeError):
        _run_task(monkeypatch, raw)


def test_unknown_scope_and_missing_meta_db_fail_without_retry(monkeypatch, variable, raw):
    from airflow.sdk.exceptions import AirflowFailException
    with pytest.raises(AirflowFailException):
        _run_task(monkeypatch, raw, {"process_name": "NOPE"})
    variable({**BASE, "meta_db": ""})
    with pytest.raises(AirflowFailException):
        _run_task(monkeypatch, raw)


def test_as_of_and_run_params_from_a_trigger(monkeypatch, variable, raw):
    build_universe(raw.db)
    um_config(raw.db)
    report = _run_task(monkeypatch, raw, {"as_of": "2026-09-01", "run_params": '{"batch_id": "2026-08-01"}',
                                          "dry_run": "true"})
    run = report["runs"][0]
    assert run["as_of_date"] == "2026-09-01" and run["dry_run"] is True and run["sample_run_id"] is None
    with pytest.raises(ConfigError):
        dags._as_of({"as_of": "09/01/2026"}, None, "America/Chicago")
    assert dags._as_of({}, None, "America/Chicago") <= date.today()
