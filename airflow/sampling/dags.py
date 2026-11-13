from __future__ import annotations

import html
import json
import logging
import os
from datetime import date, datetime, timedelta
from typing import Optional

from airflow.providers.smtp.operators.smtp import EmailOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import DAG, Connection, Param, Variable
from airflow.timetables.trigger import MultipleCronTriggerTimetable
try:
    from airflow.sdk.exceptions import AirflowFailException
except ImportError:
    from airflow.exceptions import AirflowFailException

from sampling import __version__
from sampling.db import connect_teradata, error_text
from sampling.admin import purge_selections
from sampling.engine import run_scope
from sampling.settings import DEFAULT_TIMEZONE, ConfigError, Settings, _flag, cron_match, zone


logger = logging.getLogger(__name__)

DEFAULT_CONNECTION_ID = "govt_rgltry_teradata"
CONNECTION_TYPE = "teradata"
TD_CONN_KEY = "td_conn_var"
SCOPE_KEYS = ("project", "table", "run_type")
RUN_KEYS = ("run_key", "run_params", "extra_filters", "seed", "as_of", "rules_as_of", "dry_run")
TRIGGER_KEYS = (*SCOPE_KEYS, *RUN_KEYS, "log_level")
SQL_TRIGGER_KEYS = ("run_params", "extra_filters")
DEFAULT_TRIGGER_KEYS = tuple(k for k in TRIGGER_KEYS if k not in SQL_TRIGGER_KEYS)
PASSED_ON = ("project", "table", "run_type", "run_key", "as_of")
SETTINGS_KEYS = ("meta_db", "load_env", "environment", "env_value", "timezone", "chunk_rows", "access_lock")
DEFAULT_TIMEOUT = timedelta(hours=2)
DEFAULT_RETRIES = 1
DEFAULT_ASSIGNMENT_GROUP = "D&AE - EDE Govt Compliance"
DEFAULT_TAGS = ["GOVT_RGLTRY", "Sampling"]
_TEXT = ["null", "string"]


def _connection_id(config: dict, connection_id: Optional[str] = None) -> str:
    kind = str(config.get("connection_type") or CONNECTION_TYPE).strip().lower()
    if kind != CONNECTION_TYPE:
        raise ConfigError(f"connection_type must be '{CONNECTION_TYPE}', got {config.get('connection_type')!r}")
    return str(connection_id or config.get("connection_id") or config.get(TD_CONN_KEY) or DEFAULT_CONNECTION_ID)


def _set_env(key: str, value) -> None:
    if value is None or value == "":
        return
    os.environ[str(key)] = str(value)


def _load_teradata_connection(conn_id: str) -> None:
    conn = Connection.get(conn_id)
    if not conn.host or not conn.login or not conn.password:
        raise ConfigError(f"Airflow Connection '{conn_id}' needs host, login and password")
    _set_env("TERADATA_HOST", conn.host)
    _set_env("TERADATA_USER", conn.login)
    _set_env("TERADATA_PASSWORD", conn.password)
    extra = conn.extra_dejson or {}
    _set_env("TERADATA_LOGMECH", extra.get("logmech") or os.environ.get("TERADATA_LOGMECH") or "LDAP")
    logger.info("Loaded Teradata connection '%s': host=%s user=%s logmech=%s (password not logged)",
                conn_id, conn.host, conn.login, os.environ.get("TERADATA_LOGMECH"))


def _keys(value: Optional[dict]) -> dict:
    return {str(k).lower(): v for k, v in (value or {}).items()}


def _load_variable(variable_key: str) -> dict:
    config = Variable.get(variable_key, default=None, deserialize_json=True)
    if config is None:
        raise ConfigError(f"Airflow Variable '{variable_key}' is not set")
    if not isinstance(config, dict):
        raise ConfigError(f"Airflow Variable '{variable_key}' must be a JSON object, got {type(config).__name__}")
    config = _keys(config)
    if isinstance(config.get("runs"), list):
        config["runs"] = [_keys(r) if isinstance(r, dict) else r for r in config["runs"]]
    return config


def load_dag_config(variable_key: str) -> dict:
    try:
        return _load_variable(variable_key)
    except Exception as e:
        logger.warning("sampling DAG configuration: %s", e)
        return {}


def run_scopes(config: dict) -> list:
    runs = config.get("runs")
    if not runs:
        return [{}]
    if not isinstance(runs, list) or not all(isinstance(r, dict) for r in runs):
        raise ConfigError("'runs' must be a list of JSON objects")
    return runs


def timed_runs(config: dict) -> bool:
    try:
        return any(r.get("schedule_interval") for r in run_scopes(config))
    except ConfigError:
        return False


def chosen_runs(variable: dict, dag_run, tz: str = DEFAULT_TIMEZONE) -> list:
    runs = run_scopes(variable)
    names = [n.strip().upper() for n in str(_keys(getattr(dag_run, "conf", None)).get("run") or "").split(",")
             if n.strip()]
    if names:
        by_name = {str(r.get("name") or "").upper(): i for i, r in enumerate(runs)}
        unknown = [n for n in names if n not in by_name]
        if unknown:
            raise ConfigError(f"run {unknown[0]} is not a name in runs ({', '.join(n for n in by_name if n) or 'none'})")
        return [by_name[n] for n in names]
    if str(getattr(dag_run.run_type, "value", dag_run.run_type)) != "scheduled":
        return [None]
    when = (getattr(dag_run, "run_after", None) or dag_run.logical_date).astimezone(zone(tz)).replace(tzinfo=None)
    picked = [i for i, r in enumerate(runs) if r.get("schedule_interval") and cron_match(r["schedule_interval"], when)]
    own = variable.get("schedule_interval")
    if own and cron_match(own, when):
        picked.append(None)
    if not picked:
        logger.warning("no runs entry is scheduled at %s; the Variable's own scope is used", when)
    return picked or [None]


def _schedule(dag_config: dict, tz: str):
    if not timed_runs(dag_config):
        return dag_config.get("schedule_interval")
    crons = [c for c in [dag_config.get("schedule_interval"), *(r.get("schedule_interval") for r in dag_config["runs"])]
             if c]
    return MultipleCronTriggerTimetable(*dict.fromkeys(crons), timezone=tz)


def conf_value(dag_run, params, key: str):
    value = _keys(getattr(dag_run, "conf", None)).get(key)
    return value if value not in (None, "", []) else (params or {}).get(key) or ""


def allowed_trigger_keys(variable: dict) -> tuple:
    names = variable.get("trigger_keys")
    if names in (None, "", []):
        return DEFAULT_TRIGGER_KEYS
    names = [str(n).strip().lower() for n in (names if isinstance(names, list) else str(names).split(","))]
    unknown = sorted(set(names) - set(TRIGGER_KEYS))
    if unknown:
        raise ConfigError(f"trigger_keys {unknown} are not trigger keys ({', '.join(TRIGGER_KEYS)})")
    return tuple(n for n in TRIGGER_KEYS if n in names)


def _list(value) -> list:
    if value in (None, "", []):
        return []
    items = value if isinstance(value, list) else str(value).split(",")
    return [str(v).strip().lower() for v in items if str(v).strip()]


def trigger_values(given: Optional[dict], allowed: tuple = DEFAULT_TRIGGER_KEYS, quiet: tuple = ()) -> dict:
    out, ignored = {}, []
    for key, value in _keys(given).items():
        if value in (None, "", []):
            continue
        if key in allowed:
            out[key] = value
        elif key not in quiet:
            ignored.append(key)
    if ignored:
        logger.warning("not taken from the trigger (set them in the Variable): %s", ", ".join(sorted(ignored)))
    return out


def resolve_config(variable_key: str, overrides: Optional[dict] = None, scope_index: Optional[int] = None) -> dict:
    variable = _load_variable(variable_key)
    scope = dict(run_scopes(variable)[scope_index]) if scope_index is not None else {}
    as_params = _list(variable.get("trigger_run_params"))
    given = trigger_values(overrides, allowed_trigger_keys(variable), quiet=tuple(as_params))
    config = {k: v for layer in (variable, scope, given) for k, v in layer.items()
              if k not in ("runs", "trigger_keys", "trigger_run_params", "schedule_interval", "name")}
    picked = {k: v for k, v in _keys(overrides).items() if k in as_params and v not in (None, "", [])}
    if picked:
        config["run_params"] = {**_run_params(config.get("run_params")), **picked}
        logger.info("run parameters from the trigger: %s", ", ".join(sorted(picked)))
    filters = [f for layer in (variable, scope, given) for f in _filters(layer.get("extra_filters"))]
    config["extra_filters"] = " AND ".join(f"({f})" for f in filters) if len(filters) > 1 else \
        (filters[0] if filters else None)
    if not config.get("meta_db"):
        raise ConfigError(f"'meta_db' is required in Airflow Variable '{variable_key}'")
    return config


def _filters(value) -> list:
    if value in (None, "", []):
        return []
    items = value if isinstance(value, list) else [value]
    if not all(isinstance(v, str) for v in items):
        raise ConfigError(f"extra_filters must be SQL text (a condition, or a list of them), got {value!r}")
    return [v.strip() for v in items if v.strip()]


def _run_params(value, name: str = "run_params") -> dict:
    if value in (None, ""):
        return {}
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except json.JSONDecodeError as e:
            raise ConfigError(f"{name} must be a JSON object, got {value!r}") from e
    if not isinstance(value, dict):
        raise ConfigError(f"{name} must be a JSON object")
    return value


def _as_of(config: dict, dag_run, tz: str) -> date:
    if config.get("as_of"):
        try:
            return date.fromisoformat(str(config["as_of"]).strip())
        except ValueError as e:
            raise ConfigError(f"as_of must be YYYY-MM-DD, got {config['as_of']!r}") from e
    when = getattr(dag_run, "run_after", None) or getattr(dag_run, "logical_date", None)
    when = when or datetime.now(zone(tz))
    return when.astimezone(zone(tz)).date()


def _rules_as_of(config: dict) -> Optional[date]:
    value = config.get("rules_as_of")
    if value in (None, ""):
        return None
    try:
        return date.fromisoformat(str(value).strip())
    except ValueError as e:
        raise ConfigError(f"rules_as_of must be YYYY-MM-DD, got {value!r}") from e


def _rerun_reason(config: dict) -> Optional[str]:
    value = config.get("rerun_reason")
    return None if value is None else str(value)


def _triggered_by(dag_run) -> str:
    if dag_run is None:
        return "airflow"
    run_type = str(getattr(dag_run.run_type, "value", dag_run.run_type) or "manual")
    user = getattr(dag_run, "triggering_user_name", None)
    return f"airflow-{run_type}" + (f"-{user}" if user else "")


def run_sampling_task(variable_key: str, overrides: Optional[dict] = None, scope_index: Optional[int] = None,
                      dag_run=None, connection_id: Optional[str] = None) -> dict:
    sampling_log = logging.getLogger("sampling")
    previous = sampling_log.level
    try:
        return _run_sampling_task(variable_key, overrides, scope_index, dag_run, connection_id)
    finally:
        sampling_log.setLevel(previous)


def _run_sampling_task(variable_key: str, overrides: Optional[dict], scope_index: Optional[int], dag_run,
                       connection_id: Optional[str]) -> dict:
    try:
        config = resolve_config(variable_key, overrides, scope_index)
        settings = Settings.from_mapping({k: config.get(k) for k in SETTINGS_KEYS})
        level = str(config.get("log_level") or "INFO").strip().upper()
        if not isinstance(logging.getLevelName(level), int):
            raise ConfigError(f"log_level must be DEBUG, INFO, WARNING, ERROR or CRITICAL, got {level!r}")
        logging.getLogger("sampling").setLevel(level)
        _load_teradata_connection(_connection_id(config, connection_id))
        scope = {k: config.get(k) for k in SCOPE_KEYS if config.get(k) not in (None, "")}
        kwargs = {"run_key": config.get("run_key") or None, "run_params": _run_params(config.get("run_params")),
                  "extra_filters": config.get("extra_filters") or None,
                  "seed": config.get("seed"), "as_of": _as_of(config, dag_run, settings.timezone),
                  "triggered_by": _triggered_by(dag_run), "dry_run": _flag(config.get("dry_run"), False),
                  "rerun_reason": _rerun_reason(config), "rules_as_of": _rules_as_of(config)}
    except ConfigError as e:
        raise AirflowFailException(f"configuration: {e}") from e

    logger.info("sampling %s | SAMPLING | settings %s | scope %s | run_key %s | as_of %s | extra_filters %s%s", __version__,
                settings.describe(), scope or "none", kwargs["run_key"] or "(as_of date)", kwargs["as_of"],
                kwargs["extra_filters"] or "none",
                " | DRY RUN" if kwargs["dry_run"] else "")
    if kwargs["rules_as_of"]:
        logger.info("rules and config in effect on %s", kwargs["rules_as_of"])
    if kwargs["rerun_reason"] is not None:
        logger.info("rerun_reason from the Variable: %r", kwargs["rerun_reason"])
    try:
        conn = connect_teradata(settings.meta_db, settings.access_lock)
    except Exception as e:
        raise RuntimeError(f"cannot connect to Teradata: {error_text(e)}") from e
    try:
        outcome = run_scope(conn, settings, **scope, **kwargs)
    except ConfigError as e:
        raise AirflowFailException(f"configuration: {e}") from e
    finally:
        conn.close()

    text = json.dumps(outcome, indent=2, default=str)
    failed = [r for r in outcome["runs"] if r.get("status") != "COMPLETED"]
    short = [r for r in outcome["runs"] if r.get("shortfall")]
    if failed:
        logger.error("SAMPLING report:\n%s", text)
        message = f"{outcome['result']}: " + "; ".join(f"{_sample(r)}: {r.get('error_type')} {r.get('error')}"
                                                        for r in failed)
        if any(r.get("error_kind") != "config" for r in failed):
            raise RuntimeError(message)
        raise AirflowFailException(message)
    if short and _flag(config.get("fail_on_shortfall"), False):
        logger.error("SAMPLING report:\n%s", text)
        raise AirflowFailException("sample(s) short of target_cnt: " + ", ".join(
            f"{_sample(r)} {r['selected']}/{r['target_cnt']}" for r in short))
    (logger.warning if short else logger.info)("SAMPLING report:\n%s", text)
    return json.loads(text)


def _sample(report: dict) -> str:
    return ".".join(str(report.get(k) or "") for k in ("project_cd", "table_nm", "run_ty"))


def run_timed_task(variable_key: str, overrides: Optional[dict] = None, dag_run=None,
                   connection_id: Optional[str] = None) -> dict:
    try:
        variable = _load_variable(variable_key)
        tz = variable.get("schedule_timezone") or variable.get("timezone") or DEFAULT_TIMEZONE
        chosen = chosen_runs(variable, dag_run, tz)
    except ConfigError as e:
        raise AirflowFailException(f"configuration: {e}") from e
    overrides = {k: v for k, v in _keys(overrides).items() if k != "run"}
    runs, failed = [], []
    for index in chosen:
        entry = "the Variable's own scope" if index is None else f"runs entry {index + 1}"
        logger.info("SAMPLING | %s", entry)
        try:
            runs += run_sampling_task(variable_key, overrides, index, dag_run, connection_id).get("runs", [])
        except Exception as e:
            logger.error("SAMPLING | %s | failed: %s", entry, e)
            failed.append((f"{entry}: {e}", isinstance(e, AirflowFailException)))
    if failed:
        text = f"{len(failed)} of {len(chosen)} run(s) failed: {'; '.join(t for t, _ in failed)}"
        raise AirflowFailException(text) if all(problem for _, problem in failed) else RuntimeError(text)
    return {"result": "ALL SAMPLES COMPLETED", "runs": runs, "failed": 0}


def purge_task(variable_key: str, connection_id: Optional[str] = None) -> dict:
    try:
        config = resolve_config(variable_key)
        settings = Settings.from_mapping({k: config.get(k) for k in SETTINGS_KEYS})
        _load_teradata_connection(_connection_id(config, connection_id))
        runs = _load_variable(variable_key).get("runs")
        scopes = [resolve_config(variable_key, scope_index=i) for i in range(len(runs))] if runs else [config]
    except ConfigError as e:
        raise AirflowFailException(f"configuration: {e}") from e
    try:
        conn = connect_teradata(settings.meta_db, settings.access_lock)
    except Exception as e:
        raise RuntimeError(f"cannot connect to Teradata: {error_text(e)}") from e
    out = []
    try:
        for scope in scopes:
            out.append(purge_selections(conn, settings, config.get("retention_days"), apply=True,
                                        **{k: scope.get(k) for k in SCOPE_KEYS if scope.get(k) not in (None, "")}))
    except ConfigError as e:
        raise AirflowFailException(f"configuration: {e}") from e
    finally:
        conn.close()
    logger.info("PURGE report:\n%s", json.dumps(out, indent=2, default=str))
    return {"scopes": out}


def sample_summary(ti, task_ids: list) -> str:
    runs = []
    for task_id in task_ids:
        report = ti.xcom_pull(task_ids=task_id) or {}
        runs += report.get("runs", []) if isinstance(report, dict) else []
    if not runs:
        return "<p>No sample was made.</p>"
    head = ("Sample", "Run key", "As of", "Status", "Selected", "Target", "Short", "Target table", "Rules (hash)")
    cells = [[_sample(r), r.get("run_key"), r.get("as_of_date"), r.get("status"),
              r.get("selected"), r.get("target_cnt"), r.get("shortfall"), r.get("tgt_table_nm") or "",
              str(r.get("config_hash") or "")[:12]] for r in runs]
    rows = "".join("<tr>" + "".join(f"<td>{html.escape(str(c if c is not None else ''))}</td>" for c in row)
                   + "</tr>" for row in cells)
    return ("<table border=\"1\" cellpadding=\"4\" cellspacing=\"0\"><tr>"
            + "".join(f"<th>{h}</th>" for h in head) + f"</tr>{rows}</table>"
            + "<p>The selected cases are in GOVT_RGLTRY_SampleSelection (Selected_Ind = 1, Curr_Ind = 1) "
              "and in the target table.</p>")


def _failure_alert_ticket(assignment_group: str):
    def failure_alert_ticket(context):
        try:
            from snow import snow_integrations
        except ImportError:
            logger.warning("snow integration is not installed; no ServiceNow incident for %s",
                           context.get("task_instance"))
            return
        snow_integrations.create_incident(context, assignment_group)
    return failure_alert_ticket


def _names(value) -> list:
    if isinstance(value, str):
        return [v.strip() for v in value.split(",") if v.strip()]
    return list(value or [])


def _retries(dag_config: dict) -> int:
    value = dag_config.get("retries")
    try:
        return max(0, int(value)) if value not in (None, "") else DEFAULT_RETRIES
    except (TypeError, ValueError):
        logger.warning("retries must be a whole number; using %s", DEFAULT_RETRIES)
        return DEFAULT_RETRIES


def _minutes(dag_config: dict, key: str, default: timedelta) -> timedelta:
    try:
        value = int(dag_config.get(key) or 0)
    except (TypeError, ValueError):
        logger.warning("%s must be a whole number of minutes; using %s", key, default)
        return default
    return timedelta(minutes=value) if value > 0 else default


def _task_id(scope: dict, index: int, scopes: list) -> str:
    if len(scopes) == 1:
        return "run_sampling"

    def base(s: dict, i: int) -> str:
        name = "_".join(str(s[k]) for k in ("name", "project", "table", "run_type") if s.get(k)) or str(i + 1)
        return "run_sampling_" + "".join(c if c.isalnum() else "_" for c in name).lower()
    names = [base(s, i) for i, s in enumerate(scopes)]
    return names[index] if names.count(names[index]) == 1 else f"{names[index]}_{index + 1}"


def _overrides(context, names: tuple = TRIGGER_KEYS) -> dict:
    conf = _keys(context["dag_run"].conf) if context.get("dag_run") else {}
    for name in names:
        if name not in conf and context["params"].get(name) not in (None, "", []):
            conf[name] = context["params"][name]
    return {k: v for k, v in conf.items() if v not in (None, "", [])}


def build_sampling_dag(dag_id: str, variable_key: str) -> DAG:
    dag_config = load_dag_config(variable_key)
    tz = dag_config.get("schedule_timezone") or dag_config.get("timezone") or DEFAULT_TIMEZONE
    try:
        start_date = datetime(2026, 1, 1, tzinfo=zone(tz))
    except Exception:
        logger.warning("timezone %r is not valid; %s is used", tz, DEFAULT_TIMEZONE)
        tz = DEFAULT_TIMEZONE
        start_date = datetime(2026, 1, 1, tzinfo=zone(tz))
    default_args = {
        "owner": dag_config.get("owner") or "oss",
        "depends_on_past": False,
        "start_date": start_date,
        "retries": _retries(dag_config),
        "retry_delay": _minutes(dag_config, "retry_delay_minutes", timedelta(minutes=5)),
        "on_failure_callback": _failure_alert_ticket(dag_config.get("snow_assignment_group")
                                                     or DEFAULT_ASSIGNMENT_GROUP),
    }
    timed = timed_runs(dag_config)
    names = (*TRIGGER_KEYS, "run") if timed else TRIGGER_KEYS
    params = {name: Param(None, type=_TEXT) for name in names}
    dag = DAG(dag_id=dag_id, description=f"config-driven stratified sampling ({variable_key})",
              default_args=default_args, schedule=_schedule(dag_config, tz), catchup=False,
              max_active_runs=1, dagrun_timeout=_minutes(dag_config, "dagrun_timeout_minutes", timedelta(hours=8)),
              params=params, user_defined_macros={"conf_value": conf_value,
                                                  "sample_summary": lambda ti: sample_summary(ti, task_ids)},
              tags=_names(dag_config.get("tags")) or DEFAULT_TAGS)

    def run(scope_index: Optional[int], **context):
        return run_sampling_task(variable_key, overrides=_overrides(context), scope_index=scope_index,
                                 dag_run=context.get("dag_run"))

    def run_timed(**context):
        return run_timed_task(variable_key, overrides=_overrides(context, names), dag_run=context.get("dag_run"))

    try:
        scopes = run_scopes(dag_config)
    except ConfigError as e:
        logger.warning("%s: %s", dag_id, e)
        scopes = [{}]
    timeout = _minutes(dag_config, "execution_timeout_minutes", DEFAULT_TIMEOUT)
    pool = {"pool": dag_config["pool"]} if dag_config.get("pool") else {}
    task_ids = []
    if timed:
        tasks = [PythonOperator(task_id="run_sampling", dag=dag, python_callable=run_timed,
                                execution_timeout=timeout, show_return_value_in_logs=False, **pool)]
    else:
        tasks = [PythonOperator(task_id=_task_id(scope, i, scopes), dag=dag, python_callable=run,
                                op_kwargs={"scope_index": i if dag_config.get("runs") else None},
                                execution_timeout=timeout, show_return_value_in_logs=False, **pool)
                 for i, scope in enumerate(scopes)]
    task_ids.extend(t.task_id for t in tasks)

    given = "{{ conf_value(dag_run, params, '%s') }}"
    triggers = [TriggerDagRunOperator(task_id=f"trigger_{str(target).lower()}", dag=dag, trigger_dag_id=str(target),
                                      trigger_rule="all_success", conf={k: given % k for k in PASSED_ON})
                for target in _names(dag_config.get("trigger_dags"))]
    for trigger in triggers:
        tasks >> trigger

    purge = []
    if dag_config.get("retention_days") not in (None, ""):
        purge = [PythonOperator(task_id="purge_selections", dag=dag, trigger_rule="all_success",
                                python_callable=lambda **_: purge_task(variable_key),
                                execution_timeout=timeout, show_return_value_in_logs=False, **pool)]
        tasks >> purge[0]

    email_recipient = dag_config.get("email_recipient")
    if email_recipient:
        load_env = dag_config.get("load_env") or dag_config.get("environment") or ""
        upstream = [*tasks, *triggers, *purge]
        success = EmailOperator(task_id="success_email", dag=dag, to=email_recipient,
                                trigger_rule="none_failed_min_one_success",
                                subject=f"Airflow EDEG-GOVT_RGLTRY {load_env} Success: {dag_id} DAG",
                                html_content=f"DAG {dag_id} run {{{{ run_id }}}} completed successfully")
        fail = EmailOperator(task_id="fail_email", dag=dag, to=email_recipient, trigger_rule="one_failed",
                             subject=f"Airflow EDEG-GOVT_RGLTRY {load_env} Failure: {dag_id} DAG",
                             html_content=f"DAG {dag_id} run {{{{ run_id }}}} failed")
        upstream >> success
        upstream >> fail

    notify = _names(dag_config.get("notify_recipients"))
    if notify:
        load_env = dag_config.get("load_env") or dag_config.get("environment") or ""
        ready = EmailOperator(task_id="sample_ready_email", dag=dag, to=notify, trigger_rule="all_success",
                              subject=f"{load_env} sample ready: {dag_id} {{{{ ds }}}}".strip(),
                              html_content=f"<p>DAG {dag_id} run {{{{ run_id }}}}:</p>{{{{ sample_summary(ti) }}}}")
        tasks >> ready
    return dag
