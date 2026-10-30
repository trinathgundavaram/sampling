"""Airflow: the sampling DAG, its task, and the bridge from the Variable + Teradata Connection to the engine.

Everything about a DAG comes from its own Airflow Variable (JSON); the DAG file only names the dag_id and the
Variable. The Teradata login comes from the Airflow Connection the Variable names in `td_conn_var` and is
exported as TERADATA_HOST / TERADATA_USER / TERADATA_PASSWORD / TERADATA_LOGMECH for sampling.db. No .env file is
read on Airflow. See airflow_resources/docs/AIRFLOW_DEPLOYMENT.md.
"""
from __future__ import annotations

import html
import json
import logging
import os
from datetime import date, datetime, timedelta
from typing import Optional
from zoneinfo import ZoneInfo

import pendulum
from airflow.providers.smtp.operators.smtp import EmailOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import DAG, Connection, Param, Variable
try:
    from airflow.sdk.exceptions import AirflowFailException
except ImportError:  # Airflow 3.0
    from airflow.exceptions import AirflowFailException

from sampling import __version__
from sampling.db import connect_teradata, error_text
from sampling.engine import run_scope
from sampling.settings import DEFAULT_TIMEZONE, ConfigError, Settings, _flag

logger = logging.getLogger(__name__)

DEFAULT_CONNECTION_ID = "compliance_teradata"
TD_CONN_KEY = "td_conn_var"
SCOPE_KEYS = ("config_id", "project", "process_name")
RUN_KEYS = ("run_key", "run_params", "seed", "as_of", "dry_run")
TRIGGER_KEYS = (*SCOPE_KEYS, *RUN_KEYS, "log_level")   # what a trigger may change for one run
PASSED_ON = ("project", "process_name", "run_key", "as_of")
SETTINGS_KEYS = ("meta_db", "load_env", "environment", "env_value", "timezone", "chunk_rows", "access_lock")
DEFAULT_TIMEOUT = timedelta(hours=2)
DEFAULT_RETRIES = 1
DEFAULT_ASSIGNMENT_GROUP = "D&AE - EDE Govt Compliance"
DEFAULT_TAGS = ["Compliance", "Sampling"]
_TEXT = ["null", "string"]


# ---------------------------------------------------------------------------
# Variable, Connection -> environment and settings
# ---------------------------------------------------------------------------

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
    """A Variable / trigger object with its keys in lower case (keys are not case sensitive)."""
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
    """The Variable for the DAG file ({} when missing or invalid, so a bad Variable cannot break parsing)."""
    try:
        return _load_variable(variable_key)
    except Exception as e:  # noqa: BLE001
        logger.warning("sampling DAG configuration: %s", e)
        return {}


def run_scopes(config: dict) -> list:
    """The Variable's `runs` (one task each), or one scope: the Variable itself."""
    runs = config.get("runs")
    if not runs:
        return [{}]
    if not isinstance(runs, list) or not all(isinstance(r, dict) for r in runs):
        raise ConfigError("'runs' must be a list of JSON objects")
    return runs


def conf_value(dag_run, params, key: str):
    """A trigger value by key in any case, else the form's parameter (passed on to the triggered DAGs)."""
    value = _keys(getattr(dag_run, "conf", None)).get(key)
    return value if value not in (None, "", []) else (params or {}).get(key) or ""


def allowed_trigger_keys(variable: dict) -> tuple:
    """The Variable's trigger_keys (default: all of TRIGGER_KEYS). An upstream DAG passing values this DAG must
    not take - the framework passes its own project, say - is limited this way."""
    names = variable.get("trigger_keys")
    if names in (None, "", []):
        return TRIGGER_KEYS
    names = [str(n).strip().lower() for n in (names if isinstance(names, list) else str(names).split(","))]
    unknown = sorted(set(names) - set(TRIGGER_KEYS))
    if unknown:
        raise ConfigError(f"trigger_keys {unknown} are not trigger keys ({', '.join(TRIGGER_KEYS)})")
    return tuple(n for n in TRIGGER_KEYS if n in names)


def _list(value) -> list:
    """A list setting given as a JSON list or as one comma-separated string, lower case."""
    if value in (None, "", []):
        return []
    items = value if isinstance(value, list) else str(value).split(",")
    return [str(v).strip().lower() for v in items if str(v).strip()]


def trigger_values(given: Optional[dict], allowed: tuple = TRIGGER_KEYS, quiet: tuple = ()) -> dict:
    """What a trigger may change: the scope, the run's inputs and the log level (only the `allowed` ones). The
    database, the Connection and the environment stay the Variable's."""
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
    """The Variable, then one entry of its `runs`, then the trigger's values (later wins)."""
    variable = _load_variable(variable_key)
    scope = dict(run_scopes(variable)[scope_index]) if scope_index is not None else {}
    if scope and any(k in scope for k in SCOPE_KEYS):  # a runs entry names its own scope: it replaces the Variable's
        variable = {k: v for k, v in variable.items() if k not in SCOPE_KEYS}
    as_params = _list(variable.get("trigger_run_params"))
    given = trigger_values(overrides, allowed_trigger_keys(variable), quiet=tuple(as_params))
    config = {k: v for layer in (variable, scope, given) for k, v in layer.items()
              if k not in ("runs", "trigger_keys", "trigger_run_params")}
    # trigger_run_params: values an upstream DAG passes (the framework's run_type, table ...) become run parameters
    picked = {k: v for k, v in _keys(overrides).items() if k in as_params and v not in (None, "", [])}
    if picked:
        config["run_params"] = {**_run_params(config.get("run_params")), **picked}
        logger.info("run parameters from the trigger: %s", ", ".join(sorted(picked)))
    if not config.get("meta_db"):
        raise ConfigError(f"'meta_db' is required in Airflow Variable '{variable_key}'")
    return config


def _run_params(value) -> dict:
    if value in (None, ""):
        return {}
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except json.JSONDecodeError as e:
            raise ConfigError(f"run_params must be a JSON object, got {value!r}") from e
    if not isinstance(value, dict):
        raise ConfigError("run_params must be a JSON object")
    return value


def _as_of(config: dict, dag_run, tz: str) -> date:
    """The run's business date: `as_of` (YYYY-MM-DD) if given, else the DAG run's time in the business zone."""
    if config.get("as_of"):
        try:
            return date.fromisoformat(str(config["as_of"]).strip())
        except ValueError as e:
            raise ConfigError(f"as_of must be YYYY-MM-DD, got {config['as_of']!r}") from e
    when = getattr(dag_run, "run_after", None) or getattr(dag_run, "logical_date", None)
    when = when or datetime.now(ZoneInfo(tz))
    return when.astimezone(ZoneInfo(tz)).date()


def _triggered_by(dag_run) -> str:
    if dag_run is None:
        return "airflow"
    run_type = str(getattr(dag_run.run_type, "value", dag_run.run_type) or "manual")
    user = getattr(dag_run, "triggering_user_name", None)
    return f"airflow-{run_type}" + (f"-{user}" if user else "")


def run_sampling_task(variable_key: str, overrides: Optional[dict] = None, scope_index: Optional[int] = None,
                      dag_run=None, connection_id: Optional[str] = None) -> dict:
    """Run the sampling scope of the Variable (or one `runs` entry). A configuration problem fails the task at once
    (AirflowFailException, no retry); a technical error raises RuntimeError, which Airflow retries."""
    try:
        config = resolve_config(variable_key, overrides, scope_index)
        settings = Settings.from_mapping({k: config.get(k) for k in SETTINGS_KEYS})
        level = str(config.get("log_level") or "INFO").upper()
        logging.getLogger("sampling").setLevel(level)
        _load_teradata_connection(connection_id or config.get(TD_CONN_KEY) or config.get("connection_id")
                                  or DEFAULT_CONNECTION_ID)
        scope = {k: config.get(k) for k in SCOPE_KEYS if config.get(k) not in (None, "")}
        kwargs = {"run_key": config.get("run_key") or None, "run_params": _run_params(config.get("run_params")),
                  "seed": config.get("seed"), "as_of": _as_of(config, dag_run, settings.timezone),
                  "triggered_by": _triggered_by(dag_run), "dry_run": _flag(config.get("dry_run"), False)}
    except ConfigError as e:
        raise AirflowFailException(f"configuration: {e}") from e

    logger.info("sampling %s | settings %s | scope %s | run_key=%s as_of=%s%s", __version__, settings.describe(),
                scope or "none", kwargs["run_key"] or "(as_of date)", kwargs["as_of"],
                " | DRY RUN" if kwargs["dry_run"] else "")
    try:
        conn = connect_teradata(settings.meta_db, settings.access_lock)
    except Exception as e:  # noqa: BLE001
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
        logger.error("sampling report:\n%s", text)
        message = f"{outcome['result']}: " + "; ".join(f"config {r.get('config_id')}: {r.get('error_type')} "
                                                        f"{r.get('error')}" for r in failed)
        if any(r.get("error_kind") != "config" for r in failed):
            raise RuntimeError(message)
        raise AirflowFailException(message)
    if short and _flag(config.get("fail_on_shortfall"), False):
        logger.error("sampling report:\n%s", text)
        raise AirflowFailException("sample(s) short of target_volume: " + ", ".join(
            f"config {r['config_id']} {r['selected']}/{r['target_volume']}" for r in short))
    (logger.warning if short else logger.info)("sampling report:\n%s", text)
    return json.loads(text)


# ---------------------------------------------------------------------------
# The DAG
# ---------------------------------------------------------------------------

def sample_summary(ti, task_ids: list) -> str:
    """HTML table of the samples a DAG run made (from its tasks' reports): counts and names only, never case data."""
    runs = []
    for task_id in task_ids:
        report = ti.xcom_pull(task_ids=task_id) or {}
        runs += report.get("runs", []) if isinstance(report, dict) else []
    if not runs:
        return "<p>No sample was made.</p>"
    head = ("Sample", "Run key", "As of", "Status", "Selected", "Target", "Short", "Output table", "Rules (hash)")
    cells = [[r.get("sample_name") or r.get("config_id"), r.get("run_key"), r.get("as_of_date"), r.get("status"),
              r.get("selected"), r.get("target_volume"), r.get("shortfall"), r.get("output_table") or "",
              str(r.get("config_hash") or "")[:12]] for r in runs]
    rows = "".join("<tr>" + "".join(f"<td>{html.escape(str(c if c is not None else ''))}</td>" for c in row)
                   + "</tr>" for row in cells)
    return ("<table border=\"1\" cellpadding=\"4\" cellspacing=\"0\"><tr>"
            + "".join(f"<th>{h}</th>" for h in head) + f"</tr>{rows}</table>"
            + "<p>The selected cases are in gre_sample_selections (selected_flag = 1, etl_is_curr_ind = 'Y') "
              "and in the output table.</p>")


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
    """A list setting of the Variable (trigger_dags, tags) given as a list or as one comma-separated string."""
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
    """run_sampling, or run_sampling_<project>_<process>_<config> per `runs` entry (numbered when they clash)."""
    if len(scopes) == 1:
        return "run_sampling"

    def base(s: dict, i: int) -> str:
        name = "_".join(str(s[k]) for k in ("name", "project", "process_name", "config_id") if s.get(k)) or str(i + 1)
        return "run_sampling_" + "".join(c if c.isalnum() else "_" for c in name).lower()
    names = [base(s, i) for i, s in enumerate(scopes)]
    return names[index] if names.count(names[index]) == 1 else f"{names[index]}_{index + 1}"


def _overrides(context) -> dict:
    """What the trigger gave (its configuration, else the form's parameters); empty values are left out."""
    conf = _keys(context["dag_run"].conf) if context.get("dag_run") else {}
    for name in TRIGGER_KEYS:
        if name not in conf and context["params"].get(name) not in (None, "", []):
            conf[name] = context["params"][name]
    return {k: v for k, v in conf.items() if v not in (None, "", [])}


def build_sampling_dag(dag_id: str, variable_key: str) -> DAG:
    """The sampling DAG; everything about it comes from its own Variable."""
    dag_config = load_dag_config(variable_key)
    tz = dag_config.get("schedule_timezone") or dag_config.get("timezone") or DEFAULT_TIMEZONE
    try:
        start_date = pendulum.datetime(2026, 1, 1, tz=tz)
    except Exception:  # noqa: BLE001
        tz = DEFAULT_TIMEZONE
        start_date = pendulum.datetime(2026, 1, 1, tz=tz)
    default_args = {
        "owner": dag_config.get("owner") or "oss",
        "depends_on_past": False,
        "start_date": start_date,
        "retries": _retries(dag_config),
        "retry_delay": _minutes(dag_config, "retry_delay_minutes", timedelta(minutes=5)),
        "on_failure_callback": _failure_alert_ticket(dag_config.get("snow_assignment_group")
                                                     or DEFAULT_ASSIGNMENT_GROUP),
    }
    params = {name: Param(None, type=_TEXT) for name in TRIGGER_KEYS}
    dag = DAG(dag_id=dag_id, description=f"config-driven stratified sampling ({variable_key})",
              default_args=default_args, schedule=dag_config.get("schedule_interval"), catchup=False,
              max_active_runs=1, dagrun_timeout=_minutes(dag_config, "dagrun_timeout_minutes", timedelta(hours=8)),
              params=params, user_defined_macros={"conf_value": conf_value,
                                                  "sample_summary": lambda ti: sample_summary(ti, task_ids)},
              tags=_names(dag_config.get("tags")) or DEFAULT_TAGS)

    def run(scope_index: Optional[int], **context):
        return run_sampling_task(variable_key, overrides=_overrides(context), scope_index=scope_index,
                                 dag_run=context.get("dag_run"))

    try:
        scopes = run_scopes(dag_config)
    except ConfigError as e:
        logger.warning("%s: %s", dag_id, e)
        scopes = [{}]
    timeout = _minutes(dag_config, "execution_timeout_minutes", DEFAULT_TIMEOUT)
    pool = {"pool": dag_config["pool"]} if dag_config.get("pool") else {}
    task_ids = []   # filled below: the summary macro reads the run tasks' reports
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

    email_recipient = dag_config.get("email_recipient")
    if email_recipient:
        load_env = dag_config.get("load_env") or dag_config.get("environment") or ""
        upstream = [*tasks, *triggers]
        success = EmailOperator(task_id="success_email", dag=dag, to=email_recipient,
                                trigger_rule="none_failed_min_one_success",
                                subject=f"Airflow EDEG-Compliance {load_env} Success: {dag_id} DAG",
                                html_content=f"DAG {dag_id} run {{{{ run_id }}}} completed successfully")
        fail = EmailOperator(task_id="fail_email", dag=dag, to=email_recipient, trigger_rule="one_failed",
                             subject=f"Airflow EDEG-Compliance {load_env} Failure: {dag_id} DAG",
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
