# Stratified Sampling: Airflow + Teradata

A config-driven stratified sampling engine that runs from an Airflow DAG with its metadata tables and its
source tables in **Teradata**. It follows the layout and connection style of the GOVT_RGLTRY framework's
Airflow package (`framework/airflow`, branch `feature/framework-package`): one DAG file, one DAG id, one
`<name>_config_var` Variable, the Teradata login from the Airflow Connection named in its `connection_id`, read with
`BaseHook.get_connection`.

The engine itself (`sampling/engine.py`, `db.py`, `settings.py`) exists once, in `airflow/sampling`: the local
version (`local/`, see [Local version](#local-version-command-line)) imports it from there. Only `dags.py` imports
Airflow; a test checks that the local command line starts in a Python without Airflow.

```
airflow/                                    copied into the DAGs folder
  govt_rgltry_umcm_sampling.py           the DAG file: dag_id + Variable name only
  sampling/
    dags.py        the DAG (schedule, tasks, triggers, emails, ServiceNow callback), Variable + Connection ->
                   the engine (the only Airflow-aware module)
    engine.py      the sampling algorithm, audit, errors, scope runs, validation
    db.py          Teradata connection wrapper, transactions, {key} parameters, run ids
    settings.py    meta_db, environment ($env), time zone, batch size
    .airflowignore keeps Airflow from parsing the package as DAG files (deploy it too: a dot file)

airflow_resources/                          (not deployed with the DAGs)
  docs/AIRFLOW_DEPLOYMENT.md                this guide
  docs/OPERATIONAL_RUNBOOK.md               tables, values, procedures, monitoring
  docs/OPEN_ITEMS.md                        what is still open
  examples/umcm_dev/                        the Variable (file name = Variable name), DEV
  requirements.txt                          Python packages for the workers

sql/                                        shared by both versions
  schema.sql                                Teradata DDL ({{META_DB}} template), 5 tables
  schema_drop.sql                           drops them (fresh redeploy only)

local/                                      the local version (command line, dev.env); uses airflow/sampling
```

## How it fits together

1. **One DAG, one Variable.** The DAG file only names them; the schedule, the Connection, the scope and the
   settings all come from the Variable.

   | DAG id | Variable | DAG file |
   |---|---|---|
   | `GOVT_RGLTRY_UMCM_SAMPLING` | `govt_rgltry_umcm_sampling_config_var` | `govt_rgltry_umcm_sampling.py` |

2. **One Airflow Connection**: Teradata, named by the Variable's `connection_id`. The
   bridge reads it with `BaseHook.get_connection` and exports it as `TERADATA_HOST` / `TERADATA_USER` /
   `TERADATA_PASSWORD` / `TERADATA_LOGMECH`; the engine opens one Teradata
   session and uses it for the metadata tables and for the source tables (`Src_Schema_Nm` / `Src_Table_Nm` of the
   configuration rows, so they may sit in any database that login can read).
3. **A task runs one sampling scope** in-process: every active sample of a `project`, optionally only one `table`
   and / or one `run_type`. A sample is one project + table + run type (`GOVT_RGLTRY_SamplingConfig`), as the
   framework scopes its batches; one that fails does not stop the others. The task's report (JSON) is in the log
   and is the task's XCom.
4. **The task fails** on a configuration problem at once (no retry), and on a technical error (connection, a
   failed query, a failed write) after `retries` (default 1, 5 minutes apart). Every run is idempotent: a retry or
   a rerun writes a new attempt and marks the earlier one not current.

No `.env` file and no secrets manager: on Airflow the credentials come from the Connection only and the settings
from the Variable only (a `dev.env` or `SAMPLING_*` variables on a worker are ignored; they apply to the local
version).

## Deploying

1. Copy `govt_rgltry_umcm_sampling.py` and the `sampling/` folder (with its `.airflowignore`) into the DAGs
   folder, side by side.
2. Install `airflow_resources/requirements.txt` in the workers' environment. **Airflow 3** (Task SDK `airflow.sdk`,
   the `standard` and `smtp` providers; no Airflow 2 or deprecated imports), validated on **3.3.0**. The email tasks
   use the Airflow Connection `smtp_default`.
3. Create the tables once, in the metadata database, from `sql/schema.sql` (replace `{{META_DB}}`), e.g. from a
   machine with the local version: `python local/run_sampling.py deploy-schema`. The engine never creates or alters
   tables.
4. Enter the configuration rows (`GOVT_RGLTRY_SamplingConfig`, `GOVT_RGLTRY_SamplingRule`; there are no seed files:
   the rows are entered in the tables, see the runbook sections 4 and 7, and for ODAG1 OPEN_ITEMS.md section D), then run
   `python local/run_sampling.py validate --project UMCM --check-sql`.
5. Create the Connection and the Variable (below; ready-made for DEV in `airflow_resources/examples/umcm_dev`).
6. Unpause the DAG; trigger it once with `{"dry_run": "true"}` to see the counts without writing anything.

Another project gets its own DAG: copy the DAG file, give the copy its own `dag_id` and `variable_key`, and create
that Variable. The tables and run types of one project share one DAG: the Variable's scope (a project, optionally a
table and a run type), or `runs` entries, each with its own `schedule_interval` when the run types run at different
times (daily, weekly, monthly: see [Timed runs](#timed-runs)).

### Airflow Connection

**Teradata** - read with `BaseHook.get_connection`: the Connection named by the Variable's `connection_id`
(falls back to the framework's `td_conn_var`, then `govt_rgltry_teradata`). `connection_type`, when given, must be `teradata` (the only one: the engine reads and writes Teradata).
Host, login and password become `TERADATA_HOST` / `TERADATA_USER` / `TERADATA_PASSWORD`; extra `logmech` becomes
`TERADATA_LOGMECH` (else that variable, else LDAP). The password is never logged.

| Field | Value |
|---|---|
| Connection Type | Teradata, or Generic |
| Host / Login / Password | the Teradata system and the service account |
| Extra (optional) | `{"logmech": "LDAP"}` (default LDAP) |

The account needs, on the metadata database: SELECT / INSERT / UPDATE (no CREATE; DELETE only for the retention
task); on each source table (or view): SELECT; on each target table: SELECT / INSERT / UPDATE.

### Airflow Variable (`govt_rgltry_umcm_sampling_config_var`)

A JSON object; keys are not case sensitive. Example (`airflow_resources/examples/umcm_dev`):

```json
{
  "connection_type": "teradata",
  "connection_id": "DEV",
  "meta_db": "HSABC_DEV",
  "load_env": "DEV",
  "email_recipient": "TRINATH_gundavaram@example.com",
  "project": "UMCM",
  "table": "ODAG1",
  "run_type": "WKLY",
  "run_params": {},
  "schedule_interval": null,
  "log_level": "INFO",
  "retries": 1,
  "execution_timeout_minutes": 120,
  "fail_on_shortfall": false,
  "tags": ["GOVT_RGLTRY", "Sampling", "UMCM"],
  "trigger_dags": []
}
```

| Key | Meaning |
|---|---|
| `connection_id` | Airflow Connection of Teradata. Falls back to `td_conn_var`, then `govt_rgltry_teradata` |
| `connection_type` | Optional; `teradata` (any other value is refused) |
| `meta_db` | **Required.** Teradata database holding the `GOVT_RGLTRY_SAMPL*` tables. May carry `$env` (`HSABC_$ENV`: `HSABC_DEV` in DEV, `HSABC` in UAT / PROD) |
| `load_env` | DEV / QA / INT / UAT / PROD: replaces `$env` in every configured table name and SQL text (default DEV). `environment` is accepted too |
| `env_value` | Replacement text for `$env` when the environment's default does not fit (e.g. `uat`) |
| `project` / `table` / `run_type` | **The scope**: every active sample of the project (required), optionally only one table and / or one run type. Matched against `GOVT_RGLTRY_SamplingConfig` (`Project_Cd`, `Table_Nm`, `Run_Ty`) in any case |
| `run_key` | Which logical run this is; may hold parameters, e.g. `"{batch_id}"` (a PARAM rule's lookup). Default: the as-of date (`YYYY-MM-DD`). A rerun with the same `run_key` replaces the earlier sample as the current one |
| `run_params` | JSON object of values for `{key}` / `$key` tokens in any configured SQL. A value given here wins over the config's PARAM rule of the same name. `{run_key}`, `{as_of_date}`, `{project_cd}`, `{table_nm}` and `{run_ty}` (the sample's own) are always available |
| `extra_filters` | Optional SQL condition on the source table (or a list of them), ANDed to `Scope_Sql_Txt` for the run: which load, batch or SCD2 version of an incremental table to read, e.g. `"ETL_IS_CURR_IND = 'Y'"`. May use `{batch_id}` and other run parameters; checked like any configured SQL. The Variable's, a `runs` entry's and a trigger's conditions are all ANDed |
| `as_of` | Business date `YYYY-MM-DD` of the run (default: the DAG run's time in Central time) -> `As_Of_Dt_Key`, `Smpl_Cycle_Dt_Key` |
| `seed` | RANDOM / SYSTEMATIC seed (default: a new one per run, stored on the run row) |
| `runs` | Optional list of scopes, e.g. `[{"name": "weekly", "run_type": "WKLY"}, {"name": "odag1_monthly", "table": "ODAG1", "run_type": "MNT", "run_params": {...}}]`. Each entry is layered over the Variable (an entry naming only a run type samples the Variable's project and table at that run type). Without `schedule_interval` in the entries: one task each, all on the Variable's schedule. With it: [timed runs](#timed-runs) |
| `schedule_interval` | Cron (Central time) or null for manual / triggered only. In a `runs` entry: that entry's own times |
| `schedule_timezone` | Default `America/Chicago` |
| `timezone` | Business time zone for the as-of date and the timestamps written (default `America/Chicago`) |
| `log_level` | `INFO` (default) or `DEBUG` (adds every SQL statement's text and row count; never data) |
| `retries` | Retries after a technical error (default 1, 0 = never). A configuration problem is never retried. A GATE rule not met (`GATE_NOT_MET`, the data is not ready) is retried the same way |
| `retry_delay_minutes` | Minutes between retries (default 5). With GATE rules, `retries` × `retry_delay_minutes` is how long a run waits for the data |
| `execution_timeout_minutes` / `dagrun_timeout_minutes` | Task limit (default 120) / DAG run limit (default 480) |
| `fail_on_shortfall` | true = fail the task when a sample has fewer than `Target_Cnt` cases (default false: a warning) |
| `chunk_rows` | Rows per fetch and per insert batch (default 1000) |
| `access_lock` | `LOCKING ROW FOR ACCESS` on reads (default true) |
| `email_recipient` | Adds `success_email` / `fail_email` tasks (the support team) |
| `retention_days` | Adds `purge_selections` after the runs: deletes the `GOVT_RGLTRY_SampleSelection` rows older than this that nothing needs (OPERATIONAL_RUNBOOK.md section 12). Must cover the history rules' `Lookback_Days`, else that sample is skipped. Leave out until the retention period is decided (OPEN_ITEMS Q14) |
| `notify_recipients` | Adds `sample_ready_email` after a successful run (the auditors): per sample its run key, status, selected / target, shortfall, target table and rules hash. Counts and names only, never case data |
| `trigger_dags` | DAG ids started after a successful run (passing `project`, `table`, `run_type`, `run_key`, `as_of`) |
| `trigger_run_params` | Optional list of keys an upstream DAG passes that become run parameters (`{name}` tokens in the rules), e.g. the framework's `["run_type", "table"]` |
| `trigger_keys` | Optional list of the keys a trigger may set (default: all of `project`, `table`, `run_type`, `run_key`, `run_params`, `extra_filters`, `seed`, `as_of`, `dry_run`, `log_level`). Use it when an upstream DAG passes values this DAG must not take - the framework passes its own `project` - e.g. `["run_key", "as_of"]` |
| `snow_assignment_group` | ServiceNow assignment group of the failure ticket |
| `owner`, `tags`, `pool` | As usual |

### Triggering a run with values (Trigger DAG w/ config, CLI `-c`, REST API)

A trigger may change only the scope and the run's inputs: `project`, `table`, `run_type`, `run_key`,
`run_params`, `extra_filters`, `seed`, `as_of`, `dry_run`, `log_level` (and `run` on a timed DAG). Anything else (the
database, the Connection, the environment) is ignored with a warning - set it in the Variable. Examples:

```json
{"table": "ODAG1", "run_type": "WKLY", "run_key": "2026-10-30", "run_params": {"pull_date": "2026-10-30"}}
{"table": "ODAG1", "run_type": "WKLY", "run_key": "B42", "extra_filters": "BTCH_ID = 'B42'"}
{"run_type": "MNT", "as_of": "2026-10-01", "run_key": "BACKFILL_2026_10"}
{"table": "ODAG1", "dry_run": "true"}
{"run": "weekly, monthly"}
```

### Timed runs

When the run types of a project run at different times, give each `runs` entry a `name` and its own
`schedule_interval`, as in the framework. The DAG then has one task, `run_sampling`, scheduled at every entry's time
(`MultipleCronTriggerTimetable`, Central time); a scheduled DAG run samples the entries whose cron matches its time,
a trigger may name entries (`{"run": "monthly"}`, comma separated), and a manual run without `run` samples the
Variable's own scope. One entry that fails does not stop the others; the task fails at the end (retried only when
an entry failed for a technical reason).

```json
{"project": "UMCM", "table": "ODAG1",
 "runs": [{"name": "daily",   "run_type": "DLY",  "schedule_interval": "0 6 * * *"},
          {"name": "weekly",  "run_type": "WKLY", "schedule_interval": "0 8 * * 5"},
          {"name": "monthly", "run_type": "MNT",  "schedule_interval": "0 9 1 * *"}]}
```

### Started by the GOVT_RGLTRY framework

A framework step DAG starts the DAGs in its own Variable's `trigger_dags` when it succeeds, passing `project`,
`table_group`, `table`, `run_type`, `source` and `as_of`, and waits for them to finish (it fails if the triggered
DAG fails). To sample after a framework step (which one: OPEN_ITEMS Q11), no code changes on either side:

1. In the framework step's Variable, add `GOVT_RGLTRY_UMCM_SAMPLING` to `trigger_dags`.
2. In this DAG's Variable: `"schedule_interval": null`, `"trigger_keys": ["as_of"]` (the framework's `project` is
   its own, not this DAG's scope), and `"trigger_run_params": [...]` for any of its values the rules use.
3. Give the samples a PARAM rule for the batch (e.g. the latest closed batch of the framework's control table for
   `{project_cd}` / `{table_nm}` / `{run_ty}`: one row with `Table_Nm` / `Run_Ty` `*` serves every sample of the
   project) and `"run_key": "{batch_id}"`, and a GATE rule that returns a row only when that batch is ready, with
   `retries` and `retry_delay_minutes` covering how late it can be. The control table's name is configuration (the
   rule's SQL), never code.

```json
{"schedule_interval": null, "trigger_keys": ["as_of"], "trigger_run_params": ["run_type"],
 "run_key": "{batch_id}", "retries": 6, "retry_delay_minutes": 30}
```

## DAG

- One task, `run_sampling` (or `run_sampling_<name>` per `runs` entry; one `run_sampling` for timed runs), then a `trigger_<dag id>` task per
  `trigger_dags` entry, `purge_selections` with `retention_days`, then `success_email` / `fail_email`, and
  `sample_ready_email` with `notify_recipients`.
  `max_active_runs` is 1.
- A missing or invalid Variable never breaks parsing: the DAG is still there, unscheduled, and its task fails with
  the reason when run.
- Every task has `on_failure_callback`: a failed task opens a ServiceNow incident through
  `snow.snow_integrations.create_incident(context, assignment_group)`; without the `snow` package a warning is
  logged instead (same as the GOVT_RGLTRY framework).

## What the log shows

`sampling <version> | settings {...} | scope {...} | run_key=... as_of=...`, the Teradata host and user (never the
password), the candidate pull SQL of each sample, then the report:

```json
{
  "result": "ALL SAMPLES COMPLETED",
  "scope": {"project": "UMCM", "table": "ODAG1", "run_type": "WKLY"},
  "runs": [{
      "project_cd": "UMCM", "table_nm": "ODAG1", "run_ty": "WKLY", "status": "COMPLETED",
      "smpl_run_id": "UMCM.ODAG1.WKLY::2026-10-30::attempt-1::airflow-scheduled::20261030T080001.204512::a1b2c3",
      "run_key": "2026-10-30", "as_of_date": "2026-10-30", "method": "RANKED", "seed": null,
      "target_cnt": 150, "in_scope": 1000, "excluded": 20, "candidates": 980, "selected": 150, "shortfall": 0,
      "retired_runs": 0, "exclusions": {"AUTO_APPROVED": 12, "...": 0},
      "buckets": {"Denied": {"candidates": 777, "target": 120, "selected": 118}, "...": {}},
      "by_stratum": {"Denied/Part B": {"candidates": 96, "target": 15, "selected": 15}, "...": {}}
  }],
  "failed": 0
}
```

A failed sample has `status: ERROR`, `error_kind` (`config` / `technical`), `error_type` and `error`. Row data from
the source is never logged.

## Local version (command line)

`local/` runs the same engine (imported from `airflow/sampling`) with a command line, for development, DEV rehearsal and admin work. Credentials
come from `local/dev.env` (copy `dev.env.example`; gitignored) or the shell.

```bash
cd local
cp dev.env.example dev.env          # fill in TERADATA_* and SAMPLING_META_DB
pip install -r requirements.txt
python run_sampling.py test-connection
python run_sampling.py deploy-schema --dry-run
python run_sampling.py validate --project UMCM --check-sql
python run_sampling.py run --project UMCM --run-type WKLY --as-of 2026-10-30 --dry-run
python run_sampling.py run --project UMCM --table ODAG1 --run-type WKLY --run-key 2026-10-30 --run-param pull_date=2026-10-30
python run_sampling.py runs --project UMCM --table ODAG1
python run_sampling.py errors
python run_sampling.py mark-failed --run-id <run_id>
```

| Airflow Variable | dev.env / shell |
|---|---|
| Connection in `connection_id` | `TERADATA_HOST`, `TERADATA_USER`, `TERADATA_PASSWORD`, `TERADATA_LOGMECH` |
| `meta_db` | `SAMPLING_META_DB` |
| `load_env` / `env_value` | `SAMPLING_ENVIRONMENT` / `SAMPLING_ENV_VALUE` |
| `timezone`, `chunk_rows`, `access_lock` | `SAMPLING_TIMEZONE`, `SAMPLING_CHUNK_ROWS`, `SAMPLING_ACCESS_LOCK` |
| `log_level` | `SAMPLING_LOG_LEVEL` (console plus one file per run in `local/logs`, or `SAMPLING_LOG_DIR`) |
| scope (`--project`, `--table`, `--run-type`), `run_key`, `run_params`, `extra_filters`, `as_of`, `seed`, `dry_run` | command options (`--filter "SQL"`, repeat, for `extra_filters`) |

Exit codes: 0 = done, 1 = done with problems (a sample failed or fell short, validation found problems),
2 = could not run. A variable set in the shell wins over `dev.env`.

## Time zone

The as-of date, the run ids and every timestamp the engine writes (`Start_Dtts`, `End_Dtts`, `Load_Dtts`,
`LastUpdt_Dtts`) are Central time (`America/Chicago`), as in the GOVT_RGLTRY framework. The
schedule is Central time too. Column defaults (`CURRENT_TIMESTAMP`, and `USER` for `LoadUser_ID` / `LastUpdt_ID`) are the Teradata system's.

## Environment in names (`$env`)

`Src_Schema_Nm`, `Tgt_Schema_Nm`, `Scope_Sql_Txt` and every rule's SQL can carry `$env` so one set of configuration
rows works in every environment (the framework convention): `UMCM_$ENV_T` is `UMCM_DEV_T` in DEV, `UMCM_QA_T` in QA
and `UMCM_T` in UAT and PROD. The token keeps its casing (`$env` -> `dev`,
`$ENV` -> `DEV`); `env_value` overrides the replacement for one environment.

## Teradata specifics

- 5 MULTISET tables (`sql/schema.sql`), named and typed as the framework's (`_Cd`, `_Nm`, `_Txt`, `_Ind`,
  `_Cnt`, `_Dtts`, the `Active_Ind` / `Load_Dtts` / `LoadUser_ID` / `LastUpdt_Dtts` / `LastUpdt_ID` audit columns);
  no CLOB, no identity columns, no secondary indexes, no foreign keys (`validate` checks the configuration). Names
  in the rows (project, table, run type, columns) are matched in any case.
- A cycle whose sample was finalised (`finalize`, local version) is never replaced by the DAG: the task fails as
  `FINALIZED` without retry, and the final sample stays current. Replacing it needs `--force-reason` from the
  command line; there is no trigger key for it.
- `Curr_Ind = 1` on `GOVT_RGLTRY_SamplingRun` and `GOVT_RGLTRY_SampleSelection` marks the current sample of each
  project + table + run type and `Run_Key`. Errors are on the run row (`Error_Ty`, `Error_Txt`, the end of the
  traceback in `Error_Detail_Txt`); the counts per exclusion rule and bucket are its `Run_Rpt_Txt` (JSON in a
  VARCHAR(16000), e.g. `NEW JSON(Run_Rpt_Txt).JSONExtractValue('$.buckets.Denied.selected')`); the configuration a
  run used is on its run row and its rules in `GOVT_RGLTRY_SamplingRunRule`.
- Reads use `LOCKING ROW FOR ACCESS` (no read lock on the source or the metadata tables; switch off with
  `access_lock: false`).
- A run's writes (every candidate, the target table rows, retiring the earlier run, closing the run row) are **one
  transaction**: a failure leaves nothing half written, only the run row with status ERROR.
- Bucket values compare like Teradata does: trailing blanks ignored, not case specific (`'DENIED  '` is in the
  `Denied` bucket).
- Writes are batched `executemany` of `chunk_rows` rows. The candidate pull is fetched `chunk_rows` at a time, but
  the whole candidate set is held in memory (stratifying needs every candidate's bucket): keep `Scope_Sql_Txt` tight.

## Validation status

- The engine passes its test suite (115 tests, 14 of them one per configurable rule of gaps A-G) against a stand-in for `teradatasql` backed by DuckDB, including the
  frozen UM regression of `DQ_COMPLETE_REPO/sampling` (same per-bucket counts).
- **Airflow 3.3.0** (installed with its official constraints: Task SDK 1.3.0, standard provider 1.15.0, smtp
  provider 3.0.1): the DAG file parses without import errors or warnings from this code, also with
  `DAG_DISCOVERY_SAFE_MODE` off; the task was run in-process through `run_sampling_task()` with the Variable and
  Connection as `AIRFLOW_VAR_*` / `AIRFLOW_CONN_*`: success, a configuration problem (AirflowFailException, no
  retry), a technical error (RuntimeError, retried), dry run and trigger values.
- It has **not** been run against a real Teradata system or a running Airflow scheduler. Before production, on
  DEV: create the tables, load the configuration, run `validate --check-sql`, a dry run and a real run from the
  local version, then trigger the DAG, and check the tables (runbook section 11). See `OPEN_ITEMS.md`.
