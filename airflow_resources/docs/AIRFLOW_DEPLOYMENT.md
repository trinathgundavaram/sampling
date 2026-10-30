# Stratified Sampling: Airflow + Teradata

A config-driven stratified sampling engine that runs from an Airflow DAG with its metadata tables and its
universe tables in **Teradata**. It follows the layout and connection style of the CMS Compliance Framework's
Airflow package (`framework/airflow`, branch `feature/framework-package`): one DAG file, one DAG id, one
`<name>_config_var` Variable, the Teradata login from the Airflow Connection named in `td_conn_var`.

The engine itself (`sampling/engine.py`, `db.py`, `settings.py`) is the same code in the Airflow version and the
local version (`local/`, see [Local version](#local-version-command-line)); a test fails if the two copies drift.

```
airflow/                                    copied into the DAGs folder
  cms_compliance_umcm_sampling.py           the DAG file: dag_id + Variable name only
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
  schema.sql                                Teradata DDL ({{META_DB}} template), 4 tables
  schema_drop.sql                           drops them (fresh redeploy only)
  output_table_template.sql                 DDL template of a config's output snapshot table
  seed/umcm_sample_config.sql               example configuration rows (every kind of rule once)
  seed/odag1_config_template.sql            the ODAG1 (COMO) configuration, inactive, TODOs open

local/                                      the local version (command line, dev.env)
```

## How it fits together

1. **One DAG, one Variable.** The DAG file only names them; the schedule, the Connection, the scope and the
   settings all come from the Variable.

   | DAG id | Variable | DAG file |
   |---|---|---|
   | `CMS_COMPLIANCE_UMCM_SAMPLING` | `cms_compliance_umcm_sampling_config_var` | `cms_compliance_umcm_sampling.py` |

2. **One Airflow Connection**: Teradata, named by the Variable's `td_conn_var`. The bridge exports it as
   `TERADATA_HOST` / `TERADATA_USER` / `TERADATA_PASSWORD` / `TERADATA_LOGMECH`; the engine opens one Teradata
   session and uses it for the metadata tables and for the universe tables (which are fully qualified in the
   configuration rows, so they may sit in any database that login can read).
3. **A task runs one sampling scope** in-process: one `config_id`, or every active config of a `project` and / or
   `process_name`. Each config is one sample; one that fails does not stop the others. The task's report (JSON) is
   in the log and is the task's XCom.
4. **The task fails** on a configuration problem at once (no retry), and on a technical error (connection, a
   failed query, a failed write) after `retries` (default 1, 5 minutes apart). Every run is idempotent: a retry or
   a rerun writes a new attempt and marks the earlier one not current.

No `.env` file and no secrets manager: on Airflow the credentials come from the Connection only and the settings
from the Variable only (a `dev.env` or `SAMPLING_*` variables on a worker are ignored; they apply to the local
version).

## Deploying

1. Copy `cms_compliance_umcm_sampling.py` and the `sampling/` folder (with its `.airflowignore`) into the DAGs
   folder, side by side.
2. Install `airflow_resources/requirements.txt` in the workers' environment. **Airflow 3** (Task SDK `airflow.sdk`,
   the `standard` and `smtp` providers; no Airflow 2 or deprecated imports), validated on **3.3.0**. The email tasks
   use the Airflow Connection `smtp_default`.
3. Create the tables once, in the metadata database, from `sql/schema.sql` (replace `{{META_DB}}`), e.g. from a
   machine with the local version: `python local/run_sampling.py deploy-schema`. The engine never creates or alters
   tables.
4. Load the configuration rows (`gre_sampling_config`, `gre_sampling_rule`; example:
   `sql/seed/umcm_sample_config.sql`, see the runbook section 4), then run
   `python local/run_sampling.py validate --project UMCM --check-sql`.
5. Create the Connection and the Variable (below; ready-made for DEV in `airflow_resources/examples/umcm_dev`).
6. Unpause the DAG; trigger it once with `{"dry_run": "true"}` to see the counts without writing anything.

Another sample (another project or cadence) with its own schedule gets its own DAG: copy the DAG file, give the
copy its own `dag_id` and `variable_key`, and create that Variable. Several samples on the same schedule can share
one DAG through `process_name` or `runs` (below).

### Airflow Connection

**Teradata** - the Connection named by the Variable's `td_conn_var` (falls back to `connection_id`, then
`compliance_teradata`). The same Connection the compliance framework uses works here.

| Field | Value |
|---|---|
| Connection Type | Teradata, or Generic |
| Host / Login / Password | the Teradata system and the service account |
| Extra (optional) | `{"logmech": "LDAP"}` (default LDAP) |

The account needs, on the metadata database: SELECT / INSERT / UPDATE (no CREATE, no DELETE); on each universe
table (or view): SELECT.

### Airflow Variable (`cms_compliance_umcm_sampling_config_var`)

A JSON object; keys are not case sensitive. Example (`airflow_resources/examples/umcm_dev`):

```json
{
  "td_conn_var": "DEV",
  "meta_db": "HSABC_DEV",
  "load_env": "DEV",
  "email_recipient": "TRINATH_gundavaram@example.com",
  "project": "UMCM",
  "process_name": "UMCM_WEEKLY_SAMPLE",
  "run_params": {},
  "schedule_interval": null,
  "log_level": "INFO",
  "retries": 1,
  "execution_timeout_minutes": 120,
  "fail_on_shortfall": false,
  "tags": ["Compliance", "Sampling", "UMCM"],
  "trigger_dags": []
}
```

| Key | Meaning |
|---|---|
| `td_conn_var` | Airflow Connection of Teradata. Falls back to `connection_id`, then `compliance_teradata` |
| `meta_db` | **Required.** Teradata database holding the `gre_sampling_*` tables |
| `load_env` | DEV / QA / INT / UAT / PROD: replaces `$env` in every configured table name and SQL text (default DEV). `environment` is accepted too |
| `env_value` | Replacement text for `$env` when the environment's default does not fit (e.g. `uat`) |
| `config_id` / `project` / `process_name` | **The scope** (at least one): one config, or every active config of the project and / or process. Matched against `gre_sampling_config` |
| `run_key` | Which logical run this is; may hold parameters, e.g. `"{batch_id}"` (a PARAM rule's lookup). Default: the as-of date (`YYYY-MM-DD`). A rerun with the same `run_key` replaces the earlier sample as the current one |
| `run_params` | JSON object of values for `{key}` / `$key` tokens in any configured SQL. A value given here wins over the config's PARAM rule of the same name. `{run_key}` and `{as_of_date}` are always available |
| `as_of` | Business date `YYYY-MM-DD` of the run (default: the DAG run's time in Central time) -> `sample_cycle` |
| `seed` | RANDOM / SYSTEMATIC seed (default: a new one per run, stored on the run row) |
| `runs` | Optional list of scopes, one task each, e.g. `[{"name": "weekly", "process_name": "UMCM_WEEKLY_SAMPLE"}, {"config_id": 7, "run_params": {...}}]`. An entry that names a scope key replaces the Variable's scope; its other keys are layered over the Variable |
| `schedule_interval` | Cron (Central time) or null for manual / triggered only |
| `schedule_timezone` | Default `America/Chicago` |
| `timezone` | Business time zone for the as-of date and the timestamps written (default `America/Chicago`) |
| `log_level` | `INFO` (default) or `DEBUG` (adds every SQL statement's text and row count; never data) |
| `retries` | Retries after a technical error (default 1, 0 = never). A configuration problem is never retried |
| `execution_timeout_minutes` / `dagrun_timeout_minutes` | Task limit (default 120) / DAG run limit (default 480) |
| `fail_on_shortfall` | true = fail the task when a sample has fewer than `target_volume` cases (default false: a warning) |
| `chunk_rows` | Rows per fetch and per insert batch (default 1000) |
| `access_lock` | `LOCKING ROW FOR ACCESS` on reads (default true) |
| `email_recipient` | Adds `success_email` / `fail_email` tasks |
| `trigger_dags` | DAG ids started after a successful run (passing `project`, `process_name`, `run_key`, `as_of`) |
| `trigger_keys` | Optional list of the keys a trigger may set (default: all of `config_id`, `project`, `process_name`, `run_key`, `run_params`, `seed`, `as_of`, `dry_run`, `log_level`). Use it when an upstream DAG passes values this DAG must not take - the framework passes its own `project` - e.g. `["run_key", "as_of"]` |
| `snow_assignment_group` | ServiceNow assignment group of the failure ticket |
| `owner`, `tags`, `pool` | As usual |

### Triggering a run with values (Trigger DAG w/ config, CLI `-c`, REST API)

A trigger may change only the scope and the run's inputs: `config_id`, `project`, `process_name`, `run_key`,
`run_params`, `seed`, `as_of`, `dry_run`, `log_level`. Anything else (the database, the Connection, the
environment) is ignored with a warning - set it in the Variable. Examples:

```json
{"config_id": "1", "run_key": "2026-10-30", "run_params": {"pull_date": "2026-10-30"}}
{"process_name": "UMCM_WEEKLY_SAMPLE", "as_of": "2026-10-23", "run_key": "BACKFILL_2026_10_23"}
{"config_id": "1", "dry_run": "true"}
{"config_id": "1", "run_key": "2026-10-30", "seed": "1234567890123"}
```

## DAG

- One task, `run_sampling` (or `run_sampling_<name>` per `runs` entry), then a `trigger_<dag id>` task per
  `trigger_dags` entry, then `success_email` / `fail_email`. `max_active_runs` is 1.
- A missing or invalid Variable never breaks parsing: the DAG is still there, unscheduled, and its task fails with
  the reason when run.
- Every task has `on_failure_callback`: a failed task opens a ServiceNow incident through
  `snow.snow_integrations.create_incident(context, assignment_group)`; without the `snow` package a warning is
  logged instead (same as the compliance framework).

## What the log shows

`sampling <version> | settings {...} | scope {...} | run_key=... as_of=...`, the Teradata host and user (never the
password), the scope count and candidate pull SQL of each config, then the report:

```json
{
  "result": "ALL SAMPLES COMPLETED",
  "scope": {"project": "UMCM", "process_name": "UMCM_WEEKLY_SAMPLE"},
  "runs": [{
      "config_id": 1, "sample_name": "UMCM_WEEKLY_SAMPLE", "status": "COMPLETED",
      "sample_run_id": "UMCM.UMCM_WEEKLY_SAMPLE::2026-10-30::attempt-1::airflow-scheduled::20261030T080001.204512::a1b2c3",
      "run_key": "2026-10-30", "as_of_date": "2026-10-30", "method": "RANKED", "seed": null,
      "target_volume": 150, "in_scope": 1000, "excluded": 20, "candidates": 980, "selected": 150, "shortfall": 0,
      "retired_runs": 0, "exclusions": {"AUTO_APPROVED": 12, "...": 0},
      "buckets": {"Denied": {"candidates": 777, "target": 120, "selected": 118}, "...": {}},
      "by_stratum": {"Denied/Part B": {"candidates": 96, "target": 15, "selected": 15}, "...": {}}
  }],
  "failed": 0
}
```

A failed config has `status: ERROR`, `error_kind` (`config` / `technical`), `error_type` and `error`. Row data from
the universe is never logged.

## Local version (command line)

`local/` holds the same engine with a command line, for development, DEV rehearsal and admin work. Credentials
come from `local/dev.env` (copy `dev.env.example`; gitignored) or the shell.

```bash
cd local
cp dev.env.example dev.env          # fill in TERADATA_* and SAMPLING_META_DB
pip install -r requirements.txt
python run_sampling.py test-connection
python run_sampling.py deploy-schema --dry-run --seed
python run_sampling.py validate --project UMCM --check-sql
python run_sampling.py run --process-name UMCM_WEEKLY_SAMPLE --as-of 2026-10-30 --dry-run
python run_sampling.py run --config-id 1 --run-key 2026-10-30 --run-param pull_date=2026-10-30
python run_sampling.py runs --config-id 1
python run_sampling.py errors
python run_sampling.py mark-failed --run-id <run_id>
```

| Airflow Variable | dev.env / shell |
|---|---|
| Connection in `td_conn_var` | `TERADATA_HOST`, `TERADATA_USER`, `TERADATA_PASSWORD`, `TERADATA_LOGMECH` |
| `meta_db` | `SAMPLING_META_DB` |
| `load_env` / `env_value` | `SAMPLING_ENVIRONMENT` / `SAMPLING_ENV_VALUE` |
| `timezone`, `chunk_rows`, `access_lock` | `SAMPLING_TIMEZONE`, `SAMPLING_CHUNK_ROWS`, `SAMPLING_ACCESS_LOCK` |
| `log_level` | `SAMPLING_LOG_LEVEL` (console plus one file per run in `local/logs`, or `SAMPLING_LOG_DIR`) |
| scope, `run_key`, `run_params`, `as_of`, `seed`, `dry_run` | command options |

Exit codes: 0 = done, 1 = done with problems (a sample failed or fell short, validation found problems),
2 = could not run. A variable set in the shell wins over `dev.env`.

## Time zone

The as-of date, the run ids and every timestamp the engine writes (`started_at`, `ended_at`, `load_datetime` of
selections, `last_updated_datetime`) are Central time (`America/Chicago`), as in the compliance framework. The
schedule is Central time too. Column defaults (`CURRENT_TIMESTAMP`) are the Teradata system's time.

## Environment in names (`$env`)

`universe_table`, `scope_sql` and `exclusion_sql` can carry `$env` so one set of configuration rows works in every
environment (the GRE / framework convention): `UMCM_$ENV_T.um_universe` is `UMCM_DEV_T.um_universe` in DEV,
`UMCM_QA_T...` in QA and `UMCM_T.um_universe` in UAT and PROD. The token keeps its casing (`$env` -> `dev`,
`$ENV` -> `DEV`); `env_value` overrides the replacement for one environment.

## Teradata specifics

- 4 MULTISET tables (`sql/schema.sql`); no identity columns, no foreign keys (`validate` checks the
  configuration).
- `etl_is_curr_ind` on `gre_sampling_run` and `gre_sample_selections` marks the current sample of each
  `(config_id, run_key)`. Errors are on the run row (`error_type`, `error_message`, `error_detail`), and the counts per
  exclusion rule and bucket are its `run_report` (JSON in a CLOB: read it with Teradata's JSON functions, e.g.
  `NEW JSON(run_report).JSONExtractValue('$.buckets.Denied.selected')`).
- Reads use `LOCKING ROW FOR ACCESS` (no read lock on the universe or the metadata tables; switch off with
  `access_lock: false`).
- A run's writes (every candidate, the output snapshot, retiring the earlier run, closing the run row) are **one
  transaction**: a failure leaves nothing half written, only the run row with status ERROR.
- Bucket values compare like Teradata does: trailing blanks ignored, not case specific (`'DENIED  '` is in the
  `Denied` bucket).
- Writes are batched `executemany` of `chunk_rows` rows. The candidate pull is fetched `chunk_rows` at a time, but
  the whole candidate set is held in memory (stratifying needs every candidate's bucket): keep `scope_sql` tight.

## Validation status

- The engine passes its test suite (58 tests, 14 of them one per configurable rule of gaps A-G) against a stand-in for `teradatasql` backed by DuckDB, including the
  frozen UM regression of `DQ_COMPLETE_REPO/sampling` (same per-bucket counts).
- **Airflow 3.3.0** (installed with its official constraints: Task SDK 1.3.0, standard provider 1.15.0, smtp
  provider 3.0.1): the DAG file parses without import errors or warnings from this code, also with
  `DAG_DISCOVERY_SAFE_MODE` off; the task was run in-process through `run_sampling_task()` with the Variable and
  Connection as `AIRFLOW_VAR_*` / `AIRFLOW_CONN_*`: success, a configuration problem (AirflowFailException, no
  retry), a technical error (RuntimeError, retried), dry run and trigger values.
- It has **not** been run against a real Teradata system or a running Airflow scheduler. Before production, on
  DEV: create the tables, load the configuration, run `validate --check-sql`, a dry run and a real run from the
  local version, then trigger the DAG, and check the tables (runbook section 11). See `OPEN_ITEMS.md`.
