# Stratified Sampling – Operational Runbook

Who this is for: the people who configure samples, run them, and support them on Airflow and Teradata.
How to deploy is in [AIRFLOW_DEPLOYMENT.md](AIRFLOW_DEPLOYMENT.md). What is still open is in
[OPEN_ITEMS.md](OPEN_ITEMS.md).

## Contents

1. [What sampling does (business view)](#1-what-sampling-does-business-view)
2. [The life of a run](#2-the-life-of-a-run)
3. [Tables: map and roles](#3-tables-map-and-roles)
4. [Configuration tables (you fill these)](#4-configuration-tables-you-fill-these)
5. [Tables the engine writes (read only)](#5-tables-the-engine-writes-read-only)
6. [Reference lists (all codes and values)](#6-reference-lists-all-codes-and-values)
7. [Recipes: from a requirement to rows](#7-recipes-from-a-requirement-to-rows)
8. [Airflow: Connection, Variable, triggers](#8-airflow-connection-variable-triggers)
9. [Local command line](#9-local-command-line)
10. [Monitoring and support procedures](#10-monitoring-and-support-procedures)
11. [First run on DEV (checklist)](#11-first-run-on-dev-checklist)

---

## 1. What sampling does (business view)

Every cycle (a week, say) an audit team needs a fixed number of cases (`target_volume`, e.g. 150) drawn from what
was loaded, with:

- **exclusions**: cases that must never be audited (auto-approvals, no prior authorisation needed, a service
  type, a case already audited recently);
- **a mix**: shares per group (80 % denials, 10 % withdrawals ...), groups within groups (functional areas, but
  only within denials), "everything there is" for small groups, "the rest" for the big one;
- **priority**: which cases go first within a group (revisions first, then inpatient, then expedited ...);
  cases left over from last cycle first;
- **variety**: not ten cases of the same code when other codes exist;
- **a snapshot**: the selected cases with the columns the auditors need, in a table of their own.

All of that is **configuration rows holding SQL**, in the style of the GRE rules engine: no field name and no
value is in the code. A new sample, or a changed mix, is new rows, not a release.

Every run is recorded: what was in scope, what was excluded and why, every candidate's bucket and rank, what was
selected, the counts per rule and per bucket, and the audit row. A rerun of the same cycle replaces the earlier
sample as the current one; nothing is ever deleted.

## 2. The life of a run

One run = one `gre_sampling_config` row (a task may run several, see [8](#8-airflow-connection-variable-triggers)).

| Step | What happens | Configured in | Fails as |
|---|---|---|---|
| 1. Parameters | Each `gre_sampling_param` lookup runs in `seq_no` order; the first column of the first row becomes `{param_name}`. Values passed with the run win. `{as_of_date}` is always there. The `run_key` template is filled (`{batch_id}` → `B42`) | `gre_sampling_param`, the run's `run_params`, `run_key` | `PARAM_FAILURE` (technical, retried) – usually "data not loaded yet" |
| 2. Audit row | `gre_sampling_audit` row, status `RUNNING` | – | – |
| 3. Check | Every rule row is checked (types, fractions, names) | all configuration tables | `CONFIG_ERROR` (not retried) |
| 4. Pull | One `SELECT` of the universe rows in `scope_sql`. Teradata evaluates every bucket condition, exclusion rule and priority criterion as a column of that pull | `gre_sampling_config` | `PULL_FAILURE` |
| 5. Exclude | A row matching any exclusion rule is not a candidate; the first matching rule (`seq_no`) is its reason. History rules read earlier runs | `gre_sampling_exclusion` | |
| 6. Order | Criterion 1, then 2, … then `priority_rank_sql`, then `key_columns` | `gre_sampling_priority`, `priority_rank_sql` | |
| 7. Stratify | Level by level, each parent's target is split between its buckets (FRACTION, then ALL, then REMAINDER; equal shares water-filled) | `gre_sampling_strata`, `gre_sampling_bucket` | |
| 8. Select | Per leaf bucket: RANKED (first in order), RANDOM (seeded shuffle) or SYSTEMATIC (every k-th); rotating across `diversity_expr` values | `sampling_method`, `diversity_expr`, `diversity_cap` | `SELECT_FAILURE` |
| 9. Top up | `TOPUP`: a short sample is filled from the other candidates in priority order (the diversity cap still holds) | `shortfall_policy` | |
| 10. Snapshot | The selected cases' `gre_sampling_output_col` values are read back from the universe | `output_table`, `gre_sampling_output_col` | `OUTPUT_FAILURE` |
| 11. Persist | **One transaction**: selections (and excluded rows), bucket per level, counts, snapshot; the earlier current run of the same `(config_id, run_key)` → `etl_is_curr_ind = 'N'`; audit → `COMPLETED` | `keep_excluded` | `PERSIST_FAILURE` (nothing half written) |

A failure writes a `gre_sampling_errors` row and closes the audit row as `ERROR`. A configuration problem fails the
task at once; a technical one is retried (`retries`).

## 3. Tables: map and roles

All in the metadata database (`meta_db`), DDL in `sql/schema.sql`.

| Table | Role | Who writes |
|---|---|---|
| `gre_sampling_config` | One row per sample | you |
| `gre_sampling_param` | Run parameters looked up by SQL | you |
| `gre_sampling_exclusion` | Exclusion rules | you |
| `gre_sampling_priority` | Priority criteria | you |
| `gre_sampling_strata` | Stratification levels | you |
| `gre_sampling_bucket` | Buckets of a level and their quota | you |
| `gre_sampling_output_col` | Columns of the output snapshot | you |
| `gre_sample_selections` | Every row considered: candidate, selected, excluded | engine |
| `gre_sample_selection_attrs` | Each candidate's bucket per level | engine |
| `gre_sampling_run_stats` | Counts per exclusion rule and per bucket | engine |
| `gre_sampling_audit` | One row per run attempt | engine |
| `gre_sampling_errors` | One row per failure | engine |
| *`output_table`* | The snapshot of the selected cases (one per config, you create it: `sql/output_table_template.sql`) | engine |

```
gre_sampling_config (config_id)
 ├── gre_sampling_param        (config_id)
 ├── gre_sampling_exclusion    (config_id)
 ├── gre_sampling_priority     (config_id)
 ├── gre_sampling_output_col   (config_id)
 └── gre_sampling_strata       (config_id, level_order)
      └── gre_sampling_bucket  (strata_id, parent_path)

gre_sampling_audit.run_id = sample_run_id of gre_sample_selections / _attrs / _run_stats / output_table
                          = run_id of gre_sampling_errors
```

## 4. Configuration tables (you fill these)

### 4.0 How to make entries (all configuration tables)

- **Never delete a row**: set `act_ind = 0` (1 = in use). Ids are yours to choose; keep them unique per table.
- **SQL text** is a fragment run against the universe row: a condition (`flag = 'Y'`), an expression
  (`LOWER(code)`), or for `gre_sampling_param` a whole `SELECT`. Quotes are doubled inside an `INSERT` literal.
- **`$env`** anywhere is replaced by the environment (`DEV` → `dev` ...; UAT / PROD → empty): `EXPORT_FEED_$ENV_T`.
- **`{name}` / `$name`** is replaced by a run parameter, quotes doubled; you write the quotes:
  `BTCH_ID = '{batch_id}'`. A token with no value is a configuration error.
- **Order** is `seq_no` (then the id) everywhere, `level_order` for levels.
- After any change: `python local/run_sampling.py validate --config-id N --check-sql` (section 9).

### 4.1 gre_sampling_config – one row per sample

| Column | Meaning |
|---|---|
| `config_id`, `act_ind` | Id; 1 = active |
| `project_name`, `process_name` | Scope keys: a DAG run samples every active config of its project and / or process |
| `sample_name` | Shown in run ids and reports |
| `universe_table` | `database.table` or view to sample from (`$env` allowed) |
| `key_columns` | Comma separated columns that identify one case. Must be unique within the scope (checked every run) |
| `scope_sql` | WHERE condition: this run's rows (default `1=1`) |
| `exclusion_sql` | Optional quick exclusion, not itemised. Prefer `gre_sampling_exclusion` rows |
| `target_volume` | Sample size |
| `sampling_method` | `RANKED` / `RANDOM` / `SYSTEMATIC` ([6.1](#61-sampling-methods)) |
| `priority_rank_sql` | Optional ORDER BY list after the priority criteria, e.g. `AUTHZN_REQ_DT_TM DESC` |
| `rounding_mode` | `FLOOR` / `ROUND` / `CEIL` of each bucket's share (default `FLOOR`) |
| `diversity_expr` | Optional expression: within each bucket, picks rotate across its values |
| `diversity_cap` | Optional: at most this many picks per `diversity_expr` value in the whole sample |
| `shortfall_policy` | `TOPUP` (default) / `NONE` |
| `keep_excluded` | 1 (default) = store excluded rows with their reason too |
| `output_table` | Optional `database.table` of the snapshot (needs `gre_sampling_output_col` rows) |
| `schedule_cron` | Documentation only: the schedule is the Variable's |

### 4.2 gre_sampling_param – run parameters looked up by SQL

| Column | Meaning |
|---|---|
| `param_name` | The token name: `batch_id` → `{batch_id}` |
| `param_sql` | A `SELECT` returning one value (first column, first row). May use `{as_of_date}` and earlier parameters |
| `seq_no` | Lookup order |

No value (no row, or NULL) fails the run as `PARAM_FAILURE` and is retried: typically the load has not finished.
A value passed with the run (`run_params`) is used instead of the lookup.

Example – the latest framework batch:
```sql
INSERT INTO HSABC_DEV.gre_sampling_param (param_id, config_id, act_ind, seq_no, param_name, param_sql)
VALUES (1, 1, 1, 1, 'batch_id', 'SELECT MAX(BTCH_ID) FROM EXPORT_FEED_$ENV_T.SOME_TABLE');
```

### 4.3 gre_sampling_exclusion – exclusion rules

| Column | Meaning |
|---|---|
| `exclusion_name` | The reason stored on the excluded row and in the counts |
| `rule_type` | `SQL` (default): `exclusion_sql` is true · `PRIOR_SELECTION`: selected by an earlier run |
| `exclusion_sql` | SQL rule: a condition. A NULL result (unknown) does **not** exclude |
| `lookback_days`, `history_scope` | `PRIOR_SELECTION`: how far back by `sample_cycle`; `CONFIG` (this sample) or `PROJECT` (any sample of the project) |

The first matching rule by `seq_no` is the reason; each rule's count is in `gre_sampling_run_stats`.

### 4.4 gre_sampling_priority – priority criteria

| Column | Meaning |
|---|---|
| `criterion_name` | Name |
| `rule_type` | `CONDITION`: rows where `criterion_sql` is true first · `EXPRESSION`: by the value of `criterion_sql` (`sort_dir`, NULLs last) · `PRIOR_CANDIDATE`: considered in an earlier run but never selected, first (carry-over) · `PRIOR_SELECTION`: selected earlier, last |
| `criterion_sql` | CONDITION / EXPRESSION |
| `sort_dir` | EXPRESSION: `ASC` / `DESC` |
| `lookback_days`, `history_scope` | PRIOR_*: as in 4.3 |

RANKED takes the first ones, SYSTEMATIC steps through the order, RANDOM ignores it.

### 4.5 gre_sampling_strata – stratification levels

| Column | Meaning |
|---|---|
| `level_order` | 0 = first split, 1 = within it, … |
| `level_name` | Name |
| `stratify_expr` | **VALUE level**: a column or expression; each value is a bucket. **NULL = CONDITION level**: the buckets are the `gre_sampling_bucket` rows' `bucket_sql` |
| `applies_to` | Path pattern of the parents this level splits: `*` (default) = all, `Denied` = only within Denied, `Denied/*` … (`*` and `?` wildcards, not case specific). Under other parents the level is skipped |
| `unlisted_policy` | What happens to a value with no bucket row (VALUE level), or rows matching no condition (CONDITION level, bucket `OTHER`): `SHARE` (default) = share `1 − SUM(fractions)` equally · `REMAINDER` = as quota_mode REMAINDER · `SKIP` = never selected |

No levels = no stratification: the whole pool is one bucket (`ALL`).

### 4.6 gre_sampling_bucket – buckets and quotas

| Column | Meaning |
|---|---|
| `strata_id` | Its level |
| `parent_path` | Path pattern of the parents this row applies under (default `*`). When several patterns match a parent, the most specific wins (fewest wildcards, then longest), so one level can have a different mix per parent |
| `bucket_name` | VALUE level: the value (no case, trailing blanks ignored). CONDITION level: the bucket's name. Never `/` |
| `bucket_sql` | CONDITION level: the condition. Conditions are tried in `seq_no` order, the first true one wins |
| `quota_mode` | `FRACTION`: `target_fraction` × the parent's target · `ALL`: every case there is (up to `max_count`, within what the FRACTION buckets leave) · `REMAINDER`: what all the others leave unused, shared equally between REMAINDER buckets |
| `target_fraction` | FRACTION: 0 to 1. A level's fractions under one `parent_path` add up to 1 at most |
| `max_count` | Optional cap, any mode |

How a parent's target T is split between its buckets:

1. FRACTION buckets: `round(T × fraction)`; unlisted SHARE buckets: `round(T × (1 − Σ fractions) / n)`.
2. ALL buckets share what is left of T after step 1, equally, each up to what it has.
3. REMAINDER buckets share T minus what was *actually* selected in 1 and 2 – so a short FRACTION bucket's unused
   quota flows to them.
4. "Share equally" is water-filled: a bucket that has fewer cases than its share keeps all it has and the others
   share the rest again. 20 between buckets of 2, 50 and 50 cases → 2, 9, 9.

### 4.7 gre_sampling_output_col – the snapshot columns

| Column | Meaning |
|---|---|
| `column_name` | Column of `output_table` (not one of the engine's own: `sample_run_id`, `config_id`, `run_key`, `sample_cycle`, `case_key`, `selection_order`, `priority_rank`, `stratum_path`, `etl_is_curr_ind`, `load_datetime`, `last_updated_datetime`) |
| `expr_sql` | A column or expression of the universe row |

Create the table once from `sql/output_table_template.sql` (the engine never creates or alters tables).

## 5. Tables the engine writes (read only)

| Table | One row per | Key columns to read |
|---|---|---|
| `gre_sample_selections` | candidate and excluded row of a run | `selected_flag`, `selection_order`, `priority_rank`, `excluded_flag`, `exclusion_reason`, `stratum_path`, `etl_is_curr_ind` |
| `gre_sample_selection_attrs` | candidate × level | `strata_id`, `bucket_value` |
| `gre_sampling_run_stats` | exclusion rule, bucket node | `stat_type`, `stat_name`, `candidate_cnt`, `target_cnt`, `selected_cnt` |
| `gre_sampling_audit` | run attempt | `status`, `run_key`, `random_seed`, `run_params_txt`, totals, `shortfall`, `error_message` |
| `gre_sampling_errors` | failure | `error_type`, `error_message`, `error_detail`, `active_ind` |

**Current sample** of a cycle: `etl_is_curr_ind = 'Y'` for its `(config_id, run_key)`. A rerun turns the earlier
rows to `N` (with `last_updated_datetime`) in the same transaction that writes the new ones.

## 6. Reference lists (all codes and values)

### 6.1 Sampling methods
| Code | Meaning | Needs |
|---|---|---|
| `RANKED` | The first n of each bucket in priority order | priority rows or `priority_rank_sql` |
| `RANDOM` | A seeded shuffle of each bucket (seed on the audit row; rerun with `--seed` to reproduce) | – |
| `SYSTEMATIC` | Seeded start, then every k-th in priority order. No `diversity_expr` | priority rows or `priority_rank_sql` |

### 6.2 Quota modes, unlisted policies, rule types
| Table.column | Values |
|---|---|
| `gre_sampling_bucket.quota_mode` | `FRACTION`, `ALL`, `REMAINDER` |
| `gre_sampling_strata.unlisted_policy` | `SHARE`, `REMAINDER`, `SKIP` |
| `gre_sampling_exclusion.rule_type` | `SQL`, `PRIOR_SELECTION` |
| `gre_sampling_priority.rule_type` | `CONDITION`, `EXPRESSION`, `PRIOR_CANDIDATE`, `PRIOR_SELECTION` |
| `*.history_scope` | `CONFIG`, `PROJECT` |
| `gre_sampling_config.shortfall_policy` | `TOPUP`, `NONE` |
| `gre_sampling_config.rounding_mode` | `FLOOR`, `ROUND`, `CEIL` |

### 6.3 Statuses and error types
| Column | Values |
|---|---|
| `gre_sampling_audit.status` | `RUNNING`, `COMPLETED`, `ERROR` |
| `gre_sampling_errors.error_type` | `CONFIG_ERROR` (fix the rows, not retried) · `PARAM_FAILURE` · `PULL_FAILURE` · `SELECT_FAILURE` · `OUTPUT_FAILURE` · `PERSIST_FAILURE` (technical, retried) |
| `gre_sampling_run_stats.stat_type` | `EXCLUSION`, `BUCKET` |

### 6.4 Placeholders in SQL text
| Token | Value |
|---|---|
| `$env` | Environment value (`dev`, `qa`, `int`, empty for UAT / PROD, or the Variable's `env_value`) |
| `{as_of_date}` | The run's business date `YYYY-MM-DD` |
| `{run_key}` | The run key |
| `{name}` / `$name` | A `run_params` value or a `gre_sampling_param` lookup |

### 6.5 Paths
A bucket's path is its bucket names joined by `/` from level 0: `Denied/Part B`. A row that matches no CONDITION
bucket is in `OTHER`; a NULL value is in `NULL`. No levels: `ALL`.

## 7. Recipes: from a requirement to rows

The worked example is `sql/seed/umcm_sample_config.sql` (every kind of rule once).

| Requirement | Rows |
|---|---|
| Sample only this week's batch | `gre_sampling_param` `batch_id` = `SELECT MAX(...)`, `scope_sql` `BTCH_ID = '{batch_id}'`, Variable `run_key` `"{batch_id}"` |
| Never audit auto-approvals | `gre_sampling_exclusion` `SQL` rule |
| Never audit a case twice in 90 days | `gre_sampling_exclusion` `PRIOR_SELECTION`, `lookback_days` 90 |
| Last week's unselected cases first | `gre_sampling_priority` `PRIOR_CANDIDATE`, seq 1 |
| Revisions first, then inpatient | `gre_sampling_priority` `CONDITION` rows in that order |
| 80 % denials, 10 % withdrawals | level 0 `VALUE`, FRACTION buckets |
| Every withdrawal there is, denials the rest | Withdrawn `ALL`, Denied `REMAINDER` |
| Functional areas only within denials | level 1 `applies_to` `Denied`, buckets `parent_path` `Denied` |
| Areas defined by several columns | level 1 `stratify_expr` NULL (CONDITION), one bucket per area with `bucket_sql` |
| Equal portions | buckets `REMAINDER` (or no bucket rows: unlisted `SHARE`) |
| No more than 3 of one code | `diversity_expr` the code, `diversity_cap` 3 |
| Exactly 150 or flag it | `shortfall_policy` `NONE`, Variable `fail_on_shortfall` true |
| A table of the selected cases for the auditors | `output_table` + `gre_sampling_output_col` rows |

## 8. Airflow: Connection, Variable, triggers

Full reference in [AIRFLOW_DEPLOYMENT.md](AIRFLOW_DEPLOYMENT.md).

- **Connection**: the Variable's `td_conn_var` (default `compliance_teradata`), type Teradata, login and password
  of the service account, `{"logmech": "LDAP"}` in extra.
- **Variable** `cms_compliance_umcm_sampling_config_var`: `meta_db`, `load_env`, the scope (`project` /
  `process_name` / `config_id`), `schedule_interval`, emails, retries.
- **Triggering** (UI "Trigger DAG w/ config", CLI, REST, or an upstream DAG): `config_id`, `project`,
  `process_name`, `run_key`, `run_params`, `seed`, `as_of`, `dry_run`, `log_level` – limited by the Variable's
  `trigger_keys` when set. The database, the Connection and the environment are never taken from a trigger.

```bash
airflow dags trigger CMS_COMPLIANCE_UMCM_SAMPLING -c '{"run_key": "{batch_id}", "dry_run": true}'
```

## 9. Local command line

From `local/`, with `dev.env` (copy of `dev.env.example`: Teradata login and `SAMPLING_META_DB`; never committed).

| Command | What it does | Writes |
|---|---|---|
| `test-connection` | Logs in, shows the session and which sampling tables exist | – |
| `deploy-schema [--dry-run] [--drop-first] [--seed]` | Creates the 12 tables (and the example rows) | DDL |
| `validate --config-id N [--check-sql] [--run-param k=v]` | Checks the rows; `--check-sql` also runs the parameter lookups and `EXPLAIN`s every query | – |
| `show-config --config-id N` | Every rule row of a config and its problems (JSON) | – |
| `run --config-id N \| --project P \| --process-name X [--run-key K] [--run-param k=v] [--as-of D] [--seed S] [--dry-run]` | Runs the sample(s) | yes, except `--dry-run` |
| `runs [--config-id N] [--status S]` | Recent audit rows | – |
| `errors [--config-id N]` | Active errors | – |
| `mark-failed --run-id R` | Closes a run stuck in `RUNNING` (e.g. the worker died) | audit |

Exit codes: 0 done, 1 a sample failed, 2 a usage or configuration problem. One log file per command in
`local/logs`.

## 10. Monitoring and support procedures

### 10.1 Did this week's sample run?
```sql
SELECT run_id, run_key, status, total_in_scope, total_excluded, total_selected, shortfall, error_message
FROM HSABC_DEV.gre_sampling_audit
WHERE sample_config_id = 1 ORDER BY started_at DESC;
```

### 10.2 The current sample of a cycle
```sql
SELECT case_key, selection_order, stratum_path, priority_rank
FROM HSABC_DEV.gre_sample_selections
WHERE config_id = 1 AND run_key = 'B42' AND etl_is_curr_ind = 'Y' AND selected_flag = 1
ORDER BY selection_order;
```

### 10.3 Why is the sample short / why this mix?
```sql
SELECT stat_type, stat_name, candidate_cnt, target_cnt, selected_cnt
FROM HSABC_DEV.gre_sampling_run_stats
WHERE config_id = 1 AND run_key = 'B42' AND etl_is_curr_ind = 'Y'
ORDER BY stat_type, stat_name;
```
A bucket with `candidate_cnt < target_cnt` is short; with `TOPUP` the rest came from other buckets.

### 10.4 A run failed
1. `errors --config-id N` (or `gre_sampling_errors WHERE active_ind = 'Y'`).
2. `CONFIG_ERROR`: fix the rows (`validate --check-sql`), then rerun. `PARAM_FAILURE`: is the load finished?
   Others: the message and `error_detail` (traceback); rerun when the cause is gone.
3. Rerun with the same `run_key`: the new run becomes current, the failed attempt stays recorded.

### 10.5 A run is stuck in RUNNING
The worker was killed mid-run (nothing of the run was written: the writes are one transaction).
`mark-failed --run-id <run_id>`, then rerun.

### 10.6 Redo a cycle
Run again with the same `run_key` (and `--seed` from the audit row to reproduce a RANDOM draw). The earlier run
turns `etl_is_curr_ind = 'N'` everywhere, the snapshot table included.

### 10.7 Change a mix, a rule, a priority
New rows, or `act_ind = 0` on the old ones, then `validate --check-sql` and a `--dry-run`. The next run uses them;
earlier runs are not changed.

## 11. First run on DEV (checklist)

1. `deploy-schema --dry-run`, then `deploy-schema` (DBA rights on `meta_db`).
2. Create each `output_table` from `sql/output_table_template.sql`.
3. Load the configuration rows (fill the TODOs of `sql/seed/odag1_config_template.sql` first, `act_ind = 1`).
4. `validate --config-id N --check-sql`.
5. `run --config-id N --dry-run`: check the report's `exclusions`, `buckets` and `by_stratum`.
6. `run --config-id N`; check 10.1 – 10.3 and the snapshot table.
7. Deploy the DAG, set the Connection and the Variable, trigger with `dry_run`, then for real.
