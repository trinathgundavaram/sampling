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

All of that is **configuration rows holding SQL**, in the style of the GRE rules engine: one config row per sample
and one rule row per rule. No field name and no value is in the code. A new sample, or a changed mix, is new rows,
not a release.

Every run is recorded: what was in scope, what was excluded and why, every candidate's group and rank, what was
selected, the counts per rule and per group, the outcome, and **the exact configuration and rules it used** (with a
hash, so two runs made by the same rules are recognisable). A rerun of the same cycle replaces the earlier sample as
the current one; nothing is ever deleted.

Once the auditors start on a sample it can be **finalised**: a rerun of that cycle is then refused unless someone
gives a reason, which is recorded. A case the auditors cannot review can be **replaced** by the next candidate of
its group, with the reason and who did it recorded on both cases.

## 2. The life of a run

One run = one `gre_sampling_config` row (a task may run several, see [8](#8-airflow-connection-variable-triggers)).

| Step | What happens | Configured in | Fails as |
|---|---|---|---|
| 1. Parameters | Each PARAM rule runs in `seq_no` order; the first column of the first row becomes `{rule_name}`. Values passed with the run win. `{as_of_date}` is always there. The `run_key` template is filled (`{batch_id}` → `B42`) | PARAM rules, the run's `run_params`, `run_key` | `PARAM_FAILURE` (technical, retried) – usually "data not loaded yet" |
| 2. Run row | `gre_sampling_run` row, status `RUNNING`, with the `config_hash` of the rules loaded. If the cycle's current sample is `FINAL`, the run stops here unless it was given a force reason | – | `FINALIZED` (not retried) |
| 3. Check | Every rule row is checked (classes, types, fractions, names, columns a class does not use) | both configuration tables | `CONFIG_ERROR` (not retried) |
| 4. Pull | One `SELECT` of the universe rows in `scope_sql`. Teradata evaluates every bucket condition, exclusion rule and priority criterion as a column of that pull | `gre_sampling_config` | `PULL_FAILURE` |
| 5. Exclude | A row matching any EXCLUSION rule is not a candidate; the first matching rule (`seq_no`) is its reason. History rules read earlier runs | EXCLUSION rules | |
| 6. Order | PRIORITY rule 1, then 2, … then `priority_rank_sql`, then `key_columns` | PRIORITY rules, `priority_rank_sql` | |
| 7. Stratify | Level by level, each parent's target is split between its buckets (FRACTION, then ALL, then REMAINDER; equal shares water-filled) | LEVEL and BUCKET rules | |
| 8. Select | Per leaf bucket: RANKED (first in order), RANDOM (seeded shuffle) or SYSTEMATIC (every k-th); rotating across `diversity_expr` values | `sampling_method`, `diversity_expr`, `diversity_cap` | `SELECT_FAILURE` |
| 9. Top up | `TOPUP`: a short sample is filled from the other candidates in priority order (the diversity cap still holds) | `shortfall_policy` | |
| 10. Snapshot | The selected cases' OUTPUT values are read back from the universe | `output_table`, OUTPUT rules | `OUTPUT_FAILURE` |
| 11. Persist | **One transaction**: selections (and excluded rows) with their bucket path, snapshot; the earlier current run of the same `(config_id, run_key)` → `etl_is_curr_ind = 'N'`; run row → `COMPLETED`, current, with its `run_report` | `keep_excluded` | `PERSIST_FAILURE` (nothing half written) |
| Later | `finalize` makes the sample `FINAL`; `replace` swaps a case (see [10.8](#108-finalise-a-sample), [10.9](#109-replace-a-case)) | – | – |

A failure closes the run row as `ERROR` with `error_type`, `error_message` and (technical errors) the traceback in
`error_detail`. A configuration problem fails the task at once; a technical one is retried (`retries`).

## 3. Tables: map and roles

All in the metadata database (`meta_db`), DDL in `sql/schema.sql`.

| Table | Role | Who writes |
|---|---|---|
| `gre_sampling_config` | One row per sample | you |
| `gre_sampling_rule` | One row per rule: PARAM, EXCLUSION, PRIORITY, LEVEL, BUCKET, OUTPUT | you |
| `gre_sampling_run` | One row per run attempt: outcome, totals, errors, counts per rule and bucket | engine |
| `gre_sample_selections` | One row per case considered: candidate, selected, excluded | engine |
| *`output_table`* | The snapshot of the selected cases (optional, one per config, you create it: `sql/output_table_template.sql`) | engine |

```
gre_sampling_config (config_id)
 └── gre_sampling_rule (config_id, rule_class)
       LEVEL rows (level_order) ← BUCKET rows (same level_order)

gre_sampling_run.run_id = sample_run_id of gre_sample_selections and of the output_table
```

## 4. Configuration tables (you fill these)

### 4.0 How to make entries

- **Never delete a row**: set `act_ind = 0` (1 = in use). `rule_id` is yours to choose: unique in the whole
  table (e.g. 1–99 for config 1, 101–199 for config 2).
- **SQL text** (`rule_sql`, `scope_sql` …) is a fragment run against the universe row: a condition (`flag = 'Y'`),
  an expression (`LOWER(code)`), or for PARAM a whole `SELECT`. Quotes are doubled inside an `INSERT` literal.
- **`$env`** anywhere is replaced by the environment (`DEV` → `dev` ...; UAT / PROD → empty): `EXPORT_FEED_$ENV_T`.
- **`{name}` / `$name`** is replaced by a run parameter, quotes doubled; you write the quotes:
  `BTCH_ID = '{batch_id}'`. A token with no value is a configuration error.
- **Order** is `seq_no` (then `rule_id`) within a class; `level_order` for levels.
- **Only the columns of the class** (table in 4.2): `validate` reports any other one that is set.
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
| `exclusion_sql` | Optional quick exclusion, not itemised. Prefer EXCLUSION rules |
| `target_volume` | Sample size |
| `sampling_method` | `RANKED` / `RANDOM` / `SYSTEMATIC` ([6.1](#61-sampling-methods)) |
| `priority_rank_sql` | Optional ORDER BY list after the PRIORITY rules, e.g. `AUTHZN_REQ_DT_TM DESC` |
| `rounding_mode` | `FLOOR` / `ROUND` / `CEIL` of each bucket's share (default `FLOOR`) |
| `diversity_expr` | Optional expression: within each bucket, picks rotate across its values |
| `diversity_cap` | Optional: at most this many picks per `diversity_expr` value in the whole sample |
| `shortfall_policy` | `TOPUP` (default) / `NONE` |
| `keep_excluded` | 1 (default) = store excluded rows with their reason too |
| `output_table` | Optional `database.table` of the snapshot (needs OUTPUT rules) |
| `schedule_cron` | Documentation only: the schedule is the Variable's |

### 4.2 gre_sampling_rule – one row per rule

Common columns: `rule_id`, `config_id`, `act_ind`, `rule_class`, `seq_no`, `rule_name`, `description`. The others,
by class:

| Column | PARAM | EXCLUSION | PRIORITY | LEVEL | BUCKET | OUTPUT |
|---|---|---|---|---|---|---|
| `rule_name` | the `{token}` | the reason stored | name | name | the value / bucket name (no `/`) | the output column |
| `rule_type` | | `SQL` (default) / `PRIOR_SELECTION` | `CONDITION` (default) / `EXPRESSION` / `PRIOR_CANDIDATE` / `PRIOR_SELECTION` | | | |
| `rule_sql` | a `SELECT` | SQL: the condition | CONDITION / EXPRESSION | the expression whose values are the buckets, or NULL (buckets by condition) | condition-level: the condition | the expression |
| `level_order` | | | | 0, 1, … | its level | |
| `parent_path` | | | | the parents it splits (default `*`) | the parents it applies under (default `*`) | |
| `unlisted_policy` | | | | `SHARE` (default) / `REMAINDER` / `SKIP` | | |
| `quota_mode` | | | | | `FRACTION` (default) / `ALL` / `REMAINDER` | |
| `target_fraction` | | | | | FRACTION: 0 to 1 | |
| `max_count` | | | | | optional cap | |
| `sort_dir` | | | EXPRESSION: `ASC` (default) / `DESC` | | | |
| `lookback_days`, `history_scope` | | PRIOR_SELECTION | PRIOR_* | | | |

**PARAM** – a run parameter. The first column of the first row of `rule_sql` becomes `{rule_name}`; a later PARAM
may use an earlier one. No value (no row, or NULL) fails the run as `PARAM_FAILURE`, which is retried: typically the
load has not finished. A value passed with the run (`run_params`) is used instead of the lookup.

**EXCLUSION** – `SQL`: `rule_sql` true = excluded (a NULL result, unknown, does **not** exclude).
`PRIOR_SELECTION`: selected by a current run of another `run_key` within `lookback_days` (by `sample_cycle`), of
this config (`history_scope` `CONFIG`, default) or of any config of the project (`PROJECT`). The first matching rule
by `seq_no` is the reason; each rule's count is in the run's `run_report`.

**PRIORITY** – `CONDITION`: rows where `rule_sql` is true first. `EXPRESSION`: by the value of `rule_sql`
(`sort_dir`, NULLs last). `PRIOR_CANDIDATE`: considered by an earlier run but never selected, first (carry-over).
`PRIOR_SELECTION`: selected earlier, last. RANKED takes the first ones, SYSTEMATIC steps through the order, RANDOM
ignores it.

**LEVEL** – a split. `rule_sql` set (**value level**): each value of the expression is a bucket. `rule_sql` NULL
(**condition level**): the buckets are its BUCKET rows' conditions, tried in `seq_no` order, the first true one
wins; rows matching none are bucket `OTHER`. `parent_path`: the parents this level splits – `*` = all, `Denied` =
only within Denied, `Denied/*` … (`*` and `?` wildcards, not case specific); under other parents the level is
skipped. `unlisted_policy`: what happens to a value with no BUCKET row, or to `OTHER` – `SHARE` = share
`1 − Σ fractions` equally, `REMAINDER` = as quota_mode REMAINDER, `SKIP` = never selected. No LEVEL rules = no
stratification: the whole pool is one bucket (`ALL`).

**BUCKET** – a bucket of the LEVEL with the same `level_order`. `parent_path`: when several patterns match a
parent, the most specific wins (fewest wildcards, then longest), so one level can have a different mix per parent.
How a parent's target T is split:

1. FRACTION buckets: `round(T × fraction)`; unlisted SHARE buckets: `round(T × (1 − Σ fractions) / n)`.
2. ALL buckets share what is left of T after step 1, equally, each up to what it has.
3. REMAINDER buckets share T minus what was *actually* selected in 1 and 2 – so a short FRACTION bucket's unused
   quota flows to them.
4. "Share equally" is water-filled: a bucket with fewer cases than its share keeps all it has and the others share
   the rest again. 20 between buckets of 2, 50 and 50 cases → 2, 9, 9.

A level's fractions under one `parent_path` add up to 1 at most.

**OUTPUT** – a column of `output_table`: `rule_name` the column (not one the engine writes itself:
`sample_run_id`, `config_id`, `run_key`, `sample_cycle`, `case_key`, `selection_order`, `priority_rank`,
`stratum_path`, `etl_is_curr_ind`, `load_datetime`, `last_updated_datetime`), `rule_sql` its expression. Create the
table once from `sql/output_table_template.sql` (the engine never creates or alters tables).

## 5. Tables the engine writes (read only)

| Table | One row per | Columns to read |
|---|---|---|
| `gre_sampling_run` | run attempt | `status`, `etl_is_curr_ind`, `run_key`, `random_seed`, `run_params_txt`, totals, `shortfall`, `error_type`, `error_message`, `error_detail`, `run_report`, `config_hash`, `finalized_by` / `finalized_at`, `override_reason` |
| `gre_sample_selections` | candidate and excluded row of a run | `selected_flag`, `selection_order`, `priority_rank`, `excluded_flag`, `exclusion_reason`, `stratum_path`, `selection_status`, `linked_case_key`, `status_reason` / `status_by` / `status_at`, `etl_is_curr_ind` |

**Current sample** of a cycle: `etl_is_curr_ind = 'Y'` for its `(config_id, run_key)`, on the run row and on its
selections. A successful rerun turns the earlier rows to `N` (with `last_updated_datetime`) in the same transaction
that writes the new ones; a failed rerun leaves the earlier sample current.

**`run_report`** (JSON): `{"config_hash", "config": {"config": the config row, "rules": [the active rule rows]},
"exclusions": {rule_name: count}, "buckets": {path: {"candidates", "target", "selected"}}}` – every bucket node, not
only the leaves (`Denied` and `Denied/Part B`). A failed run has the `config` part only. `config_hash` is the
SHA-256 of that `config` part, without who changed a row and when: equal hashes = the same rules.

**`selection_status`**: `SELECTED` for the cases the run picked; `REPLACED` for a case taken out later
(`selected_flag` back to 0) and `REPLACEMENT` for the case put in for it (`selected_flag` 1, the same
`selection_order`); `linked_case_key` points from each to the other. The current sample is always
`selected_flag = 1`.

**`stratum_path`** is the case's bucket at every level, `/` separated: the group per level of any candidate, and
counts per bucket, are a `GROUP BY` on it.

## 6. Reference lists (all codes and values)

### 6.1 Sampling methods
| Code | Meaning | Needs |
|---|---|---|
| `RANKED` | The first n of each bucket in priority order | PRIORITY rules or `priority_rank_sql` |
| `RANDOM` | A seeded shuffle of each bucket (seed on the run row; rerun with `--seed` to reproduce) | – |
| `SYSTEMATIC` | Seeded start, then every k-th in priority order. No `diversity_expr` | PRIORITY rules or `priority_rank_sql` |

### 6.2 Rule classes and their values
| Column | Values |
|---|---|
| `gre_sampling_rule.rule_class` | `PARAM`, `EXCLUSION`, `PRIORITY`, `LEVEL`, `BUCKET`, `OUTPUT` |
| `rule_type` (EXCLUSION) | `SQL`, `PRIOR_SELECTION` |
| `rule_type` (PRIORITY) | `CONDITION`, `EXPRESSION`, `PRIOR_CANDIDATE`, `PRIOR_SELECTION` |
| `quota_mode` (BUCKET) | `FRACTION`, `ALL`, `REMAINDER` |
| `unlisted_policy` (LEVEL) | `SHARE`, `REMAINDER`, `SKIP` |
| `history_scope` | `CONFIG`, `PROJECT` |
| `gre_sampling_config.shortfall_policy` | `TOPUP`, `NONE` |
| `gre_sampling_config.rounding_mode` | `FLOOR`, `ROUND`, `CEIL` |

### 6.3 Statuses and error types
| Column | Values |
|---|---|
| `gre_sampling_run.status` | `RUNNING`, `COMPLETED`, `FINAL` (finalised), `ERROR` |
| `gre_sampling_run.error_type` | `CONFIG_ERROR` (fix the rows, not retried) · `FINALIZED` (the cycle's sample is final, not retried) · `PARAM_FAILURE` · `PULL_FAILURE` · `SELECT_FAILURE` · `OUTPUT_FAILURE` · `PERSIST_FAILURE` (technical, retried) · `ABANDONED` (closed by `mark-failed`) |
| `gre_sample_selections.selection_status` | `SELECTED`, `REPLACED`, `REPLACEMENT`, NULL (a candidate not picked, or excluded) |

### 6.4 Placeholders in SQL text
| Token | Value |
|---|---|
| `$env` | Environment value (`dev`, `qa`, `int`, empty for UAT / PROD, or the Variable's `env_value`) |
| `{as_of_date}` | The run's business date `YYYY-MM-DD` |
| `{run_key}` | The run key |
| `{name}` / `$name` | A `run_params` value or a PARAM rule |

### 6.5 Paths
A bucket's path is its bucket names joined by `/` from level 0: `Denied/Part B`. A row that matches no condition
bucket is in `OTHER`; a NULL value is in `NULL`. No levels: `ALL`.

## 7. Recipes: from a requirement to rows

The worked example is `sql/seed/umcm_sample_config.sql` (every kind of rule once).

| Requirement | Rows |
|---|---|
| Sample only this week's batch | PARAM `batch_id` = `SELECT MAX(...)`, `scope_sql` `BTCH_ID = '{batch_id}'`, Variable `run_key` `"{batch_id}"` |
| Never audit auto-approvals | EXCLUSION, `SQL` |
| Never audit a case twice in 90 days | EXCLUSION, `PRIOR_SELECTION`, `lookback_days` 90 |
| Last week's unselected cases first | PRIORITY, `PRIOR_CANDIDATE`, `seq_no` 1 |
| Revisions first, then inpatient | PRIORITY `CONDITION` rules in that order |
| 80 % denials, 10 % withdrawals | LEVEL 0 with `rule_sql` the column, FRACTION BUCKETs |
| Every withdrawal there is, denials the rest | BUCKET Withdrawn `ALL`, BUCKET Denied `REMAINDER` |
| Functional areas only within denials | LEVEL 1 `parent_path` `Denied`, its BUCKETs `parent_path` `Denied` |
| Areas defined by several columns | LEVEL 1 with `rule_sql` NULL, one BUCKET per area with its condition in `rule_sql` |
| Equal portions | BUCKETs `REMAINDER` (or no BUCKET rows: unlisted `SHARE`) |
| No more than 3 of one code | `diversity_expr` the code, `diversity_cap` 3 |
| Exactly 150 or flag it | `shortfall_policy` `NONE`, Variable `fail_on_shortfall` true |
| A table of the selected cases for the auditors | `output_table` + OUTPUT rules |

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
| `deploy-schema [--dry-run] [--drop-first] [--seed]` | Creates the 4 tables (and the example rows) | DDL |
| `validate --config-id N [--check-sql] [--run-param k=v]` | Checks the rows; `--check-sql` also runs the PARAM lookups and `EXPLAIN`s every query | – |
| `show-config --config-id N` | Every rule of a config, by class, and its problems (JSON) | – |
| `run --config-id N \| --project P \| --process-name X [--run-key K] [--run-param k=v] [--as-of D] [--seed S] [--dry-run] [--force-reason R]` | Runs the sample(s); `--force-reason` replaces a `FINAL` sample | yes, except `--dry-run` |
| `finalize --config-id N --run-key K [--by U]` | Makes the current sample of the cycle `FINAL` | run row |
| `replace --config-id N --run-key K --case-key C --reason R [--any-bucket] [--by U]` | Replaces one selected case by the next candidate of its bucket | selections, output |
| `runs [--config-id N] [--status S]` | Recent run rows (with `config_hash`, `finalized_by`, `override_reason`) | – |
| `errors [--config-id N]` | Open errors: cycles whose latest attempt failed | – |
| `mark-failed --run-id R` | Closes a run stuck in `RUNNING` (e.g. the worker died) | run row |

Exit codes: 0 done, 1 a sample failed, 2 a usage or configuration problem. One log file per command in
`local/logs`.

## 10. Monitoring and support procedures

### 10.1 Did this week's sample run?
```sql
SELECT run_id, run_key, status, etl_is_curr_ind, total_in_scope, total_excluded, total_selected, shortfall,
       error_type, error_message
FROM HSABC_DEV.gre_sampling_run
WHERE config_id = 1 ORDER BY started_at DESC;
```

### 10.2 The current sample of a cycle
```sql
SELECT case_key, selection_order, stratum_path, priority_rank
FROM HSABC_DEV.gre_sample_selections
WHERE config_id = 1 AND run_key = 'B42' AND etl_is_curr_ind = 'Y' AND selected_flag = 1
ORDER BY selection_order;
```

### 10.3 Why is the sample short / why this mix?
Candidates and selections per bucket, and the reasons for exclusion:
```sql
SELECT stratum_path, COUNT(*) AS candidates, SUM(selected_flag) AS selected
FROM HSABC_DEV.gre_sample_selections
WHERE config_id = 1 AND run_key = 'B42' AND etl_is_curr_ind = 'Y' AND excluded_flag = 0
GROUP BY 1 ORDER BY 1;

SELECT exclusion_reason, COUNT(*) FROM HSABC_DEV.gre_sample_selections
WHERE config_id = 1 AND run_key = 'B42' AND etl_is_curr_ind = 'Y' AND excluded_flag = 1
GROUP BY 1;
```
The targets per bucket are in the run's `run_report` (`buckets.<path>.target`): a bucket with fewer candidates
than its target is short; with `TOPUP` the rest came from other buckets.

### 10.4 A run failed
1. `errors --config-id N` (or `gre_sampling_run WHERE status = 'ERROR'`).
2. `CONFIG_ERROR`: fix the rows (`validate --check-sql`), then rerun. `PARAM_FAILURE`: is the load finished?
   Others: `error_message` and `error_detail` (traceback); rerun when the cause is gone.
3. Rerun with the same `run_key`: the new run becomes current, the failed attempt stays recorded.

### 10.5 A run is stuck in RUNNING
The worker was killed mid-run (nothing of the run was written: the writes are one transaction).
`mark-failed --run-id <run_id>`, then rerun.

### 10.6 Redo a cycle
Run again with the same `run_key` (and `--seed` from the run row to reproduce a RANDOM draw). The earlier run
turns `etl_is_curr_ind = 'N'` everywhere, the snapshot table included.

### 10.7 Change a mix, a rule, a priority
New rule rows, or `act_ind = 0` on the old ones, then `validate --check-sql` and a `--dry-run`. The next run uses
them; earlier runs are not changed. The new run's `config_hash` differs from the previous one; the old rules stay
readable in the old run's `run_report`.

### 10.8 Finalise a sample
When the auditors start on a cycle: `finalize --config-id N --run-key K`. From then on a run of that run key, by the
DAG or by hand, fails as `FINALIZED` (no retry) and the final sample stays current. To replace it anyway (e.g. the
rules were wrong), with sign-off: `run --config-id N --run-key K --force-reason "<why, approved by whom>"` – the
reason is stored on the new run row, and the final run stays recorded with `etl_is_curr_ind = 'N'`. A force is only
possible from the command line, never from a DAG trigger.

### 10.9 Replace a case
A selected case cannot be reviewed (no record, duplicate …):
`replace --config-id N --run-key K --case-key "<case_key>" --reason "<why>"`. The next candidate of the same bucket
in priority order goes in (RANDOM: a draw seeded by the run's seed, the same every time), with the same
`selection_order`; the output snapshot gets its row and the old row turns `etl_is_curr_ind = 'N'`, evaluated with
the rules of that run (its snapshot), not today's. When the bucket has no candidate left, `--any-bucket` takes the
next one overall (the mix then differs from the quotas: the reason records why). Allowed on a `FINAL` sample. A
replaced case does not come back as carry-over (`PRIOR_CANDIDATE`) in later cycles. Not checked by a replacement:
`diversity_cap`.

## 11. First run on DEV (checklist)

1. `deploy-schema --dry-run`, then `deploy-schema` (DBA rights on `meta_db`).
2. Create each `output_table` from `sql/output_table_template.sql`.
3. Load the configuration rows (fill the TODOs of `sql/seed/odag1_config_template.sql` first, `act_ind = 1`).
4. `validate --config-id N --check-sql`.
5. `run --config-id N --dry-run`: check the report's `exclusions`, `buckets` and `by_stratum`.
6. `run --config-id N`; check 10.1 – 10.3 and the snapshot table.
7. Deploy the DAG, set the Connection and the Variable, trigger with `dry_run`, then for real.
8. Try `finalize`, a rerun (expect `FINALIZED`) and `replace` on a DEV cycle, so the team knows them before audit.
