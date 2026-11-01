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
12. [Retention](#12-retention)

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
| 1. Parameters | Each PARAM rule runs in `seq_no` order; the first column of the first row becomes `{rule_name}`. Values passed with the run win. `{as_of_date}` is always there. The `run_key` template is filled (`{batch_id}` → `B42`). The run's `extra_filters` are kept with the parameters | PARAM rules, the run's `run_params`, `extra_filters`, `run_key` | `PARAM_FAILURE` (technical, retried) – usually "data not loaded yet" |
| 2. Run row | `gre_sampling_run` row, status `RUNNING`, with the `config_hash` of the rules loaded. If the cycle's current sample is `FINAL`, the run stops here unless it was given a force reason | – | `FINALIZED` (not retried) |
| 3a. Gates | Every GATE rule's `SELECT` must return a row (e.g. "the batch is closed"), else the run waits: it fails and Airflow retries it after `retry_delay_minutes` | GATE rules | `GATE_NOT_MET` (retried), `GATE_FAILURE` |
| 3. Check | Every rule row is checked (classes, types, fractions, names, columns a class does not use) | both configuration tables | `CONFIG_ERROR` (not retried) |
| 4. Pull | One `SELECT` of the universe's key columns in `scope_sql` AND the run's `extra_filters` | `gre_sampling_config`, `extra_filters` | `PULL_FAILURE` |
| 4a. Rule queries | Each EXCLUSION / PRIORITY / LEVEL / BUCKET rule's `SELECT` runs on its own; the pulled cases it returns (by `key_columns`) match it, or get its value. Cases it returns outside the scope are ignored. The count per rule is in `run_report` (`rule_queries`) | the rules | `QUERY_RULE_FAILURE` (technical, retried); no key columns in the result: `CONFIG_ERROR` |
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
| `gre_sampling_rule` | One row per rule: PARAM, GATE, EXCLUSION, PRIORITY, LEVEL, BUCKET, OUTPUT | you |
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
- **SQL text**: every rule's `rule_sql` (PARAM, GATE, EXCLUSION, PRIORITY, LEVEL, BUCKET) is a whole `SELECT`, as a
  GRE rule (4.2); OUTPUT `rule_sql` is a column expression, and the config's `scope_sql` / `exclusion_sql` a
  condition on the universe row. Quotes are doubled inside an `INSERT` literal.
- **`$env`** anywhere is replaced by the environment (`DEV` → `dev` ...; UAT / PROD → empty): `EXPORT_FEED_$ENV_T`.
- **`{name}` / `$name`** is replaced by a run parameter, quotes doubled; you write the quotes:
  `BTCH_ID = '{batch_id}'`. A token with no value is a configuration error.
- **Order** is `seq_no` (then `rule_id`) within a class; `level_order` for levels.
- **Only the columns of the class** (table in 4.2): `validate` reports any other one that is set.
- **Only reading SQL**: every SQL column is checked before a run (and by `validate`, and by `load-config`). Refused:
  a statement word (`INSERT`, `UPDATE`, `DELETE`, `MERGE`, `DROP`, `CREATE`, `ALTER`, `REPLACE`, `RENAME`, `GRANT`,
  `REVOKE`, `CALL`, `EXEC`, `COLLECT`, `COMMIT` …), a `;`, a `--` comment (it would comment out the rest of the
  query built around the fragment: use `/* */`), unbalanced parentheses or quotes; PARAM and GATE must be a
  `SELECT` (`WITH …` and `LOCKING ROW FOR ACCESS SELECT …` are fine). Text inside quotes is not checked:
  `disp = 'DELETE'` is a value. The rows run under the service account: this stops a pasted script or a slip,
  it is not a permission (who may write `gre_sampling_rule` is).
- **Easier than INSERTs**: `export-config` writes a config and its rules as a JSON file; edit it and
  `load-config --file … ` shows what would change, `--apply` writes it in one transaction (4.3).
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

| Column | PARAM | GATE | EXCLUSION | PRIORITY | LEVEL | BUCKET | OUTPUT |
|---|---|---|---|---|---|---|---|
| `rule_name` | the `{token}` | name (in the message) | the reason stored | name | name | the value / bucket name (no `/`) | the output column |
| `rule_type` | | | `QUERY` (default) / `PRIOR_SELECTION` | `QUERY` (default) / `EXPRESSION` / `PRIOR_CANDIDATE` / `PRIOR_SELECTION` | | `QUERY` (default) | |
| `rule_sql` | a `SELECT` | a `SELECT`: a row = ready | QUERY: a `SELECT` of the key columns | QUERY: a `SELECT` of the key columns; EXPRESSION: of the key columns and a value | a `SELECT` of the key columns and the value, or NULL (buckets by their `SELECT`s) | bucket level: a `SELECT` of the key columns, or NULL (the rest) | the expression |
| `level_order` | | | | | 0, 1, … | its level | |
| `parent_path` | | | | | the parents it splits (default `*`) | the parents it applies under (default `*`) | |
| `unlisted_policy` | | | | | `SHARE` (default) / `REMAINDER` / `SKIP` | | |
| `quota_mode` | | | | | | `FRACTION` (default) / `ALL` / `REMAINDER` | |
| `target_fraction` | | | | | | FRACTION: 0 to 1 | |
| `max_count` | | | | | | optional cap | |
| `sort_dir` | | | | EXPRESSION: `ASC` (default) / `DESC` | | | |
| `lookback_days`, `history_scope` | | | PRIOR_SELECTION | PRIOR_* | | | |

**PARAM** – a run parameter. The first column of the first row of `rule_sql` becomes `{rule_name}`; a later PARAM
may use an earlier one. No value (no row, or NULL) fails the run as `PARAM_FAILURE`, which is retried: typically the
load has not finished. A value passed with the run (`run_params`) is used instead of the lookup.

**Run-time filters (`extra_filters`)** – for a universe table holding several loads, batches or SCD2 versions of a
case, the run can be given a SQL condition (the Variable, a `runs` entry, a trigger, or `--filter "SQL"` locally),
as the GRE's `extra_filters`, e.g. `BTCH_ID = 'B42' AND ETL_IS_CURR_IND = 'Y'`, or an SCD2 version valid on the
run date: `DATE '{as_of_date}' BETWEEN EFF_DT AND END_DT`. Each layer's (and each `--filter`'s) condition is ANDed
to `scope_sql`, so the count, the pull and the output snapshot read the same rows; it is kept in `run_params_txt`,
so a later case replacement reads it too. It is checked like any configured SQL (only reading, no `;`, no `--`).
When the filters leave two rows with the same `key_columns`, the run stops with "key_columns do not identify a case
uniquely": add the filter that picks one version. (A rule's own `SELECT` reads its tables itself: give a value
`SELECT` the same version filter, or it returns a case twice.)

**GATE** – a readiness check, run after the PARAM rules (so it may use `{batch_id}`). Its `SELECT` returning no
row means "not ready": the run ends as `GATE_NOT_MET` and Airflow retries it (`retries`, `retry_delay_minutes`).
Nothing is pulled or written but the run row. Example – sample only a closed batch:
`SELECT 1 FROM <control table> WHERE <batch column> = '{batch_id}' AND <status column> = '<closed>'`.

**Every selection rule is a SQL statement**, as a GRE rule: a whole `SELECT` returning the `key_columns` of the
cases it matches (same column names; joins, subqueries, `WITH`, any table – anything that reads). Each runs on its
own after the pull; only the cases in the run's scope (`scope_sql` and `extra_filters`) count, so a rule need not
repeat them (a filter on the batch keeps it fast). It may use `$env`, `{batch_id}` and the other run parameters. It
must be a `SELECT` that only reads (checked like a PARAM rule); `validate --check-sql` EXPLAINs every one. Where a
value is needed (a PRIORITY `EXPRESSION`, a LEVEL by value) the `SELECT` returns one more column, the value, and each
case at most once. A rule whose `SELECT` fails to run fails the run as `QUERY_RULE_FAILURE` (retried); one returning
no key columns is a `CONFIG_ERROR`. The cases each rule returned are counted in `run_report` (`rule_queries`).
The former condition form (`rule_type` `SQL` / `CONDITION`) is reported by `validate` with how to rewrite it.

**EXCLUSION** – `QUERY` (default): the cases the `SELECT` returns are excluded. `PRIOR_SELECTION`: selected by a
current run of another `run_key` within `lookback_days` (by `sample_cycle`), of this config (`history_scope`
`CONFIG`, default) or of any config of the project (`PROJECT`). The first matching rule by `seq_no` is the reason;
each rule's count is in the run's `run_report`.

**PRIORITY** – `QUERY` (default): the cases the `SELECT` returns first. `EXPRESSION`: a `SELECT` of the key columns
and a value, the cases ordered by it (`sort_dir`; cases it does not return, or NULL, last). `PRIOR_CANDIDATE`:
considered by an earlier run but never selected, first (carry-over). `PRIOR_SELECTION`: selected earlier, last.
RANKED takes the first ones, SYSTEMATIC steps through the order, RANDOM ignores it.

Example – 21% of the sample Behavioral Health, BH meaning a code on a list and a BH provider:

```sql
INSERT INTO {{META_DB}}.gre_sampling_rule (rule_id, config_id, act_ind, rule_class, seq_no, rule_name, level_order, parent_path, unlisted_policy)
VALUES (241, 2, 1, 'LEVEL', 1, 'program', 1, '*', 'SKIP');
INSERT INTO {{META_DB}}.gre_sampling_rule (rule_id, config_id, act_ind, rule_class, seq_no, rule_name, level_order, parent_path, quota_mode, target_fraction, rule_sql)
VALUES (242, 2, 1, 'BUCKET', 1, 'Behavioral Health', 1, '*', 'FRACTION', 0.21,
        'SELECT a.AUTHZN_ID, a.SRC_DATA_KEY
         FROM EXPORT_FEED_$ENV_T.SDO_CMPLNC_CLNCL_AUD_UM_AUTHZN_ODAG1 a
         JOIN REF_$ENV.BH_PROC_CODES c ON c.PROC_CD = a.PROC_CD AND c.ACT_IND = 1
         JOIN REF_$ENV.PROVIDER p ON p.PROV_ID = a.SVC_PROV_ID
         WHERE a.BTCH_ID = ''{batch_id}'' AND p.PROV_TY IN (''PSY'', ''LCSW'')');
-- no rule_sql: every case no earlier bucket took
INSERT INTO {{META_DB}}.gre_sampling_rule (rule_id, config_id, act_ind, rule_class, seq_no, rule_name, level_order, parent_path, quota_mode)
VALUES (243, 2, 1, 'BUCKET', 2, 'Other', 1, '*', 'REMAINDER');
```

(The tables and columns are illustrations.)

**LEVEL** – a split. `rule_sql` set (**value level**): a `SELECT` of the key columns and a value; each value is a
bucket (a case it does not return is in `NULL`). `rule_sql` NULL (**bucket level**): the buckets are its BUCKET rows'
`SELECT`s, tried in `seq_no` order, the first that returned the case wins; a BUCKET without `rule_sql` takes every
case left (only one, and last); cases matching none are bucket `OTHER`. `parent_path`: the parents this level
splits – `*` = all, `Denied` =
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

### 4.3 A configuration as a file

```bash
python local/run_sampling.py export-config --config-id 1 --file umcm.json
python local/run_sampling.py load-config --file umcm.json
python local/run_sampling.py load-config --file umcm.json --apply
```

The file is `{"config": {gre_sampling_config columns}, "rules": [{gre_sampling_rule columns}, ...]}` (the audit
columns are left out and set by the load). Without `--apply` the load only shows: the config columns that change
(`from` / `to`), the rules added, changed (per column) and switched off, and the problems. It is refused when the
rules it would make active have any problem `validate` finds, or a `rule_id` belongs to another config. A rule in
the tables but not in the file is switched off (`act_ind = 0`), never deleted; a rule in the file with
`act_ind 0` stays and is ignored. `created_by` / `last_updated_by` = `--by` (default your user name). Keep the
file in version control: its history is the rules' history, next to each run's `config_hash`.

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

`gre_sample_selections` grows by one row per candidate per run; `purge` removes the rows no longer needed
(section 12). `gre_sampling_run` rows are never deleted.

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
| `gre_sampling_rule.rule_class` | `PARAM`, `GATE`, `EXCLUSION`, `PRIORITY`, `LEVEL`, `BUCKET`, `OUTPUT` |
| `rule_type` (EXCLUSION) | `QUERY` (default), `PRIOR_SELECTION` |
| `rule_type` (PRIORITY) | `QUERY` (default), `EXPRESSION`, `PRIOR_CANDIDATE`, `PRIOR_SELECTION` |
| `rule_type` (BUCKET) | `QUERY` (default) |
| `quota_mode` (BUCKET) | `FRACTION`, `ALL`, `REMAINDER` |
| `unlisted_policy` (LEVEL) | `SHARE`, `REMAINDER`, `SKIP` |
| `history_scope` | `CONFIG`, `PROJECT` |
| `gre_sampling_config.shortfall_policy` | `TOPUP`, `NONE` |
| `gre_sampling_config.rounding_mode` | `FLOOR`, `ROUND`, `CEIL` |

### 6.3 Statuses and error types
| Column | Values |
|---|---|
| `gre_sampling_run.status` | `RUNNING`, `COMPLETED`, `FINAL` (finalised), `ERROR` |
| `gre_sampling_run.error_type` | `CONFIG_ERROR` (fix the rows, not retried) · `FINALIZED` (the cycle's sample is final, not retried) · `GATE_NOT_MET` (data not ready, retried) · `GATE_FAILURE` · `PARAM_FAILURE` · `PULL_FAILURE` · `QUERY_RULE_FAILURE` · `SELECT_FAILURE` · `OUTPUT_FAILURE` · `PERSIST_FAILURE` (technical, retried) · `ABANDONED` (closed by `mark-failed`) |
| `gre_sample_selections.selection_status` | `SELECTED`, `REPLACED`, `REPLACEMENT`, NULL (a candidate not picked, or excluded) |

### 6.4 Placeholders in SQL text
| Token | Value |
|---|---|
| `$env` | Environment value (`dev`, `qa`, `int`, empty for UAT / PROD, or the Variable's `env_value`) |
| `{as_of_date}` | The run's business date `YYYY-MM-DD` |
| `{run_key}` | The run key |
| `{name}` / `$name` | A `run_params` value or a PARAM rule |

### 6.4a Dates held as text

Some universe columns are dates stored as `VARCHAR(10)` (ODAG1 `DETRMTN_NOTFTN_DT`: its format is Q6 of
OPEN_ITEMS.md). Compare them as dates, with the format they are written in:

| Stored as | Condition (in a rule's `WHERE`, `scope_sql`, `extra_filters`) |
|---|---|
| `2026-08-07` | `CAST(col AS DATE FORMAT 'YYYY-MM-DD') = DATE '{as_of_date}'` |
| `08/07/2026` | `CAST(col AS DATE FORMAT 'MM/DD/YYYY') = DATE '{as_of_date}'` |
| `20260807` | `CAST(col AS DATE FORMAT 'YYYYMMDD') = DATE '{as_of_date}'` |
| some rows not dates | `CASE WHEN col LIKE '____-__-__' THEN CAST(col AS DATE FORMAT 'YYYY-MM-DD') END` (a bad value then compares as NULL instead of failing the pull) |

A reporting lag: decisions 7 to 14 days before the run's date –
`CAST(col AS DATE FORMAT 'YYYY-MM-DD') BETWEEN DATE '{as_of_date}' - 14 AND DATE '{as_of_date}' - 7`.
A week by its Monday: `TD_WEEK_BEGIN(CAST(col AS DATE FORMAT 'YYYY-MM-DD'))`. A date column needs no `CAST`.
Check each with `validate --check-sql`, then on the universe:
`SELECT col, COUNT(*) FROM <universe> WHERE <the CAST> IS NULL GROUP BY 1` should return nothing.

### 6.5 Paths
A bucket's path is its bucket names joined by `/` from level 0: `Denied/Part B`. A case no bucket `SELECT`
returned is in `OTHER`; a NULL value (or a case a value `SELECT` does not return) is in `NULL`. No levels: `ALL`.

## 7. Recipes: from a requirement to rows

The worked example is `sql/seed/umcm_sample_config.sql` (every kind of rule once).

| Requirement | Rows |
|---|---|
| Sample only after the batch is closed | GATE: `SELECT 1 FROM <control table> WHERE … = '{batch_id}' AND <closed>` |
| Sample only this week's batch | PARAM `batch_id` = `SELECT MAX(...)`, `scope_sql` `BTCH_ID = '{batch_id}'`, Variable `run_key` `"{batch_id}"` |
| Never audit auto-approvals | EXCLUSION: `SELECT <keys> FROM <universe> WHERE <auto-approved>` |
| Never audit a case twice in 90 days | EXCLUSION, `PRIOR_SELECTION`, `lookback_days` 90 |
| Last week's unselected cases first | PRIORITY, `PRIOR_CANDIDATE`, `seq_no` 1 |
| Revisions first, then inpatient | PRIORITY rules in that order, each a `SELECT` of its cases |
| 80 % denials, 10 % withdrawals | LEVEL 0 with `rule_sql` `SELECT <keys>, <disposition column> FROM <universe>`, FRACTION BUCKETs named by the values |
| Every withdrawal there is, denials the rest | BUCKET Withdrawn `ALL`, BUCKET Denied `REMAINDER` |
| Functional areas only within denials | LEVEL 1 `parent_path` `Denied`, its BUCKETs `parent_path` `Denied` |
| Areas defined by several columns or other tables | LEVEL 1 with `rule_sql` NULL, one BUCKET per area with its `SELECT` (any joins) in `rule_sql`, a last BUCKET without `rule_sql` for the rest |
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
- **Started by the framework**: add this DAG to a framework step Variable's `trigger_dags`; here
  `trigger_keys` `["as_of"]` and `trigger_run_params` for the values the rules use (AIRFLOW_DEPLOYMENT.md,
  "Started by the compliance framework").
- **Auditors** get `sample_ready_email` (counts, run key, output table, rules hash) with `notify_recipients`.
- **Triggering** (UI "Trigger DAG w/ config", CLI, REST, or an upstream DAG): `config_id`, `project`,
  `process_name`, `run_key`, `run_params`, `extra_filters`, `seed`, `as_of`, `dry_run`, `log_level` – limited by the Variable's
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
| `export-config --config-id N [--file F]` | One config and all its rule rows as JSON (4.3) | – |
| `load-config --file F [--apply] [--by U]` | Checks a JSON config, shows the changes; `--apply` writes them (4.3) | config, rules |
| `purge --config-id N \| --project P \| --process-name X --older-than-days D [--apply]` | Counts (`--apply`: deletes) the selection rows old runs no longer need (12) | selections |

Exit codes: 0 done, 1 a sample failed (or `load-config` found problems, or `purge` skipped a config), 2 a usage
or configuration problem. One log file per command in
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

### 10.4a The run is waiting for data (GATE_NOT_MET)
The message names the GATE rules not met. Airflow retries the task `retries` times, `retry_delay_minutes` apart;
each attempt is a run row. When the retries run out the task fails: check the load, then clear the task (or
rerun locally) once the data is there.

### 10.5 A run is stuck in RUNNING
The worker was killed mid-run (nothing of the run was written: the writes are one transaction).
`mark-failed --run-id <run_id>`, then rerun.

### 10.6 Redo a cycle
Run again with the same `run_key` (and `--seed` from the run row to reproduce a RANDOM draw). The earlier run
turns `etl_is_curr_ind = 'N'` everywhere, the snapshot table included.

### 10.7 Change a mix, a rule, a priority
New rule rows, or `act_ind = 0` on the old ones (or `export-config`, edit, `load-config`), then
`validate --check-sql` and a `--dry-run`. The next run uses
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

## 12. Retention

`gre_sample_selections` holds every candidate of every run. `purge --older-than-days D` deletes, per config of the
scope, for runs whose as-of date is more than D days ago:

| Run | Rows deleted |
|---|---|
| replaced by a rerun (`etl_is_curr_ind = 'N'`), not FINAL | all of them |
| current, `COMPLETED` | the excluded rows and the candidates never selected |
| `FINAL` (current or not) | none |
| newer than D days | none |

The selected, `REPLACED` and `REPLACEMENT` rows of current runs stay, so the sample of every cycle, the cases
taken out and why stay readable, and no-repeat rules keep working. The run rows (with each run's rules, counts per
bucket and hash) are never deleted. Not touched: the output snapshot tables.

D must be at least the longest `lookback_days` of the active history rules that read the config's rows (its own
and the `PROJECT`-scope rules of its project), or the config is skipped with the reason: a carry-over or a
no-repeat rule would otherwise lose the cycles it looks back on. A replacement (10.9) needs the unselected
candidates of its run: replace before the run is older than D.

Without `--apply` only the counts are shown. In Airflow, the Variable's `retention_days` adds a
`purge_selections` task after the runs (it applies). How long the candidate rows must be kept is a records
retention decision (OPEN_ITEMS.md Q14): until it is made, leave `retention_days` out.
