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

Every cycle (a day, a week, a month) an audit team needs a number of cases (`Target_Val`: e.g. 150 cases, or 5 % of
the cases there are) drawn from what was loaded, with:

- **exclusions**: cases that must never be audited (auto-approvals, no prior authorisation needed, a service
  type, a case already audited recently);
- **a mix**: shares per group (80 % denials, 10 % withdrawals ...), groups within groups (functional areas, but
  only within denials), "everything there is" for small groups, "the rest" for the big one;
- **priority**: which cases go first within a group (revisions first, then inpatient, then expedited ...);
  cases left over from last cycle first;
- **variety**: not ten cases of the same code when other codes exist;
- **a target table**: the selected cases' source rows, every column, in a table of each sample's own.

A **sample** is one **project + table + run type** (`Project_Cd`, `Table_Nm`, `Run_Ty`: `UMCM` / `ODAG1` /
`WKLY`), as the GOVT_RGLTRY framework scopes its batches. A project may sample several tables, and a table may be
sampled daily, weekly and monthly, each with its own rules or sharing them.

All of that is **configuration rows holding SQL**: one config row per sample and one rule row per rule. No field
name and no value is in the code. A new sample, or a changed mix, is new rows, not a release.

Every run is recorded: what was in scope, what was excluded and why, every candidate's group and rank, what was
selected, the counts per rule and per group, the outcome, and **the exact configuration and rules it used** (with a
hash, so two runs made by the same rules are recognisable). A rerun of the same cycle replaces the earlier sample as
the current one; nothing is ever deleted.

Once the auditors start on a sample it can be **finalised**: a rerun of that cycle is then refused unless someone
gives a reason, which is recorded. A case the auditors cannot review can be **replaced** by the next candidate of
its group, with the reason and who did it recorded on both cases.

## 2. The life of a run

One run = one `GOVT_RGLTRY_SamplingConfig` row (a task may run several, see [8](#8-airflow-connection-variable-triggers)).

| Step | What happens | Configured in | Fails as |
|---|---|---|---|
| 0. Rules | The rule rows of the project whose `Table_Nm` is the sample's table or `*` and whose `Run_Ty` is its run type or `*`; of rows with the same class, `Rule_Nm`, `Level_Nbr` and `Parent_Path_Txt`, the most specific wins ([4.2](#42-govt_rgltry_samplingrule--one-row-per-rule)) | `GOVT_RGLTRY_SamplingRule` | |
| 1. Parameters | Each PARAM rule runs in `Seq_Nbr` order; the first column of the first row becomes `{Rule_Nm}`. Values passed with the run win. `{as_of_date}`, `{project_cd}`, `{table_nm}`, `{run_ty}` are always there. The `run_key` template is filled (`{batch_id}` → `B42`). The run's `extra_filters` are kept with the parameters | PARAM rules, the run's `run_params`, `extra_filters`, `run_key` | `PARAM_FAILURE` (technical, retried) – usually "data not loaded yet" |
| 2. Run row | `GOVT_RGLTRY_SamplingRun` row, `Run_Stat` `RUNNING`, with the configuration it uses, its `Config_Hash`, and its rules in `GOVT_RGLTRY_SamplingRunRule`. If the cycle's current sample is `FINAL`, the run stops here unless it was given a force reason | – | `FINALIZED` (not retried) |
| 3a. Gates | Every GATE rule's `SELECT` must return a row (e.g. "the batch is closed"), else the run waits: it fails and Airflow retries it after `retry_delay_minutes` | GATE rules | `GATE_NOT_MET` (retried), `GATE_FAILURE` |
| 3. Check | Every row is checked (classes, types, numbers and percentages, names, columns a class does not use, every SQL a `SELECT`) | both configuration tables | `CONFIG_ERROR` (not retried) |
| 4. Pull | The key columns of the rows `Scope_Sql_Txt` returns (NULL: every source row), the run's `extra_filters` applied to them | `GOVT_RGLTRY_SamplingConfig`, `extra_filters` | `PULL_FAILURE` |
| 4a. Rule queries | Each EXCLUSION / PRIORITY / LEVEL / BUCKET rule's `SELECT` runs on its own; the pulled cases it returns (by `Key_Columns_Txt`) match it, or get its value. Cases it returns outside the scope are ignored | the rules | `QUERY_RULE_FAILURE` (technical, retried); no key columns in the result: `CONFIG_ERROR` |
| 5. Exclude | A row matching any EXCLUSION rule is not a candidate; the first matching rule (`Seq_Nbr`) is its reason. History rules read earlier runs | EXCLUSION rules | |
| 6. Target | `Target_Ty` `CNT`: `Target_Val` cases; `PCT`: `Target_Val` % of the candidates (rounded by `Rounding_Cd`), within `Min_Target_Cnt` / `Max_Target_Cnt` | `GOVT_RGLTRY_SamplingConfig` | |
| 6a. Order | PRIORITY rule 1, then 2, … then the key columns | PRIORITY rules | |
| 7. Stratify | Level by level, each parent's target is split between its buckets (CNT, then PCT, then ALL, then REMAINDER; equal shares water-filled) | LEVEL and BUCKET rules | |
| 8. Select | Per leaf bucket: RANKED (first in order), RANDOM (seeded shuffle) or SYSTEMATIC (every k-th); rotating across `Diversity_Sql_Txt` values | `Smpl_Method_Cd`, `Diversity_Sql_Txt`, `Diversity_Cap_Ty` / `_Val` | `SELECT_FAILURE` |
| 9. Top up | `TOPUP`: a short sample is filled from the other candidates in priority order (the diversity cap still holds) | `Shortfall_Cd` | |
| 10. Target table | The selected cases' source rows (every column) are read back from the source; the table's columns are checked against the source ([4.3](#43-the-target-table)) | `Tgt_Schema_Nm`, `Tgt_Table_Nm` | `CONFIG_ERROR` (columns), `OUTPUT_FAILURE` |
| 11. Persist | **One transaction**: selections (and excluded rows) with their bucket path, the target table rows; the earlier current run of the same sample and `Run_Key` → `Curr_Ind = 0`; run row → `COMPLETED`, current, with its counts (`Run_Rpt_Txt`) | `Keep_Excluded_Ind` | `PERSIST_FAILURE` (nothing half written) |
| Later | `finalize` makes the sample `FINAL`; `replace` swaps a case (see [10.8](#108-finalise-a-sample), [10.9](#109-replace-a-case)) | – | – |

A failure closes the run row as `ERROR` with `Error_Ty`, `Error_Txt` and (technical errors) the end of the
traceback in `Error_Detail_Txt`. A configuration problem fails the task at once; a technical one is retried
(`retries`).

## 3. Tables: map and roles

All in the metadata database (`meta_db`), DDL in `sql/schema.sql`. Names and types follow the framework's
conventions (`_Cd` code, `_Nm` name, `_Txt` text, `_Sql_Txt` SQL, `_Ind` 1/0, `_Cnt` count, `_Nbr` number, `_Dtts`
timestamp, `_Dt_Key` date) and every table has its audit columns `Load_Dtts`, `LoadUser_ID`, `LastUpdt_Dtts`,
`LastUpdt_ID` (the configuration tables also `Active_Ind`). No CLOB columns, no secondary indexes.

| Table | Role | Who writes |
|---|---|---|
| `GOVT_RGLTRY_SamplingConfig` | One row per sample (project + table + run type) | you |
| `GOVT_RGLTRY_SamplingRule` | One row per rule: PARAM, GATE, EXCLUSION, PRIORITY, LEVEL, BUCKET; of one sample, or shared (`*`) | you |
| `GOVT_RGLTRY_SamplingRun` | One row per run attempt: outcome, totals, errors, counts per rule and bucket, the configuration it used | engine |
| `GOVT_RGLTRY_SamplingRunRule` | One row per rule a run used (a copy, as it was) | engine |
| `GOVT_RGLTRY_SampleSelection` | One row per case considered: candidate, selected, excluded | engine |
| *`Tgt_Schema_Nm`.`Tgt_Table_Nm`* | The target table: the selected cases' source rows (optional, one per sample, created by another process, [4.3](#43-the-target-table)) | engine |

```
GOVT_RGLTRY_SamplingConfig (Project_Cd, Table_Nm, Run_Ty)
 └── GOVT_RGLTRY_SamplingRule (Project_Cd, Table_Nm or '*', Run_Ty or '*', Rule_Class_Cd)
       LEVEL rows (Level_Nbr) ← BUCKET rows (same Level_Nbr)

GOVT_RGLTRY_SamplingRun.Smpl_Run_ID = Smpl_Run_ID of GOVT_RGLTRY_SamplingRunRule and GOVT_RGLTRY_SampleSelection
                                    = SMPL_RUN_ID of the target table
```

## 4. Configuration tables (you fill these)

### 4.0 How to make entries

- **Never delete a row**: set `Active_Ind = 0` (1 = in use). `Rule_ID` is yours to choose: unique in the whole
  table (e.g. 1–99 for one sample, 101–199 for the next).
- **Names in any case**: `Project_Cd`, `Table_Nm`, `Run_Ty`, the key columns and the target table's columns are
  matched without regard to case (`umcm` = `UMCM`).
- **SQL text**: every SQL column is a whole `SELECT`: every rule's `Rule_Sql_Txt` (PARAM, GATE, EXCLUSION,
  PRIORITY, LEVEL, BUCKET, 4.2), and the config's `Scope_Sql_Txt` (the run's source rows) and `Diversity_Sql_Txt`
  (the key columns and a value) (4.1). Only `extra_filters`, given when a run is triggered, is a condition. Quotes are
  doubled inside an `INSERT` literal.
- **A number or a percentage**: the sample size (`Target_Ty` + `Target_Val`), a bucket's quota (`Quota_Cd` `CNT` /
  `PCT` + `Quota_Val`), a bucket's cap (`Max_Ty` + `Max_Val`) and the diversity cap (`Diversity_Cap_Ty` +
  `Diversity_Cap_Val`) are each `CNT` (that many cases) or `PCT` (that percentage, 0–100: `5` = 5 %).
- **`$env`** anywhere is replaced by the environment (`DEV` → `dev` ...; UAT / PROD → empty): `EXPORT_FEED_$ENV_T`.
- **`{name}` / `$name`** is replaced by a run parameter, quotes doubled; you write the quotes:
  `BTCH_ID = '{batch_id}'`. A token with no value is a configuration error. `{project_cd}`, `{table_nm}` and
  `{run_ty}` are the sample's own, so one shared rule serves every table and run type of a project.
- **Order** is `Seq_Nbr` (then `Rule_ID`) within a class; `Level_Nbr` for levels.
- **Only the columns of the class** (table in 4.2): `validate` reports any other one that is set.
- **Only reading SQL**: every SQL column is checked before a run (and by `validate`). Refused:
  a statement word (`INSERT`, `UPDATE`, `DELETE`, `MERGE`, `DROP`, `CREATE`, `ALTER`, `REPLACE`, `RENAME`, `GRANT`,
  `REVOKE`, `CALL`, `EXEC`, `COLLECT`, `COMMIT` …), a `;`, a `--` comment (it would comment out the rest of the
  query built around the fragment: use `/* */`), unbalanced parentheses or quotes; PARAM and GATE must be a
  `SELECT` (`WITH …` and `LOCKING ROW FOR ACCESS SELECT …` are fine). Text inside quotes is not checked:
  `disp = 'DELETE'` is a value. The rows run under the service account: this stops a pasted script or a slip,
  it is not a permission (who may write `GOVT_RGLTRY_SamplingRule` is).
- **Everything is a row** in `GOVT_RGLTRY_SamplingConfig` / `GOVT_RGLTRY_SamplingRule`: no configuration files. The
  rows (and each run's copy of them: its run row and its `GOVT_RGLTRY_SamplingRunRule` rows, with `Config_Hash`)
  are the rules' history.
- After any change: `python local/run_sampling.py validate --project P --table T --run-type R --check-sql`
  (section 9).

### 4.1 GOVT_RGLTRY_SamplingConfig – one row per sample

| Column | Meaning |
|---|---|
| `Project_Cd`, `Table_Nm`, `Run_Ty` | The sample (unique together): the framework's project, the table sampled (e.g. `ODAG1`), the run type (`DLY`, `WKLY`, `MNT` …). A DAG run samples every active config of its project, optionally of one table and / or run type |
| `Smpl_Desc` | Optional description |
| `Src_Schema_Nm`, `Src_Table_Nm` | The source table (or view) to sample from: its database and its name, each a plain name, `$env` allowed |
| `Key_Columns_Txt` | Comma separated columns that identify one case. Must be unique within the scope (checked every run) |
| `Scope_Sql_Txt` | A `SELECT` of this run's source rows (any joins), e.g. `SELECT * FROM EXPORT_FEED_$ENV_T.ODAG1 WHERE BTCH_ID = '{batch_id}' AND ETL_IS_CURR_IND = 'Y'`. It must return the key columns, one row per case; the target table gets the rows (and columns) it returns. NULL = every row of the source |
| `Target_Ty` | `CNT` (default): `Target_Val` cases / `PCT`: `Target_Val` % of the candidates (the cases left after the scope and the exclusions) |
| `Target_Val` | The sample size: `150` (CNT) or `5` = 5 % (PCT) |
| `Min_Target_Cnt`, `Max_Target_Cnt` | Optional, PCT only: at least / at most this many cases (5 %, but at least 30 and at most 200). A minimum above the candidates leaves the sample short (`Shortfall_Cd`) |
| `Smpl_Method_Cd` | `RANKED` / `RANDOM` / `SYSTEMATIC` ([6.1](#61-sampling-methods)) |
| `Rounding_Cd` | `FLOOR` / `ROUND` / `CEIL` of every percentage: the target, each bucket's share, a cap (default `FLOOR`) |
| `Diversity_Sql_Txt` | Optional `SELECT` of the key columns and one value (any joins, e.g. a code family from a reference table): within each bucket, picks rotate across the values (cases it does not return form one group, `NULL`) |
| `Diversity_Cap_Ty`, `Diversity_Cap_Val` | Optional: at most this many picks per value – `CNT` `3`, or `PCT` `10` = 10 % of the target of the bucket being picked (of the whole sample's target during the top-up), at least 1 |
| `Shortfall_Cd` | `TOPUP` (default) / `NONE` |
| `Keep_Excluded_Ind` | 1 (default) = store excluded rows with their reason too |
| `Tgt_Schema_Nm`, `Tgt_Table_Nm` | Optional: the target table's database and name (both or neither), `$env` allowed, [4.3](#43-the-target-table) |
| `Active_Ind` | 1 = in use |

The schedule is not a column: it is the Airflow Variable's (`schedule_interval`, per `runs` entry for timed runs).

### 4.2 GOVT_RGLTRY_SamplingRule – one row per rule

Common columns: `Rule_ID`, `Project_Cd`, `Table_Nm`, `Run_Ty`, `Active_Ind`, `Rule_Class_Cd`, `Seq_Nbr`,
`Rule_Nm`, `Rule_Desc`.

**Which samples a rule is for**: `Table_Nm` a table or `*` (every table of the project), `Run_Ty` a run type or
`*` (every run type). A sample uses the rows with its table or `*` and its run type or `*`. When several rows are
the same rule – the same class, `Rule_Nm`, `Level_Nbr` and `Parent_Path_Txt` – the most specific wins: the table
and run type, then the table (`Run_Ty` `*`), then the run type (`Table_Nm` `*`), then `*` / `*`. So the run types
of a table share a rule by `*`, and one run type changes it with its own row of the same name.

The others, by class:

| Column | PARAM | GATE | EXCLUSION | PRIORITY | LEVEL | BUCKET |
|---|---|---|---|---|---|---|
| `Rule_Nm` | the `{token}` | name (in the message) | the reason stored | name | name | the value / bucket name (no `/`) |
| `Rule_Ty` | | | `QUERY` (default) / `PRIOR_SELECTION` | `QUERY` (default) / `EXPRESSION` / `PRIOR_CANDIDATE` / `PRIOR_SELECTION` | | `QUERY` (default) |
| `Rule_Sql_Txt` | a `SELECT` | a `SELECT`: a row = ready | QUERY: a `SELECT` of the key columns | QUERY: a `SELECT` of the key columns; EXPRESSION: of the key columns and a value | a `SELECT` of the key columns and the value, or NULL (buckets by their `SELECT`s) | bucket level: a `SELECT` of the key columns, or NULL (the rest) |
| `Level_Nbr` | | | | | 0, 1, … | its level |
| `Parent_Path_Txt` | | | | | the parents it splits (default `*`) | the parents it applies under (default `*`) |
| `Unlisted_Cd` | | | | | `SHARE` (default) / `REMAINDER` / `SKIP` | |
| `Quota_Cd` | | | | | | `PCT` (default) / `CNT` / `ALL` / `REMAINDER` |
| `Quota_Val` | | | | | | PCT: 0 to 100 (% of the parent's target); CNT: a number of cases |
| `Max_Ty`, `Max_Val` | | | | | | optional cap: `CNT` a number, or `PCT` a % of the bucket's candidates |
| `Sort_Dir_Cd` | | | | EXPRESSION: `ASC` (default) / `DESC` | | |
| `Lookback_Days`, `History_Scope_Cd` | | | PRIOR_SELECTION | PRIOR_* | | |

**PARAM** – a run parameter. The first column of the first row of `Rule_Sql_Txt` becomes `{Rule_Nm}`; a later
PARAM may use an earlier one. No value (no row, or NULL) fails the run as `PARAM_FAILURE`, which is retried:
typically the load has not finished. A value passed with the run (`run_params`) is used instead of the lookup.
Example – the latest closed batch of the sample's own project, table and run type, one row (`*` / `*`) for the
whole project (the control table is configuration: name yours):
`SELECT MAX(Btch_ID) FROM <control table> WHERE Project_Cd = '{project_cd}' AND Table_Nm = '{table_nm}' AND
Run_Ty = '{run_ty}' AND <closed>`.

**Run-time filters (`extra_filters`)** – for a source table holding several loads, batches or SCD2 versions of a
case, the run can be given a SQL condition (the Variable, a `runs` entry, a trigger, or `--filter "SQL"` locally),
e.g. `BTCH_ID = 'B42' AND ETL_IS_CURR_IND = 'Y'`, or an SCD2 version valid on the
run date: `DATE '{as_of_date}' BETWEEN EFF_DT AND END_DT`. The conditions (each layer's, each `--filter`'s, ANDed)
apply to the rows `Scope_Sql_Txt` returns, so the pull and the target table read the same rows; they are kept in
`Run_Parm_Txt`,
so a later case replacement reads it too. It is checked like any configured SQL (only reading, no `;`, no `--`).
When the filters leave two rows with the same key columns, the run stops with "key_columns_txt do not identify a
case uniquely": add the filter that picks one version. (A rule's own `SELECT` reads its tables itself: give a value
`SELECT` the same version filter, or it returns a case twice.)

**GATE** – a readiness check, run after the PARAM rules (so it may use `{batch_id}`). Its `SELECT` returning no
row means "not ready": the run ends as `GATE_NOT_MET` and Airflow retries it (`retries`, `retry_delay_minutes`).
Nothing is pulled or written but the run row. Example – sample only a closed batch:
`SELECT 1 FROM <control table> WHERE <batch column> = '{batch_id}' AND <status column> = '<closed>'`.

**Every selection rule is a SQL statement**: a whole `SELECT` returning the key columns of the cases it matches
(same column names, any case; joins, subqueries, `WITH`, any table – anything that reads). Each runs on its own
after the pull; only the cases in the run's scope (`Scope_Sql_Txt`, `extra_filters`) count, so a rule need not
repeat them (a filter on the batch keeps it fast). It may use `$env`, `{batch_id}` and the other run parameters. It
must be a `SELECT` that only reads (checked like a PARAM rule); `validate --check-sql` EXPLAINs every one. Where a
value is needed (a PRIORITY `EXPRESSION`, a LEVEL by value) the `SELECT` returns one more column, the value, and each
case at most once. A rule whose `SELECT` fails to run fails the run as `QUERY_RULE_FAILURE` (retried); one returning
no key columns is a `CONFIG_ERROR`. The former condition form (`Rule_Ty` `SQL` / `CONDITION`) is reported by
`validate` with how to rewrite it.

**EXCLUSION** – `QUERY` (default): the cases the `SELECT` returns are excluded. `PRIOR_SELECTION`: selected by a
current run of another `Run_Key` within `Lookback_Days` (by `Smpl_Cycle_Dt_Key`), of this sample
(`History_Scope_Cd` `SAMPLE`, default), of any run type of its table (`TABLE`: a monthly sample does not repeat the
weekly samples' cases) or of any sample of the project (`PROJECT`). The first matching rule by `Seq_Nbr` is the
reason; each rule's count is in the run's `Run_Rpt_Txt`.

**PRIORITY** – `QUERY` (default): the cases the `SELECT` returns first. `EXPRESSION`: a `SELECT` of the key columns
and a value, the cases ordered by it (`Sort_Dir_Cd`; cases it does not return, or NULL, last). `PRIOR_CANDIDATE`:
considered by an earlier run but never selected, first (carry-over). `PRIOR_SELECTION`: selected earlier, last.
Both read `History_Scope_Cd` like the exclusion. RANKED takes the first ones, SYSTEMATIC steps through the order,
RANDOM ignores it. To order by a column (newest first, say), use an `EXPRESSION` rule: `SELECT <keys>,
AUTHZN_REQ_DT_TM FROM <source> WHERE <the current version>` with `Sort_Dir_Cd` `DESC`. After the PRIORITY rules,
ties are broken by the key columns.

Example – 21 % of the ODAG1 weekly sample Behavioral Health, BH meaning a code on a list and a BH provider:

```sql
INSERT INTO {{META_DB}}.GOVT_RGLTRY_SamplingRule (Rule_ID, Project_Cd, Table_Nm, Run_Ty, Rule_Class_Cd, Seq_Nbr, Rule_Nm, Level_Nbr, Parent_Path_Txt, Unlisted_Cd)
VALUES (241, 'UMCM', 'ODAG1', 'WKLY', 'LEVEL', 1, 'program', 1, '*', 'SKIP');
INSERT INTO {{META_DB}}.GOVT_RGLTRY_SamplingRule (Rule_ID, Project_Cd, Table_Nm, Run_Ty, Rule_Class_Cd, Seq_Nbr, Rule_Nm, Level_Nbr, Parent_Path_Txt, Quota_Cd, Quota_Val, Rule_Sql_Txt)
VALUES (242, 'UMCM', 'ODAG1', 'WKLY', 'BUCKET', 1, 'Behavioral Health', 1, '*', 'PCT', 21,
        'SELECT a.AUTHZN_ID, a.SRC_DATA_KEY
         FROM EXPORT_FEED_$ENV_T.SDO_CMPLNC_CLNCL_AUD_UM_AUTHZN_ODAG1 a
         JOIN REF_$ENV.BH_PROC_CODES c ON c.PROC_CD = a.PROC_CD AND c.ACT_IND = 1
         JOIN REF_$ENV.PROVIDER p ON p.PROV_ID = a.SVC_PROV_ID
         WHERE a.BTCH_ID = ''{batch_id}'' AND p.PROV_TY IN (''PSY'', ''LCSW'')');
-- no Rule_Sql_Txt: every case no earlier bucket took
INSERT INTO {{META_DB}}.GOVT_RGLTRY_SamplingRule (Rule_ID, Project_Cd, Table_Nm, Run_Ty, Rule_Class_Cd, Seq_Nbr, Rule_Nm, Level_Nbr, Parent_Path_Txt, Quota_Cd)
VALUES (243, 'UMCM', 'ODAG1', 'WKLY', 'BUCKET', 2, 'Other', 1, '*', 'REMAINDER');
```

(The tables and columns are illustrations. `Active_Ind`, `Load_Dtts`, `LoadUser_ID`, `LastUpdt_Dtts` and
`LastUpdt_ID` take their defaults.)

**LEVEL** – a split. `Rule_Sql_Txt` set (**value level**): a `SELECT` of the key columns and a value; each value is
a bucket (a case it does not return is in `NULL`). `Rule_Sql_Txt` NULL (**bucket level**): the buckets are its
BUCKET rows' `SELECT`s, tried in `Seq_Nbr` order, the first that returned the case wins; a BUCKET without
`Rule_Sql_Txt` takes every case left (only one, and last); cases matching none are bucket `OTHER`.
`Parent_Path_Txt`: the parents this level splits – `*` = all, `Denied` = only within Denied, `Denied/*` … (`*` and
`?` wildcards, not case specific); under other parents the level is skipped. `Unlisted_Cd`: what happens to a value
with no BUCKET row, or to `OTHER` – `SHARE` = share what the PCT and CNT buckets leave equally, `REMAINDER` = as `Quota_Cd`
REMAINDER, `SKIP` = never selected. No LEVEL rules = no stratification: the whole pool is one bucket (`ALL`).

**BUCKET** – a bucket of the LEVEL with the same `Level_Nbr`. `Parent_Path_Txt`: when several patterns match a
parent, the most specific wins (fewest wildcards, then longest), so one level can have a different mix per parent.
How a parent's target T is split:

1. CNT buckets: `Quota_Val` cases each, then PCT buckets: `round(T × Quota_Val / 100)`, both in `Seq_Nbr` order
   and never more than what is left of T: when the quotas add up to more than T (fixed numbers under a small
   percentage target, say), the later buckets are cut and the run report's `notes` says which. Unlisted SHARE
   buckets: `round((T × (1 − Σ PCT / 100) − Σ CNT) / n)`.
2. ALL buckets share what is left of T after step 1, equally, each up to what it has.
3. REMAINDER buckets share T minus what was *actually* selected in 1 and 2 – so a short PCT or CNT bucket's unused
   quota flows to them.
4. "Share equally" is water-filled: a bucket with fewer cases than its share keeps all it has and the others share
   the rest again. 20 between buckets of 2, 50 and 50 cases → 2, 9, 9.
5. `Max_Ty` / `Max_Val` caps a bucket at every step: `CNT` a number, `PCT` a percentage of the bucket's candidates.

A level's PCT `Quota_Val` values under one `Parent_Path_Txt` add up to 100 at most.

### 4.3 The target table

Each sample may name its own target table: `Tgt_Schema_Nm` and `Tgt_Table_Nm` (both, or neither; each a plain
name, `$env` allowed: `UMCM_$ENV_T` + `ODAG1_WKLY_SAMPLE`). The table is **created by another process**; the engine
never creates or alters it, it reads its columns and inserts rows. One row per selected case:

| Columns of the target table | Value |
|---|---|
| any column the source (`Src_Schema_Nm`.`Src_Table_Nm`) has | copied from the case's source row, read under the run's scope and `extra_filters` |
| `ETL_IS_CURR_IND` (**required**) | `'Y'`; `'N'` once a rerun or a replacement retires the row |
| `ETL_IS_DEL_IND` | `'N'`; `'Y'` once retired |
| `ETL_LOAD_DT_TM` | when the row was written |
| `ETL_LAST_UPDT_DT_TM` | when the row was written, then when it was retired |
| `SMPL_RUN_KEY` (**required**) | the cycle's `Run_Key` |
| `SMPL_RUN_ID` | the run (`GOVT_RGLTRY_SamplingRun.Smpl_Run_ID`) |
| `SMPL_RUN_TY` | the sample's run type |
| `SMPL_CYCLE_DT` | the run's as-of date |
| `SMPL_ORDER_NBR` | the case's `Selection_Order_Nbr` |
| `SMPL_STRATUM_PATH` | the case's bucket path (`Denied/Part B`) |

The `ETL_*` and `SMPL_*` columns are always set by sampling, **even when the source has a column of the same name**
(the source's own audit values are not copied). Column names are matched in any case. Only the columns the table
has are written; it must have the two required ones and the key columns. A column that is neither in the source nor
in the list above fails the run as `CONFIG_ERROR`; `validate --check-sql` reports it before a run. A rerun of a
cycle retires the current rows of its `SMPL_RUN_KEY`, as the framework's loads do (`ETL_IS_CURR_IND = 'N'`,
`ETL_IS_DEL_IND = 'Y'`, `ETL_LAST_UPDT_DT_TM` now), so give each sample its own table (two run types sharing one
table would retire each other's rows of the same run key). The service account needs SELECT, INSERT and UPDATE on
it. OUTPUT rules are no longer used (a row of that class is reported: set `Active_Ind = 0`).

## 5. Tables the engine writes (read only)

| Table | One row per | Columns to read |
|---|---|---|
| `GOVT_RGLTRY_SamplingRun` | run attempt | `Run_Stat`, `Curr_Ind`, `Project_Cd` / `Table_Nm` / `Run_Ty`, `Run_Key`, `As_Of_Dt_Key`, `Random_Seed_Nbr`, `Run_Parm_Txt`, the counts (`In_Scope_Cnt`, `Excluded_Cnt`, `Candidate_Cnt`, `Target_Cnt` – the number of cases the run targeted, a PCT target resolved – `Selected_Cnt`, `Shortfall_Cnt`), `Error_Ty`, `Error_Txt`, `Error_Detail_Txt`, `Run_Rpt_Txt`, `Config_Hash`, `Finalized_By` / `Finalized_Dtts`, `Override_Rsn_Txt`, and the configuration it used (`Src_Schema_Nm` … `Tgt_Table_Nm`) |
| `GOVT_RGLTRY_SamplingRunRule` | rule a run used | every rule column, as it was when the run started |
| `GOVT_RGLTRY_SampleSelection` | candidate and excluded row of a run | `Selected_Ind`, `Selection_Order_Nbr`, `Priority_Rank_Nbr`, `Excluded_Ind`, `Exclusion_Rsn_Txt`, `Stratum_Path_Txt`, `Selection_Stat`, `Linked_Case_Key`, `Stat_Rsn_Txt` / `Stat_By` / `Stat_Dtts`, `Curr_Ind` |

**Current sample** of a cycle: `Curr_Ind = 1` for its sample (project, table, run type) and `Run_Key`, on the run row
and on its selections. A successful rerun turns the earlier rows to 0 (with `LastUpdt_Dtts`, `LastUpdt_ID`) in the
same transaction that writes the new ones; a failed rerun leaves the earlier sample current. The same `Run_Key` of
two run types is two samples.

**The rules a run used**: its run row holds the configuration columns, `GOVT_RGLTRY_SamplingRunRule` its rule rows
(the resolved ones, with their own `Table_Nm` / `Run_Ty`: `*` for a shared rule). `Config_Hash` is the SHA-256 of
the configuration and rules as loaded, without who changed a row and when: equal hashes = the same rules.

**`Run_Rpt_Txt`** (JSON, VARCHAR(16000)): `{"exclusions": {Rule_Nm: count}, "buckets": {path: {"candidates",
"target", "selected"}}, "notes": [...]}` – every bucket node, not only the leaves (`Denied` and `Denied/Part B`);
`notes` (only when there are some) names each quota cut to fit its parent's target. A failed run has none. When the buckets do not fit, they are left out of it (the selections' `Stratum_Path_Txt` has them).

**`Selection_Stat`**: `SELECTED` for the cases the run picked; `REPLACED` for a case taken out later
(`Selected_Ind` back to 0) and `REPLACEMENT` for the case put in for it (`Selected_Ind` 1, the same
`Selection_Order_Nbr`); `Linked_Case_Key` points from each to the other. The current sample is always
`Selected_Ind = 1`.

**`Stratum_Path_Txt`** is the case's bucket at every level, `/` separated: the group per level of any candidate,
and counts per bucket, are a `GROUP BY` on it.

`GOVT_RGLTRY_SampleSelection` grows by one row per candidate per run; `purge` removes the rows no longer needed
(section 12). `GOVT_RGLTRY_SamplingRun` and `GOVT_RGLTRY_SamplingRunRule` rows are never deleted.

## 6. Reference lists (all codes and values)

### 6.1 Sampling methods
| Code | Meaning | Needs |
|---|---|---|
| `RANKED` | The first n of each bucket in priority order | PRIORITY rules |
| `RANDOM` | A seeded shuffle of each bucket (seed on the run row; rerun with `--seed` to reproduce) | – |
| `SYSTEMATIC` | Seeded start, then every k-th in priority order. No `Diversity_Sql_Txt` | PRIORITY rules |

### 6.2 Rule classes and their values
| Column | Values |
|---|---|
| `GOVT_RGLTRY_SamplingRule.Rule_Class_Cd` | `PARAM`, `GATE`, `EXCLUSION`, `PRIORITY`, `LEVEL`, `BUCKET` |
| `Table_Nm`, `Run_Ty` (rule) | a table / run type, or `*` = all of them |
| `Rule_Ty` (EXCLUSION) | `QUERY` (default), `PRIOR_SELECTION` |
| `Rule_Ty` (PRIORITY) | `QUERY` (default), `EXPRESSION`, `PRIOR_CANDIDATE`, `PRIOR_SELECTION` |
| `Rule_Ty` (BUCKET) | `QUERY` (default) |
| `Quota_Cd` (BUCKET) | `PCT` (default), `CNT`, `ALL`, `REMAINDER` |
| `Target_Ty`, `Max_Ty`, `Diversity_Cap_Ty` | `CNT` (a number of cases), `PCT` (a percentage, 0–100) |
| `Unlisted_Cd` (LEVEL) | `SHARE`, `REMAINDER`, `SKIP` |
| `History_Scope_Cd` | `SAMPLE` (default), `TABLE`, `PROJECT` |
| `GOVT_RGLTRY_SamplingConfig.Shortfall_Cd` | `TOPUP`, `NONE` |
| `GOVT_RGLTRY_SamplingConfig.Rounding_Cd` | `FLOOR`, `ROUND`, `CEIL` |

### 6.3 Statuses and error types
| Column | Values |
|---|---|
| `GOVT_RGLTRY_SamplingRun.Run_Stat` | `RUNNING`, `COMPLETED`, `FINAL` (finalised), `ERROR` |
| `GOVT_RGLTRY_SamplingRun.Error_Ty` | `CONFIG_ERROR` (fix the rows, not retried) · `FINALIZED` (the cycle's sample is final, not retried) · `GATE_NOT_MET` (data not ready, retried) · `GATE_FAILURE` · `PARAM_FAILURE` · `PULL_FAILURE` · `QUERY_RULE_FAILURE` · `SELECT_FAILURE` · `OUTPUT_FAILURE` · `PERSIST_FAILURE` (technical, retried) · `ABANDONED` (closed by `mark-failed`) |
| `GOVT_RGLTRY_SampleSelection.Selection_Stat` | `SELECTED`, `REPLACED`, `REPLACEMENT`, NULL (a candidate not picked, or excluded) |

### 6.4 Placeholders in SQL text
| Token | Value |
|---|---|
| `$env` | Environment value (`dev`, `qa`, `int`, empty for UAT / PROD, or the Variable's `env_value`) |
| `{as_of_date}` | The run's business date `YYYY-MM-DD` |
| `{run_key}` | The run key |
| `{project_cd}`, `{table_nm}`, `{run_ty}` | The sample's project, table and run type (upper case) |
| `{name}` / `$name` | A `run_params` value or a PARAM rule |

### 6.4a Dates held as text

Some source columns are dates stored as `VARCHAR(10)` (ODAG1 `DETRMTN_NOTFTN_DT`: its format is Q6 of
OPEN_ITEMS.md). Compare them as dates, with the format they are written in:

| Stored as | Condition (in the `WHERE` of a rule's or the scope's `SELECT`, `extra_filters`) |
|---|---|
| `2026-08-07` | `CAST(col AS DATE FORMAT 'YYYY-MM-DD') = DATE '{as_of_date}'` |
| `08/07/2026` | `CAST(col AS DATE FORMAT 'MM/DD/YYYY') = DATE '{as_of_date}'` |
| `20260807` | `CAST(col AS DATE FORMAT 'YYYYMMDD') = DATE '{as_of_date}'` |
| some rows not dates | `CASE WHEN col LIKE '____-__-__' THEN CAST(col AS DATE FORMAT 'YYYY-MM-DD') END` (a bad value then compares as NULL instead of failing the pull) |

A reporting lag: decisions 7 to 14 days before the run's date –
`CAST(col AS DATE FORMAT 'YYYY-MM-DD') BETWEEN DATE '{as_of_date}' - 14 AND DATE '{as_of_date}' - 7`.
A week by its Monday: `TD_WEEK_BEGIN(CAST(col AS DATE FORMAT 'YYYY-MM-DD'))`. A date column needs no `CAST`.
Check each with `validate --check-sql`, then on the source:
`SELECT col, COUNT(*) FROM <source> WHERE <the CAST> IS NULL GROUP BY 1` should return nothing.

### 6.5 Paths
A bucket's path is its bucket names joined by `/` from level 0: `Denied/Part B`. A case no bucket `SELECT`
returned is in `OTHER`; a NULL value (or a case a value `SELECT` does not return) is in `NULL`. No levels: `ALL`.

## 7. Recipes: from a requirement to rows

There are no seed files: the rows are entered in the tables. The ODAG1 rows known so far are in OPEN_ITEMS.md,
section D.

| Requirement | Rows |
|---|---|
| Sample only after the batch is closed | GATE: `SELECT 1 FROM <control table> WHERE … = '{batch_id}' AND <closed>` |
| Sample only this week's batch | PARAM `batch_id` = `SELECT MAX(...)`, `Scope_Sql_Txt` `SELECT * FROM <source> WHERE BTCH_ID = '{batch_id}'`, Variable `run_key` `"{batch_id}"` |
| One batch lookup for every table and run type | PARAM and GATE rows with `Table_Nm` / `Run_Ty` `*` whose SQL uses `{project_cd}`, `{table_nm}`, `{run_ty}` |
| A daily, a weekly and a monthly sample of one table | three config rows (`Run_Ty` `DLY`, `WKLY`, `MNT`); shared rules `Run_Ty` `*`, the differences a row per run type; Variable `runs` entries with their own `schedule_interval` |
| The monthly sample has its own mix | its LEVEL / BUCKET rows with `Run_Ty` `MNT` (the same `Rule_Nm` replaces the shared row) |
| Never audit auto-approvals | EXCLUSION: `SELECT <keys> FROM <source> WHERE <auto-approved>` |
| Never audit a case twice in 90 days | EXCLUSION, `PRIOR_SELECTION`, `Lookback_Days` 90 |
| … nor a case a weekly sample of the table took | the same, `History_Scope_Cd` `TABLE` |
| Last week's unselected cases first | PRIORITY, `PRIOR_CANDIDATE`, `Seq_Nbr` 1 |
| Revisions first, then inpatient | PRIORITY rules in that order, each a `SELECT` of its cases |
| 80 % denials, 10 % withdrawals | LEVEL 0 with `Rule_Sql_Txt` `SELECT <keys>, <disposition column> FROM <source>`, PCT BUCKETs named by the values (`Quota_Val` 80, 10) |
| Every withdrawal there is, denials the rest | BUCKET Withdrawn `ALL`, BUCKET Denied `REMAINDER` |
| Functional areas only within denials | LEVEL 1 `Parent_Path_Txt` `Denied`, its BUCKETs `Parent_Path_Txt` `Denied` |
| Areas defined by several columns or other tables | LEVEL 1 with `Rule_Sql_Txt` NULL, one BUCKET per area with its `SELECT` (any joins) in `Rule_Sql_Txt`, a last BUCKET without `Rule_Sql_Txt` for the rest |
| Equal portions | BUCKETs `REMAINDER` (or no BUCKET rows: unlisted `SHARE`) |
| No more than 3 of one code | `Diversity_Sql_Txt` `SELECT <keys>, <code> FROM <source>`, `Diversity_Cap_Ty` `CNT`, `Diversity_Cap_Val` 3 |
| 5 % of the week's cases, at least 30, at most 200 | `Target_Ty` `PCT`, `Target_Val` 5, `Min_Target_Cnt` 30, `Max_Target_Cnt` 200 |
| Always 10 inpatient cases, the rest by mix | BUCKET Inpatient `Quota_Cd` `CNT`, `Quota_Val` 10; the others PCT / REMAINDER |
| Exactly 150 or flag it | `Shortfall_Cd` `NONE`, Variable `fail_on_shortfall` true |
| A table of the selected cases for the auditors | `Tgt_Schema_Nm` + `Tgt_Table_Nm` (4.3) |

## 8. Airflow: Connection, Variable, triggers

Full reference in [AIRFLOW_DEPLOYMENT.md](AIRFLOW_DEPLOYMENT.md).

- **Connection**: the Variable's `connection_id` (else `td_conn_var`, else `govt_rgltry_teradata`), type
  Teradata, login and password of the service account, `{"logmech": "LDAP"}` in extra.
- **Variable** `govt_rgltry_umcm_sampling_config_var`: `meta_db`, `load_env`, the scope (`project`, optionally
  `table` / `run_type`), `schedule_interval` (or `runs` entries each with their own, for the run types of a
  project), emails, retries.
- **Started by the framework**: add this DAG to a framework step Variable's `trigger_dags`; here
  `trigger_keys` `["as_of"]` and `trigger_run_params` for the values the rules use (AIRFLOW_DEPLOYMENT.md,
  "Started by the GOVT_RGLTRY framework").
- **Auditors** get `sample_ready_email` (counts, run key, target table, rules hash) with `notify_recipients`.
- **Triggering** (UI "Trigger DAG w/ config", CLI, REST, or an upstream DAG): `project`, `table`, `run_type`,
  `run_key`, `run_params`, `extra_filters`, `seed`, `as_of`, `dry_run`, `log_level` (`run` on a timed DAG) – limited by the Variable's
  `trigger_keys` when set. The database, the Connection and the environment are never taken from a trigger.

```bash
airflow dags trigger GOVT_RGLTRY_UMCM_SAMPLING -c '{"run_key": "{batch_id}", "dry_run": true}'
```

## 9. Local command line

From `local/`, with `dev.env` (copy of `dev.env.example`: Teradata login and `SAMPLING_META_DB`; never committed).

| Command | What it does | Writes |
|---|---|---|
| `test-connection` | Logs in, shows the session and which sampling tables exist | – |
A **scope** is `--project P [--table T] [--run-type R]`; a **sample** is all three. Names in any case.

| Command | What it does | Writes |
|---|---|---|
| `test-connection` | Logs in, shows the session and which sampling tables exist | – |
| `deploy-schema [--dry-run] [--drop-first]` | Creates the 5 tables | DDL |
| `validate <scope> [--check-sql] [--run-param k=v]` | Checks the rows; `--check-sql` also runs the PARAM lookups and `EXPLAIN`s every query | – |
| `show-config <sample>` | The config and the rules it uses, by class, and its problems (JSON) | – |
| `run <scope> [--run-key K] [--run-param k=v] [--as-of D] [--seed S] [--dry-run] [--force-reason R]` | Runs the sample(s); `--force-reason` replaces a `FINAL` sample | yes, except `--dry-run` |
| `finalize <sample> --run-key K [--by U]` | Makes the current sample of the cycle `FINAL` | run row |
| `replace <sample> --run-key K --case-key C --reason R [--any-bucket] [--by U]` | Replaces one selected case by the next candidate of its bucket | selections, target table |
| `runs [<scope>] [--run-key K] [--status S]` | Recent run rows (with `Config_Hash`, `Finalized_By`, `Override_Rsn_Txt`) | – |
| `errors [<scope>]` | Open errors: cycles whose latest attempt failed | – |
| `mark-failed --run-id R` | Closes a run stuck in `RUNNING` (e.g. the worker died) | run row |
| `purge <scope> --older-than-days D [--apply]` | Counts (`--apply`: deletes) the selection rows old runs no longer need (12) | selections |

Exit codes: 0 done, 1 a sample failed (or `purge` skipped a sample), 2 a usage
or configuration problem. One log file per command in
`local/logs`.

## 10. Monitoring and support procedures

### 10.1 Did this week's sample run?
```sql
SELECT Smpl_Run_ID, Run_Key, Run_Stat, Curr_Ind, In_Scope_Cnt, Excluded_Cnt, Selected_Cnt, Shortfall_Cnt,
       Error_Ty, Error_Txt
FROM HSABC_DEV.GOVT_RGLTRY_SamplingRun
WHERE Project_Cd = 'UMCM' AND Table_Nm = 'ODAG1' AND Run_Ty = 'WKLY' ORDER BY Start_Dtts DESC;
```

### 10.2 The current sample of a cycle
```sql
SELECT Case_Key, Selection_Order_Nbr, Stratum_Path_Txt, Priority_Rank_Nbr
FROM HSABC_DEV.GOVT_RGLTRY_SampleSelection
WHERE Project_Cd = 'UMCM' AND Table_Nm = 'ODAG1' AND Run_Ty = 'WKLY' AND Run_Key = 'B42'
  AND Curr_Ind = 1 AND Selected_Ind = 1
ORDER BY Selection_Order_Nbr;
```

### 10.3 Why is the sample short / why this mix?
Candidates and selections per bucket, and the reasons for exclusion:
```sql
SELECT Stratum_Path_Txt, COUNT(*) AS candidates, SUM(Selected_Ind) AS selected
FROM HSABC_DEV.GOVT_RGLTRY_SampleSelection
WHERE Project_Cd = 'UMCM' AND Table_Nm = 'ODAG1' AND Run_Ty = 'WKLY' AND Run_Key = 'B42'
  AND Curr_Ind = 1 AND Excluded_Ind = 0
GROUP BY 1 ORDER BY 1;

SELECT Exclusion_Rsn_Txt, COUNT(*) FROM HSABC_DEV.GOVT_RGLTRY_SampleSelection
WHERE Project_Cd = 'UMCM' AND Table_Nm = 'ODAG1' AND Run_Ty = 'WKLY' AND Run_Key = 'B42'
  AND Curr_Ind = 1 AND Excluded_Ind = 1
GROUP BY 1;
```
The targets per bucket are in the run's `Run_Rpt_Txt` (`buckets.<path>.target`): a bucket with fewer candidates
than its target is short; with `TOPUP` the rest came from other buckets.

### 10.4 A run failed
1. `errors --project P` (or `GOVT_RGLTRY_SamplingRun WHERE Run_Stat = 'ERROR'`).
2. `CONFIG_ERROR`: fix the rows (`validate --check-sql`), then rerun. `PARAM_FAILURE`: is the load finished?
   Others: `Error_Txt` and `Error_Detail_Txt` (the end of the traceback); rerun when the cause is gone.
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
turns `Curr_Ind = 0` everywhere, and `ETL_IS_CURR_IND = 'N'`, `ETL_IS_DEL_IND = 'Y'` in the target table.

### 10.7 Change a mix, a rule, a priority
New rule rows, or `Active_Ind = 0` on the old ones, then
`validate --check-sql` and a `--dry-run`. The next run uses
them; earlier runs are not changed. The new run's `Config_Hash` differs from the previous one; the old rules stay
readable in the old run's `GOVT_RGLTRY_SamplingRunRule` rows. A rule shared by `*` changes every sample using it:
to change one run type only, add a row for it with the same `Rule_Nm`.

### 10.8 Finalise a sample
When the auditors start on a cycle: `finalize --project P --table T --run-type R --run-key K`. From then on a run of that run key, by the
DAG or by hand, fails as `FINALIZED` (no retry) and the final sample stays current. To replace it anyway (e.g. the
rules were wrong), with sign-off: `run --project P --table T --run-type R --run-key K --force-reason "<why,
approved by whom>"` – the reason is stored on the new run row (`Override_Rsn_Txt`), and the final run stays
recorded with `Curr_Ind = 0`. A force is only
possible from the command line, never from a DAG trigger.

### 10.9 Replace a case
A selected case cannot be reviewed (no record, duplicate …):
`replace --project P --table T --run-type R --run-key K --case-key "<Case_Key>" --reason "<why>"`. The next
candidate of the same bucket in priority order goes in (RANDOM: a draw seeded by the run's seed, the same every
time), with the same `Selection_Order_Nbr`; the target table gets its source row and the old row is retired
(`ETL_IS_CURR_IND = 'N'`), evaluated with the configuration and rules of that run (its run row and its
`GOVT_RGLTRY_SamplingRunRule` rows), not today's. When the bucket has no candidate left, `--any-bucket` takes the
next one overall (the mix then differs from the quotas: the reason records why). Allowed on a `FINAL` sample. A
replaced case does not come back as carry-over (`PRIOR_CANDIDATE`) in later cycles. Not checked by a replacement:
the diversity cap (`Diversity_Cap_Val`).

## 11. First run on DEV (checklist)

1. `deploy-schema --dry-run`, then `deploy-schema` (DBA rights on `meta_db`).
2. Have each target table created (4.3: the source's columns plus the `ETL_*` / `SMPL_*` ones).
3. Enter the configuration rows (OPEN_ITEMS.md section D, once its TODOs are answered; `Active_Ind = 1`).
4. `validate --project UMCM --table ODAG1 --run-type WKLY --check-sql`.
5. `run --project UMCM --table ODAG1 --run-type WKLY --dry-run`: check the report's `exclusions`, `buckets` and
   `by_stratum`.
6. The same without `--dry-run`; check 10.1 – 10.3 and the target table.
7. Deploy the DAG, set the Connection and the Variable, trigger with `dry_run`, then for real.
8. Try `finalize`, a rerun (expect `FINALIZED`) and `replace` on a DEV cycle, so the team knows them before audit.

## 12. Retention

`GOVT_RGLTRY_SampleSelection` holds every candidate of every run. `purge <scope> --older-than-days D` deletes, per
sample of the scope, for runs whose as-of date (`As_Of_Dt_Key`) is more than D days ago:

| Run | Rows deleted |
|---|---|
| replaced by a rerun (`Curr_Ind = 0`), not FINAL | all of them |
| current, `COMPLETED` | the excluded rows and the candidates never selected |
| `FINAL` (current or not) | none |
| newer than D days | none |

The selected, `REPLACED` and `REPLACEMENT` rows of current runs stay, so the sample of every cycle, the cases
taken out and why stay readable, and no-repeat rules keep working. The run rows (with each run's rules, counts per
bucket and hash) are never deleted. Not touched: the target tables.

D must be at least the longest `Lookback_Days` of the active history rules that may read the sample's rows (the
`SAMPLE`-scope rules of its table and run type, the `TABLE`-scope rules of its table, the `PROJECT`-scope rules of
its project, `*` included), or the sample is skipped with the reason: a carry-over or a
no-repeat rule would otherwise lose the cycles it looks back on. A replacement (10.9) needs the unselected
candidates of its run: replace before the run is older than D.

Without `--apply` only the counts are shown. In Airflow, the Variable's `retention_days` adds a
`purge_selections` task after the runs (it applies). How long the candidate rows must be kept is a records
retention decision (OPEN_ITEMS.md Q14): until it is made, leave `retention_days` out.
