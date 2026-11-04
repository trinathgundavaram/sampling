# Open items

## A. Questions for the ODAG1 (COMO weekly) configuration

Each `TODO(Qn)` in the ODAG1 rows (section D) waits for one of these. The engine needs no change for
any answer: they become rows.

| # | Question | Fills |
|---|---|---|
| Q1 | The values of `AUTHZN_REQ_DETRMTN_TY` (approved / denied / withdrawn / dismissed …) and each one's share or mode (fraction, all there is, the remainder). How expedited is coded in `AUTHZN_REQ_STD_OR_EXPDT_IND` | level 0 buckets, a priority SELECT |
| Q2 | How a case maps to a functional area: which of `ISS_DESC`, `SVC_CTGRY_CD`, `REQ_PROV_TY`, `SVC_PROV_TY` … and the values / conditions (or tables to join) per area, with each area's share | level 1 BUCKET rules (`rule_sql`, a SELECT each) |
| Q3 | Where "Revision" comes from: ODAG1 has no such column (a SELECT may join the table that has it) | a priority SELECT |
| Q4 | How to identify auto-approvals, SHRPA / no prior authorisation required, diabetic supplies (columns, values, or another table to join) | exclusion rules |
| Q5 | The columns that identify one case uniquely in the scope (`AUTHZN_ID`? with `SRC_DATA_KEY`?) **Decided 2026-10-30:** `AUTHZN_ID, SRC_DATA_KEY`. | `key_columns` |
| Q6 | Which rows a cycle samples: the latest framework batch (`BTCH_ID`), or a decision-date window (`DETRMTN_NOTFTN_DT`, stored as VARCHAR(10): its format?). How the 7–14 day reporting lag of the requirements applies **Decided 2026-10-30:** one framework `WKLY` batch per sample: PARAM `batch_id` = the latest ODAG1 WKLY batch with every source closed, `scope_sql` `BTCH_ID = '{batch_id}'`, `run_key` `{batch_id}`. Needs `WKLY` added to ODAG1's sources in the framework (they list `DLY,MNT` only), and Q15. | `scope_sql`, a PARAM rule, `run_key` |
| Q7 | The out-of-network share and how OON is identified | a bucket or priority |
| Q8 | Do the functional-area shares apply only within denials? When there are too few cases: top up from other groups, or leave the sample short (and fail the task?) **Decided 2026-10-30:** `TOPUP`, `fail_on_shortfall` false (the shortfall is recorded, the run succeeds). The functional-area-within-denials part stays with Q2. | LEVEL `parent_path`, `shortfall_policy`, `fail_on_shortfall` |
| Q9 | The code-variety rule: which code (procedure / diagnosis / service category), and a maximum per code or just rotation **Later:** no code-variety rule for now (`diversity_expr` NULL). | `diversity_expr`, `diversity_cap` |
| Q10 | The sample table's database and name (created by another process with the source's columns plus the sampling columns) | `output_db`, `output_table` |
| Q11 | Scheduled (which day and time, Central), or started by a framework step (which one: EXPORT_LOAD, BATCH_CLOSE …)? Which framework table and status say a batch is ready to sample? How late can it be (retries × delay)? **Decided 2026-10-30:** started by hand (`schedule_interval` null); GATE `BATCH_CLOSED` refuses a batch with a source still open. | Variable `schedule_interval` / framework `trigger_dags`, `trigger_keys`, a GATE rule, `retries`, `retry_delay_minutes` |
| Q12 | Does a case selected in one cycle stay out of later cycles, and for how long? Should last cycle's unselected cases go first? **Decided 2026-10-30:** EXCLUSION `PRIOR_SELECTION` 90 days and PRIORITY `PRIOR_CANDIDATE` 14 days (first), this config only. | `PRIOR_SELECTION` exclusion, `PRIOR_CANDIDATE` priority |
| Q13 | Who receives the "sample ready" email | Variable `notify_recipients` |
| Q14 | How long must the candidate rows of a run be kept (records retention)? The selected cases and the run rows are kept in any case **Decided 2026-10-30:** 10 years (records retention): no purge, `retention_days` stays unset. | Variable `retention_days` / `purge --older-than-days` |
| Q15 | The framework's metadata database per environment (the one holding `ComplianceRequestControl`: `HSABC_DEV` in its example Variable), whether Teradata grants the sampling service account SELECT on it, and ODAG1's `Project_Cd` there (`CMSUNIV` in its example setup). Is the sampling universe `SDO_CMPLNC_CLNCL_AUD_UM_AUTHZN_ODAG1` (template) or the framework's export `SDO_CMSUNIV_ODAG1_2022`, and does it carry `BTCH_ID`? **Decided 2026-10-30:** `ComplianceRequestControl` is in `HSABC_$ENV` (`HSABC_DEV` in DEV, `HSABC` in UAT / PROD), written that way in the PARAM and GATE rules; the sampling service account can read it. Still open: `Project_Cd`, the universe table. | PARAM `batch_id`, GATE `BATCH_CLOSED`, `universe_table`, `scope_sql` |

## B. Data the requirements need that sampling cannot supply

From the Section 4 review: these are not in ODAG1, so no configuration can select on them until a source exists.

- Revision indicator (Q3).
- SHRPA / prior-authorisation-required list.
- Contract status (active / terminated).
- Concurrent review flag beyond `AUTHZN_CONCURNT_REVW_TY` (confirm it is the same thing).
- Withdrawal detail beyond `WTHDRL_DESC`.
- COMO history before the first sampling run (the history rules only see runs of this engine).

## C. Before production

- Run on a real Teradata system: `deploy-schema`, `validate --check-sql`, a dry run, a run (runbook section 11).
  Tested so far only on the DuckDB stand-in.
- Run the DAG on a real Airflow 3.3.0 scheduler and worker (parsed and run in-process in tests only).
- DBA review of the primary indexes in `sql/schema.sql` (no secondary indexes are created), and of the space of
  `GOVT_RGLTRY_SAMPLE_SELECTIONS` (one row per candidate per run).
- The Variable's `email_recipient`, `snow_assignment_group` and the Connection for QA / UAT / PROD.
- The candidate pull holds the scope in worker memory: confirm the weekly scope size.

## D. The ODAG1 rows known so far (config_id 2, to enter with `act_ind = 0`)

There are no seed files: these rows are entered in `GOVT_RGLTRY_SAMPLING_CONFIG` / `GOVT_RGLTRY_SAMPLING_RULE` once their TODOs are
answered, then `act_ind = 1` and `validate --config-id 2 --check-sql`. `<ODAG1>` is
`EXPORT_FEED_$ENV_T.SDO_CMPLNC_CLNCL_AUD_UM_AUTHZN_ODAG1`; `<CURRENT>` is
`BTCH_ID = '{batch_id}' AND ETL_IS_CURR_IND = 'Y' AND ETL_IS_DEL_IND = 'N'`.

**Config:** project `UMCM`, process and sample `COMO_WEEKLY_SAMPLE`, universe `<ODAG1>`, key_columns
`AUTHZN_ID, SRC_DATA_KEY`, scope_sql `<CURRENT>`, target 150, `RANKED`, `FLOOR`, no diversity (Q9), `TOPUP`,
keep_excluded 1, output_db / output_table TODO(Q10) (the sample table, runbook 4.3).

| rule_id | class | seq | name | type / settings | rule_sql |
|---|---|---|---|---|---|
| 201 | PARAM | 1 | `batch_id` | | `SELECT MAX(Btch_ID) FROM (SELECT Btch_ID FROM HSABC_$ENV.ComplianceRequestControl WHERE Project_Cd = 'CMSUNIV' AND Table_Nm = 'ODAG1' AND Run_Ty = 'WKLY' GROUP BY Btch_ID HAVING MIN(Batch_Close_Ind) = 1) closed_batches` (`Project_Cd` TODO(Q15)) |
| 205 | GATE | 1 | `BATCH_CLOSED` | | `SELECT 1 FROM HSABC_$ENV.ComplianceRequestControl WHERE Btch_ID = '{batch_id}' HAVING COUNT(*) > 0 AND MIN(Batch_Close_Ind) = 1` |
| 210 | EXCLUSION | 0 | `SELECTED_IN_LAST_90_DAYS` | `PRIOR_SELECTION`, 90 days, `CONFIG` | |
| 211–213 | EXCLUSION | 1–3 | `AUTO_APPROVED`, `NO_PA_REQUIRED`, `DIABETIC_SUPPLIES` | `QUERY` | `SELECT AUTHZN_ID, SRC_DATA_KEY FROM <ODAG1> WHERE <CURRENT> AND` TODO(Q4) |
| 220 | PRIORITY | 0 | `CARRY_OVER_14_DAYS` | `PRIOR_CANDIDATE`, 14 days, `CONFIG` | |
| 221 | PRIORITY | 1 | `REVISION` | `QUERY` | `SELECT AUTHZN_ID, SRC_DATA_KEY FROM <ODAG1> WHERE <CURRENT> AND` TODO(Q3) |
| 222 | PRIORITY | 2 | `EXPEDITED` | `QUERY` | as 221, TODO(Q1) a condition on `AUTHZN_REQ_STD_OR_EXPDT_IND` |
| 231 | LEVEL | 1 | `determination` | level 0, parent `*`, `SKIP` | `SELECT AUTHZN_ID, SRC_DATA_KEY, AUTHZN_REQ_DETRMTN_TY FROM <ODAG1> WHERE <CURRENT>` |
| 232 | BUCKET | 1 | TODO(Q1) the denial value | level 0, `REMAINDER` (other values and quotas TODO(Q1)) | |
| 241 | LEVEL | 1 | `functional_area` | level 1, parent TODO(Q8) the denial bucket, `SHARE` | (none: buckets select) |
| 242 | BUCKET | 1 | TODO(Q2) one per area | level 1, `FRACTION` TODO(Q2) | `SELECT AUTHZN_ID, SRC_DATA_KEY FROM <ODAG1> WHERE <CURRENT> AND` TODO(Q2) |
