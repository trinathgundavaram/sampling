# Open items

## A. Questions for the ODAG1 (COMO weekly) configuration

Each `TODO(Qn)` in the ODAG1 rows (section D) waits for one of these. The engine needs no change for
any answer: they become rows.

| # | Question | Fills |
|---|---|---|
| Q1 | The values of `AUTHZN_REQ_DETRMTN_TY` (approved / denied / withdrawn / dismissed …) and each one's share or mode (a percentage, a number of cases, all there is, the remainder). How expedited is coded in `AUTHZN_REQ_STD_OR_EXPDT_IND` **Answered 2026-11-04:** the values are `Approved`, `Denied`, `Dismissed` (no withdrawal value yet: withdrawals are not in the universe); `AUTHZN_REQ_STD_OR_EXPDT_IND` `E` = expedited, `S` = standard; `REQ_DENY_FOR_LACK_OF_MED_NECTY_IND` `Y` = denied for medical necessity (also `N`, `None`); concurrent review = `AUTH_REVIEW_TYPE` `Concurrent (Continued Stay)`. Still open: the 10 % withdrawals without a value | level 0 buckets, a priority SELECT |
| Q2 | How a case maps to a functional area: which of `ISS_DESC`, `SVC_CTGRY_CD`, `REQ_PROV_TY`, `SVC_PROV_TY` … and the values / conditions (or tables to join) per area, with each area's share | level 1 BUCKET rules (`Rule_Sql_Txt`, a SELECT each) |
| Q3 | Where "Revision" comes from **Answered 2026-11-04:** ODAG1's `RVSN_NBR`. **Decided 2026-11-04:** only initial authorisations: an EXCLUSION of `RVSN_NBR <> '1'` | an exclusion SELECT |
| Q4 | How to identify auto-approvals, SHRPA / no prior authorisation required, diabetic supplies (columns, values, or another table to join) **Answered 2026-11-04:** all three (and "does not require clinical review") come from the SHRPA / PA Source of Truth feed, to be loaded into a table and joined on the CPT code. Still open: that table's name and columns, and a CPT code column in ODAG1 | exclusion rules |
| Q5 | The columns that identify one case uniquely in the scope (`AUTHZN_ID`? with `SRC_DATA_KEY`?) **Decided 2026-10-30:** `AUTHZN_ID, SRC_DATA_KEY`. | `Key_Columns_Txt` |
| Q6 | Which rows a cycle samples: the latest framework batch (`BTCH_ID`), or a decision-date window (`DETRMTN_NOTFTN_DT`, stored as VARCHAR(10): its format?). How the 7–14 day reporting lag of the requirements applies **Decided 2026-10-30:** one framework `WKLY` batch per sample: PARAM `batch_id` = the latest batch of the sample's project / table / run type (`{project_cd}`, `{table_nm}`, `{run_ty}`) with every source closed, `Scope_Sql_Txt` `BTCH_ID = '{batch_id}'`, `run_key` `{batch_id}`. Needs `WKLY` added to ODAG1's sources in the framework (they list `DLY,MNT` only), and Q15. | `Scope_Sql_Txt`, a PARAM rule, `run_key` |
| Q7 | The out-of-network share and how OON is identified **Answered 2026-11-04:** OON = `SVC_PROV_TY = 'Non par'` (the values are `PAr` and `Non par`; Teradata compares them without regard to case); at most 10 among the denials | a CAP rule (`Parent_Path_Txt` the denial bucket, `Max_Val` 10) |
| Q8 | Do the functional-area shares apply only within denials? When there are too few cases: top up from other groups, or leave the sample short (and fail the task?) **Decided 2026-10-30:** `TOPUP`, `fail_on_shortfall` false (the shortfall is recorded, the run succeeds). The functional-area-within-denials part stays with Q2. | LEVEL `Parent_Path_Txt`, `Shortfall_Cd`, `fail_on_shortfall` |
| Q9 | The code-variety rule: which code (procedure / diagnosis / service category), and a maximum per code or just rotation **Answered 2026-11-04:** the CPT code, at most 3 per code in the sample (a CAP rule, `PER_VALUE`, `Max_Val` 3); ODAG1 has no CPT code column yet (to be added). | `Diversity_Sql_Txt`, `Diversity_Cap_Cnt` |
| Q10 | The target table's database and name (created by another process with the source's columns plus the sampling columns) **Decided 2026-11-04:** `EXPORT_FEED_$ENV_T.SDO_CMPLNC_CLNCL_AUD_UM_AUTHZN_ODAG1_Sample`; only ODAG1 is sampled (one sample, `UMCM` / `ODAG1` / `WKLY`); its rule IDs are 200–299 | `Tgt_Schema_Nm`, `Tgt_Table_Nm` |
| Q11 | Scheduled (which day and time, Central), or started by a framework step (which one: EXPORT_LOAD, BATCH_CLOSE …)? Which framework table and status say a batch is ready to sample? How late can it be (retries × delay)? **Decided 2026-10-30:** started by hand (`schedule_interval` null); GATE `BATCH_CLOSED` refuses a batch with a source still open. | Variable `schedule_interval` / framework `trigger_dags`, `trigger_keys`, a GATE rule, `retries`, `retry_delay_minutes` |
| Q12 | Does a case selected in one cycle stay out of later cycles, and for how long? Should last cycle's unselected cases go first? **Decided 2026-10-30:** EXCLUSION `PRIOR_SELECTION` 90 days and PRIORITY `PRIOR_CANDIDATE` 14 days (first), this sample only (`History_Scope_Cd` `SAMPLE`). | `PRIOR_SELECTION` exclusion, `PRIOR_CANDIDATE` priority |
| Q13 | Who receives the "sample ready" email | Variable `notify_recipients` |
| Q14 | How long must the candidate rows of a run be kept (records retention)? The selected cases and the run rows are kept in any case **Decided 2026-10-30:** 10 years (records retention): no purge, `retention_days` stays unset. | Variable `retention_days` / `purge --older-than-days` |
| Q15 | The framework's metadata database per environment (the one holding `ComplianceRequestControl`: `HSABC_DEV` in its example Variable), whether Teradata grants the sampling service account SELECT on it, and ODAG1's `Project_Cd` there (`CMSUNIV` in its example setup). Is the sampling source `SDO_CMPLNC_CLNCL_AUD_UM_AUTHZN_ODAG1` (template) or the framework's export `SDO_CMSUNIV_ODAG1_2022`, and does it carry `BTCH_ID`? **Decided 2026-10-30:** `ComplianceRequestControl` is in `HSABC_$ENV` (`HSABC_DEV` in DEV, `HSABC` in UAT / PROD), written that way in the PARAM and GATE rules; the sampling service account can read it. **Answered 2026-11-04:** ODAG1's `Project_Cd` in `ComplianceRequestControl` is `UMCM` (the sample's own `{project_cd}`); the scope does not filter on the source's `RUN_TY` (the batch decides); the source is `EXPORT_FEED_$ENV_T.SDO_CMPLNC_CLNCL_AUD_UM_AUTHZN_ODAG1`, which carries `BTCH_ID` (its DDL was given). | PARAM `batch_id`, GATE `BATCH_CLOSED`, `Src_Schema_Nm` / `Src_Table_Nm`, `Scope_Sql_Txt` |

## B. Data the requirements need that sampling cannot supply

From the Section 4 review: these are not in ODAG1, so no configuration can select on them until a source exists.

- SHRPA / PA Source of Truth feed (auto-approvals, no PA, diabetic supplies, clinical review required): a table to
  be loaded, joined on the CPT code.
- A CPT code column in ODAG1 (the SHRPA join and the 3-per-code limit).
- Audit-risk codes from earlier COMO reviews: no historic data for now.
- Contract status (active / terminated).
- Concurrent review flag beyond `AUTHZN_CONCURNT_REVW_TY` (confirm it is the same thing).
- Withdrawal detail beyond `WTHDRL_DESC`.
- COMO history before the first sampling run (the history rules only see runs of this engine).

## C. Before production

- Run on a real Teradata system: `deploy-schema`, `validate --check-sql`, a dry run, a run (runbook section 11).
  Tested so far only on the DuckDB stand-in.
- Run the DAG on a real Airflow 3.3.0 scheduler and worker (parsed and run in-process in tests only).
- DBA review of the primary indexes in `sql/schema.sql` (no secondary indexes are created), and of the space of
  `GOVT_RGLTRY_SampleSelection` (one row per candidate per run) and `GOVT_RGLTRY_SamplingRunRule` (one row per rule
  per run).
- The Variable's `email_recipient`, `snow_assignment_group` and the Connection for QA / UAT / PROD.
- The candidate pull holds the scope in worker memory: confirm the weekly scope size.

## D. The ODAG1 rows known so far (sample `UMCM` / `ODAG1` / `WKLY`, to enter with `Active_Ind = 0`)

There are no seed files: these rows are entered in `GOVT_RGLTRY_SamplingConfig` / `GOVT_RGLTRY_SamplingRule` once
their TODOs are answered, then `Active_Ind = 1` and `validate --project UMCM --table ODAG1 --run-type WKLY
--check-sql`. `<ODAG1>` is `EXPORT_FEED_$ENV_T.SDO_CMPLNC_CLNCL_AUD_UM_AUTHZN_ODAG1`; `<CURRENT>` is
`BTCH_ID = '{batch_id}' AND ETL_IS_CURR_IND = 'Y' AND ETL_IS_DEL_IND = 'N'`.

**Config:** `Project_Cd` `UMCM`, `Table_Nm` `ODAG1`, `Run_Ty` `WKLY`, `Src_Schema_Nm` `EXPORT_FEED_$ENV_T`,
`Src_Table_Nm` `SDO_CMPLNC_CLNCL_AUD_UM_AUTHZN_ODAG1`, `Key_Columns_Txt` `AUTHZN_ID, SRC_DATA_KEY`,
`Scope_Sql_Txt` `SELECT * FROM <ODAG1> WHERE <CURRENT>`, `Target_Ty` `CNT`, `Target_Val` 150 (a percentage is
possible: `PCT` with `Min_Target_Cnt` / `Max_Target_Cnt`), `RANKED`, `FLOOR`, no diversity (Q9), `TOPUP`, `Keep_Excluded_Ind`
1, `Tgt_Schema_Nm` `EXPORT_FEED_$ENV_T`, `Tgt_Table_Nm` `SDO_CMPLNC_CLNCL_AUD_UM_AUTHZN_ODAG1_Sample` (the target table, runbook 4.3).

The PARAM and GATE rows use the sample's own `{project_cd}`, `{table_nm}` and `{run_ty}`, so with `Table_Nm` /
`Run_Ty` `*` they serve every table and run type of the project (a monthly ODAG1 sample finds its `MNT` batch with
the same rows). The other rows are ODAG1 weekly only.

| Rule_ID | Table_Nm / Run_Ty | Class | Seq | Rule_Nm | Type / settings | Rule_Sql_Txt |
|---|---|---|---|---|---|---|
| 201 | `*` / `*` | PARAM | 1 | `batch_id` | | `SELECT MAX(Btch_ID) FROM (SELECT Btch_ID FROM HSABC_$ENV.ComplianceRequestControl WHERE Project_Cd = '{project_cd}' AND Table_Nm = '{table_nm}' AND Run_Ty = '{run_ty}' GROUP BY Btch_ID HAVING MIN(Batch_Close_Ind) = 1) closed_batches` |
| 205 | `*` / `*` | GATE | 1 | `BATCH_CLOSED` | | `SELECT 1 FROM HSABC_$ENV.ComplianceRequestControl WHERE Btch_ID = '{batch_id}' AND Project_Cd = '{project_cd}' AND Table_Nm = '{table_nm}' AND Run_Ty = '{run_ty}' HAVING COUNT(*) > 0 AND MIN(Batch_Close_Ind) = 1` |
| 210 | `ODAG1` / `WKLY` | EXCLUSION | 0 | `SELECTED_IN_LAST_90_DAYS` | `PRIOR_SELECTION`, 90 days, `SAMPLE` | |
| 211–213 | `ODAG1` / `WKLY` | EXCLUSION | 1–3 | `AUTO_APPROVED`, `NO_PA_REQUIRED`, `DIABETIC_SUPPLIES` | `QUERY` | `SELECT AUTHZN_ID, SRC_DATA_KEY FROM <ODAG1> WHERE <CURRENT> AND` TODO(Q4) |
| 220 | `ODAG1` / `WKLY` | PRIORITY | 0 | `CARRY_OVER_14_DAYS` | `PRIOR_CANDIDATE`, 14 days, `SAMPLE` | |
| 221 | `ODAG1` / `WKLY` | PRIORITY | 1 | `REVISION` | `QUERY` | `SELECT AUTHZN_ID, SRC_DATA_KEY FROM <ODAG1> WHERE <CURRENT> AND` TODO(Q3) |
| 222 | `ODAG1` / `WKLY` | PRIORITY | 2 | `EXPEDITED` | `QUERY` | as 221, TODO(Q1) a condition on `AUTHZN_REQ_STD_OR_EXPDT_IND` |
| 231 | `ODAG1` / `WKLY` | LEVEL | 1 | `determination` | level 0, parent `*`, `SKIP` | `SELECT AUTHZN_ID, SRC_DATA_KEY, AUTHZN_REQ_DETRMTN_TY FROM <ODAG1> WHERE <CURRENT>` |
| 232 | `ODAG1` / `WKLY` | BUCKET | 1 | TODO(Q1) the denial value | level 0, `REMAINDER` (other values and quotas TODO(Q1)) | |
| 241 | `ODAG1` / `WKLY` | LEVEL | 1 | `functional_area` | level 1, parent TODO(Q8) the denial bucket, `SHARE` | (none: buckets select) |
| 242 | `ODAG1` / `WKLY` | BUCKET | 1 | TODO(Q2) one per area | level 1, `PCT` `Quota_Val` TODO(Q2) (or `CNT` cases) | `SELECT AUTHZN_ID, SRC_DATA_KEY FROM <ODAG1> WHERE <CURRENT> AND` TODO(Q2) |
