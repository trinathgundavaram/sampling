# Open items

## A. Questions for the ODAG1 (COMO weekly) configuration

Each `TODO(Qn)` in the ODAG1 rows (section D) waits for one of these. The engine needs no change for
any answer: they become rows.

| # | Question | Fills |
|---|---|---|
| Q1 | The values of `AUTHZN_REQ_DETRMTN_TY` and each one's share; how expedited is coded **Answered 2026-11-04:** the values are `Approved`, `Denied`, `Dismissed`; `AUTHZN_REQ_STD_OR_EXPDT_IND` `E` = expedited, `S` = standard, and it is the type the case was processed as (a case processed as standard is standard, whatever was requested); `REQ_DENY_FOR_LACK_OF_MED_NECTY_IND` `Y` = denied for medical necessity. **Answered 2026-11-06:** within every functional area (within each of IPRF / LTAC / SNF for IP PAC): 12 % dismissals, 8 % approvals, denials the rest (80 % and the rounding left over, so the sample stays at 150). No withdrawals. Expedited cases still go first. **Answered 2026-11-10:** `Partially Approved` counts as `Approved` (the level 2 SELECT maps it) | level 2 buckets, priority 221 |
| Q2 | How a case maps to a functional area, and each area's share **Answered 2026-11-06:** in this order: Part B 13 %, BH 8 % (fewer only when there are not enough, never more), DME (OP PAC) 20 %, HHC (OP PAC) 8 %, IP PAC 22 % (equal portions of IPRF, LTAC, SNF); Pre Cert, OP Medical and IP Medical share the rest equally, and take what any area cannot fill. A case in more than one area goes to the first in this order. **Answered 2026-11-10:** the areas come from `ISS_DESC` (comma separated: field 1 the Request Type, field 2 the Category for outpatient types or the Bed Type for inpatient): Part B = `Drugs - Biologics Part B`; BH = a `Behavioral Health…` Request Type, or `Outpatient` with Category `Behavioral Health`; DME = `Outpatient` with Category `Durable Medical Equipment…`; HHC = `Outpatient` with Category `Home Health Services`; IP PAC = `Inpatient` with Bed Type `Rehab` (IPRF), `LTAC` or `SNF…`; OP Medical = any other `Outpatient`; IP Medical = any other `Inpatient`. Pre Cert is not used for now: its share goes to OP Medical and IP Medical, which share the rest equally. A Request Type outside these goes to `OTHER` and is not sampled. The values come from a small data set: profile the full source before production (C) | level 0 and level 1 BUCKET rules (`Rule_Sql_Txt`, a SELECT each), CAP 260 |
| Q3 | Where "Revision" comes from **Answered 2026-11-04:** ODAG1's `RVSN_NBR`. **Decided 2026-11-04:** only initial authorisations: an EXCLUSION of `RVSN_NBR <> '1'` | an exclusion SELECT |
| Q4 | How to identify auto-approvals, SHRPA / no prior authorisation required, diabetic supplies **Answered 2026-11-04:** all of them (and "does not require clinical review") come from the SHRPA / PA Source of Truth feed, to be loaded into a table and joined on the CPT code. **Answered 2026-11-06:** diabetic supplies are excluded only when SHRPA says no PA is required, so `NO_PA_REQUIRED` covers them (no rule of their own). **Answered 2026-11-10:** diabetic supplies are excluded by their own list of 25 codes (rule 213, on `CPT_PROC_CD`), without waiting for SHRPA. Still open: that table's name and columns (ODAG1's CPT code is `CPT_PROC_CD`, Q9). **Answered 2026-11-10:** the SHRPA procedure codes table is `HSLABBOSS.UMCM_SHRPA_PROCEDURE_CODES`. Joined on `PROCEDURE_CODE` = `CPT_PROC_CD`; no authorisation required when `IS_Auth_Required = 'No Auth Required - Check to see if a global rule applies'`; auto-approved when `IS_Auth_Required` contains `approve` (Teradata compares without regard to case). Only the current rows (`ETL_IS_CURR_IND = 'Y'`, `ETL_IS_DEL_IND = 'N'`). **Confirmed 2026-11-10:** the table is `HSLABBOSS.UMCM_SHRPA_PROCEDURE_CODES` (with the columns of the `UMCM_SHRPA` DDL given). **Answered 2026-11-10:** `status_indicator`, `Auth_Start_Date` and `Auth_Term_Date` are not used: the current rows decide (`ETL_IS_CURR_IND = 'Y'`, with `ETL_IS_DEL_IND = 'N'` as for ODAG1). `HSLABBOSS` is used in every environment for now | exclusion rules 211, 212, 213 |
| Q5 | The columns that identify one case uniquely in the scope (`AUTHZN_ID`? with `SRC_DATA_KEY`?) **Decided 2026-10-30:** `AUTHZN_ID, SRC_DATA_KEY`. | `Key_Columns_Txt` |
| Q6 | Which rows a cycle samples: the latest framework batch (`BTCH_ID`), or a decision-date window (`DETRMTN_NOTFTN_DT`, stored as VARCHAR(10): its format?). How the 7–14 day reporting lag of the requirements applies **Decided 2026-10-30:** one framework `WKLY` batch per sample: PARAM `batch_id` = the latest batch of the sample's project / table / run type (`{project_cd}`, `{table_nm}`, `{run_ty}`) with every source closed, `Scope_Sql_Txt` `BTCH_ID = '{batch_id}'`, `run_key` `{batch_id}`. Needs `WKLY` added to ODAG1's sources in the framework (they list `DLY,MNT` only), and Q15. | `Scope_Sql_Txt`, a PARAM rule, `run_key` |
| Q7 | The out-of-network share and how OON is identified **Answered 2026-11-04:** OON = `SVC_PROV_TY = 'Non par'` (the values are `PAr` and `Non par`; Teradata compares them without regard to case). **2026-11-12:** the caps compare `UPPER(TRIM(SVC_PROV_TY)) = 'NON PAR'`, so the match does not depend on the column being NOT CASESPECIFIC. **Answered 2026-11-06:** at most 10 OON in each functional area's denials (IPRF, LTAC and SNF each on their own), not the whole sample. A ceiling only: OON cases are not preferred, they are limited when they come up. Dismissals and approvals have no OON limit | CAP rules 262–270 (`Parent_Path_Txt` an area's denials) |
| Q8 | Do the functional-area shares apply only within denials? When there are too few cases: top up from other groups, or leave the sample short (and fail the task?) **Decided 2026-10-30:** `TOPUP`, `fail_on_shortfall` false (the shortfall is recorded, the run succeeds). **Answered 2026-11-06:** the areas split the whole sample, and the determination mix applies within each area | LEVEL `Parent_Path_Txt`, `Shortfall_Cd`, `fail_on_shortfall` |
| Q9 | The code-variety rule: which code (procedure / diagnosis / service category), and a maximum per code or just rotation **Answered 2026-11-04:** the CPT code, at most 3 per code in the sample (a CAP rule, `PER_VALUE`, `Max_Val` 3); **Answered 2026-11-06:** ODAG1 now has the CPT code in the new column `CPT_PROC_CD`. **Answered 2026-11-12:** `CPT_PROC_CD` can hold several codes separated by commas. Rules 211, 212 and 213 exclude an auth when ANY of its codes matches (the list is compared with the commas around each code, spaces removed, so `A42330` does not match `A4233`). Cap 261 is left as is: the whole list counts as one value. **2026-11-12:** spaces are removed first (`A4233, A4234` and `A4233,A4234` are one value) and a blank list is not limited (it is not one shared value). | CAP rule 261 |
| Q10 | The target table's database and name (created by another process with the source's columns plus the sampling columns) **Decided 2026-11-04:** `EXPORT_FEED_$ENV_T.SDO_CMPLNC_CLNCL_AUD_UM_AUTHZN_ODAG1_Sample`; only ODAG1 is sampled (one sample, `UMCM` / `ODAG1` / `WKLY`); its rule IDs are 200–299 | `Tgt_Schema_Nm`, `Tgt_Table_Nm` |
| Q11 | Scheduled (which day and time, Central), or started by a framework step (which one: EXPORT_LOAD, BATCH_CLOSE …)? Which framework table and status say a batch is ready to sample? How late can it be (retries × delay)? **Decided 2026-10-30:** started by hand (`schedule_interval` null); the PARAM `batch_id` lookup takes only a batch whose every source is closed (the GATE rule class was removed 2026-11-10). | Variable `schedule_interval` / framework `trigger_dags`, `trigger_keys`, `retries`, `retry_delay_minutes` |
| Q12 | Does a case selected in one cycle stay out of later cycles, and for how long? Should last cycle's unselected cases go first? **Decided 2026-10-30:** EXCLUSION `PRIOR_SELECTION` 90 days and PRIORITY `PRIOR_CANDIDATE` 14 days (first), this sample only (`History_Scope_Cd` `SAMPLE`). | `PRIOR_SELECTION` exclusion, `PRIOR_CANDIDATE` priority |
| Q13 | Who receives the "sample ready" email **Answered 2026-11-06:** no one specific: the DAG's usual notifications only (`success_email` / `fail_email` to `email_recipient`), `notify_recipients` stays unset | Variable `notify_recipients` |
| Q14 | How long must the candidate rows of a run be kept (records retention)? The selected cases and the run rows are kept in any case **Decided 2026-10-30:** 10 years (records retention): no purge, `retention_days` stays unset. | Variable `retention_days` / `purge --older-than-days` |
| Q15 | The framework's metadata database per environment (the one holding `GOVT_RGLTRY_RequestControl`: `HSABC_DEV` in its example Variable), whether Teradata grants the sampling service account SELECT on it, and ODAG1's `Project_Cd` there (`CMSUNIV` in its example setup). Is the sampling source `SDO_CMPLNC_CLNCL_AUD_UM_AUTHZN_ODAG1` (template) or the framework's export `SDO_CMSUNIV_ODAG1_2022`, and does it carry `BTCH_ID`? **Decided 2026-10-30:** `GOVT_RGLTRY_RequestControl` is in `HSABC_$ENV` (`HSABC_DEV` in DEV, `HSABC` in UAT / PROD), written that way in the PARAM rule; the sampling service account can read it. **Answered 2026-11-04:** ODAG1's `Project_Cd` in `GOVT_RGLTRY_RequestControl` is `UMCM` (the sample's own `{project_cd}`); the scope does not filter on the source's `RUN_TY` (the batch decides); the source is `EXPORT_FEED_$ENV_T.SDO_CMPLNC_CLNCL_AUD_UM_AUTHZN_ODAG1`, which carries `BTCH_ID` (its DDL was given). | PARAM `batch_id`, `Src_Schema_Nm` / `Src_Table_Nm`, `Scope_Sql_Txt` |
| Q16 | For the 6-monthly revalidation (8.01): does `HSLABBOSS.UMCM_SHRPA_PROCEDURE_CODES` keep its old row versions with `ETL_LOAD_DT_TM` / `ETL_LAST_UPDT_DT_TM` as ODAG1 does (answered 2026-11-13: ODAG1 keeps them, closed by `ETL_LAST_UPDT_DT_TM`)? **Answered 2026-11-13:** yes: rules 211 and 212 read it with `{current_rows:s}`, so a replay sees SHRPA as it was when the run started | rules 211, 212 |

## B. Data the requirements need that sampling cannot supply

From the Section 4 review: these are not in ODAG1, so no configuration can select on them until a source exists.

- SHRPA / PA Source of Truth feed (auto-approvals, no PA, diabetic supplies, clinical review required):
  `HSLABBOSS.UMCM_SHRPA_PROCEDURE_CODES`, `PROCEDURE_CODE` matched to any code of ODAG1's `CPT_PROC_CD` (a comma list).
- Contract status (active / terminated).
- Withdrawal detail beyond `WTHDRL_DESC`.

Not needed (2026-11-06): audit-risk codes from earlier COMO reviews and COMO history before the first run (no COMO
historic data requirement), and the concurrent review flag.

## C. Before production

- Run on a real Teradata system: create the tables by hand, `validate --check-sql`, a dry run, a run (runbook section 11).
  Tested so far only on the DuckDB stand-in.
- Run the DAG on a real Airflow 3.3.0 scheduler and worker (parsed and run in-process in tests only).
- DBA review of the primary indexes in `sql/schema.sql` (no secondary indexes are created), and of the space of
  `GOVT_RGLTRY_SampleSelection` (one row per candidate per run) and `GOVT_RGLTRY_SamplingRunRule` (one row per rule
  per run).
- The Variable's `email_recipient`, `snow_assignment_group` and the Connection for QA / UAT / PROD.
- The candidate pull holds the scope in worker memory: confirm the weekly scope size.
- Profile `ISS_DESC` (Request Type, Category / Bed Type, counts) on the full source and one weekly batch: the area
  values of Q2 come from a small data set. After the first DEV run, count the candidates whose `Stratum_Path_Txt`
  starts with `OTHER` (no area matched).

## D. The ODAG1 rows known so far (sample `UMCM` / `ODAG1` / `WKLY`, to enter with `Active_Ind = 0`)

The INSERT statements for all of them are in `ODAG1_WKLY_inserts.sql` (the config with `Active_Ind = 0`). These rows are entered in `GOVT_RGLTRY_SamplingConfig` / `GOVT_RGLTRY_SamplingRule` once
their TODOs are answered, then `Active_Ind = 1` and `validate --project UMCM --table ODAG1 --run-type WKLY
--check-sql`. `<ODAG1>` is `EXPORT_FEED_$ENV_T.SDO_CMPLNC_CLNCL_AUD_UM_AUTHZN_ODAG1`; `<CURRENT>` is
`BTCH_ID = '{batch_id}' AND {current_rows}` (the current row version; in a replay the version current when the run
started, OPERATIONAL_RUNBOOK.md 6.4); `<RT>` is
`TRIM(STRTOK(NULLIF(TRIM(ISS_DESC), ''), ',', 1))` (the Request Type) and `<C2>` the same with field 2 (the Category /
Bed Type; `NULLIF` because `STRTOK` fails on an empty string, error 9134).

**Config:** `Project_Cd` `UMCM`, `Table_Nm` `ODAG1`, `Run_Ty` `WKLY`, `Src_Schema_Nm` `EXPORT_FEED_$ENV_T`,
`Src_Table_Nm` `SDO_CMPLNC_CLNCL_AUD_UM_AUTHZN_ODAG1`, `Key_Columns_Txt` `AUTHZN_ID, SRC_DATA_KEY`,
`Scope_Sql_Txt` `SELECT * FROM <ODAG1> WHERE <CURRENT>`, `Target_Ty` `CNT`, `Target_Val` 150 (configurable: change `Target_Val`; a percentage is
possible: `PCT` with `Min_Target_Cnt` / `Max_Target_Cnt`), `RANKED`, `ROUND` (12 / 80 / 8 comes closest; FLOOR left the small areas without approvals), no `Diversity_Sql_Txt` (the 3 per CPT code
limit is CAP 261), `TOPUP`, `Keep_Excluded_Ind`
1, `Tgt_Schema_Nm` `EXPORT_FEED_$ENV_T`, `Tgt_Table_Nm` `SDO_CMPLNC_CLNCL_AUD_UM_AUTHZN_ODAG1_Sample` (the target table, runbook 4.3).

The PARAM row uses the sample's own `{project_cd}`, `{table_nm}` and `{run_ty}`, so with `Table_Nm` /
`Run_Ty` `*` they serve every table and run type of the project (a monthly ODAG1 sample finds its `MNT` batch with
the same rows). The other rows are ODAG1 weekly only.

| Rule_ID | Table_Nm / Run_Ty | Class | Seq | Rule_Nm | Type / settings | Rule_Sql_Txt |
|---|---|---|---|---|---|---|
| 201 | `*` / `*` | PARAM | 1 | `batch_id` | | `SELECT TOP 1 Btch_ID FROM HSABC_$ENV.GOVT_RGLTRY_RequestControl WHERE Project_Cd = '{project_cd}' AND Table_Nm = '{table_nm}' AND Run_Ty = '{run_ty}' GROUP BY Btch_ID HAVING MIN(Batch_Close_Ind) = 1 ORDER BY SUBSTR(Btch_ID, 1, 8) DESC, CHARACTER_LENGTH(Btch_ID) DESC, Btch_ID DESC` |
| 210 | `ODAG1` / `WKLY` | EXCLUSION | 0 | `SELECTED_IN_LAST_90_DAYS` | `PRIOR_SELECTION`, 90 days, `SAMPLE` | |
| 211 | `ODAG1` / `WKLY` | EXCLUSION | 1 | `AUTO_APPROVED` | `QUERY` | `SELECT o.AUTHZN_ID, o.SRC_DATA_KEY FROM <ODAG1> o WHERE <CURRENT> AND EXISTS (SELECT 1 FROM HSLABBOSS.UMCM_SHRPA_PROCEDURE_CODES s WHERE POSITION(',' || TRIM(s.PROCEDURE_CODE) || ',' IN ',' || OREPLACE(o.CPT_PROC_CD, ' ', '') || ',') > 0 AND {current_rows:s} AND s.IS_Auth_Required LIKE '%approve%')` (`EXISTS`, not a join: a code with several SHRPA rows still gives one key, and `<CURRENT>` stays ODAG1's) |
| 212 | `ODAG1` / `WKLY` | EXCLUSION | 2 | `NO_PA_REQUIRED` | `QUERY` | as 211, with `s.IS_Auth_Required = 'No Auth Required - Check to see if a global rule applies'` in place of the `LIKE` |
| 213 | `ODAG1` / `WKLY` | EXCLUSION | 3 | `DIABETIC_SUPPLIES` | `QUERY` | `SELECT AUTHZN_ID, SRC_DATA_KEY FROM <ODAG1> WHERE <CURRENT> AND ',' || OREPLACE(CPT_PROC_CD, ' ', '') || ',' LIKE ANY ('%,A4233,%', '%,A4234,%', '%,A4235,%', '%,A4236,%', '%,A4238,%', '%,A4239,%', '%,A4244,%', '%,A4245,%', '%,A4246,%', '%,A4247,%', '%,A4250,%', '%,A4253,%', '%,A4255,%', '%,A4256,%', '%,A4257,%', '%,A4258,%', '%,A4259,%', '%,A4271,%', '%,E0607,%', '%,E0784,%', '%,E2100,%', '%,E2101,%', '%,E2102,%', '%,E2103,%', '%,K0552,%')` |
| 214 | `ODAG1` / `WKLY` | EXCLUSION | 4 | `REVISION` | `QUERY` | `SELECT AUTHZN_ID, SRC_DATA_KEY FROM <ODAG1> WHERE <CURRENT> AND RVSN_NBR <> '1'` |
| 220 | `ODAG1` / `WKLY` | PRIORITY | 0 | `CARRY_OVER_14_DAYS` | `PRIOR_CANDIDATE`, 14 days, `SAMPLE` | |
| 221 | `ODAG1` / `WKLY` | PRIORITY | 1 | `EXPEDITED` | `QUERY` | `SELECT AUTHZN_ID, SRC_DATA_KEY FROM <ODAG1> WHERE <CURRENT> AND AUTHZN_REQ_STD_OR_EXPDT_IND = 'E'` |
| 231 | `ODAG1` / `WKLY` | LEVEL | 1 | `functional_area` | level 0, parent `*`, `SKIP` | (none: buckets select) |
| 232 | `ODAG1` / `WKLY` | BUCKET | 1 | `Part B` | level 0, `PCT` 13 | `SELECT AUTHZN_ID, SRC_DATA_KEY FROM <ODAG1> WHERE <CURRENT> AND <RT> = 'Drugs - Biologics Part B'` |
| 233 | `ODAG1` / `WKLY` | BUCKET | 2 | `BH` | level 0, `PCT` 8 | as 232, `AND (<RT> LIKE 'Behavioral Health%' OR (<RT> = 'Outpatient' AND <C2> = 'Behavioral Health'))` |
| 234 | `ODAG1` / `WKLY` | BUCKET | 3 | `DME` | level 0, `PCT` 20 | as 232, `AND <RT> = 'Outpatient' AND <C2> LIKE 'Durable Medical Equipment%'` |
| 235 | `ODAG1` / `WKLY` | BUCKET | 4 | `HHC` | level 0, `PCT` 8 | as 232, `AND <RT> = 'Outpatient' AND <C2> = 'Home Health Services'` |
| 236 | `ODAG1` / `WKLY` | BUCKET | 5 | `IP PAC` | level 0, `PCT` 22 | as 232, `AND <RT> = 'Inpatient' AND (<C2> IN ('Rehab', 'LTAC') OR <C2> LIKE 'SNF%')` |
| 238 | `ODAG1` / `WKLY` | BUCKET | 6 | `OP Medical` | level 0, `REMAINDER` | as 232, `AND <RT> = 'Outpatient'` (the outpatient cases no earlier area took) |
| 239 | `ODAG1` / `WKLY` | BUCKET | 7 | `IP Medical` | level 0, `REMAINDER` | as 232, `AND <RT> = 'Inpatient'` (the inpatient cases IP PAC did not take) |
| 241 | `ODAG1` / `WKLY` | LEVEL | 2 | `ip_pac_type` | level 1, parent `IP PAC`, `SKIP` | (none: buckets select) |
| 242 | `ODAG1` / `WKLY` | BUCKET | 1 | `IPRF` | level 1, parent `IP PAC`, `REMAINDER` (equal, a short one's share goes to the others) | as 232, `AND <RT> = 'Inpatient' AND <C2> = 'Rehab'` |
| 243 | `ODAG1` / `WKLY` | BUCKET | 2 | `LTAC` | as 242 | as 232, `AND <RT> = 'Inpatient' AND <C2> = 'LTAC'` |
| 244 | `ODAG1` / `WKLY` | BUCKET | 3 | `SNF` | as 242 | as 232, `AND <RT> = 'Inpatient' AND <C2> LIKE 'SNF%'` |
| 251 | `ODAG1` / `WKLY` | LEVEL | 3 | `determination` | level 2, parent `*`, `SKIP` | `SELECT AUTHZN_ID, SRC_DATA_KEY, CASE WHEN AUTHZN_REQ_DETRMTN_TY = 'Partially Approved' THEN 'Approved' ELSE AUTHZN_REQ_DETRMTN_TY END AS DETRMTN_TY FROM <ODAG1> WHERE <CURRENT>` |
| 252–254 | `ODAG1` / `WKLY` | BUCKET | 1–3 | `Dismissed`, `Approved`, `Denied` | level 2, `PCT` 12, `PCT` 8, `REMAINDER` | |
| 260 | `ODAG1` / `WKLY` | CAP | 1 | `BH_MAX_8_PCT` | `QUERY`, parent `*`, `PCT` 8 (of the 150) | the BH SELECT of 233 |
| 261 | `ODAG1` / `WKLY` | CAP | 2 | `PER_CPT_CODE` | `PER_VALUE`, parent `*`, `CNT` 3 | `SELECT AUTHZN_ID, SRC_DATA_KEY, NULLIF(OREPLACE(CPT_PROC_CD, ' ', ''), '') AS CPT_PROC_CD FROM <ODAG1> WHERE <CURRENT>` |
| 262–270 | `ODAG1` / `WKLY` | CAP | 3–11 | `OON_PART_B`, `OON_BH`, `OON_DME`, `OON_HHC`, `OON_IPRF`, `OON_LTAC`, `OON_SNF`, `OON_OP_MEDICAL`, `OON_IP_MEDICAL` | `QUERY`, parent `Part B/Denied` … (`IP PAC/IPRF/Denied` …), `CNT` 10 | `SELECT AUTHZN_ID, SRC_DATA_KEY FROM <ODAG1> WHERE <CURRENT> AND UPPER(TRIM(SVC_PROV_TY)) = 'NON PAR'` |
