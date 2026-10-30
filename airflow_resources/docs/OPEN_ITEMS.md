# Open items

## A. Questions for the ODAG1 (COMO weekly) configuration

Each `TODO(Qn)` in `sql/seed/odag1_config_template.sql` waits for one of these. The engine needs no change for
any answer: they become rows.

| # | Question | Fills |
|---|---|---|
| Q1 | The values of `AUTHZN_REQ_DETRMTN_TY` (approved / denied / withdrawn / dismissed …) and each one's share or mode (fraction, all there is, the remainder). How expedited is coded in `AUTHZN_REQ_STD_OR_EXPDT_IND` | level 0 buckets, a priority condition |
| Q2 | How a case maps to a functional area: which of `ISS_DESC`, `SVC_CTGRY_CD`, `REQ_PROV_TY`, `SVC_PROV_TY` … and the values / conditions per area, with each area's share | level 1 BUCKET rules (`rule_sql`) |
| Q3 | Where "Revision" comes from: ODAG1 has no such column | a priority condition |
| Q4 | How to identify auto-approvals, SHRPA / no prior authorisation required, diabetic supplies (columns, values, or another table to join) | exclusion rules |
| Q5 | The columns that identify one case uniquely in the scope (`AUTHZN_ID`? with `SRC_DATA_KEY`?) **Decided 2026-10-30:** `AUTHZN_ID, SRC_DATA_KEY`. | `key_columns` |
| Q6 | Which rows a cycle samples: the latest framework batch (`BTCH_ID`), or a decision-date window (`DETRMTN_NOTFTN_DT`, stored as VARCHAR(10): its format?). How the 7–14 day reporting lag of the requirements applies **Decided 2026-10-30:** one framework `WKLY` batch per sample: PARAM `batch_id` = the latest ODAG1 WKLY batch with every source closed, `scope_sql` `BTCH_ID = '{batch_id}'`, `run_key` `{batch_id}`. Needs `WKLY` added to ODAG1's sources in the framework (they list `DLY,MNT` only), and Q15. | `scope_sql`, a PARAM rule, `run_key` |
| Q7 | The out-of-network share and how OON is identified | a bucket or priority |
| Q8 | Do the functional-area shares apply only within denials? When there are too few cases: top up from other groups, or leave the sample short (and fail the task?) **Decided 2026-10-30:** `TOPUP`, `fail_on_shortfall` false (the shortfall is recorded, the run succeeds). The functional-area-within-denials part stays with Q2. | LEVEL `parent_path`, `shortfall_policy`, `fail_on_shortfall` |
| Q9 | The code-variety rule: which code (procedure / diagnosis / service category), and a maximum per code or just rotation **Later:** no code-variety rule for now (`diversity_expr` NULL). | `diversity_expr`, `diversity_cap` |
| Q10 | The output table's database and name, its columns, and whether GRE exceptions go into it | `output_table`, OUTPUT rules |
| Q11 | Scheduled (which day and time, Central), or started by a framework step (which one: EXPORT_LOAD, BATCH_CLOSE …)? Which framework table and status say a batch is ready to sample? How late can it be (retries × delay)? **Decided 2026-10-30:** started by hand (`schedule_interval` null); GATE `BATCH_CLOSED` refuses a batch with a source still open. | Variable `schedule_interval` / framework `trigger_dags`, `trigger_keys`, a GATE rule, `retries`, `retry_delay_minutes` |
| Q12 | Does a case selected in one cycle stay out of later cycles, and for how long? Should last cycle's unselected cases go first? **Decided 2026-10-30:** EXCLUSION `PRIOR_SELECTION` 90 days and PRIORITY `PRIOR_CANDIDATE` 14 days (first), this config only. | `PRIOR_SELECTION` exclusion, `PRIOR_CANDIDATE` priority |
| Q13 | Who receives the "sample ready" email | Variable `notify_recipients` |
| Q14 | How long must the candidate rows of a run be kept (records retention)? The selected cases and the run rows are kept in any case **Decided 2026-10-30:** 10 years (CMS records): no purge, `retention_days` stays unset. | Variable `retention_days` / `purge --older-than-days` |
| Q15 | The framework's metadata database per environment (the one holding `ComplianceRequestControl`: `HSABC_DEV` in its example Variable), whether Teradata grants the sampling service account SELECT on it, and ODAG1's `Project_Cd` there (`CMSUNIV` in its example setup). Is the sampling universe `SDO_CMPLNC_CLNCL_AUD_UM_AUTHZN_ODAG1` (template) or the framework's export `SDO_CMSUNIV_ODAG1_2022`, and does it carry `BTCH_ID`? **Decided 2026-10-30:** `ComplianceRequestControl` is in `HSABC_$ENV` (`HSABC_DEV` in DEV, `HSABC` in UAT / PROD), written that way in the PARAM and GATE rules. Still open: SELECT grant, `Project_Cd`, the universe table. | PARAM `batch_id`, GATE `BATCH_CLOSED`, `universe_table`, `scope_sql` |
| Q16 | Reuse of the GRE rules engine's results: should a case with a current GRE exception for its batch (`HSABC_$ENV.gre_exceptions`, `run_key` = the batch ID, `etl_is_curr_ind = 'Y'`) be left out of the sample, flagged in the output, or neither? Which ODAG1 GRE rule groups count, and are their `src_key_cols` `AUTHZN_ID` and `SRC_DATA_KEY`? (the match is on `src_key_value` `AUTHZN_ID=…\|SRC_DATA_KEY=…`) **Decided 2026-10-30:** neither: a GRE exception is a separate matter from sampling, so the sample does not read `gre_exceptions` and no engine change is made. | nothing |

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
- DBA review of the primary indexes and secondary indexes in `sql/schema.sql`, and of the space of
  `gre_sample_selections` (one row per candidate per run).
- The Variable's `email_recipient`, `snow_assignment_group` and the Connection for QA / UAT / PROD.
- The candidate pull holds the scope in worker memory: confirm the weekly scope size.
