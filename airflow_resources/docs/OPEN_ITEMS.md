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
| Q5 | The columns that identify one case uniquely in the scope (`AUTHZN_ID`? with `SRC_DATA_KEY`?) | `key_columns` |
| Q6 | Which rows a cycle samples: the latest framework batch (`BTCH_ID`), or a decision-date window (`DETRMTN_NOTFTN_DT`, stored as VARCHAR(10): its format?). How the 7–14 day reporting lag of the requirements applies | `scope_sql`, a PARAM rule, `run_key` |
| Q7 | The out-of-network share and how OON is identified | a bucket or priority |
| Q8 | Do the functional-area shares apply only within denials? When there are too few cases: top up from other groups, or leave the sample short (and fail the task?) | LEVEL `parent_path`, `shortfall_policy`, `fail_on_shortfall` |
| Q9 | The code-variety rule: which code (procedure / diagnosis / service category), and a maximum per code or just rotation | `diversity_expr`, `diversity_cap` |
| Q10 | The output table's database and name, its columns, and whether GRE exceptions go into it | `output_table`, OUTPUT rules |
| Q11 | Scheduled (which day and time, Central) or triggered by the framework after its load (which DAG, and which of its values to accept: `trigger_keys`) | Variable `schedule_interval`, `trigger_keys` |
| Q12 | Does a case selected in one cycle stay out of later cycles, and for how long? Should last cycle's unselected cases go first? | `PRIOR_SELECTION` exclusion, `PRIOR_CANDIDATE` priority |

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
