# ODAG1 sampling dashboard

Two files:

| File | What it is |
|---|---|
| `ODAG1_dashboard_views.sql` | 10 reporting views in HSLABBOSS (`V_SMPL_DSH_*`). Run once, by hand, like the table DDL. |
| `ODAG1_Sampling_Dashboard.twb` | Tableau workbook: 9 dashboards, 50 worksheets, one data source per view. |

## Setup

1. Run `ODAG1_dashboard_views.sql` in HSLABBOSS. The views use `REPLACE VIEW`, so it can be run again.
2. Open the workbook in Tableau Desktop (2023.3 or later).
3. Each data source points at server `TERADATA_SERVER`. Replace it with the Teradata host (Data > each source > Edit connection), sign in, and save.
4. Publish to Tableau Server and set extracts to refresh weekly on Friday after the sampling run, and monthly on the 1st after the refinement run.

## The views

| View | Grain | Built from |
|---|---|---|
| V_SMPL_DSH_UNIVERSE | One row per current ODAG1 universe row | ODAG1, SHRPA, SampleSelection |
| V_SMPL_DSH_DQ | One row per universe row and issue | V_SMPL_DSH_UNIVERSE |
| V_SMPL_DSH_SAMPLE | One row per current sampled case | ODAG1_Sample, SampleRun, SampleSelection, RunStat |
| V_SMPL_DSH_RUN | One row per run, reruns included | SampleRun |
| V_SMPL_DSH_RUN_STAT | One row per figure of a current run | RunStat, SampleRun |
| V_SMPL_DSH_REVAL | One row per revalidation | SampleRevalidation |
| V_SMPL_DSH_REFINE | One row per refinement finding or total | SamplingRefinement |
| V_SMPL_DSH_RULE | One row per rule version | SamplingRule |
| V_SMPL_DSH_BATCH | One row per framework batch | RequestControl, SampleRun |
| V_SMPL_DSH_RETENTION | One row per table and data year | All sampling tables, ODAG1, ODAG1_Sample |

Derived fields in the universe and sample views follow the rule table: functional area (rules 232 to 244), request type, IP/PAC type, determination (Partially Approved counts as Approved), expedited or standard, network, the SHRPA flags, diabetic supplies, initial authorization, the clinical auditor flag and auditor type.

## Dashboards and requirements

| Dashboard | Requirements | What it shows |
|---|---|---|
| 0 About | All | What each dashboard covers |
| 1 Weekly sample | 4.01, 4.03 | Cases selected, against the weekly target, clinical auditor, expedited and out-of-network counts; selected by area and determination; target vs selected per area; the case list in priority order with auditor type, CPT codes and stratum weight |
| 2 Universe and funnel | 4.01 | Universe by area and determination; what the sample did with each row; cases removed by each exclusion; the dashboard's own exclusion check against the engine; universe share vs sample share; sampling fraction per stratum |
| 3 Universe risks and errors | 4.03 | Risks and errors in the universe report by type, week and severity; decisions within the timeframe; average decision days; every row with an issue |
| 4 Operations and delivery | 4.03, 4.04, 10.01 | Delivered by Friday's deadline; runs per weekday; batches closed and sampled; run duration; reruns and overrides with reasons; failed runs |
| 5 Trends and aberrations | 7.01 to 7.04 | Universe volume per month and change with an alert over the threshold; denial rate; late decisions per quarter; sample mix; risks by year and quarter; shortfall; top CPT codes; out-of-network denials sampled |
| 6 Internal controls | 8.01 to 8.03 | Revalidation results and sign-off; finalized samples; refinement findings open and reviewed, with the decisions; why cases were left out, topped up or replaced; replacements by reason |
| 7 Configuration | 9, 10.02 | The rules; each stratum's target and what it got; caps and make-ups; runs whose configuration changed |
| 8 Retention | 6.01, 6.02 | Rows kept per table and data year |

## Parameters

| Parameter | Default | Used by |
|---|---|---|
| Alert threshold (% change) | 0.2 | Volume change alert (7.02) |
| Standard decision days | 14 | Timeliness (dashboards 3 and 5) |
| Expedited decision days | 3 | Timeliness (dashboards 3 and 5) |
| Delivery deadline hour (Friday, CT) | 15 | On-time delivery (4.04) |
| Weekly target | 150 | Selected minus target (dashboard 1) |

## Assumptions to confirm

1. SHRPA clinical review flag: `Clinical_Mgt_Route` not blank. If another column is meant, change `Clinical_Mgt_Route` in the SQL.
2. ODAG1 date columns are text. They are read with `TRYCAST(OREPLACE(TRIM(x), '/', '-') AS DATE)`, which needs Teradata 16.20 or later and dates in year-month-day order. A different format needs a `FORMAT` clause in those casts in the SQL.
3. A run's Run_Key is the framework BTCH_ID, which is how the universe joins to SampleSelection.
4. The deadline uses the run's As_Of date; if that is not the Friday of the week, the deadline moves with it.
5. The timeframe defaults (14 and 3 days) are placeholders; set them from the contract.
6. Only MBR_ID is shown from the member; no names or addresses. Restrict the workbook on the server like any PHI report.

## Not covered (no data yet)

Audit results and error rates found by auditors, contract status, concurrent review, and alert delivery by email (7.x alerts show on dashboard 5 only).

## Regenerating

The SQL and the workbook were built from one field list, so every field a sheet uses exists in its view. When a view changes, update the matching data source in the workbook (Data > Refresh).
