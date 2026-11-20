# ODAG1 Sampling dashboard: build it from scratch in Tableau

This guide builds the whole workbook by hand in Tableau Desktop: one data source with 16 tables, 9 parameters, 32 calculated fields, 28 worksheets and **4 dashboards**. Only the 4 dashboards are visible at the end: every worksheet is hidden, and the dashboards are linked by buttons and click actions.

Nothing is created in Teradata: Tableau only reads. The sampling tables and the ODAG1 table are read from **HSLABBOSS**; the GRE rules engine tables from **HSABC_V**.

Follow the parts in order and type every name exactly as written, because the calculated fields refer to fields and parameters by name.

## Contents

1. Before you start
2. The data source
3. The parameters
4. The calculated fields
5. The worksheets
6. The dashboards
7. The actions (links between sheets)
8. Hide the sheets and check
9. What each requirement uses

The four dashboards:

| Dashboard | What it shows | Requirements |
|---|---|---|
| 1 Weekly sample | The weekly sample for one batch: KPIs, mix, targets and caps, universe funnel, CPT codes, prioritized case list | Requirement 4 (4.01 sample selection, 4.03 sample output: prioritized cases, functional area, clinical auditor, universe risks) and 10.02 (targets and caps). |
| 2 Universe risks and errors | Universe risks and errors from the GRE rules engine and the engine's own checks, with the full ODAG1 record of whatever you click | Requirement 2 (2.01 to 2.04: risks and errors by rule, result type, category, risk level and priority, with the full ODAG1 record) and 1.03 (every universe field). |
| 3 Operations and controls | Universe pulls, sampling runs, batches, revalidation, GRE runs and errors, refinement, retention | Requirement 1 (1.02 pulls by frequency), 2.06 and 4.04 (runs on schedule), 3 and 6 (storage and retention), 8 (revalidation, human sign-off, refinement) and 10.01. |
| 4 Trends and configuration | Monthly and weekly trends with aberration alerts, and the sampling and GRE rule configuration | Requirement 7 (7.01 to 7.04: trends, aberrations over the alert threshold, high-risk frequency), 2.03 and 4.02 (one-year look back), 9 and 10.02 (configuration). |

## Part 1: Before you start

1. Pull the repository on the PC (PowerShell):

   ```
   git -C C:\Script\frameworks\GOV_RGLTRY\Sampling pull
   ```

2. Open `C:\Script\frameworks\GOV_RGLTRY\Sampling\airflow_resources\docs\tableau\custom_sql\`. Each of the 16 files holds one complete query: open it in Notepad, **Ctrl+A**, **Ctrl+C**, and paste it into Tableau.
3. Your Teradata user needs SELECT on **HSLABBOSS** and **HSABC_V**.
4. Start Tableau Desktop, **File > New**, and save straight away (**File > Save As**, type **Tableau Workbook (*.twb)**) as `C:\Script\frameworks\GOV_RGLTRY\Sampling\airflow_resources\docs\tableau\ODAG1_Sampling_Dashboard_v2.twb`. Press **Ctrl+S** often.

## Part 2: The data source

### 2.1 Connect

1. On the Start page, **Connect > To a Server > Teradata** (under *More...* if not listed).
2. Enter the server, your authentication, user name and password, **Sign In**.
3. On the Data Source page, in **Database**, choose **HSLABBOSS**. Leave the connection **Live**.

### 2.2 Add the 16 tables

Every table is a Custom SQL query. The queries name their database in full, so the one connection reads HSLABBOSS and HSABC_V. For the first table, drag **New Custom SQL** (bottom of the left pane) onto the empty canvas. For every other one, drag **New Custom SQL** so it lands next to the table in *Drag onto*; Tableau draws a relationship line. Paste the file, **OK**, then rename the table (its drop-down arrow on the canvas, **Rename**). In **Edit Relationship**, pick the left field and the right field, keep **=**, and close.

| Step | File | Rename to | Drag onto | Relationship (left = right) |
|---|---|---|---|---|
| 1 | `01_Universe.sql` | Universe | the empty canvas | none, first table |
| 2 | `02_Runs.sql` | Runs | Universe | `Smpl_Run_ID` = `Runs_Smpl_Run_ID` |
| 3 | `04_Sample.sql` | Sample | Universe | `Case_Key` = `Smp_Case_Key` |
| 4 | `06_Universe_risks_and_errors.sql` | Universe risks and errors | Universe | `Case_Key` = `DQ_Case_Key` |
| 5 | `16_ODAG1_data.sql` | ODAG1 data | Universe | four pairs, see below |
| 6 | `05_Run_figures.sql` | Run figures | Runs | `Runs_Smpl_Run_ID` = `Stat_Smpl_Run_ID` |
| 7 | `07_Revalidation.sql` | Revalidation | Runs | `Runs_Smpl_Run_ID` = `Reval_Smpl_Run_ID` |
| 8 | `09_Batches.sql` | Batches | Runs | `Runs_Smpl_Run_ID` = `Batch_Smpl_Run_ID` |
| 9 | `10_Retention.sql` | Retention | Runs | `Model_Link` = `Retention_Link` |
| 10 | `03_Rules.sql` | Rules | Run figures | `Stat_Rule_ID` = `Rule_ID` |
| 11 | `08_Refinement.sql` | Refinement | Rules | `Rule_ID` = `Refine_Rule_ID` |
| 12 | `11_GRE_rules.sql` | GRE rules | Runs | `Model_Link` = `GRE_Link` |
| 13 | `12_GRE_results.sql` | GRE results | GRE rules | `GR_Rule_ID` = `GS_Rule_ID` |
| 14 | `13_GRE_exceptions.sql` | GRE exceptions | GRE rules | `GR_Rule_ID` = `GX_Rule_ID` |
| 15 | `15_GRE_rule_errors.sql` | GRE rule errors | GRE rules | `GR_Rule_ID` = `GE_Rule_ID` |
| 16 | `14_GRE_rule_audit.sql` | GRE rule audit | GRE results | `GS_Run_ID` = `GA_Run_ID` |

**ODAG1 data** (step 5) is the ODAG1 table itself, every row, version and pull (daily, weekly, monthly), plus a few helper columns. Its columns have the same names as Universe's, so Tableau shows them with ` (ODAG1 data)` after the name. That is expected for this table only. In **Edit Relationship** click **Add more fields** until there are four pairs:

| Universe | ODAG1 data |
|---|---|
| `BTCH_ID` | `BTCH_ID (ODAG1 data)` |
| `AUTHZN_ID` | `AUTHZN_ID (ODAG1 data)` |
| `SRC_DATA_KEY` | `SRC_DATA_KEY (ODAG1 data)` |
| `AUTHZN_REVSN_NUM` | `AUTHZN_REVSN_NUM (ODAG1 data)` |

Check after each table: click it and look at the grid at the bottom. Apart from ODAG1 data, no field name may end in brackets (for example `Stat_Run_Key (Runs)`). If one does, that table got the wrong query: its drop-down, **Edit Custom SQL Query**, paste the right file.

The finished canvas:

```
Universe ─┬─ Runs ─┬─ Run figures ── Rules ── Refinement
          │        ├─ Revalidation
          │        ├─ Batches
          │        ├─ Retention
          │        └─ GRE rules ─┬─ GRE results ── GRE rule audit
          │                      ├─ GRE exceptions
          │                      └─ GRE rule errors
          ├─ Sample
          ├─ Universe risks and errors
          └─ ODAG1 data
```

Finally click the data source name at the top left and rename it **ODAG1 Sampling**.

Which table holds what:

| Table | Holds |
|---|---|
| Universe | the current weekly ODAG1 universe, one row per case, with derived area, determination, timeliness, the engine's data checks and the sampling outcome |
| Runs | one row per sampling run: status, counts, timings, reasons, delivery |
| Sample | the selected cases with their bucket, weight and reason |
| Run figures | targets, caps, candidates and selections per bucket and cap |
| Universe risks and errors | one row per data issue the sampling engine found in the weekly universe |
| Revalidation, Refinement, Batches, Retention | internal controls, framework batches and stored rows by year |
| Rules | the sampling configuration |
| GRE rules, GRE results, GRE exceptions, GRE rule audit, GRE rule errors | the rules engine: data validation rules for ODAG1 and ODAG1_FILE, results per rule and run, one exception per record per rule, runs, and rules that failed to run |
| ODAG1 data | every row of the ODAG1 table, for the record drill-through and the universe pulls |

## Part 3: The parameters

Go to **Sheet 1**. Two of the parameters take their starting value from a calculated field, so create those two fields first. In the Data pane click the arrow at the top, **Create Calculated Field...**, type the name, paste the formula, **OK**:

- **Latest batch** (uses Runs):

  ```
  { MAX([Run_Key]) }
  ```

- **Latest GRE run** (uses GRE results):

  ```
  { MAX([GS_Run_Key]) }
  ```

Now create the parameters: Data pane arrow, **Create Parameter...**.

| Name | Data type | Value when workbook opens | Allowable values | Settings |
|---|---|---|---|---|
| Alert threshold (% change) | Float | 0.2 | Range | Minimum 0, Maximum 1, Step size 0.05 |
| Standard decision days | Integer | 14 | Range | Minimum 1, Maximum 60, Step size 1 |
| Expedited decision days | Integer | 3 | Range | Minimum 1, Maximum 14, Step size 1 |
| Delivery deadline hour (Friday, CT) | Integer | 15 | Range | Minimum 0, Maximum 23, Step size 1 |
| Weekly target | Integer | 150 | Range | Minimum 1, Maximum 1000, Step size 1 |
| Batch | String | Latest batch | List | When workbook opens, take the values from field **Run_Key** (Runs) |
| GRE run | String | Latest GRE run | List | When workbook opens, take the values from field **GS_Run_Key** (GRE results) |
| Selected record | String | (leave empty) | All |  |
| High-risk compliance flags | Integer | 3 | Range | Minimum 1, Maximum 20, Step size 1 |

For **Batch** and **GRE run**: in *Value when workbook opens* pick the calculated field (**Latest batch**, **Latest GRE run**); under *Allowable values* choose **List**, then **When workbook opens** and pick the field named in the table. The drop-downs then always list the current batches and runs and open on the latest.

**Selected record** holds the record you click; the click actions fill it (Part 7). **High-risk compliance flags** is how many Compliance Flags make a record high risk (requirement 2.04 says *high volume* without a number; 3 is a starting value you can change on the dashboard).

## Part 4: The calculated fields

Create these in this order (later ones use earlier ones). A field that uses one table's columns only must stay that way; do not mix tables in one row-level formula.

### Candidates

```
SUM(IF [Sample_Status] = 'Selected' OR [Sample_Status] = 'Not selected' OR [Sample_Status] = 'Replaced' THEN 1 ELSE 0 END)
```

### Universe share (candidates)

```
[Candidates] / TOTAL([Candidates])
```

### Sample share

```
SUM([Selected_Ind]) / TOTAL(SUM([Selected_Ind]))
```

### Denial rate

```
SUM(IF [Determination] = 'Denied' THEN 1 ELSE 0 END) / SUM([Case_Cnt])
```

### Late decision

```
IF [Processing] = 'Expedited' AND [Decision_Days] > [Expedited decision days] THEN 'Late' ELSEIF [Processing] = 'Standard' AND [Decision_Days] > [Standard decision days] THEN 'Late' ELSEIF ISNULL([Decision_Days]) THEN 'No dates' ELSE 'On time' END
```

### Late decision rate

```
SUM(IF [Late decision] = 'Late' THEN 1 ELSE 0 END) / SUM([Case_Cnt])
```

### Universe change vs prior month

```
(SUM([Case_Cnt]) - LOOKUP(SUM([Case_Cnt]), -1)) / LOOKUP(SUM([Case_Cnt]), -1)
```

### Aberration (Universe)

```
IF ABS([Universe change vs prior month]) > [Alert threshold (% change)] THEN 'Over threshold' ELSE 'Within threshold' END
```

### Delivery deadline

```
DATEADD('hour', [Delivery deadline hour (Friday, CT)], DATETIME(DATEADD('day', 6 - DATEPART('weekday', [As_Of_Dt_Key]), [As_Of_Dt_Key])))
```

### Delivery

```
IF [Run_Stat] <> 'COMPLETED' AND [Run_Stat] <> 'FINAL' THEN 'Not delivered' ELSEIF [End_Dtts] <= [Delivery deadline] THEN 'On time' ELSE 'Late' END
```

### In-scope change vs prior month

```
(SUM([In_Scope_Cnt]) - LOOKUP(SUM([In_Scope_Cnt]), -1)) / LOOKUP(SUM([In_Scope_Cnt]), -1)
```

### Aberration (Runs)

```
IF ABS([In-scope change vs prior month]) > [Alert threshold (% change)] THEN 'Over threshold' ELSE 'Within threshold' END
```

### Clinical auditor cases

```
SUM(IF [Smp_Needs_Clinical_Auditor] = 'Yes' THEN 1 ELSE 0 END)
```

### Expedited cases

```
SUM(IF [Smp_Processing] = 'Expedited' THEN 1 ELSE 0 END)
```

### Selected vs target

```
SUM([Smp_Case_Cnt]) - [Weekly target]
```

### Out-of-network denials

```
SUM(IF [Smp_Determination] = 'Denied' AND [Smp_OON_Ind] = 1 THEN 1 ELSE 0 END)
```

### In batch (Universe)

Uses Universe.

```
[BTCH_ID] = [Batch]
```

### In batch (Sample)

Uses Sample.

```
[SMPL_RUN_KEY] = [Batch]
```

### In batch (Run figures)

Uses Run figures.

```
[Stat_Run_Key] = [Batch]
```

### In batch (Issues)

Uses Universe risks and errors.

```
[DQ_BTCH_ID] = [Batch]
```

### In GRE run (results)

Uses GRE results.

```
[GS_Run_Key] = [GRE run]
```

### In GRE run (exceptions)

Uses GRE exceptions.

```
[GX_Run_Key] = [GRE run]
```

### Record key (Sample)

Uses Sample.

```
REGEXP_REPLACE(STR([Smp_SRC_DATA_KEY]), '[.]0*$', '')
```

### Record key (Issues)

Uses Universe risks and errors.

```
REGEXP_REPLACE(STR([DQ_SRC_DATA_KEY]), '[.]0*$', '')
```

### Selected record row

Uses ODAG1 data.

```
[Src_Data_Key_Txt] = [Selected record]
```

### Compliance flags (record)

Uses GRE exceptions.

```
{ FIXED [GX_Run_Key], [GX_Src_Key_Value] : SUM(IF CONTAINS(UPPER([GX_Severity]), 'COMPLIANCE') THEN [GX_Exception_Cnt] ELSE 0 END) }
```

### Errors and timeliness (record)

Uses GRE exceptions.

```
{ FIXED [GX_Run_Key], [GX_Src_Key_Value] : SUM(IF CONTAINS(UPPER([GX_Severity]), 'VALIDATION') OR CONTAINS(UPPER([GX_Severity]), 'TIMELI') THEN [GX_Exception_Cnt] ELSE 0 END) }
```

### Record risk

Uses GRE exceptions.

```
IF [Errors and timeliness (record)] >= 1 OR [Compliance flags (record)] >= [High-risk compliance flags] THEN 'High'
ELSEIF [Compliance flags (record)] >= 1 THEN 'Medium'
ELSE 'Low' END
```

### Risk order

Uses GRE exceptions.

```
[Errors and timeliness (record)] * 1000 + [Compliance flags (record)]
```

### High-risk records

Uses GRE exceptions.

```
COUNTD(IF [Record risk] = 'High' THEN [GX_Src_Key_Value] END)
```

Notes:

- **In batch (...)** and **In GRE run (...)** are the filters behind the Batch and GRE run drop-downs. The sampling run key is the batch id, so one Batch value works for Universe, Sample, Run figures and the engine checks.
- **Record key (...)**, `GX_Src_Data_Key` and `Src_Data_Key_Txt` are the same ODAG1 `SRC_DATA_KEY` written as text without a trailing `.0`, so a click on a case, an engine check or a GRE risk finds the ODAG1 rows.
- **Record risk** follows requirement 2.04: **High** when a record has a Data Validation Error or a Timeliness result, or at least *High-risk compliance flags* Compliance Flags; **Medium** when it has any Compliance Flag; **Low** otherwise. **Risk order** only sorts the list: records with errors or timeliness results first, then by number of Compliance Flags.
- The result type comes from the GRE rule's `severity` (Compliance Flag, Data Validation Error, Timeliness) and the category from `issue_category_name`; the exceptions query brings both onto every exception as `GX_Severity` and `GX_Issue_Category`.

## Part 5: The worksheets

General rules:

- Drag fields from the **Data** pane; type the name in its search box to find it.
- A plain name means the field as a dimension (blue). `SUM(x)` is the default for a measure. For `AVG`, `MIN`, `MAX`, `COUNTD`, right-click the pill, **Measure**, and pick it. `ATTR(x)`: right-click, **Attribute**.
- A filter on a calculated True/False field: tick **True** only.
- In lists, **Rows** take many fields; that is intended. Widen columns on the dashboard if text is cut.
- Build the sheets in this order; each says which dashboard it goes on.

### Sheets for 1 Weekly sample

#### 5.1 Sample KPIs

Tables: Sample.

1. Create a new worksheet and rename its tab **Sample KPIs**.
1. Drag `Measure Names` onto **Columns** and `Measure Values` onto **Text** on the Marks card.
1. On the **Measure Values** card (below Marks), drag every pill off, then drag in, in this order: `SUM(Smp_Case_Cnt)`, `Selected vs target`, `Clinical auditor cases`, `Expedited cases`, `Out-of-network denials`, `SUM(Smp_OON_Ind)`.
1. Right-click each column header, **Edit Alias...**, and rename them: **Selected**, **Vs weekly target**, **Need clinical auditor**, **Expedited**, **OON denials**, **Out of network**.
1. Click **Text** on the Marks card, **...**, select all, font size **20**, **bold**, OK. Set **Fit** (toolbar) to **Entire View**.
1. Drag `In batch (Sample)` onto **Filters**, tick only **True**, OK.
1. Double-click the title, type `Weekly sample, batch` and a space, then click **Insert > Parameters.Batch**, OK.

#### 5.2 Sample mix

Tables: Sample.

1. Create a new worksheet and rename its tab **Sample mix**.
1. **Columns:** `SUM(Smp_Case_Cnt)`.
1. **Rows:** drag, in this order, `Smp_Functional_Area`.
1. On the Marks card choose **Bar**.
1. Drag `Smp_Determination` onto **Color** on the Marks card.
1. Drag `Expedited cases` onto **Tooltip** on the Marks card.
1. Drag `Out-of-network denials` onto **Tooltip** on the Marks card.
1. Drag `Clinical auditor cases` onto **Tooltip** on the Marks card.
1. Drag `In batch (Sample)` onto **Filters**, tick only **True**, OK.
1. Sort `Smp_Functional_Area`: click the pill's arrow, **Sort**, Sort By **Field**, **Descending**, `SUM(Smp_Case_Cnt)`, close.
1. Double-click the title and type: **Sample by functional area and determination (click a bar to filter the case list)**

#### 5.3 Targets and caps

Tables: Run figures.

1. Create a new worksheet and rename its tab **Targets and caps**.
1. **Rows:** drag, in this order, `Stat_Kind`, `Stat_Nm`.
1. Drag `Measure Names` onto **Columns** and `Measure Values` onto **Text**. On the **Measure Values** card drag every pill off, then drag in, in this order: `SUM(Stat_Target_Cnt)`, `SUM(Stat_Selected_Cnt)`, `SUM(Stat_Candidate_Cnt)`, `SUM(Limit_Cnt)`, `SUM(Over_Cnt)`, `SUM(Replaced_Cnt)`, `SUM(Stat_Case_Cnt)`, `AVG(Stat_Sampling_Fraction)` (right-click the pill: **Measure > Average**).
1. Right-click each column header, **Edit Alias...**: **Target**, **Selected**, **Candidates**, **Cap limit**, **Over cap**, **Replaced**, **Cases**, **Sampling fraction**.
1. Drag `ATTR(Stat_Txt)` (right-click the pill: **Attribute**) onto **Tooltip** on the Marks card.
1. Drag `Stat_Rule_ID` (if it turns green, right-click it: **Dimension**, then **Discrete**) onto **Detail** on the Marks card.
1. Drag `In batch (Run figures)` onto **Filters**, tick only **True**, OK.
1. Drag `Run_Curr_Ind` onto **Filters**, tick only **1**, OK.
1. Drag `Stat_Kind` onto **Filters**, tick every value except **Parent bucket**, OK.
1. Double-click the title and type: **Targets, caps and what was picked (click a bucket to filter the case list)**

#### 5.4 Case list

Tables: Sample.

1. Create a new worksheet and rename its tab **Case list**.
1. **Rows:** drag, in this order, `SMPL_ORDER_NBR` (if it turns green, right-click it: **Dimension**, then **Discrete**), `Smp_Case_Key`, `Smp_Functional_Area`, `Smp_Determination`, `Smp_Processing`, `Smp_Network`, `Smp_Auditor_Type`, `Smp_CPT_List`, `SMPL_STRATUM_PATH`, `Selection_Stat`, `Stat_Rsn_Txt`.
1. On the Marks card choose **Text**.
1. Drag `AVG(Design_Weight)` (right-click the pill: **Measure > Average**) onto **Text** on the Marks card.
1. Drag `Record key (Sample)` onto **Detail** on the Marks card.
1. Drag `In batch (Sample)` onto **Filters**, tick only **True**, OK.
1. Sort `SMPL_ORDER_NBR`: click the pill's arrow, **Sort**, Sort By **Alphabetic** (for numbers this is by value), **Ascending**, close.
1. Double-click the title and type: **Cases to audit, in priority order (click a case to open its ODAG1 record)**

#### 5.5 Universe funnel

Tables: Universe.

1. Create a new worksheet and rename its tab **Universe funnel**.
1. **Columns:** `SUM(Case_Cnt)`.
1. **Rows:** drag, in this order, `Sample_Status`, `Exclusion_Rsn_Txt`.
1. On the Marks card choose **Bar**.
1. Drag `Exclusion_Check` onto **Color** on the Marks card.
1. Drag `SUM(Case_Cnt)` onto **Label** on the Marks card.
1. Drag `In batch (Universe)` onto **Filters**, tick only **True**, OK.
1. Double-click the title and type: **Universe to sample: in scope, excluded, candidates, selected**

#### 5.6 Universe by area

Tables: Universe.

1. Create a new worksheet and rename its tab **Universe by area**.
1. **Columns:** `Determination`.
1. **Rows:** drag, in this order, `Functional_Area`.
1. On the Marks card choose **Square**.
1. Drag `SUM(Case_Cnt)` onto **Color** on the Marks card.
1. Drag `SUM(Case_Cnt)` onto **Label** on the Marks card.
1. Drag `Universe share (candidates)` onto **Tooltip** on the Marks card.
1. Drag `Sample share` onto **Tooltip** on the Marks card.
1. Drag `AVG(Decision_Days)` (right-click the pill: **Measure > Average**) onto **Tooltip** on the Marks card.
1. Drag `Late decision rate` onto **Tooltip** on the Marks card.
1. Drag `Denial rate` onto **Tooltip** on the Marks card.
1. Drag `In batch (Universe)` onto **Filters**, tick only **True**, OK.
1. Double-click the title and type: **Universe by functional area and determination (click to filter the funnel and CPT codes)**

#### 5.7 Top CPT codes

Tables: Universe.

1. Create a new worksheet and rename its tab **Top CPT codes**.
1. **Columns:** `SUM(Case_Cnt)`.
1. **Rows:** drag, in this order, `CPT_First`.
1. On the Marks card choose **Bar**.
1. Drag `Sample_Status` onto **Color** on the Marks card.
1. Drag `In batch (Universe)` onto **Filters**, tick only **True**, OK.
1. Drag `CPT_First` onto **Filters**, open the **Top** tab, **By field**: Top **15** by **Case_Cnt**, **Sum**, OK. Then right-click the **In batch (Universe)** filter pill and choose **Add to Context** so the top 15 is within the batch.
1. Sort `CPT_First`: click the pill's arrow, **Sort**, Sort By **Field**, **Descending**, `SUM(Case_Cnt)`, close.
1. Double-click the title and type: **Top 15 CPT codes in the universe**

### Sheets for 2 Universe risks and errors

#### 5.8 Risk KPIs

Tables: GRE exceptions.

1. Create a new worksheet and rename its tab **Risk KPIs**.
1. Drag `Measure Names` onto **Columns** and `Measure Values` onto **Text** on the Marks card.
1. On the **Measure Values** card (below Marks), drag every pill off, then drag in, in this order: `SUM(GX_Exception_Cnt)`, `COUNTD(GX_Src_Key_Value)` (right-click the pill: **Measure > Count (Distinct)**), `High-risk records`, `SUM(GX_Open_Ind)`, `COUNTD(GX_Rule_ID)` (right-click the pill: **Measure > Count (Distinct)**).
1. Right-click each column header, **Edit Alias...**, and rename them: **Risks and errors**, **Records**, **High-risk records**, **Open**, **Rules hit**.
1. Click **Text** on the Marks card, **...**, select all, font size **20**, **bold**, OK. Set **Fit** (toolbar) to **Entire View**.
1. Drag `In GRE run (exceptions)` onto **Filters**, tick only **True**, OK.
1. Double-click the title, type `Universe risks and errors, GRE run` and a space, then click **Insert > Parameters.GRE run**, OK.

#### 5.9 Results by type

Tables: GRE exceptions.

1. Create a new worksheet and rename its tab **Results by type**.
1. **Columns:** `SUM(GX_Exception_Cnt)`.
1. **Rows:** drag, in this order, `GX_Severity`, `GX_Issue_Category`.
1. On the Marks card choose **Bar**.
1. Drag `GX_Severity` onto **Color** on the Marks card.
1. Drag `SUM(GX_Exception_Cnt)` onto **Label** on the Marks card.
1. Drag `In GRE run (exceptions)` onto **Filters**, tick only **True**, OK.
1. Double-click the title and type: **By result type (Compliance Flag, Data Validation Error, Timeliness) and category**

#### 5.10 GRE results by rule

Tables: GRE results, GRE rules.

1. Create a new worksheet and rename its tab **GRE results by rule**.
1. **Columns:** `SUM(GS_Failed_Records)`.
1. **Rows:** drag, in this order, `GR_Dgr_Nbr` (if it turns green, right-click it: **Dimension**, then **Discrete**), `GR_Rule_Nm`.
1. On the Marks card choose **Bar**.
1. Drag `GS_Status` onto **Color** on the Marks card.
1. Drag `GR_Rule_ID` (if it turns green, right-click it: **Dimension**, then **Discrete**) onto **Detail** on the Marks card.
1. Drag `AVG(GS_Failure_Pct)` (right-click the pill: **Measure > Average**) onto **Tooltip** on the Marks card.
1. Drag `ATTR(GS_Threshold_Pct_Used)` (right-click the pill: **Attribute**) onto **Tooltip** on the Marks card.
1. Drag `SUM(GS_Total_Records)` onto **Tooltip** on the Marks card.
1. Drag `ATTR(GS_Error_Message)` (right-click the pill: **Attribute**) onto **Tooltip** on the Marks card.
1. Drag `ATTR(GR_Severity)` (right-click the pill: **Attribute**) onto **Tooltip** on the Marks card.
1. Drag `ATTR(GR_Business_Rule)` (right-click the pill: **Attribute**) onto **Tooltip** on the Marks card.
1. Drag `In GRE run (results)` onto **Filters**, tick only **True**, OK.
1. Sort `GR_Rule_Nm`: click the pill's arrow, **Sort**, Sort By **Field**, **Descending**, `SUM(GS_Failed_Records)`, close.
1. Double-click the title and type: **Every rule in the run: pass, warn, fail, error (click a rule to filter the list)**

#### 5.11 Risk and error list

Tables: GRE exceptions.

1. Create a new worksheet and rename its tab **Risk and error list**.
1. **Rows:** drag, in this order, `Risk order` (if it turns green, right-click it: **Dimension**, then **Discrete**), `Record risk`, `GX_Src_Data_Key`, `GX_Severity`, `GX_Issue_Category`, `GX_Dgr_Nbr` (if it turns green, right-click it: **Dimension**, then **Discrete**), `GX_Rule_Nm`, `GX_Element_Name`, `GX_Src_Key_Value`, `GX_Exception_Flag`, `GX_Batch`, `GX_Load_Dt` (right-click the pill: **Exact Date**, then **Discrete**).
1. On the Marks card choose **Text**.
1. Drag `SUM(GX_Exception_Cnt)` onto **Text** on the Marks card.
1. Drag `Record risk` onto **Color** on the Marks card.
1. Drag `GX_Rule_ID` (if it turns green, right-click it: **Dimension**, then **Discrete**) onto **Detail** on the Marks card.
1. Drag `ATTR(GX_Business_Rule)` (right-click the pill: **Attribute**) onto **Tooltip** on the Marks card.
1. Drag `In GRE run (exceptions)` onto **Filters**, tick only **True**, OK.
1. Sort `Risk order`: click the pill's arrow, **Sort**, Sort By **Alphabetic** (for numbers this is by value), **Descending**, close.
1. Right-click the `Risk order` pill and untick **Show Header** (it only sets the order).
1. Double-click the title and type: **Risks and errors by record, highest risk first (click a row to open the ODAG1 record)**

#### 5.12 Engine checks

Tables: Universe risks and errors.

1. Create a new worksheet and rename its tab **Engine checks**.
1. **Rows:** drag, in this order, `Severity_Txt`, `Issue_Txt`, `Record key (Issues)`, `DQ_AUTHZN_ID`, `Column_Nm`, `Value_Txt`, `DQ_Functional_Area`, `DQ_Determination`, `DQ_Sample_Status`.
1. On the Marks card choose **Text**.
1. Drag `SUM(Issue_Cnt)` onto **Text** on the Marks card.
1. Drag `In batch (Issues)` onto **Filters**, tick only **True**, OK.
1. **Analysis > Totals > Add All Subtotals** and **Show Column Grand Totals**, so each issue type shows its count.
1. Double-click the title and type: **Checks the sampling engine ran on the weekly universe (click a row to open the ODAG1 record)**

#### 5.13 ODAG1 record

Tables: ODAG1 data.

1. Create a new worksheet and rename its tab **ODAG1 record**.
1. In the Data pane, under **ODAG1 data**, click the first column (`BTCH_ID (ODAG1 data)`), then **Shift+click** the last original column of the table (the one just above `Src_Data_Key_Txt`). Drag the whole selection onto **Rows**. Tableau asks whether to add all of them: **Add all members**. These are every column of the ODAG1 table.
1. Drag `Src_Data_Key_Txt` and `Odag1_Run_Ty` to the front of **Rows** so the key and pull type show first.
1. Drag `ODAG1 data (Count)` onto **Text** on the Marks card.
1. Drag `Selected record row` onto **Filters**, tick only **True**, OK.
1. Double-click the title, type `ODAG1 record` and a space, then click **Insert > Parameters.Selected record** and type `: every version and every pull`, OK.

### Sheets for 3 Operations and controls

#### 5.14 Universe pulls

Tables: ODAG1 data.

1. Create a new worksheet and rename its tab **Universe pulls**.
1. **Rows:** drag, in this order, `Odag1_Run_Ty`, `BTCH_ID (ODAG1 data)`, `Odag1_Batch_Dt` (right-click the pill: **Exact Date**, then **Discrete**), `FILE_NM (ODAG1 data)`, `UNVRS_VRSN_CD (ODAG1 data)`.
1. Drag `Measure Names` onto **Columns** and `Measure Values` onto **Text**. On the **Measure Values** card drag every pill off, then drag in, in this order: `ODAG1 data (Count)`, `COUNTD(AUTHZN_ID (ODAG1 data))` (right-click the pill: **Measure > Count (Distinct)**), `MIN(Odag1_Determination_Dt)` (right-click the pill: **Measure > Minimum**), `MAX(Odag1_Determination_Dt)` (right-click the pill: **Measure > Maximum**).
1. Right-click each column header, **Edit Alias...**: **Rows**, **Authorizations**, **Decisions from**, **Decisions to**.
1. Drag `Odag1_Curr_Ind` onto **Filters**, tick only **Y**, OK.
1. Drag `Odag1_Del_Ind` onto **Filters**, tick only **N**, OK.
1. Sort `BTCH_ID (ODAG1 data)`: click the pill's arrow, **Sort**, Sort By **Alphabetic** (for numbers this is by value), **Descending**, close.
1. Double-click the title and type: **Universe pulls: daily, weekly and monthly (click a weekly pull to make it the batch)**

#### 5.15 Run log

Tables: Runs.

1. Create a new worksheet and rename its tab **Run log**.
1. **Rows:** drag, in this order, `Run_Key`, `Run_Ty`, `Run_Kind`, `Run_Stat`, `Delivery`, `Start_Dtts` (right-click the pill: **Exact Date**, then **Discrete**), `Triggered_By`, `Run_Weekday`, `Error_Txt`, `Rerun_Rsn_Txt`, `Override_Rsn_Txt`, `Finalized_By`.
1. Drag `Measure Names` onto **Columns** and `Measure Values` onto **Text**. On the **Measure Values** card drag every pill off, then drag in, in this order: `SUM(Selected_Cnt)`, `SUM(Target_Cnt)`, `SUM(Shortfall_Cnt)`, `SUM(Duration_Min)`, `SUM(Config_Changed_Ind)`.
1. Right-click each column header, **Edit Alias...**: **Selected**, **Target**, **Shortfall**, **Minutes**, **Config changed**.
1. Sort `Run_Key`: click the pill's arrow, **Sort**, Sort By **Alphabetic** (for numbers this is by value), **Descending**, close.
1. Double-click the title and type: **Sampling runs (click a run to make its batch current)**

#### 5.16 Batches

Tables: Batches.

1. Create a new worksheet and rename its tab **Batches**.
1. **Rows:** drag, in this order, `Batch_Btch_ID`, `Batch_Run_Ty`, `Batch_Batch_Dt` (right-click the pill: **Exact Date**, then **Discrete**), `Batch_Status`, `Sampled_Status`, `Batch_Run_Stat`.
1. Drag `Measure Names` onto **Columns** and `Measure Values` onto **Text**. On the **Measure Values** card drag every pill off, then drag in, in this order: `SUM(Source_Cnt)`, `SUM(Closed_Source_Cnt)`, `SUM(Batch_Selected_Cnt)`.
1. Right-click each column header, **Edit Alias...**: **Sources**, **Sources closed**, **Selected**.
1. Sort `Batch_Btch_ID`: click the pill's arrow, **Sort**, Sort By **Alphabetic** (for numbers this is by value), **Descending**, close.
1. Double-click the title and type: **Framework batches and whether each was sampled**

#### 5.17 Revalidation

Tables: Revalidation.

1. Create a new worksheet and rename its tab **Revalidation**.
1. **Rows:** drag, in this order, `Reval_Run_Key`, `Reval_Dtts` (right-click the pill: **Exact Date**, then **Discrete**), `Reval_Stat`, `Signed_Off`, `Signoff_By`, `Reval_Error_Txt`.
1. Drag `Measure Names` onto **Columns** and `Measure Values` onto **Text**. On the **Measure Values** card drag every pill off, then drag in, in this order: `SUM(Orig_Selected_Cnt)`, `SUM(Replay_Selected_Cnt)`, `SUM(Same_Cnt)`, `SUM(Only_Orig_Cnt)`, `SUM(Only_Replay_Cnt)`, `SUM(Missing_Cnt)`, `SUM(Days_To_Signoff)`.
1. Right-click each column header, **Edit Alias...**: **Original**, **Replay**, **Same**, **Only original**, **Only replay**, **Missing**, **Days to sign-off**.
1. Drag `ATTR(Diff_Txt)` (right-click the pill: **Attribute**) onto **Tooltip** on the Marks card.
1. Double-click the title and type: **Six-monthly revalidation against history and sign-off**

#### 5.18 GRE run log

Tables: GRE rule audit.

1. Create a new worksheet and rename its tab **GRE run log**.
1. **Rows:** drag, in this order, `GA_Run_Key`, `GA_Rule_Group`, `GA_Status`, `GA_Started_Dtts` (right-click the pill: **Exact Date**, then **Discrete**), `GA_Triggered_By`.
1. Drag `Measure Names` onto **Columns** and `Measure Values` onto **Text**. On the **Measure Values** card drag every pill off, then drag in, in this order: `SUM(GA_Total_Rules)`, `SUM(GA_Rules_Succeeded)`, `SUM(GA_Rules_Errored)`.
1. Right-click each column header, **Edit Alias...**: **Rules**, **Succeeded**, **Errored**.
1. Drag `ATTR(GA_Run_Params)` (right-click the pill: **Attribute**) onto **Tooltip** on the Marks card.
1. Sort `GA_Run_Key`: click the pill's arrow, **Sort**, Sort By **Alphabetic** (for numbers this is by value), **Descending**, close.
1. Double-click the title and type: **GRE rules engine runs (click a run to make it the GRE run)**

#### 5.19 GRE errors

Tables: GRE rule errors.

1. Create a new worksheet and rename its tab **GRE errors**.
1. **Rows:** drag, in this order, `GE_Run_Key`, `GE_Rule_ID` (if it turns green, right-click it: **Dimension**, then **Discrete**), `GE_Error_Type`, `GE_Error_Message`, `GE_Occurred_Dtts` (right-click the pill: **Exact Date**, then **Discrete**).
1. On the Marks card choose **Text**.
1. Drag `SUM(GE_Error_Cnt)` onto **Text** on the Marks card.
1. Double-click the title and type: **GRE rules that failed to run**

#### 5.20 Refinement

Tables: Refinement.

1. Create a new worksheet and rename its tab **Refinement**.
1. **Rows:** drag, in this order, `Row_Kind`, `Finding_Ty`, `Finding_Nm`, `Refine_Rule_ID` (if it turns green, right-click it: **Dimension**, then **Discrete**), `Review_Status`, `Finding_Txt`.
1. On the Marks card choose **Text**.
1. Drag `SUM(Refine_Case_Cnt)` onto **Text** on the Marks card.
1. Drag `ATTR(Review_Txt)` (right-click the pill: **Attribute**) onto **Tooltip** on the Marks card.
1. Drag `ATTR(Threshold_Txt)` (right-click the pill: **Attribute**) onto **Tooltip** on the Marks card.
1. Double-click the title and type: **Opportunities to refine the rules and their human review**

#### 5.21 Retention

Tables: Retention.

1. Create a new worksheet and rename its tab **Retention**.
1. **Columns:** `Data_Year` (if it turns green, right-click it: **Dimension**, then **Discrete**).
1. **Rows:** drag, in this order, `Object_Nm`.
1. On the Marks card choose **Text**.
1. Drag `SUM(Row_Cnt)` onto **Text** on the Marks card.
1. Double-click the title and type: **Inputs and outputs kept, rows by data year (10-year retention)**

### Sheets for 4 Trends and configuration

#### 5.22 Run trends

Tables: Runs.

1. Create a new worksheet and rename its tab **Run trends**.
1. **Columns:** `MONTH(As_Of_Dt_Key)` (right-click the pill and choose the second **Month** (May 2015)).
1. **Rows:** `SUM(In_Scope_Cnt)`, then `In-scope change vs prior month`, then `SUM(Shortfall_Cnt)` (one below the other).
1. On the Marks card choose **Line** on the **All** tab.
1. On the Marks tab for `In-scope change vs prior month`, choose **Bar** and drag `Aberration (Runs)` onto **Color**.
1. Change calculations (the ones with a triangle) compute **Table (across)**, which is the default; leave it.
1. Drag `Curr_Ind` onto **Filters**, tick only **1**, OK.
1. Double-click the title and type: **In-scope volume, change vs prior month and shortfall**

#### 5.23 Universe trends

Tables: Universe.

1. Create a new worksheet and rename its tab **Universe trends**.
1. **Columns:** `MONTH(Batch_Dt)` (right-click the pill and choose the second **Month** (May 2015)).
1. **Rows:** `SUM(Case_Cnt)`, then `Universe change vs prior month`, then `Denial rate`, then `Late decision rate` (one below the other).
1. On the Marks card choose **Line** on the **All** tab.
1. On the Marks tab for `Universe change vs prior month`, choose **Bar** and drag `Aberration (Universe)` onto **Color**.
1. Change calculations (the ones with a triangle) compute **Table (across)**, which is the default; leave it.
1. Double-click the title and type: **Universe volume, change vs prior month, denial and late decision rates**

#### 5.24 Sample trends

Tables: Sample.

1. Create a new worksheet and rename its tab **Sample trends**.
1. **Columns:** `MONTH(SMPL_CYCLE_DT)` (right-click the pill and choose the second **Month** (May 2015)).
1. **Rows:** `SUM(Smp_Case_Cnt)`, then `Out-of-network denials`, then `Clinical auditor cases` (one below the other).
1. On the Marks card choose **Line** on the **All** tab.
1. On the Marks tab for `SUM(Smp_Case_Cnt)`, choose **Area** and drag `Smp_Area_Group` onto **Color**.
1. Change calculations (the ones with a triangle) compute **Table (across)**, which is the default; leave it.
1. Double-click the title and type: **Sample size by area, OON denials, clinical auditor cases**

#### 5.25 Risk trends

Tables: GRE exceptions.

1. Create a new worksheet and rename its tab **Risk trends**.
1. **Columns:** `MONTH(GX_Load_Dt)` (right-click the pill and choose the second **Month** (May 2015)).
1. **Rows:** `SUM(GX_Exception_Cnt)`, then `High-risk records` (one below the other).
1. On the Marks card choose **Line** on the **All** tab.
1. On the Marks tab for `SUM(GX_Exception_Cnt)`, choose **Bar** and drag `GX_Severity` onto **Color**.
1. Change calculations (the ones with a triangle) compute **Table (across)**, which is the default; leave it.
1. Drag `GX_Batch` onto **Filters**, **All**, OK (the dashboard shows it as the daily, weekly, monthly choice).
1. Double-click the title and type: **Risks and errors by month and result type, and high-risk records**

#### 5.26 Engine check trends

Tables: Universe risks and errors.

1. Create a new worksheet and rename its tab **Engine check trends**.
1. **Columns:** `WEEK(DQ_Batch_Dt)` (right-click the pill and choose **More > Week Number** and then **Continuous**).
1. **Rows:** `SUM(Issue_Cnt)` (one below the other).
1. On the Marks card choose **Line** on the **All** tab.
1. On the Marks tab for `SUM(Issue_Cnt)`, choose **Bar** and drag `Severity_Txt` onto **Color**.
1. Change calculations (the ones with a triangle) compute **Table (across)**, which is the default; leave it.
1. Double-click the title and type: **Engine checks by week**

#### 5.27 Sampling rules

Tables: Rules.

1. Create a new worksheet and rename its tab **Sampling rules**.
1. **Rows:** drag, in this order, `Seq_Nbr` (if it turns green, right-click it: **Dimension**, then **Discrete**), `Rule_ID` (if it turns green, right-click it: **Dimension**, then **Discrete**), `Rule_Class_Cd`, `Rule_Nm`, `Rule_Ty`, `Level_Nbr` (if it turns green, right-click it: **Dimension**, then **Discrete**), `Parent_Path_Txt`, `Quota_Cd`, `In_Effect`.
1. Drag `Measure Names` onto **Columns** and `Measure Values` onto **Text**. On the **Measure Values** card drag every pill off, then drag in, in this order: `MIN(Quota_Val)` (right-click the pill: **Measure > Minimum**), `MIN(Max_Val)` (right-click the pill: **Measure > Minimum**).
1. Right-click each column header, **Edit Alias...**: **Quota**, **Cap**.
1. Drag `ATTR(Rule_Desc)` (right-click the pill: **Attribute**) onto **Tooltip** on the Marks card.
1. Drag `ATTR(Rule_Sql_Txt)` (right-click the pill: **Attribute**) onto **Tooltip** on the Marks card.
1. Sort `Seq_Nbr`: click the pill's arrow, **Sort**, Sort By **Alphabetic** (for numbers this is by value), **Ascending**, close.
1. Double-click the title and type: **Sampling configuration: levels, buckets, quotas and caps (right-click a rule to see its run figures)**

#### 5.28 GRE rules

Tables: GRE rules.

1. Create a new worksheet and rename its tab **GRE rules**.
1. **Rows:** drag, in this order, `GR_Rule_Group`, `GR_Seq_No` (if it turns green, right-click it: **Dimension**, then **Discrete**), `GR_Rule_ID` (if it turns green, right-click it: **Dimension**, then **Discrete**), `GR_Rule_Nm`, `GR_Dgr_Nbr` (if it turns green, right-click it: **Dimension**, then **Discrete**), `GR_Severity`, `GR_Issue_Category`, `GR_Element_Name`, `GR_Act_Ind`.
1. On the Marks card choose **Text**.
1. Drag `MIN(GR_Threshold_Pct)` (right-click the pill: **Measure > Minimum**) onto **Text** on the Marks card.
1. Drag `ATTR(GR_Business_Rule)` (right-click the pill: **Attribute**) onto **Tooltip** on the Marks card.
1. Drag `ATTR(GR_Rule_Description)` (right-click the pill: **Attribute**) onto **Tooltip** on the Marks card.
1. Drag `ATTR(GR_Rule_Syntax)` (right-click the pill: **Attribute**) onto **Tooltip** on the Marks card.
1. Sort `GR_Seq_No`: click the pill's arrow, **Sort**, Sort By **Alphabetic** (for numbers this is by value), **Ascending**, close.
1. Double-click the title and type: **Data validation rules (right-click a rule to see its results)**

## Part 6: The dashboards

Each dashboard is **1600 x 1000**. For each one:

1. Click **New Dashboard** at the bottom and rename the tab.
2. In the **Dashboard** pane, **Size**: **Fixed size**, **1600** x **1000**.
3. At the bottom of the Dashboard pane click **Floating**, so every item can be placed exactly.
4. Drag each item in, then on the **Layout** tab type its **Position** (x, y) and **Size** (w, h).
5. Each sheet: on its title bar arrow choose **Fit > Entire View** for KPIs and charts, **Fit > Fit Width** for lists.

Every dashboard has the same header row:

| Item | x | y | w | h |
|---|---|---|---|---|
| Text object: the title and what it covers | 10 | 0 | 900 | 50 |
| Navigation button 1 | 920 | 8 | 215 | 36 |
| Navigation button 2 | 1145 | 8 | 215 | 36 |
| Navigation button 3 | 1370 | 8 | 220 | 36 |

Navigation buttons: drag **Navigation** from *Objects*. Click the button's arrow, **Edit Button...**: *Navigate to* the dashboard, *Button Style* **Text**, *Title* the dashboard name, OK. Each dashboard gets one button for each of the other three, in the order 1, 2, 3, 4. In Desktop, **Alt+click** a button to follow it while editing.

### 1 Weekly sample

Text object (two lines): **1 Weekly sample** *(size 16, bold)* / *Requirement 4 (4.01 sample selection, 4.03 sample output: prioritized cases, functional area, clinical auditor, universe risks) and 10.02 (targets and caps).* *(size 9)*

Buttons: **2 Universe risks and errors**, **3 Operations and controls**, **4 Trends and configuration**.

| Item | x | y | w | h |
|---|---|---|---|---|
| Parameter control **Batch** (Single value list) | 10 | 55 | 300 | 50 |
| Parameter control **Weekly target** (Slider) | 320 | 55 | 300 | 50 |
| Text object: *Pick a batch. Click a bar, bucket or area to filter; click a case to open its ODAG1 record.* | 630 | 55 | 960 | 50 |
| Sheet **Sample KPIs** | 10 | 110 | 1580 | 90 |
| Sheet **Sample mix** | 10 | 205 | 520 | 330 |
| Sheet **Targets and caps** | 540 | 205 | 1050 | 330 |
| Sheet **Universe funnel** | 10 | 540 | 520 | 220 |
| Sheet **Universe by area** | 540 | 540 | 620 | 220 |
| Sheet **Top CPT codes** | 1170 | 540 | 420 | 220 |
| Sheet **Case list** | 10 | 765 | 1580 | 230 |

Controls: select any sheet on the dashboard, its arrow (**More options**), **Parameters**, and pick the parameter; on the card's arrow choose the style shown. The parameter controls work for every sheet because the sheets filter on the **In batch** / **In GRE run** fields.

### 2 Universe risks and errors

Text object (two lines): **2 Universe risks and errors** *(size 16, bold)* / *Requirement 2 (2.01 to 2.04: risks and errors by rule, result type, category, risk level and priority, with the full ODAG1 record) and 1.03 (every universe field).* *(size 9)*

Buttons: **1 Weekly sample**, **3 Operations and controls**, **4 Trends and configuration**.

| Item | x | y | w | h |
|---|---|---|---|---|
| Parameter control **GRE run** (Single value list) | 10 | 55 | 300 | 50 |
| Parameter control **Batch** (Single value list) | 320 | 55 | 300 | 50 |
| Filter card `GX_Batch` (Multiple values (dropdown)) | 630 | 55 | 300 | 50 |
| Parameter control **High-risk compliance flags** (Slider) | 940 | 55 | 300 | 50 |
| Text object: *Click a risk or engine check to show that record's ODAG1 rows at the bottom. Click a rule to filter the list.* | 1250 | 55 | 340 | 50 |
| Sheet **Risk KPIs** | 10 | 110 | 780 | 90 |
| Sheet **Results by type** | 10 | 205 | 380 | 260 |
| Sheet **GRE results by rule** | 400 | 205 | 390 | 260 |
| Sheet **Engine checks** | 10 | 470 | 780 | 270 |
| Sheet **Risk and error list** | 800 | 110 | 790 | 630 |
| Sheet **ODAG1 record** | 10 | 745 | 1580 | 250 |

Controls: select any sheet on the dashboard, its arrow (**More options**), **Parameters**, and pick the parameter; on the card's arrow choose the style shown. The parameter controls work for every sheet because the sheets filter on the **In batch** / **In GRE run** fields.

Filter card `GX_Batch`: select the sheet **Risk and error list**, its arrow, **Filters**, `GX_Batch`. On the card's arrow choose **Multiple Values (dropdown)**, rename the card title to **Schedule (daily, weekly, monthly)**, and **Apply to Worksheets > Selected Worksheets**: **Risk KPIs**, **Results by type**, **Risk and error list**.

### 3 Operations and controls

Text object (two lines): **3 Operations and controls** *(size 16, bold)* / *Requirement 1 (1.02 pulls by frequency), 2.06 and 4.04 (runs on schedule), 3 and 6 (storage and retention), 8 (revalidation, human sign-off, refinement) and 10.01.* *(size 9)*

Buttons: **1 Weekly sample**, **2 Universe risks and errors**, **4 Trends and configuration**.

| Item | x | y | w | h |
|---|---|---|---|---|
| Parameter control **Batch** (Single value list) | 10 | 55 | 300 | 50 |
| Parameter control **GRE run** (Single value list) | 320 | 55 | 300 | 50 |
| Parameter control **Delivery deadline hour (Friday, CT)** (Slider) | 630 | 55 | 300 | 50 |
| Text object: *Click a run, batch or pull to make it the current batch (right-click: open the weekly sample). Click a GRE run to make it current.* | 940 | 55 | 650 | 50 |
| Sheet **Universe pulls** | 10 | 110 | 780 | 210 |
| Sheet **Run log** | 800 | 110 | 790 | 210 |
| Sheet **Batches** | 10 | 325 | 780 | 200 |
| Sheet **Revalidation** | 800 | 325 | 790 | 200 |
| Sheet **GRE run log** | 10 | 530 | 780 | 215 |
| Sheet **GRE errors** | 800 | 530 | 790 | 215 |
| Sheet **Refinement** | 10 | 750 | 1050 | 245 |
| Sheet **Retention** | 1070 | 750 | 520 | 245 |

Controls: select any sheet on the dashboard, its arrow (**More options**), **Parameters**, and pick the parameter; on the card's arrow choose the style shown. The parameter controls work for every sheet because the sheets filter on the **In batch** / **In GRE run** fields.

### 4 Trends and configuration

Text object (two lines): **4 Trends and configuration** *(size 16, bold)* / *Requirement 7 (7.01 to 7.04: trends, aberrations over the alert threshold, high-risk frequency), 2.03 and 4.02 (one-year look back), 9 and 10.02 (configuration).* *(size 9)*

Buttons: **1 Weekly sample**, **2 Universe risks and errors**, **3 Operations and controls**.

| Item | x | y | w | h |
|---|---|---|---|---|
| Parameter control **Alert threshold (% change)** (Slider) | 10 | 55 | 300 | 50 |
| Parameter control **Standard decision days** (Slider) | 320 | 55 | 300 | 50 |
| Parameter control **Expedited decision days** (Slider) | 630 | 55 | 300 | 50 |
| Filter card `GX_Batch` (Multiple values (dropdown)) | 940 | 55 | 300 | 50 |
| Text object: *Move the sliders to change the aberration and timeliness thresholds. Right-click a rule to jump to its results.* | 1250 | 55 | 340 | 50 |
| Sheet **Run trends** | 10 | 110 | 780 | 290 |
| Sheet **Universe trends** | 800 | 110 | 790 | 290 |
| Sheet **Sample trends** | 10 | 405 | 520 | 200 |
| Sheet **Risk trends** | 540 | 405 | 520 | 200 |
| Sheet **Engine check trends** | 1070 | 405 | 520 | 200 |
| Sheet **Sampling rules** | 10 | 610 | 780 | 385 |
| Sheet **GRE rules** | 800 | 610 | 790 | 385 |

Controls: select any sheet on the dashboard, its arrow (**More options**), **Parameters**, and pick the parameter; on the card's arrow choose the style shown. The parameter controls work for every sheet because the sheets filter on the **In batch** / **In GRE run** fields.

Filter card `GX_Batch`: select the sheet **Risk trends**, its arrow, **Filters**, `GX_Batch`. On the card's arrow choose **Multiple Values (dropdown)**, rename the card title to **Schedule (daily, weekly, monthly)**, and **Apply to Worksheets > Selected Worksheets**: **Risk trends**.

## Part 7: The actions (links between sheets)

Open each dashboard, **Dashboard > Actions...**, **Add Action**, and create the actions listed for it. *Run on* **Select** fires on a click; **Menu** puts a link in the tooltip menu (right-click or hover a mark).

- **Change Parameter**: *Source sheets* the sheet, *Target parameter*, *Source field*, *Aggregation* **None**, *Clearing the selection will* **Keep current value**.
- **Go to Sheet**: *Source sheets* the sheet, *Target sheet* the dashboard.
- **Filter**: *Source sheets* the sheet, *Target sheets* (pick the dashboard first, then the sheet), *Clearing the selection will* **Show all values**, *Filter* as listed. For **Selected fields**, add the source field and the target field as a pair.
- An action whose source field is not on Rows uses the copy on **Detail** that Part 5 added.

| # | Dashboard | Type | Source sheet | Run on | Target | Field | Name |
|---|---|---|---|---|---|---|---|
| 1 | 1 Weekly sample | Change Parameter | Case list | Select | Selected record | Record key (Sample) | Open ODAG1 record |
| 2 | 1 Weekly sample | Go to Sheet | Case list | Select | 2 Universe risks and errors |  | Go to the ODAG1 record |
| 3 | 1 Weekly sample | Filter | Sample mix | Select | Case list | All fields | Filter cases by area |
| 4 | 1 Weekly sample | Filter | Targets and caps | Select | Case list | Selected fields: Stat_Nm = SMPL_STRATUM_PATH | Filter cases by bucket |
| 5 | 1 Weekly sample | Filter | Universe by area | Select | Universe funnel, Top CPT codes | All fields | Filter funnel and CPT codes |
| 6 | 2 Universe risks and errors | Change Parameter | Risk and error list | Select | Selected record | GX_Src_Data_Key | Show ODAG1 record (risk) |
| 7 | 2 Universe risks and errors | Change Parameter | Engine checks | Select | Selected record | Record key (Issues) | Show ODAG1 record (engine check) |
| 8 | 2 Universe risks and errors | Filter | GRE results by rule | Select | Risk and error list | Selected fields: GR_Rule_ID = GX_Rule_ID | Filter risks by rule |
| 9 | 2 Universe risks and errors | Filter | Results by type | Select | Risk and error list | All fields | Filter risks by type |
| 10 | 3 Operations and controls | Change Parameter | Run log | Select | Batch | Run_Key | Make run current |
| 11 | 3 Operations and controls | Change Parameter | Batches | Select | Batch | Batch_Btch_ID | Make batch current |
| 12 | 3 Operations and controls | Change Parameter | Universe pulls | Select | Batch | BTCH_ID (ODAG1 data) | Make pull current |
| 13 | 3 Operations and controls | Go to Sheet | Run log | Menu | 1 Weekly sample |  | Open the weekly sample for this run |
| 14 | 3 Operations and controls | Change Parameter | GRE run log | Select | GRE run | GA_Run_Key | Make GRE run current |
| 15 | 3 Operations and controls | Go to Sheet | GRE run log | Menu | 2 Universe risks and errors |  | Open the risks and errors for this GRE run |
| 16 | 3 Operations and controls | Filter | Refinement | Menu | Sampling rules (on 4 Trends and configuration) | Selected fields: Refine_Rule_ID = Rule_ID | Show this rule |
| 17 | 4 Trends and configuration | Filter | Sampling rules | Menu | Targets and caps (on 1 Weekly sample) | Selected fields: Rule_ID = Stat_Rule_ID | Show run figures for this rule |
| 18 | 4 Trends and configuration | Filter | GRE rules | Menu | GRE results by rule (on 2 Universe risks and errors) | Selected fields: GR_Rule_ID = GR_Rule_ID | Show results for this rule |

How you use them:

- **Exception to actual data.** On **2 Universe risks and errors**, click a row in *Risk and error list* or *Engine checks*: *ODAG1 record* at the bottom shows every column of that record, for every version and every pull. On **1 Weekly sample**, click a case in *Case list*: Tableau opens dashboard 2 with that record shown.
- **Batch everywhere.** Pick a batch in any Batch drop-down, or click a run, batch or weekly pull on dashboard 3: all four dashboards follow, because the parameter is shared.
- **GRE run everywhere.** Pick a GRE run, or click one in *GRE run log* on dashboard 3.
- **Rules.** Right-click a sampling rule (dashboard 4) to see its targets and caps on dashboard 1; a GRE rule to see its results on dashboard 2; a refinement finding (dashboard 3) to see its rule on dashboard 4.

The drill-through needs the ODAG1 GRE rules to list `SRC_DATA_KEY` in `src_key_cols` (they do), so each exception's key reads `SRC_DATA_KEY=...`. Exceptions from the ODAG1_FILE rules are on the NAS file; they show the record only if the file carries the same SRC_DATA_KEY.

## Part 8: Hide the sheets and check

1. Right-click each dashboard tab, **Hide All Sheets**. Only the 4 dashboards remain as tabs; the sheets are still in the workbook (right-click a dashboard item, **Go to Sheet** to edit one).
2. Drag the dashboard tabs into the order 1, 2, 3, 4 and save.
3. Open **1 Weekly sample**: the KPIs show the latest batch. Change the batch: every sheet changes.
4. Click a case in the case list: dashboard 2 opens with that record's ODAG1 rows at the bottom.
5. On dashboard 2 pick a GRE run, click a risk: the ODAG1 record changes. Click a rule in *GRE results by rule*: the list filters to it.
6. On dashboard 3 click a run in the run log, then go to dashboard 1: it shows that batch.
7. On dashboard 4 move **Alert threshold (% change)**: the aberration colours change.
8. If a sheet shows a red error, open it (**Go to Sheet**) and check its pills against Part 5. If the GRE sheets are empty, check that HSABC_V has rows whose rule_group starts with ODAG1, and that the GRE run drop-down holds a run.
9. **File > Save**, close Tableau, and open the file again to make sure it opens cleanly.
10. Before putting the workbook in git, take the server name out (PowerShell):

    ```
    $f = "C:\Script\frameworks\GOV_RGLTRY\Sampling\airflow_resources\docs\tableau\ODAG1_Sampling_Dashboard_v2.twb"; (Get-Content $f -Raw) -replace "server='[^']*'", "server='TERADATA_SERVER'" | Set-Content $f -Encoding utf8
    ```

## Part 9: What each requirement uses

From *Clinical Audit Sample Optimization* (Phase 1, HealthSpring UM).

| Requirement | Where | Sheets |
|---|---|---|
| 1.01 to 1.03 Universe pull, frequency, fields | 3 Operations and controls, 2 Universe risks and errors | Universe pulls (run type, batch, file, version, rows, decision period), ODAG1 record (every field) |
| 2.01 Review the daily, weekly, monthly universe | 2 Universe risks and errors | Risk KPIs, Risk and error list, Schedule filter |
| 2.02 Cases in error or at risk, by rule | 2 Universe risks and errors, 4 Trends and configuration | Risk and error list, GRE results by rule, GRE rules (the rules themselves) |
| 2.03 One-year look back | 4 Trends and configuration | Risk trends, Engine check trends (monthly, by result type) |
| 2.04 Dashboard summary and detail: rule, result type, category, risk scoring, priority | 2 Universe risks and errors | Risk KPIs, Results by type, Risk and error list (Record risk, sorted by risk), ODAG1 record |
| 2.05 Notify teams | not a dashboard item | done by the Airflow jobs; the dashboard shows the same output |
| 2.06, 4.04 Runs every day and Friday, many users | 3 Operations and controls | Run log, GRE run log, Universe pulls |
| 3.01, 3.02, 6.01, 6.02 Store inputs and outputs 10 years | 3 Operations and controls | Retention (rows kept by data year), Universe pulls |
| 4.01 Weekly sample criteria and mix | 1 Weekly sample | Sample mix, Targets and caps, Universe funnel, Universe by area, Top CPT codes |
| 4.02 Validate against history | 3 Operations and controls, 4 Trends and configuration | Revalidation, trends |
| 4.03 Sample output by Friday 3pm CT: prioritized cases, functional area, clinical auditor, universe risks | 1 Weekly sample, 2 Universe risks and errors, 3 Operations and controls | Case list (priority order), Sample KPIs, Engine checks, Run log (Delivery on time or late) |
| 5 Queue in the audit tool | not a dashboard item | stretch requirement, outside Tableau |
| 7.01 Shared dashboards | all | the 4 dashboards |
| 7.02 Aberrations over x% vs prior month | 4 Trends and configuration | Run trends, Universe trends (Alert threshold slider) |
| 7.03, 7.04 Trends and frequency of high-risk errors | 4 Trends and configuration | Risk trends, Sample trends, Engine check trends |
| 8.01 to 8.03 Revalidation, human checkpoints, refinement | 3 Operations and controls | Revalidation, Run log (finalized by, overrides), Refinement |
| 9 Scalable | 4 Trends and configuration | Sampling rules, GRE rules (configuration, not code) |
| 10.01, 10.02 Configurable frequency and case mix | 3 Operations and controls, 4 Trends and configuration | Run log (run type), Sampling rules, Targets and caps |

Not covered by the data yet: 2.04 *results that would invalidate the CMS submission* needs a marker on the GRE rule (for example a category value); once the rules carry it, add it as a filter on *Risk and error list*.

