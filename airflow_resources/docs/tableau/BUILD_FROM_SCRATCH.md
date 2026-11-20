# ODAG1 Sampling dashboard: build it from scratch in Tableau

This guide builds the whole workbook by hand in Tableau Desktop: the data source, the 5 parameters, the 16 calculated fields, the 50 worksheets and the 9 dashboards. It uses the 10 Custom SQL files already in `airflow_resources/docs/tableau/custom_sql/` plus the ODAG1 data table itself. Nothing is created in Teradata: Tableau only reads.

Not in this guide yet: the GRE tables (rules, exceptions, results and the others) and the dashboards for requirements 1 and 2. Their table names, columns and requirement text are not in this repository; they are added once they are known.

Follow the parts in order. Type every name exactly as written: the calculated fields refer to fields and parameters by name.

## Contents

1. [Before you start](#part-1-before-you-start)
2. [The data source](#part-2-the-data-source)
3. [The parameters](#part-3-the-parameters)
4. [The calculated fields](#part-4-the-calculated-fields)
5. [The worksheets](#part-5-the-worksheets)
6. [The dashboards](#part-6-the-dashboards)
7. [Final checks](#part-7-final-checks)

## Part 1: Before you start

1. Pull the repository so the Custom SQL files are on the PC:

   ```
   git -C C:\Script\frameworks\GOV_RGLTRY\Sampling pull
   ```

2. Open the folder `C:\Script\frameworks\GOV_RGLTRY\Sampling\airflow_resources\docs\tableau\custom_sql\`. Each file holds one complete query: open it in Notepad, press **Ctrl+A**, **Ctrl+C**, and paste it into Tableau.
3. Have your Teradata server name, user name and password ready.
4. Start Tableau Desktop and choose **File > New**. Save it straight away (**File > Save As**, type **Tableau Workbook (*.twb)**) as `C:\Script\frameworks\GOV_RGLTRY\Sampling\airflow_resources\docs\tableau\ODAG1_Sampling_Dashboard_v2.twb`, and press **Ctrl+S** often.

## Part 2: The data source

### 2.1 Connect

1. On the Start page, under **Connect > To a Server**, choose **Teradata** (under *More...* if it is not listed).
2. Enter the server, choose your authentication, enter user name and password, **Sign In**.
3. On the Data Source page, in **Database**, choose **HSLABBOSS**.
4. Leave the connection **Live** (top right).

### 2.2 Add the 10 tables

Each table is a Custom SQL query. For the first one, drag **New Custom SQL** (at the bottom of the left pane) onto the empty canvas. For every other one, drag **New Custom SQL** so it lands next to the table named in *Drag onto*: Tableau then draws a relationship line between them. In the dialog paste the file, click **OK**, then rename the table: click the table's drop-down arrow on the canvas, **Rename**, type the name.

| Step | File in `custom_sql\` | Rename the table to | Drag onto | Relationship (left table field = new table field) |
|---|---|---|---|---|
| 1 | `01_Universe.sql` | Universe | the empty canvas | none, this is the first table |
| 2 | `02_Runs.sql` | Runs | Universe | `Smpl_Run_ID` = `Runs_Smpl_Run_ID` |
| 3 | `04_Sample.sql` | Sample | Universe | `Case_Key` = `Smp_Case_Key` |
| 4 | `06_Universe_risks_and_errors.sql` | Universe risks and errors | Universe | `Case_Key` = `DQ_Case_Key` |
| 5 | `05_Run_figures.sql` | Run figures | Runs | `Runs_Smpl_Run_ID` = `Stat_Smpl_Run_ID` |
| 6 | `07_Revalidation.sql` | Revalidation | Runs | `Runs_Smpl_Run_ID` = `Reval_Smpl_Run_ID` |
| 7 | `09_Batches.sql` | Batches | Runs | `Runs_Smpl_Run_ID` = `Batch_Smpl_Run_ID` |
| 8 | `10_Retention.sql` | Retention | Runs | `Model_Link` = `Retention_Link` |
| 9 | `03_Rules.sql` | Rules | Run figures | `Stat_Rule_ID` = `Rule_ID` |
| 10 | `08_Refinement.sql` | Refinement | Rules | `Rule_ID` = `Refine_Rule_ID` |
| 11 | none: the table itself, `SDO_GOVT_RGLTRY_CLNCL_AUD_UM_AUTHZN_ODAG1` | ODAG1 | Universe | four pairs, see below |

Step 11 adds the actual ODAG1 data table, every row and every version, not only the current weekly rows the Universe query keeps. Do not use New Custom SQL for it: type `SDO_GOVT_RGLTRY_CLNCL_AUD_UM_AUTHZN_ODAG1` in the **Table** search box on the left and drag the table onto Universe. Its columns have the same names as Universe's, so Tableau shows them with ` (ODAG1)` after the name, for example `BTCH_ID (ODAG1)`. That is expected for this table only. In **Edit Relationship** click **Add more fields** until there are four pairs:

| Universe | ODAG1 |
|---|---|
| `BTCH_ID` | `BTCH_ID (ODAG1)` |
| `AUTHZN_ID` | `AUTHZN_ID (ODAG1)` |
| `SRC_DATA_KEY` | `SRC_DATA_KEY (ODAG1)` |
| `AUTHZN_REVSN_NUM` | `AUTHZN_REVSN_NUM (ODAG1)` |

Setting a relationship: after you drop the new table, the **Edit Relationship** dialog opens (or click the line between the two tables). Pick the left field from the left list and the right field from the right list, keep the operator **=**, close the dialog. Leave **Performance Options** as they are.

Check after each table: click the table, and the data grid at the bottom shows its columns. Apart from the ODAG1 table, no field name may end in brackets (for example `Stat_Run_Key (Runs)`). Every query names its columns uniquely, so a bracket means a query was pasted into the wrong table: open that table's drop-down, **Edit Custom SQL Query**, and paste the right file.

The finished canvas looks like this:

```
Universe ─┬─ Runs ─┬─ Run figures ── Rules ── Refinement
          │        ├─ Revalidation
          │        ├─ Batches
          │        └─ Retention
          ├─ Sample
          ├─ ODAG1
          └─ Universe risks and errors
```

### 2.3 Name the data source

At the top left of the Data Source page, click the data source name and rename it **ODAG1 Sampling**.

Table sizes for reference (number of columns): Universe 74, Runs 42, Rules 23, Sample 69, Run figures 32, Universe risks and errors 14, Revalidation 27, Refinement 27, Batches 15, Retention 4.

## Part 3: The parameters

Go to **Sheet 1**. In the Data pane, click the small arrow at the top (next to the search box), **Create Parameter...**. Create these 5:

| Name | Data type | Current value | Allowable values | Minimum | Maximum | Step size |
|---|---|---|---|---|---|---|
| Alert threshold (% change) | Float | 0.2 | Range | 0 | 1 | 0.05 |
| Standard decision days | Integer | 14 | Range | 1 | 60 | 1 |
| Expedited decision days | Integer | 3 | Range | 1 | 14 | 1 |
| Delivery deadline hour (Friday, CT) | Integer | 15 | Range | 0 | 23 | 1 |
| Weekly target | Integer | 150 | Range | 1 | 1000 | 1 |

After each one, right-click it in the Parameters list and choose **Show Parameter**. You can close the card again; the dashboards show the sliders later.

## Part 4: The calculated fields

In the Data pane, click the arrow at the top, **Create Calculated Field...**. Type the name, paste the formula, **OK**. Create them in this order (later ones use earlier ones).

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

Fields in `[brackets]` that are not parameters or calculated fields are columns of the tables in Part 2, with the names exactly as they come from the SQL.

## Part 5: The worksheets

General rules for every sheet:

- Drag fields from the **Data** pane (left). Fields are grouped by table; type the field name in the search box to find it fast.
- A plain name like `Functional_Area` on a shelf means the field as a dimension (blue pill).
- `SUM(Field)` means drag the measure; Tableau sums it by default. `AVG(Field)` means drag it, then right-click the pill, **Measure**, **Average**.
- If you drag a numeric column that should be a dimension, right-click the pill and choose **Dimension**.
- Calculated fields from Part 4 appear under their own name (for example `Clinical auditor cases`).
- The run or batch filter (**All** values) is needed so the dashboard drop-down can control the sheet.

### 5.1 KPI Selected

Uses the table(s): Sample. Goes on dashboard **1 Weekly sample**.

1. Create a new worksheet (the **New Worksheet** icon at the bottom) and rename its tab **KPI Selected**.
1. **Marks** card: choose **Text** from the drop-down.
1. Drag `SUM(Smp_Case_Cnt)` onto **Text (Label)** on the Marks card.
1. Drag `SMPL_RUN_KEY` onto **Filters**, choose **All** on the General tab (the **Use all** option), OK. On the dashboard this becomes the run or batch drop-down.
1. Click **Text** on the Marks card, click **...**, select all the text, set the font size to **26** and **bold**, OK.
1. Double-click the sheet title and replace it with: **Cases selected**

### 5.2 KPI Clinical

Uses the table(s): Sample. Goes on dashboard **1 Weekly sample**.

1. Create a new worksheet (the **New Worksheet** icon at the bottom) and rename its tab **KPI Clinical**.
1. **Marks** card: choose **Text** from the drop-down.
1. Drag `Clinical auditor cases` onto **Text (Label)** on the Marks card.
1. Drag `SMPL_RUN_KEY` onto **Filters**, choose **All** on the General tab (the **Use all** option), OK. On the dashboard this becomes the run or batch drop-down.
1. Click **Text** on the Marks card, click **...**, select all the text, set the font size to **26** and **bold**, OK.
1. Double-click the sheet title and replace it with: **Need a clinical auditor**

### 5.3 KPI Expedited

Uses the table(s): Sample. Goes on dashboard **1 Weekly sample**.

1. Create a new worksheet (the **New Worksheet** icon at the bottom) and rename its tab **KPI Expedited**.
1. **Marks** card: choose **Text** from the drop-down.
1. Drag `Expedited cases` onto **Text (Label)** on the Marks card.
1. Drag `SMPL_RUN_KEY` onto **Filters**, choose **All** on the General tab (the **Use all** option), OK. On the dashboard this becomes the run or batch drop-down.
1. Click **Text** on the Marks card, click **...**, select all the text, set the font size to **26** and **bold**, OK.
1. Double-click the sheet title and replace it with: **Expedited**

### 5.4 KPI OON

Uses the table(s): Sample. Goes on dashboard **1 Weekly sample**.

1. Create a new worksheet (the **New Worksheet** icon at the bottom) and rename its tab **KPI OON**.
1. **Marks** card: choose **Text** from the drop-down.
1. Drag `SUM(Smp_OON_Ind)` onto **Text (Label)** on the Marks card.
1. Drag `SMPL_RUN_KEY` onto **Filters**, choose **All** on the General tab (the **Use all** option), OK. On the dashboard this becomes the run or batch drop-down.
1. Click **Text** on the Marks card, click **...**, select all the text, set the font size to **26** and **bold**, OK.
1. Double-click the sheet title and replace it with: **Out of network**

### 5.5 KPI Vs Target

Uses the table(s): Sample. Goes on dashboard **1 Weekly sample**.

1. Create a new worksheet (the **New Worksheet** icon at the bottom) and rename its tab **KPI Vs Target**.
1. **Marks** card: choose **Text** from the drop-down.
1. Drag `Selected vs target` onto **Text (Label)** on the Marks card.
1. Drag `SMPL_RUN_KEY` onto **Filters**, choose **All** on the General tab (the **Use all** option), OK. On the dashboard this becomes the run or batch drop-down.
1. Click **Text** on the Marks card, click **...**, select all the text, set the font size to **26** and **bold**, OK.
1. Double-click the sheet title and replace it with: **Selected minus the weekly target**

### 5.6 Sample by area

Uses the table(s): Sample. Goes on dashboard **1 Weekly sample**.

1. Create a new worksheet (the **New Worksheet** icon at the bottom) and rename its tab **Sample by area**.
1. **Columns:** `SUM(Smp_Case_Cnt)`.
1. **Rows:** `Smp_Functional_Area`.
1. **Marks** card: choose **Bar** from the drop-down.
1. Drag `Smp_Determination` onto **Color** on the Marks card.
1. Drag `SMPL_RUN_KEY` onto **Filters**, choose **All** on the General tab (the **Use all** option), OK. On the dashboard this becomes the run or batch drop-down.
1. Sort `Smp_Functional_Area`: click the pill's arrow, **Sort**, Sort By **Field**, **Descending**, field `SUM(Smp_Case_Cnt)`.
1. Double-click the sheet title and replace it with: **Selected cases by functional area and determination**

### 5.7 Target vs selected

Uses the table(s): Run figures. Goes on dashboard **1 Weekly sample**.

1. Create a new worksheet (the **New Worksheet** icon at the bottom) and rename its tab **Target vs selected**.
1. **Columns:** `SUM(Leaf_Target_Cnt)` and then `SUM(Leaf_Selected_Cnt)` and then `SUM(Leaf_Candidate_Cnt)` (side by side on the same shelf, separate axes).
1. **Rows:** `Path_Level_1`.
1. **Marks** card: choose **Bar** from the drop-down.
1. Drag `Stat_Run_Key` onto **Filters**, choose **All** on the General tab (the **Use all** option), OK. On the dashboard this becomes the run or batch drop-down.
1. Double-click the sheet title and replace it with: **Target, selected and candidates per functional area**

### 5.8 Determination mix

Uses the table(s): Sample. Goes on dashboard **1 Weekly sample**.

1. Create a new worksheet (the **New Worksheet** icon at the bottom) and rename its tab **Determination mix**.
1. **Columns:** `SUM(Smp_Case_Cnt)`.
1. **Rows:** `Smp_Determination`.
1. **Marks** card: choose **Bar** from the drop-down.
1. Drag `Smp_Processing` onto **Color** on the Marks card.
1. Drag `SMPL_RUN_KEY` onto **Filters**, choose **All** on the General tab (the **Use all** option), OK. On the dashboard this becomes the run or batch drop-down.
1. Double-click the sheet title and replace it with: **Determination mix, expedited first**

### 5.9 OON by area

Uses the table(s): Sample. Goes on dashboard **1 Weekly sample**.

1. Create a new worksheet (the **New Worksheet** icon at the bottom) and rename its tab **OON by area**.
1. **Columns:** `SUM(Smp_OON_Ind)`.
1. **Rows:** `Smp_Functional_Area`.
1. **Marks** card: choose **Bar** from the drop-down.
1. Drag `SMPL_RUN_KEY` onto **Filters**, choose **All** on the General tab (the **Use all** option), OK. On the dashboard this becomes the run or batch drop-down.
1. Double-click the sheet title and replace it with: **Out-of-network cases per area (cap 10 per area's denials)**

### 5.10 Case list

Uses the table(s): Sample. Goes on dashboard **1 Weekly sample**.

1. Create a new worksheet (the **New Worksheet** icon at the bottom) and rename its tab **Case list**.
1. **Rows:** `SMPL_ORDER_NBR` as a **discrete dimension** (right-click the pill, choose Dimension, then Discrete), then `Smp_AUTHZN_ID`, then `Smp_Functional_Area`, then `Smp_IP_PAC_Type`, then `Smp_Determination`, then `Smp_Processing`, then `Smp_Network`, then `Smp_Auditor_Type`, then `Smp_CPT_List`, then `Selection_Stat`, then `SMPL_STRATUM_PATH`.
1. **Marks** card: choose **Text** from the drop-down.
1. Drag `AVG(Design_Weight)` (right-click the pill, Measure, Avg) onto **Text (Label)** on the Marks card.
1. Drag `SMPL_RUN_KEY` onto **Filters**, choose **All** on the General tab (the **Use all** option), OK. On the dashboard this becomes the run or batch drop-down.
1. Double-click the sheet title and replace it with: **Cases to audit, in priority order (with the stratum weight)**

### 5.11 Universe heat map

Uses the table(s): Universe. Goes on dashboard **2 Universe and funnel**.

1. Create a new worksheet (the **New Worksheet** icon at the bottom) and rename its tab **Universe heat map**.
1. **Columns:** `Determination`.
1. **Rows:** `Functional_Area`.
1. **Marks** card: choose **Square** from the drop-down.
1. Drag `SUM(Case_Cnt)` onto **Color** on the Marks card.
1. Drag `SUM(Case_Cnt)` onto **Text (Label)** on the Marks card.
1. Drag `BTCH_ID` onto **Filters**, choose **All** on the General tab (the **Use all** option), OK. On the dashboard this becomes the run or batch drop-down.
1. Double-click the sheet title and replace it with: **Universe: functional area by determination**

### 5.12 What the sample did

Uses the table(s): Universe. Goes on dashboard **2 Universe and funnel**.

1. Create a new worksheet (the **New Worksheet** icon at the bottom) and rename its tab **What the sample did**.
1. **Columns:** `SUM(Case_Cnt)`.
1. **Rows:** `Sample_Status`.
1. **Marks** card: choose **Bar** from the drop-down.
1. Drag `BTCH_ID` onto **Filters**, choose **All** on the General tab (the **Use all** option), OK. On the dashboard this becomes the run or batch drop-down.
1. Double-click the sheet title and replace it with: **Universe rows: selected, not selected, excluded, skipped**

### 5.13 Exclusions

Uses the table(s): Run figures. Goes on dashboard **2 Universe and funnel**.

1. Create a new worksheet (the **New Worksheet** icon at the bottom) and rename its tab **Exclusions**.
1. **Columns:** `SUM(Stat_Case_Cnt)`.
1. **Rows:** `Stat_Nm`.
1. **Marks** card: choose **Bar** from the drop-down.
1. Drag `Stat_Run_Key` onto **Filters**, choose **All** on the General tab (the **Use all** option), OK. On the dashboard this becomes the run or batch drop-down.
1. Drag `Stat_Ty` onto **Filters**, tick only **EXCLUSION**, OK.
1. Sort `Stat_Nm`: click the pill's arrow, **Sort**, Sort By **Field**, **Descending**, field `SUM(Stat_Case_Cnt)`.
1. Double-click the sheet title and replace it with: **Cases removed by each exclusion**

### 5.14 Universe vs sample share

Uses the table(s): Universe. Goes on dashboard **2 Universe and funnel**.

1. Create a new worksheet (the **New Worksheet** icon at the bottom) and rename its tab **Universe vs sample share**.
1. **Columns:** `Universe share (candidates)` and then `Sample share` (side by side on the same shelf, separate axes).
1. **Rows:** `Functional_Area`.
1. **Marks** card: choose **Bar** from the drop-down.
1. Drag `BTCH_ID` onto **Filters**, choose **All** on the General tab (the **Use all** option), OK. On the dashboard this becomes the run or batch drop-down.
1. Double-click the sheet title and replace it with: **Share of candidates vs share of the sample, per area**

### 5.15 Sampling fraction

Uses the table(s): Run figures. Goes on dashboard **2 Universe and funnel**.

1. Create a new worksheet (the **New Worksheet** icon at the bottom) and rename its tab **Sampling fraction**.
1. **Columns:** `AVG(Stat_Sampling_Fraction)` (right-click the pill, Measure, Avg).
1. **Rows:** `Stat_Nm`.
1. **Marks** card: choose **Bar** from the drop-down.
1. Drag `Stat_Run_Key` onto **Filters**, choose **All** on the General tab (the **Use all** option), OK. On the dashboard this becomes the run or batch drop-down.
1. Drag `Stat_Kind` onto **Filters**, tick only **Leaf bucket**, OK.
1. Double-click the sheet title and replace it with: **Sampling fraction per stratum (selected / candidates)**

### 5.16 Exclusion check

Uses the table(s): Universe. Goes on dashboard **2 Universe and funnel**.

1. Create a new worksheet (the **New Worksheet** icon at the bottom) and rename its tab **Exclusion check**.
1. **Columns:** `SUM(Case_Cnt)`.
1. **Rows:** `Exclusion_Check`.
1. **Marks** card: choose **Bar** from the drop-down.
1. Drag `BTCH_ID` onto **Filters**, choose **All** on the General tab (the **Use all** option), OK. On the dashboard this becomes the run or batch drop-down.
1. Double-click the sheet title and replace it with: **Engine exclusions vs the dashboard's own check of the rules**

### 5.17 Issues by type

Uses the table(s): Universe risks and errors. Goes on dashboard **3 Universe risks and errors**.

1. Create a new worksheet (the **New Worksheet** icon at the bottom) and rename its tab **Issues by type**.
1. **Columns:** `SUM(Issue_Cnt)`.
1. **Rows:** `Issue_Txt`.
1. **Marks** card: choose **Bar** from the drop-down.
1. Drag `Severity_Txt` onto **Color** on the Marks card.
1. Drag `DQ_BTCH_ID` onto **Filters**, choose **All** on the General tab (the **Use all** option), OK. On the dashboard this becomes the run or batch drop-down.
1. Sort `Issue_Txt`: click the pill's arrow, **Sort**, Sort By **Field**, **Descending**, field `SUM(Issue_Cnt)`.
1. Double-click the sheet title and replace it with: **Risks and errors found in the universe report**

### 5.18 Issues by week

Uses the table(s): Universe risks and errors. Goes on dashboard **3 Universe risks and errors**.

1. Create a new worksheet (the **New Worksheet** icon at the bottom) and rename its tab **Issues by week**.
1. **Columns:** `WEEK(DQ_Batch_Dt)` continuous (green): in the date menu pick the second **Week Number** group's **Week** (the one showing Week 5, 2015).
1. **Rows:** `SUM(Issue_Cnt)`.
1. **Marks** card: choose **Line** from the drop-down.
1. Drag `Severity_Txt` onto **Color** on the Marks card.
1. Drag `DQ_BTCH_ID` onto **Filters**, choose **All** on the General tab (the **Use all** option), OK. On the dashboard this becomes the run or batch drop-down.
1. Double-click the sheet title and replace it with: **Issues per weekly batch**

### 5.19 Decision timeliness

Uses the table(s): Universe. Goes on dashboard **3 Universe risks and errors**.

1. Create a new worksheet (the **New Worksheet** icon at the bottom) and rename its tab **Decision timeliness**.
1. **Columns:** `SUM(Case_Cnt)`.
1. **Rows:** `Processing`.
1. **Marks** card: choose **Bar** from the drop-down.
1. Drag `Late decision` onto **Color** on the Marks card.
1. Drag `BTCH_ID` onto **Filters**, choose **All** on the General tab (the **Use all** option), OK. On the dashboard this becomes the run or batch drop-down.
1. Double-click the sheet title and replace it with: **Decisions within the timeframe (days set by the parameters)**

### 5.20 Average decision days

Uses the table(s): Universe. Goes on dashboard **3 Universe risks and errors**.

1. Create a new worksheet (the **New Worksheet** icon at the bottom) and rename its tab **Average decision days**.
1. **Columns:** `AVG(Decision_Days)` (right-click the pill, Measure, Avg).
1. **Rows:** `Functional_Area`.
1. **Marks** card: choose **Bar** from the drop-down.
1. Drag `Processing` onto **Color** on the Marks card.
1. Drag `BTCH_ID` onto **Filters**, choose **All** on the General tab (the **Use all** option), OK. On the dashboard this becomes the run or batch drop-down.
1. Double-click the sheet title and replace it with: **Average days from request to decision**

### 5.21 Issue list

Uses the table(s): Universe risks and errors. Goes on dashboard **3 Universe risks and errors**.

1. Create a new worksheet (the **New Worksheet** icon at the bottom) and rename its tab **Issue list**.
1. **Rows:** `DQ_BTCH_ID`, then `DQ_AUTHZN_ID`, then `Issue_Txt`, then `Severity_Txt`, then `Column_Nm`, then `Value_Txt`, then `DQ_Functional_Area`, then `DQ_Sample_Status`.
1. **Marks** card: choose **Text** from the drop-down.
1. Drag `SUM(Issue_Cnt)` onto **Text (Label)** on the Marks card.
1. Drag `DQ_BTCH_ID` onto **Filters**, choose **All** on the General tab (the **Use all** option), OK. On the dashboard this becomes the run or batch drop-down.
1. Double-click the sheet title and replace it with: **Every row with a risk or error**

### 5.22 Delivery

Uses the table(s): Runs. Goes on dashboard **4 Operations and delivery**.

1. Create a new worksheet (the **New Worksheet** icon at the bottom) and rename its tab **Delivery**.
1. **Columns:** `SUM(Run_Cnt)`.
1. **Rows:** `Delivery`.
1. **Marks** card: choose **Bar** from the drop-down.
1. Drag `Run_Version` onto **Filters**, tick only **Current**, OK.
1. Double-click the sheet title and replace it with: **Samples delivered by the Friday deadline**

### 5.23 Run duration

Uses the table(s): Runs. Goes on dashboard **4 Operations and delivery**.

1. Create a new worksheet (the **New Worksheet** icon at the bottom) and rename its tab **Run duration**.
1. **Columns:** `WEEK(As_Of_Dt_Key)` continuous (green): in the date menu pick the second **Week Number** group's **Week** (the one showing Week 5, 2015).
1. **Rows:** `SUM(Duration_Min)`.
1. **Marks** card: choose **Bar** from the drop-down.
1. Drag `Run_Stat` onto **Color** on the Marks card.
1. Double-click the sheet title and replace it with: **Minutes per run, by week**

### 5.24 Batches

Uses the table(s): Batches. Goes on dashboard **4 Operations and delivery**.

1. Create a new worksheet (the **New Worksheet** icon at the bottom) and rename its tab **Batches**.
1. **Columns:** `SUM(Batch_Cnt)`.
1. **Rows:** `Batch_Status`.
1. **Marks** card: choose **Bar** from the drop-down.
1. Drag `Sampled_Status` onto **Color** on the Marks card.
1. Double-click the sheet title and replace it with: **Framework batches: closed and sampled**

### 5.25 Batch list

Uses the table(s): Batches. Goes on dashboard **4 Operations and delivery**.

1. Create a new worksheet (the **New Worksheet** icon at the bottom) and rename its tab **Batch list**.
1. **Rows:** `Batch_Btch_ID`, then `Batch_Status`, then `Sampled_Status`, then `Batch_Run_Stat`.
1. **Marks** card: choose **Text** from the drop-down.
1. Drag `SUM(Batch_Selected_Cnt)` onto **Text (Label)** on the Marks card.
1. Double-click the sheet title and replace it with: **Batches and their samples**

### 5.26 Reruns and overrides

Uses the table(s): Runs. Goes on dashboard **4 Operations and delivery**.

1. Create a new worksheet (the **New Worksheet** icon at the bottom) and rename its tab **Reruns and overrides**.
1. **Rows:** `Run_Key`, then `Run_Kind`, then `Rerun_Rsn_Txt`, then `Override_Rsn_Txt`, then `Triggered_By`, then `Run_Version`.
1. **Marks** card: choose **Text** from the drop-down.
1. Drag `SUM(Selected_Cnt)` onto **Text (Label)** on the Marks card.
1. Drag `Run_Kind` onto **Filters**, tick only **Rerun**, **Override**, OK.
1. Double-click the sheet title and replace it with: **Reruns and overrides with their reasons**

### 5.27 Errors

Uses the table(s): Runs. Goes on dashboard **4 Operations and delivery**.

1. Create a new worksheet (the **New Worksheet** icon at the bottom) and rename its tab **Errors**.
1. **Rows:** `Run_Key`, then `Run_Stat`, then `Error_Ty`, then `Error_Txt`.
1. **Marks** card: choose **Text** from the drop-down.
1. Drag `SUM(Run_Cnt)` onto **Text (Label)** on the Marks card.
1. Drag `Run_Stat` onto **Filters**, tick only **ERROR**, **FAILED**, OK.
1. Double-click the sheet title and replace it with: **Failed runs**

### 5.28 Run day

Uses the table(s): Runs. Goes on dashboard **4 Operations and delivery**.

1. Create a new worksheet (the **New Worksheet** icon at the bottom) and rename its tab **Run day**.
1. **Columns:** `SUM(Run_Cnt)`.
1. **Rows:** `Run_Weekday`.
1. **Marks** card: choose **Bar** from the drop-down.
1. Drag `Run_Kind` onto **Color** on the Marks card.
1. Double-click the sheet title and replace it with: **Runs per weekday (4.04: every Friday)**

### 5.29 Universe volume

Uses the table(s): Runs. Goes on dashboard **5 Trends and aberrations**.

1. Create a new worksheet (the **New Worksheet** icon at the bottom) and rename its tab **Universe volume**.
1. **Columns:** `MONTH(As_Of_Dt_Key)` continuous (green): in the date menu pick the second **Month** (the one showing May 2015).
1. **Rows:** `SUM(In_Scope_Cnt)`.
1. **Marks** card: choose **Line** from the drop-down.
1. Drag `Run_Version` onto **Filters**, tick only **Current**, OK.
1. Double-click the sheet title and replace it with: **Universe rows in scope per month**

### 5.30 Volume change

Uses the table(s): Runs. Goes on dashboard **5 Trends and aberrations**.

1. Create a new worksheet (the **New Worksheet** icon at the bottom) and rename its tab **Volume change**.
1. **Columns:** `MONTH(As_Of_Dt_Key)` continuous (green): in the date menu pick the second **Month** (the one showing May 2015).
1. **Rows:** `In-scope change vs prior month`.
1. **Marks** card: choose **Bar** from the drop-down.
1. Drag `Aberration (Runs)` onto **Color** on the Marks card.
1. Drag `Run_Version` onto **Filters**, tick only **Current**, OK.
1. Double-click the sheet title and replace it with: **Change against the prior month (alert over the threshold)**

### 5.31 Denial rate

Uses the table(s): Universe. Goes on dashboard **5 Trends and aberrations**.

1. Create a new worksheet (the **New Worksheet** icon at the bottom) and rename its tab **Denial rate**.
1. **Columns:** `MONTH(Batch_Dt)` continuous (green): in the date menu pick the second **Month** (the one showing May 2015).
1. **Rows:** `Denial rate`.
1. **Marks** card: choose **Line** from the drop-down.
1. Drag `BTCH_ID` onto **Filters**, choose **All** on the General tab (the **Use all** option), OK. On the dashboard this becomes the run or batch drop-down.
1. Double-click the sheet title and replace it with: **Denial rate of the universe per month**

### 5.32 Late decisions trend

Uses the table(s): Universe. Goes on dashboard **5 Trends and aberrations**.

1. Create a new worksheet (the **New Worksheet** icon at the bottom) and rename its tab **Late decisions trend**.
1. **Columns:** `QUARTER(Batch_Dt)` continuous (green): in the date menu pick the second **Quarter** (the one showing Q2 2015).
1. **Rows:** `Late decision rate`.
1. **Marks** card: choose **Line** from the drop-down.
1. Drag `Processing` onto **Color** on the Marks card.
1. Drag `BTCH_ID` onto **Filters**, choose **All** on the General tab (the **Use all** option), OK. On the dashboard this becomes the run or batch drop-down.
1. Double-click the sheet title and replace it with: **Late decision rate per quarter**

### 5.33 Sample mix trend

Uses the table(s): Sample. Goes on dashboard **5 Trends and aberrations**.

1. Create a new worksheet (the **New Worksheet** icon at the bottom) and rename its tab **Sample mix trend**.
1. **Columns:** `MONTH(SMPL_CYCLE_DT)` continuous (green): in the date menu pick the second **Month** (the one showing May 2015).
1. **Rows:** `SUM(Smp_Case_Cnt)`.
1. **Marks** card: choose **Area** from the drop-down.
1. Drag `Smp_Functional_Area` onto **Color** on the Marks card.
1. Drag `SMPL_RUN_KEY` onto **Filters**, choose **All** on the General tab (the **Use all** option), OK. On the dashboard this becomes the run or batch drop-down.
1. Double-click the sheet title and replace it with: **Sampled cases per month by area**

### 5.34 Risk trend

Uses the table(s): Universe risks and errors. Goes on dashboard **5 Trends and aberrations**.

1. Create a new worksheet (the **New Worksheet** icon at the bottom) and rename its tab **Risk trend**.
1. **Columns:** `YEAR(DQ_Batch_Dt)` discrete (blue): in the date menu pick the first **Year**, then `QUARTER(DQ_Batch_Dt)` discrete (blue): in the date menu pick the first **Quarter**.
1. **Rows:** `SUM(Issue_Cnt)`.
1. **Marks** card: choose **Bar** from the drop-down.
1. Drag `Severity_Txt` onto **Color** on the Marks card.
1. Drag `DQ_BTCH_ID` onto **Filters**, choose **All** on the General tab (the **Use all** option), OK. On the dashboard this becomes the run or batch drop-down.
1. Double-click the sheet title and replace it with: **Universe risks and errors by year and quarter**

### 5.35 Shortfall trend

Uses the table(s): Runs. Goes on dashboard **5 Trends and aberrations**.

1. Create a new worksheet (the **New Worksheet** icon at the bottom) and rename its tab **Shortfall trend**.
1. **Columns:** `MONTH(As_Of_Dt_Key)` continuous (green): in the date menu pick the second **Month** (the one showing May 2015).
1. **Rows:** `SUM(Shortfall_Cnt)`.
1. **Marks** card: choose **Bar** from the drop-down.
1. Drag `Run_Version` onto **Filters**, tick only **Current**, OK.
1. Double-click the sheet title and replace it with: **Cases short of the target per month**

### 5.36 Top CPT codes

Uses the table(s): Sample. Goes on dashboard **5 Trends and aberrations**.

1. Create a new worksheet (the **New Worksheet** icon at the bottom) and rename its tab **Top CPT codes**.
1. **Columns:** `SUM(Smp_Case_Cnt)`.
1. **Rows:** `Smp_CPT_First`.
1. **Marks** card: choose **Bar** from the drop-down.
1. Drag `SMPL_RUN_KEY` onto **Filters**, choose **All** on the General tab (the **Use all** option), OK. On the dashboard this becomes the run or batch drop-down.
1. Sort `Smp_CPT_First`: click the pill's arrow, **Sort**, Sort By **Field**, **Descending**, field `SUM(Smp_Case_Cnt)`.
1. Double-click the sheet title and replace it with: **Procedure codes most often in the sample**

### 5.37 OON denials trend

Uses the table(s): Sample. Goes on dashboard **5 Trends and aberrations**.

1. Create a new worksheet (the **New Worksheet** icon at the bottom) and rename its tab **OON denials trend**.
1. **Columns:** `MONTH(SMPL_CYCLE_DT)` continuous (green): in the date menu pick the second **Month** (the one showing May 2015).
1. **Rows:** `Out-of-network denials`.
1. **Marks** card: choose **Line** from the drop-down.
1. Drag `SMPL_RUN_KEY` onto **Filters**, choose **All** on the General tab (the **Use all** option), OK. On the dashboard this becomes the run or batch drop-down.
1. Double-click the sheet title and replace it with: **High-risk cases: out-of-network denials sampled per month**

### 5.38 Revalidation status

Uses the table(s): Revalidation. Goes on dashboard **6 Internal controls**.

1. Create a new worksheet (the **New Worksheet** icon at the bottom) and rename its tab **Revalidation status**.
1. **Columns:** `SUM(Reval_Cnt)`.
1. **Rows:** `Reval_Stat`.
1. **Marks** card: choose **Bar** from the drop-down.
1. Drag `Signed_Off` onto **Color** on the Marks card.
1. Double-click the sheet title and replace it with: **Revalidation results (8.01) and sign-off**

### 5.39 Revalidation list

Uses the table(s): Revalidation. Goes on dashboard **6 Internal controls**.

1. Create a new worksheet (the **New Worksheet** icon at the bottom) and rename its tab **Revalidation list**.
1. **Rows:** `Reval_Run_Key`, then `Reval_Stat`, then `Signed_Off`, then `Signoff_By`, then `Diff_Txt`.
1. **Marks** card: choose **Text** from the drop-down.
1. Drag `SUM(Same_Cnt)` onto **Text (Label)** on the Marks card.
1. Double-click the sheet title and replace it with: **Each replay: cases that matched**

### 5.40 Refinement findings

Uses the table(s): Refinement. Goes on dashboard **6 Internal controls**.

1. Create a new worksheet (the **New Worksheet** icon at the bottom) and rename its tab **Refinement findings**.
1. **Columns:** `SUM(Finding_Cnt)`.
1. **Rows:** `Finding_Ty`.
1. **Marks** card: choose **Bar** from the drop-down.
1. Drag `Review_Status` onto **Color** on the Marks card.
1. Drag `Row_Kind` onto **Filters**, tick only **Finding**, OK.
1. Double-click the sheet title and replace it with: **Refinement findings (8.03): open and reviewed**

### 5.41 Open findings

Uses the table(s): Refinement. Goes on dashboard **6 Internal controls**.

1. Create a new worksheet (the **New Worksheet** icon at the bottom) and rename its tab **Open findings**.
1. **Rows:** `Refine_ID`, then `Finding_Ty`, then `Finding_Nm`, then `Finding_Txt`, then `Review_Status`, then `Review_Txt`.
1. **Marks** card: choose **Text** from the drop-down.
1. Drag `SUM(Finding_Cnt)` onto **Text (Label)** on the Marks card.
1. Drag `Row_Kind` onto **Filters**, tick only **Finding**, OK.
1. Double-click the sheet title and replace it with: **Findings and the decisions taken (8.02)**

### 5.42 Not picked reasons

Uses the table(s): Refinement. Goes on dashboard **6 Internal controls**.

1. Create a new worksheet (the **New Worksheet** icon at the bottom) and rename its tab **Not picked reasons**.
1. **Columns:** `SUM(Refine_Case_Cnt)`.
1. **Rows:** `Finding_Ty`, then `Finding_Nm`.
1. **Marks** card: choose **Bar** from the drop-down.
1. Drag `Row_Kind` onto **Filters**, tick only **Total**, OK.
1. Double-click the sheet title and replace it with: **Why cases were left out, topped up or replaced**

### 5.43 Replacements

Uses the table(s): Sample. Goes on dashboard **6 Internal controls**.

1. Create a new worksheet (the **New Worksheet** icon at the bottom) and rename its tab **Replacements**.
1. **Columns:** `SUM(Smp_Case_Cnt)`.
1. **Rows:** `Stat_Rsn_Txt`.
1. **Marks** card: choose **Bar** from the drop-down.
1. Drag `SMPL_RUN_KEY` onto **Filters**, choose **All** on the General tab (the **Use all** option), OK. On the dashboard this becomes the run or batch drop-down.
1. Drag `Selection_Stat` onto **Filters**, tick only **REPLACEMENT**, OK.
1. Double-click the sheet title and replace it with: **Cases replaced by reviewers, by reason**

### 5.44 Finalized

Uses the table(s): Runs. Goes on dashboard **6 Internal controls**.

1. Create a new worksheet (the **New Worksheet** icon at the bottom) and rename its tab **Finalized**.
1. **Columns:** `SUM(Run_Cnt)`.
1. **Rows:** `Run_Stat`.
1. **Marks** card: choose **Bar** from the drop-down.
1. Drag `Run_Version` onto **Filters**, tick only **Current**, OK.
1. Double-click the sheet title and replace it with: **Samples finalized by a reviewer (human checkpoint)**

### 5.45 Rules

Uses the table(s): Rules. Goes on dashboard **7 Configuration**.

1. Create a new worksheet (the **New Worksheet** icon at the bottom) and rename its tab **Rules**.
1. **Rows:** `Rule_Class_Cd`, then `Rule_ID` as a **discrete dimension** (right-click the pill, choose Dimension, then Discrete), then `Rule_Nm`, then `Parent_Path_Txt`, then `Quota_Cd`, then `Quota_Val` as a **discrete dimension** (right-click the pill, choose Dimension, then Discrete), then `Max_Val` as a **discrete dimension** (right-click the pill, choose Dimension, then Discrete), then `In_Effect`.
1. **Marks** card: choose **Text** from the drop-down.
1. Drag `SUM(Rule_Cnt)` onto **Text (Label)** on the Marks card.
1. Double-click the sheet title and replace it with: **The sampling rules (10.02: shares and counts are configurable)**

### 5.46 Quota vs actual

Uses the table(s): Run figures. Goes on dashboard **7 Configuration**.

1. Create a new worksheet (the **New Worksheet** icon at the bottom) and rename its tab **Quota vs actual**.
1. **Columns:** `SUM(Stat_Target_Cnt)` and then `SUM(Stat_Selected_Cnt)` (side by side on the same shelf, separate axes).
1. **Rows:** `Stat_Nm`.
1. **Marks** card: choose **Bar** from the drop-down.
1. Drag `Stat_Run_Key` onto **Filters**, choose **All** on the General tab (the **Use all** option), OK. On the dashboard this becomes the run or batch drop-down.
1. Drag `Stat_Kind` onto **Filters**, tick only **Leaf bucket**, OK.
1. Double-click the sheet title and replace it with: **Each stratum's target and what it got**

### 5.47 Caps

Uses the table(s): Run figures. Goes on dashboard **7 Configuration**.

1. Create a new worksheet (the **New Worksheet** icon at the bottom) and rename its tab **Caps**.
1. **Columns:** `SUM(Over_Cnt)` and then `SUM(Replaced_Cnt)` (side by side on the same shelf, separate axes).
1. **Rows:** `Stat_Nm`.
1. **Marks** card: choose **Bar** from the drop-down.
1. Drag `Stat_Run_Key` onto **Filters**, choose **All** on the General tab (the **Use all** option), OK. On the dashboard this becomes the run or batch drop-down.
1. Drag `Stat_Ty` onto **Filters**, tick only **CAP**, OK.
1. Double-click the sheet title and replace it with: **Picks each cap stopped, and how many were made up**

### 5.48 Config changes

Uses the table(s): Runs. Goes on dashboard **7 Configuration**.

1. Create a new worksheet (the **New Worksheet** icon at the bottom) and rename its tab **Config changes**.
1. **Columns:** `MONTH(As_Of_Dt_Key)` continuous (green): in the date menu pick the second **Month** (the one showing May 2015).
1. **Rows:** `SUM(Config_Changed_Ind)`.
1. **Marks** card: choose **Bar** from the drop-down.
1. Double-click the sheet title and replace it with: **Runs whose configuration changed from the run before**

### 5.49 Rows kept

Uses the table(s): Retention. Goes on dashboard **8 Retention**.

1. Create a new worksheet (the **New Worksheet** icon at the bottom) and rename its tab **Rows kept**.
1. **Columns:** `SUM(Row_Cnt)`.
1. **Rows:** `Object_Nm`.
1. **Marks** card: choose **Bar** from the drop-down.
1. Drag `Data_Year` as a **discrete dimension** (right-click the pill, choose Dimension, then Discrete) onto **Color** on the Marks card.
1. Double-click the sheet title and replace it with: **Rows kept per object (6.01, 6.02: 10 years)**

### 5.50 Rows by year

Uses the table(s): Retention. Goes on dashboard **8 Retention**.

1. Create a new worksheet (the **New Worksheet** icon at the bottom) and rename its tab **Rows by year**.
1. **Columns:** `Data_Year` as a **discrete dimension** (right-click the pill, choose Dimension, then Discrete).
1. **Rows:** `Object_Nm`.
1. **Marks** card: choose **Text** from the drop-down.
1. Drag `SUM(Row_Cnt)` onto **Text (Label)** on the Marks card.
1. Double-click the sheet title and replace it with: **Rows kept per object and year**

## Part 6: The dashboards

Every dashboard is **1600 x 1000** pixels. For each one:

1. Click the **New Dashboard** icon at the bottom and rename the tab.
2. In the **Dashboard** pane (left), under **Size**, choose **Fixed size**, width **1600**, height **1000**.
3. At the bottom of the Dashboard pane, click **Floating** so the items you drop can be placed exactly.
4. Drag each item listed below onto the dashboard. Then, with the item selected, open the **Layout** tab (next to Dashboard) and type its **Position** (x, y) and **Size** (w, h).
5. Worksheets: drag the sheet name from the *Sheets* list. Text: drag **Text** from *Objects*. Drop-downs and sliders: see the steps under each dashboard.

### 0 About

| Item | What to drag | x | y | w | h |
|---|---|---|---|---|---|
| 1 | Text object (text below) | 0 | 0 | 1600 | 1000 |

Text object content (type it in the text editor that opens; one paragraph per line):

- ODAG1 sampling dashboard *(size 18, bold)*
- Data: one Tableau data source, ODAG1 Sampling, with 10 Custom SQL tables (custom_sql folder) and the ODAG1 data table, over the sampling tables, the ODAG1 universe, the sample table, SHRPA and the framework batches. *(size 11)*
- Requirement coverage *(size 13, bold)*
- 4.01 sample criteria: 1 Weekly sample, 2 Universe and funnel *(size 11)*
- 4.03 output with functional area, clinical auditor flag, priority order; risks and errors in the universe: 1, 3 *(size 11)*
- 4.04 every Friday, 10.01 frequency: 4 Operations and delivery *(size 11)*
- 6.01 / 6.02 10-year retention: 8 Retention *(size 11)*
- 7.01 to 7.04 trends, aberrations over x %, high-risk frequency: 5 Trends and aberrations *(size 11)*
- 8.01 revalidation, 8.02 human checkpoints, 8.03 refinement: 6 Internal controls *(size 11)*
- 9 / 10.02 configuration: 7 Configuration *(size 11)*
- Not shown, no data yet: audit results (error rates found by auditors), contract status, concurrent review. *(size 11, italic)*

### 1 Weekly sample

| Item | What to drag | x | y | w | h |
|---|---|---|---|---|---|
| 1 | Text object (text below) | 0 | 0 | 1600 | 70 |
| 2 | Sheet **KPI Selected** | 256 | 70 | 269 | 232 |
| 3 | Sheet **KPI Vs Target** | 525 | 70 | 269 | 232 |
| 4 | Sheet **KPI Clinical** | 794 | 70 | 269 | 232 |
| 5 | Sheet **KPI Expedited** | 1062 | 70 | 269 | 232 |
| 6 | Sheet **KPI OON** | 1331 | 70 | 269 | 232 |
| 7 | Sheet **Sample by area** | 256 | 302 | 672 | 232 |
| 8 | Sheet **Target vs selected** | 928 | 302 | 672 | 232 |
| 9 | Sheet **Determination mix** | 256 | 535 | 672 | 232 |
| 10 | Sheet **OON by area** | 928 | 535 | 672 | 232 |
| 11 | Sheet **Case list** | 256 | 768 | 1344 | 232 |
| 12 | Drop-down filter on `SMPL_RUN_KEY` | 0 | 70 | 256 | 90 |
| 13 | Drop-down filter on `Stat_Run_Key` | 0 | 160 | 256 | 90 |

Text object content (type it in the text editor that opens; one paragraph per line):

- 1 Weekly sample *(size 18, bold)*
- Requirements 4.01 and 4.03: the cases COMO audits this week *(size 10)*

Drop-down `SMPL_RUN_KEY`: select the sheet **KPI Selected** on the dashboard, click its arrow (**More options**), **Filters**, choose the field. Then on the new filter card click its arrow: **Single Value (dropdown)**, and **Apply to Worksheets > Selected Worksheets**, tick: **KPI Selected**, **KPI Vs Target**, **KPI Clinical**, **KPI Expedited**, **KPI OON**, **Sample by area**, **Determination mix**, **OON by area**, **Case list**. Place the card at the position in the table.

Drop-down `Stat_Run_Key`: select the sheet **Target vs selected** on the dashboard, click its arrow (**More options**), **Filters**, choose the field. Then on the new filter card click its arrow: **Single Value (dropdown)**, and **Apply to Worksheets > Selected Worksheets**, tick: **Target vs selected**. Place the card at the position in the table.

### 2 Universe and funnel

| Item | What to drag | x | y | w | h |
|---|---|---|---|---|---|
| 1 | Text object (text below) | 0 | 0 | 1600 | 70 |
| 2 | Sheet **Universe heat map** | 256 | 70 | 672 | 310 |
| 3 | Sheet **What the sample did** | 928 | 70 | 672 | 310 |
| 4 | Sheet **Exclusions** | 256 | 380 | 672 | 310 |
| 5 | Sheet **Exclusion check** | 928 | 380 | 672 | 310 |
| 6 | Sheet **Universe vs sample share** | 256 | 690 | 672 | 310 |
| 7 | Sheet **Sampling fraction** | 928 | 690 | 672 | 310 |
| 8 | Drop-down filter on `BTCH_ID` | 0 | 70 | 256 | 90 |
| 9 | Drop-down filter on `Stat_Run_Key` | 0 | 160 | 256 | 90 |

Text object content (type it in the text editor that opens; one paragraph per line):

- 2 Universe and funnel *(size 18, bold)*
- Requirement 4.01: what the universe held and how the rules narrowed it *(size 10)*

Drop-down `BTCH_ID`: select the sheet **Universe heat map** on the dashboard, click its arrow (**More options**), **Filters**, choose the field. Then on the new filter card click its arrow: **Single Value (dropdown)**, and **Apply to Worksheets > Selected Worksheets**, tick: **Universe heat map**, **What the sample did**, **Exclusion check**, **Universe vs sample share**. Place the card at the position in the table.

Drop-down `Stat_Run_Key`: select the sheet **Exclusions** on the dashboard, click its arrow (**More options**), **Filters**, choose the field. Then on the new filter card click its arrow: **Single Value (dropdown)**, and **Apply to Worksheets > Selected Worksheets**, tick: **Exclusions**, **Sampling fraction**. Place the card at the position in the table.

### 3 Universe risks and errors

| Item | What to drag | x | y | w | h |
|---|---|---|---|---|---|
| 1 | Text object (text below) | 0 | 0 | 1600 | 70 |
| 2 | Sheet **Issues by type** | 256 | 70 | 672 | 310 |
| 3 | Sheet **Issues by week** | 928 | 70 | 672 | 310 |
| 4 | Sheet **Decision timeliness** | 256 | 380 | 672 | 310 |
| 5 | Sheet **Average decision days** | 928 | 380 | 672 | 310 |
| 6 | Sheet **Issue list** | 256 | 690 | 1344 | 310 |
| 7 | Drop-down filter on `DQ_BTCH_ID` | 0 | 70 | 256 | 90 |
| 8 | Drop-down filter on `BTCH_ID` | 0 | 160 | 256 | 90 |
| 9 | Slider for parameter **Standard decision days** | 0 | 250 | 256 | 90 |
| 10 | Slider for parameter **Expedited decision days** | 0 | 340 | 256 | 90 |

Text object content (type it in the text editor that opens; one paragraph per line):

- 3 Universe risks and errors *(size 18, bold)*
- Requirement 4.03: risks and errors identified in the universe report itself *(size 10)*

Drop-down `DQ_BTCH_ID`: select the sheet **Issues by type** on the dashboard, click its arrow (**More options**), **Filters**, choose the field. Then on the new filter card click its arrow: **Single Value (dropdown)**, and **Apply to Worksheets > Selected Worksheets**, tick: **Issues by type**, **Issues by week**, **Issue list**. Place the card at the position in the table.

Drop-down `BTCH_ID`: select the sheet **Decision timeliness** on the dashboard, click its arrow (**More options**), **Filters**, choose the field. Then on the new filter card click its arrow: **Single Value (dropdown)**, and **Apply to Worksheets > Selected Worksheets**, tick: **Decision timeliness**, **Average decision days**. Place the card at the position in the table.

Slider **Standard decision days**: Dashboard pane, select any sheet, its arrow, **Parameters**, **Standard decision days**. On the card's arrow choose **Slider**. Place it at the position in the table.

Slider **Expedited decision days**: Dashboard pane, select any sheet, its arrow, **Parameters**, **Expedited decision days**. On the card's arrow choose **Slider**. Place it at the position in the table.

### 4 Operations and delivery

| Item | What to drag | x | y | w | h |
|---|---|---|---|---|---|
| 1 | Text object (text below) | 0 | 0 | 1600 | 70 |
| 2 | Sheet **Delivery** | 256 | 70 | 448 | 310 |
| 3 | Sheet **Run day** | 704 | 70 | 448 | 310 |
| 4 | Sheet **Batches** | 1152 | 70 | 448 | 310 |
| 5 | Sheet **Run duration** | 256 | 380 | 672 | 310 |
| 6 | Sheet **Batch list** | 928 | 380 | 672 | 310 |
| 7 | Sheet **Reruns and overrides** | 256 | 690 | 672 | 310 |
| 8 | Sheet **Errors** | 928 | 690 | 672 | 310 |
| 9 | Slider for parameter **Delivery deadline hour (Friday, CT)** | 0 | 70 | 256 | 90 |

Text object content (type it in the text editor that opens; one paragraph per line):

- 4 Operations and delivery *(size 18, bold)*
- Requirements 4.03, 4.04 and 10.01: every Friday by 3 pm CT, batches, reruns *(size 10)*

Slider **Delivery deadline hour (Friday, CT)**: Dashboard pane, select any sheet, its arrow, **Parameters**, **Delivery deadline hour (Friday, CT)**. On the card's arrow choose **Slider**. Place it at the position in the table.

### 5 Trends and aberrations

| Item | What to drag | x | y | w | h |
|---|---|---|---|---|---|
| 1 | Text object (text below) | 0 | 0 | 1600 | 70 |
| 2 | Sheet **Universe volume** | 256 | 70 | 672 | 232 |
| 3 | Sheet **Volume change** | 928 | 70 | 672 | 232 |
| 4 | Sheet **Denial rate** | 256 | 302 | 672 | 232 |
| 5 | Sheet **Late decisions trend** | 928 | 302 | 672 | 232 |
| 6 | Sheet **Sample mix trend** | 256 | 535 | 672 | 232 |
| 7 | Sheet **Risk trend** | 928 | 535 | 672 | 232 |
| 8 | Sheet **Shortfall trend** | 256 | 768 | 448 | 232 |
| 9 | Sheet **Top CPT codes** | 704 | 768 | 448 | 232 |
| 10 | Sheet **OON denials trend** | 1152 | 768 | 448 | 232 |
| 11 | Slider for parameter **Alert threshold (% change)** | 0 | 70 | 256 | 90 |

Text object content (type it in the text editor that opens; one paragraph per line):

- 5 Trends and aberrations *(size 18, bold)*
- Requirements 7.01 to 7.04: monthly, quarterly and yearly trends and alerts *(size 10)*

Slider **Alert threshold (% change)**: Dashboard pane, select any sheet, its arrow, **Parameters**, **Alert threshold (% change)**. On the card's arrow choose **Slider**. Place it at the position in the table.

### 6 Internal controls

| Item | What to drag | x | y | w | h |
|---|---|---|---|---|---|
| 1 | Text object (text below) | 0 | 0 | 1600 | 70 |
| 2 | Sheet **Revalidation status** | 0 | 70 | 533 | 310 |
| 3 | Sheet **Finalized** | 533 | 70 | 533 | 310 |
| 4 | Sheet **Refinement findings** | 1067 | 70 | 533 | 310 |
| 5 | Sheet **Revalidation list** | 0 | 380 | 800 | 310 |
| 6 | Sheet **Open findings** | 800 | 380 | 800 | 310 |
| 7 | Sheet **Not picked reasons** | 0 | 690 | 800 | 310 |
| 8 | Sheet **Replacements** | 800 | 690 | 800 | 310 |

Text object content (type it in the text editor that opens; one paragraph per line):

- 6 Internal controls *(size 18, bold)*
- Requirements 8.01 to 8.03: revalidation, human checkpoints, refinement *(size 10)*

### 7 Configuration

| Item | What to drag | x | y | w | h |
|---|---|---|---|---|---|
| 1 | Text object (text below) | 0 | 0 | 1600 | 70 |
| 2 | Sheet **Rules** | 256 | 70 | 1344 | 310 |
| 3 | Sheet **Quota vs actual** | 256 | 380 | 672 | 310 |
| 4 | Sheet **Caps** | 928 | 380 | 672 | 310 |
| 5 | Sheet **Config changes** | 256 | 690 | 1344 | 310 |
| 6 | Slider for parameter **Weekly target** | 0 | 70 | 256 | 90 |

Text object content (type it in the text editor that opens; one paragraph per line):

- 7 Configuration *(size 18, bold)*
- Requirements 9 and 10: what is configured and how it performs *(size 10)*

Slider **Weekly target**: Dashboard pane, select any sheet, its arrow, **Parameters**, **Weekly target**. On the card's arrow choose **Slider**. Place it at the position in the table.

### 8 Retention

| Item | What to drag | x | y | w | h |
|---|---|---|---|---|---|
| 1 | Text object (text below) | 0 | 0 | 1600 | 70 |
| 2 | Sheet **Rows kept** | 0 | 70 | 1600 | 465 |
| 3 | Sheet **Rows by year** | 0 | 535 | 1600 | 465 |

Text object content (type it in the text editor that opens; one paragraph per line):

- 8 Retention *(size 18, bold)*
- Requirements 6.01 and 6.02: inputs and outputs kept for 10 years *(size 10)*

## Part 7: Final checks

1. Open every dashboard. Every sheet shows data or an empty chart, never a red error.
2. Change the run drop-down on **1 Weekly sample**: the KPIs, charts and case list change together.
3. Change the batch drop-down on **2 Universe and funnel** and **3 Universe risks and errors**.
4. Move the sliders on **4 Operations and delivery** and **5 Trends and aberrations**: the on-time and aberration colours react.
5. **File > Save**. Close Tableau and open the file again to make sure it opens without errors.
6. If you want the workbook back in git, remove the server name first (PowerShell):

   ```
   $f = "C:\Script\frameworks\GOV_RGLTRY\Sampling\airflow_resources\docs\tableau\ODAG1_Sampling_Dashboard_v2.twb"; (Get-Content $f -Raw) -replace "server='[^']*'", "server='TERADATA_SERVER'" | Set-Content $f -Encoding utf8
   ```
