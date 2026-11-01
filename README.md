# Stratified sampling on Teradata – Airflow and local

A configuration-driven sampling engine, rewritten from `DQ_COMPLETE_REPO/sampling` for **Teradata**, in two
versions sharing the same engine code:

- **`airflow/`** – the DAG `CMS_COMPLIANCE_UMCM_SAMPLING` (Airflow 3.3), in the style of the compliance
  framework's Airflow package: one DAG file, one `<name>_config_var` Variable, the Teradata login from an Airflow
  Connection read as the GRE framework's DAG reads it (`BaseHook.get_connection`, the Variable's `connection_id`).
- **`local/`** – a command line (`python run_sampling.py …`) reading the login from `dev.env`.

Like the GRE rules engine, what a sample selects is **rows holding SQL**: exclusions, priorities, levels and their
buckets (fractions, all there is, the remainder, per parent), run parameters looked up by SQL, history rules
(no repeats, carry-over), code variety, and an output snapshot. No field name or value is in the code.
Every exclusion, priority and bucket rule is a whole `SELECT` (any joins) returning the keys of the cases it
matches, as a GRE rule. For tables holding several loads, batches or SCD2 versions, a run can be given
`extra_filters`: a SQL condition ANDed to the scope, as with the GRE.

Every run keeps the rules it used (with a hash). A sample can be finalised (a rerun then needs a recorded reason),
and a case the auditors cannot review can be replaced by the next candidate of its group, with the reason kept.
Configured SQL is checked to only read; everything is driven by the metadata tables (no configuration files);
old candidate rows are purged on a retention period that keeps every sample.

```
airflow/                 DAG file + sampling/ package: the engine (engine.py, db.py, settings.py) and dags.py
local/                   run_sampling.py, dev.env.example (imports the engine from airflow/sampling)
sql/                     schema.sql (4 tables), schema_drop.sql, output_table_template.sql (DDL only, as GRE's)
airflow_resources/       docs, the example Variable, worker requirements
tests/                   pytest on a DuckDB stand-in for teradatasql; DAG tests need apache-airflow 3
```

| Read | For |
|---|---|
| [OPERATIONAL_RUNBOOK.md](airflow_resources/docs/OPERATIONAL_RUNBOOK.md) | Every table and code, recipes, support procedures |
| [AIRFLOW_DEPLOYMENT.md](airflow_resources/docs/AIRFLOW_DEPLOYMENT.md) | Deploying the DAG: Connection, Variable, triggers |
| [OPEN_ITEMS.md](airflow_resources/docs/OPEN_ITEMS.md) | Questions open for the ODAG1 configuration, checks before production |

Tests:

```bash
pip install -r local/requirements.txt && python -m pytest tests -q
```
