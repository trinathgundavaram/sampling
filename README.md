# Stratified sampling on Teradata – Airflow and local

A configuration-driven sampling engine, rewritten from `DQ_COMPLETE_REPO/sampling` for **Teradata**, in two
versions sharing the same engine code:

- **`airflow/`** – the DAG `GOVT_RGLTRY_UMCM_SAMPLING` (Airflow 3.3), in the style of the GOVT_RGLTRY
  framework's Airflow package: one DAG file, one `<name>_config_var` Variable, the Teradata login from an Airflow
  Connection read with `BaseHook.get_connection` (the Variable's `connection_id`).
- **`local/`** – a command line (`python run_sampling.py …`) reading the login from `dev.env`.

What a sample selects is **rows holding SQL**: exclusions, priorities, levels and their
buckets (a percentage, a number of cases, all there is, the remainder, per parent), limits across the whole sample
(at most 3 of one code, at most 10 out of network), run parameters looked up by SQL, history rules
(no repeats, carry-over) and code variety; the selected cases' source rows go to a target table per sample. No field name or value is in the code.
A sample is one **project + table + run type** (DLY, WKLY, MNT …), as the framework scopes its batches; a rule row
may name one table and run type or `*` for all of them (the most specific row wins), so run types share rules or
have their own.
The sample size, bucket quotas and caps are each a number of cases or a percentage (`CNT` / `PCT`): 150 cases, or 5 %
of the candidates within a minimum and a maximum.
Every SQL column is a whole `SELECT` (any joins): the scope returns the run's source rows, every exclusion, priority
and bucket rule the keys of the cases it matches, the diversity rule each case's value. For tables holding several
loads, batches or SCD2 versions, a run can be given `extra_filters`: a SQL condition on the scope's rows.

Every run keeps the configuration and the rules it used (with a hash), in plain columns (no CLOB). A sample can be finalised (a rerun then needs a recorded reason),
and a case the auditors cannot review can be replaced by the next candidate of its group, with the reason kept.
Configured SQL is checked to only read; everything is driven by the metadata tables (no configuration files);
old candidate rows are purged on a retention period that keeps every sample.

```
airflow/                 DAG file + sampling/ package: the engine (engine.py, db.py, settings.py) and dags.py
local/                   run_sampling.py, dev.env.example (imports the engine from airflow/sampling)
sql/                     schema.sql (DDL of the 5 GOVT_RGLTRY_Sampl* tables, run by hand)
airflow_resources/       docs, the example Variable, worker requirements
tests/                   pytest on a DuckDB stand-in for teradatasql; DAG tests need apache-airflow 3
```

| Read | For |
|---|---|
| [OPERATIONAL_RUNBOOK.md](airflow_resources/docs/OPERATIONAL_RUNBOOK.md) | Every table and code, recipes, support procedures |
| [AIRFLOW_DEPLOYMENT.md](airflow_resources/docs/AIRFLOW_DEPLOYMENT.md) | Deploying the DAG: Connection, Variable, triggers |
| [OPEN_ITEMS.md](airflow_resources/docs/OPEN_ITEMS.md) | Questions open for the ODAG1 configuration, checks before production |
| [diagrams/](airflow_resources/docs/diagrams/pages/) | draw.io architecture and flow diagrams, PNGs in `diagrams/png/` (runbook section 13) |

Tests:

```bash
pip install -r local/requirements.txt && python -m pytest tests -q
```
