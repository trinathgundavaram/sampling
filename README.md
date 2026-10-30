# Stratified sampling on Teradata – Airflow and local

A configuration-driven sampling engine, rewritten from `DQ_COMPLETE_REPO/sampling` for **Teradata**, in two
versions sharing the same engine code:

- **`airflow/`** – the DAG `CMS_COMPLIANCE_UMCM_SAMPLING` (Airflow 3.3), in the style of the compliance
  framework's Airflow package: one DAG file, one `<name>_config_var` Variable, the Teradata login from the
  Connection named by `td_conn_var`.
- **`local/`** – a command line (`python run_sampling.py …`) reading the login from `dev.env`.

Like the GRE rules engine, what a sample selects is **rows holding SQL**: exclusions, priorities, levels and their
buckets (fractions, all there is, the remainder, per parent), run parameters looked up by SQL, history rules
(no repeats, carry-over), code variety, and an output snapshot. No field name or value is in the code.

```
airflow/                 DAG file + sampling/ package (engine.py, db.py, settings.py shared with local/)
local/                   run_sampling.py, dev.env.example, sampling/ (same engine)
sql/                     schema.sql (12 tables), schema_drop.sql, output_table_template.sql, seed/
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
