"""UMCM stratified sampling. See airflow_resources/docs/AIRFLOW_DEPLOYMENT.md."""
import os
import sys

_DAG_DIR = os.path.dirname(os.path.abspath(__file__))
if _DAG_DIR not in sys.path:
    sys.path.insert(0, _DAG_DIR)

from sampling.dags import build_sampling_dag  # noqa: E402

dag_id = "CMS_COMPLIANCE_UMCM_SAMPLING"
variable_key = "cms_compliance_umcm_sampling_config_var"

dag = build_sampling_dag(dag_id, variable_key)
globals()[dag_id] = dag
