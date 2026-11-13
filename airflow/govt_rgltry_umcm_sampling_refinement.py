import os
import sys

_DAG_DIR = os.path.dirname(os.path.abspath(__file__))
if _DAG_DIR not in sys.path:
    sys.path.insert(0, _DAG_DIR)

from sampling.dags import build_refinement_dag

dag_id = "GOVT_RGLTRY_UMCM_SAMPLING_REFINEMENT"
variable_key = "govt_rgltry_umcm_sampling_config_var"

dag = build_refinement_dag(dag_id, variable_key)
globals()[dag_id] = dag
