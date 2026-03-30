# =============================================================================
# DAG: chilean_bank.py
# PROYECTO: Chilean Bank — Data Governance GCP
# DESCRIPCIÓN:
#   Orquesta el pipeline completo de datos del banco.
#   Corre cada hora en horario laboral chileno (8-18h, lunes-viernes).
#
# FLUJO:
#   generate_data → dlp_deidentify → dbt_run → bqml_apply → catalog_tags
#
# SCHEDULE: 0 8-18 * * 1-5 (America/Santiago)
# RETRIES : 2 por task, 5 minutos entre intentos
# =============================================================================

from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import pendulum

LOCAL_TZ = pendulum.timezone("America/Santiago")

PROJECT_DIR = "/home/airflow/gcs/data/gs-gcp-batch-chb-datagov"

default_args = {
    "owner"           : "data-engineering",
    "depends_on_past" : False,
    "retries"         : 2,
    "retry_delay"     : timedelta(minutes=5),
    "email_on_failure": False,
    "email_on_retry"  : False,
}

with DAG(
    dag_id             = "chilean_bank",
    description        = "Pipeline Data Governance — Chilean Bank",
    default_args       = default_args,
    schedule_interval  = "0 8-18 * * 1-5",
    start_date         = datetime(2026, 3, 25, tzinfo=LOCAL_TZ),
    catchup            = False,
    max_active_runs    = 1,
    tags               = ["banco-chile", "data-governance", "ley-19628"],
) as dag:

    generate_data = BashOperator(
        task_id      = "generate_data",
        bash_command = f"cd {PROJECT_DIR} && python data_generator/01_generate_data.py",
    )

    dlp_deidentify = BashOperator(
        task_id      = "dlp_deidentify",
        bash_command = f"cd {PROJECT_DIR} && python dlp/02_dlp_deidentify.py",
    )

    dbt_run = BashOperator(
        task_id      = "dbt_run",
        bash_command = f"cd {PROJECT_DIR}/dbt/chb_datagov && dbt run --profiles-dir /home/airflow/gcs/data/dbt_profiles",
    )

    bqml_apply = BashOperator(
        task_id      = "bqml_apply",
        bash_command = f"cd {PROJECT_DIR} && python ai/06_train_model.py",
    )

    catalog_tags = BashOperator(
        task_id      = "catalog_tags",
        bash_command = f"cd {PROJECT_DIR} && python governance/04_catalog_tags.py",
    )

    generate_data >> dlp_deidentify >> dbt_run >> bqml_apply >> catalog_tags
