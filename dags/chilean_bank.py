from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.email import send_email
from datetime import datetime, timedelta
import pendulum

LOCAL_TZ    = pendulum.timezone("America/Santiago")
PROJECT_DIR = "/home/airflow/gcs/data/gs-gcp-batch-chb-datagov"
EMAILS      = ["gsalfate.gcp@gmail.com", "gsalfate.code@gmail.com"]

def send_success_email(**context):
    exec_date = context["execution_date"]
    subject = "✅ Chilean Bank — Pipeline completado exitosamente"
    body = f"""
    <h2>✅ Pipeline Completado — Chilean Bank</h2>
    <p><b>Fecha ejecución:</b> {exec_date}</p>
    <h3>Tasks ejecutadas:</h3>
    <ul>
      <li>✅ generate_data — Bronze actualizado</li>
      <li>✅ dlp_deidentify — Silver desidentificado</li>
      <li>✅ dbt_run — Gold transformado</li>
      <li>✅ bqml_apply — Anomalías detectadas</li>
      <li>✅ catalog_tags — Metadata actualizada</li>
    </ul>
    """
    send_email(to=EMAILS, subject=subject, html_content=body)

def on_failure_callback(context):
    task_id  = context["task_instance"].task_id
    exec_date = context["execution_date"]
    log_url  = context["task_instance"].log_url
    subject = f"❌ Chilean Bank — FALLO en {task_id}"
    body = f"""
    <h2>❌ Task Fallida — Chilean Bank Pipeline</h2>
    <p><b>Task:</b> {task_id}</p>
    <p><b>Fecha:</b> {exec_date}</p>
    <p><b>Logs:</b> <a href="{log_url}">{log_url}</a></p>
    """
    send_email(to=EMAILS, subject=subject, html_content=body)

default_args = {
    "owner"              : "data-engineering",
    "depends_on_past"    : False,
    "retries"            : 2,
    "retry_delay"        : timedelta(minutes=5),
    "email"              : EMAILS,
    "email_on_failure"   : True,
    "email_on_retry"     : False,
    "on_failure_callback": on_failure_callback,
}

with DAG(
    dag_id            = "chilean_bank",
    description       = "Pipeline Data Governance — Chilean Bank",
    default_args      = default_args,
    schedule_interval = "0 8-18 * * 1-5",
    start_date        = datetime(2026, 3, 25, tzinfo=LOCAL_TZ),
    catchup           = False,
    max_active_runs   = 1,
    tags              = ["banco-chile", "data-governance", "ley-19628"],
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

    notify_success = PythonOperator(
        task_id         = "notify_success",
        python_callable = send_success_email,
        provide_context = True,
    )

    generate_data >> dlp_deidentify >> dbt_run >> bqml_apply >> catalog_tags >> notify_success
