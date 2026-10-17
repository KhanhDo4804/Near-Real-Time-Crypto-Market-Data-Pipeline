import pendulum

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator


with DAG(
    dag_id="crypto_market_pipeline",
    start_date=pendulum.datetime(2026, 10, 17, tz="Asia/Bangkok"),
    schedule="0 * * * *",
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 3},
) as dag:

    command = (
        "cd /opt/project && "
        "exec /opt/pipeline-env/bin/python -u redshift/run_pipeline.py "
    )
    manifest_env = {
        "MANIFEST_URI": "{{ ti.xcom_pull(task_ids='create_manifest') }}",
    }

    create_manifest = BashOperator(
        task_id="create_manifest",
        bash_command=command + "create_manifest",
        do_xcom_push=True,
        skip_on_exit_code=99,
    )

    copy_staging = BashOperator(
        task_id="copy_staging",
        bash_command=command + "copy_staging",
        env=manifest_env,
        append_env=True,
        do_xcom_push=False,
        skip_on_exit_code=None,
    )

    build_and_test = BashOperator(
        task_id="build_and_test",
        bash_command=command + "build_and_test",
        do_xcom_push=False,
        skip_on_exit_code=None,
    )

    mark_loaded_files = BashOperator(
        task_id="mark_loaded_files",
        bash_command=command + "mark_loaded_files",
        env=manifest_env,
        append_env=True,
        do_xcom_push=False,
        skip_on_exit_code=None,
    )

    create_manifest >> copy_staging >> build_and_test >> mark_loaded_files
