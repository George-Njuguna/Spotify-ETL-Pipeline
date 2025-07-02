from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime
from pendulum import timezone

with DAG(
    dag_id="run_playlist_script",
    start_date=datetime(2024, 1, 1, tzinfo=timezone("Africa/Nairobi")),
    schedule="30 20 30 * *",
    catchup=False
) as dag:

    run_script = BashOperator(
        task_id="run_playlist_etl_script",
        bash_command="python /opt/airflow/etl/Playlist_ETL.py"
    )
