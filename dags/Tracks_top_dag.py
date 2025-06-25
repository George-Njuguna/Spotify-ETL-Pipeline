from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime
from datetime import timedelta
from pendulum import timezone

with DAG(
    dag_id="run_top_tracks_artists_savedtracks_script",
    start_date=datetime(2024, 7, 29, 22, 10, tzinfo=timezone("Africa/Nairobi")),
    schedule=timedelta(weeks=2),
    catchup=False
    
) as dag:

    run_saved_tracks_script = BashOperator(
        task_id="run_saved_tracks_script", 
        bash_command="python /opt/airflow/etl/Saved_Tracks_ETL.py"  
    )

    run_top_artists_tracks_script = BashOperator(
        task_id="run_top_artists_tracks_script", 
        bash_command="python /opt/airflow/etl/Top_artists_tracks_ETL.py"  
    )

    run_saved_tracks_script >> run_top_artists_tracks_script
