from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import json
import os
import sys

# add path to sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pipelines.etl import async_process_season, async_process_event, async_process_category,\
    async_process_session, async_process_classification, \
    process_rider, process_team, process_circuit, process_country, process_constructor
default_args = {
    "owner": "Adam Nguyen",
    "depends_on_past": False,
    "start_date": datetime(2020, 1, 1),
}

dag = DAG(
    "get_data_motogp",
    default_args=default_args,
    schedule_interval="0 0 * * *", # run every day at midnight
    catchup=False
)

# define tasks
get_season_task = PythonOperator(
    task_id="get_season_task",
    python_callable=async_process_season,
    provide_context=True,
    dag=dag
)

get_event_task = PythonOperator(
    task_id="get_event_task",
    python_callable=async_process_event,
    provide_context=True,
    dag=dag
)

get_category_task = PythonOperator(
    task_id="get_category_task",
    python_callable=async_process_category,
    provide_context=True,
    dag=dag
)

get_session_task = PythonOperator(
    task_id="get_session_task",
    python_callable=async_process_session,
    provide_context=True,
    dag=dag
)

get_classification_task = PythonOperator(
    task_id="get_classification_task",
    python_callable=async_process_classification,
    provide_context=True,
    dag=dag
)

get_rider_task = PythonOperator(
    task_id="get_rider_task",
    python_callable=process_rider,
    provide_context=True,
    dag=dag
)

get_team_task = PythonOperator(
    task_id="get_team_task",
    python_callable=process_team,
    provide_context=True,
    dag=dag
)

get_circuit_task = PythonOperator(
    task_id="get_circuit_task",
    python_callable=process_circuit,
    provide_context=True,
    dag=dag
)

get_country_task = PythonOperator(
    task_id="get_country_task",
    python_callable=process_country,
    provide_context=True,
    dag=dag
)

get_constructor_task = PythonOperator(
    task_id="get_constructor_task",
    python_callable=process_constructor,
    provide_context=True,
    dag=dag
)

get_season_task >> get_event_task >> get_category_task >> get_session_task >> get_classification_task >> get_rider_task >> get_team_task >> get_circuit_task >> get_country_task >> get_constructor_task