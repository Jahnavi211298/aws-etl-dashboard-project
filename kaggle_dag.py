from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from kaggle_etl import run_kaggle_etl

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 4, 25),
    'email': ['jahnavipitchuka21@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

with DAG(
    'kaggle_dag',
    default_args=default_args,
    description='Our first DAG with ETL process!',
    catchup=False
) as dag:

    run_etl = PythonOperator(
        task_id='complete_kaggle_etl',
        python_callable=run_kaggle_etl,
        dag=dag,
    )

run_etl
