from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

default_args = {
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    'spark-pi-dag',
    default_args=default_args,
    description='Pi Calculation with Spark',
    schedule_interval=None,  # Trigger manually
    start_date=datetime(2022, 11, 17),
    catchup=False,
) as dag:
    spark_task = SparkSubmitOperator(
        task_id='spark-pi-job',
        conn_id='spark_default',  # Your Spark connection ID
        application='/home/deployer/projects/spark-jobs/sparkk_job.py',  # Full path to your Python job
        application_args=['1000000'],  # Optional: Number of samples for the Pi calculation
        name='spark-pi-job',
        dag=dag
    )
