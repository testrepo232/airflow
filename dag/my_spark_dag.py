from airflow.models import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.dates import days_ago

args = {
    'owner': 'Airflow',
}

with DAG(
    dag_id='example_spark_operator',
    default_args=args,
    schedule_interval=None,
    start_date=days_ago(2),
    tags=['example'],
) as dag:
    # [START howto_operator_spark_submit]
    
    python_submit_job = SparkSubmitOperator(
        application="./pi.py", task_id="python_job"
    )
    
#    scala_submit_job = SparkSubmitOperator(
#        application="/workspace/example-airflow-and-spark/airflow-spark-assembly-0.1.0-SNAPSHOT.jar", task_id="scala_job"
#    )
#
#    python_submit_job >> scala_submit_job
#    
