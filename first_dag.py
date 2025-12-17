from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    "start_date": datetime(2023, 1, 1),
}

with DAG(
    dag_id="my_first_dag",
    schedule_interval="@daily",
    default_args=default_args,
    catchup=False,
) as dag:

    hello = BashOperator(
        task_id="say_hello",
        bash_command="echo 'Hello Airflow! My first DAG is running successfully!'"
    )

    hello
