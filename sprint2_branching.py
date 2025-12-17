from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime


# -------- Cleaning task ----------
def cleaning_task(ti):
    data = [1, None, 2, None, 3]
    cleaned_data = [x for x in data if x is not None]

    print("Cleaned data:", cleaned_data)

    ti.xcom_push(key="cleaned_data", value=cleaned_data)


# -------- Branching decision ----------
def decide_next_step(ti):
    cleaned_data = ti.xcom_pull(
        key="cleaned_data",
        task_ids="clean_data"
    )

    if cleaned_data and len(cleaned_data) > 0:
        print("Data valid → proceeding")
        return "proceed_task"
    else:
        print("Data invalid → stopping")
        return "stop_task"


# -------- DAG ----------
with DAG(
    dag_id="sprint2_branching_demo",
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:

    clean_data = PythonOperator(
        task_id="clean_data",
        python_callable=cleaning_task
    )

    branch_decision = BranchPythonOperator(
        task_id="branch_decision",
        python_callable=decide_next_step
    )

    proceed_task = EmptyOperator(
        task_id="proceed_task"
    )

    stop_task = EmptyOperator(
        task_id="stop_task"
    )

    clean_data >> branch_decision >> [proceed_task, stop_task]
