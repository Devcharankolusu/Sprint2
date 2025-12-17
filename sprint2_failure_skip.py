from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime


# -------- Pre-processing ----------
def preprocess_data(ti):
    raw_data = [None, None]   # Change this to test behavior
    print("Raw data:", raw_data)
    ti.xcom_push(key="raw_data", value=raw_data)


# -------- Cleaning ----------
def clean_data(ti):
    raw_data = ti.xcom_pull(
        key="raw_data",
        task_ids="preprocess_data"
    )
    cleaned_data = [x for x in raw_data if x is not None]
    print("Cleaned data:", cleaned_data)
    ti.xcom_push(key="cleaned_data", value=cleaned_data)


# -------- Validation ----------
def validate_data(ti):
    cleaned_data = ti.xcom_pull(
        key="cleaned_data",
        task_ids="clean_data"
    )

    if cleaned_data:
        print("Validation passed")
        return "load_data"
    else:
        print("Validation failed")
        return "skip_load"


# -------- DAG ----------
with DAG(
    dag_id="sprint2_failure_skip_demo",
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:

    preprocess = PythonOperator(
        task_id="preprocess_data",
        python_callable=preprocess_data
    )

    clean = PythonOperator(
        task_id="clean_data",
        python_callable=clean_data
    )

    validate = BranchPythonOperator(
        task_id="validate_data",
        python_callable=validate_data
    )

    load = EmptyOperator(
        task_id="load_data"
    )

    skip = EmptyOperator(
        task_id="skip_load"
    )

    finalize = EmptyOperator(
        task_id="finalize_pipeline",
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS
    )

    preprocess >> clean >> validate >> [load, skip]
    [load, skip] >> finalize
