from airflow import DAG # pyright: ignore[reportMissingImports]
from airflow.operators.python import PythonOperator # pyright: ignore[reportMissingImports]
from datetime import datetime


# -------- Task 1: Cleaning ----------
def cleaning_task(ti):
    data = [1, None, 2, None, 3]
    cleaned_data = [x for x in data if x is not None]

    print("Cleaned data:", cleaned_data)

    # Push data to XCom
    ti.xcom_push(key="cleaned_data", value=cleaned_data)
    


# -------- Task 2: Validation ----------
def validation_task(ti):
    # Pull data from XCom
    cleaned_data = ti.xcom_pull(
        key="cleaned_data",
        task_ids="clean_data"
    )

    print("Data received from XCom:", cleaned_data)

    if len(cleaned_data) > 0:
        print("Validation SUCCESS: Data is valid")
    else:
        print("Validation FAILED: No data found")


# -------- DAG ----------
with DAG(
    dag_id="sprint2_xcom_demo",
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:

    clean_data = PythonOperator(
        task_id="clean_data",
        python_callable=cleaning_task
    )

    validate_data = PythonOperator(
        task_id="validate_data",
        python_callable=validation_task
    )

    clean_data >> validate_data
