from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime


# --------- Cleaning function ----------
def clean_data():
    # Example raw data
    raw_data = [10, None, 20, 30, None, 40]

    print("Raw data:", raw_data)

    # Cleaning logic: remove None values
    cleaned_data = [x for x in raw_data if x is not None]

    print("Cleaned data:", cleaned_data)

    return cleaned_data


# --------- DAG definition ----------
with DAG(
    dag_id="sprint2_data_cleaning",
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,  # manual run
    catchup=False,
) as dag:

    cleaning_task = PythonOperator(
        task_id="clean_data_task",
        python_callable=clean_data
    )

    cleaning_task
