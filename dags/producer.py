from airflow import DAG, Dataset
from airflow.decorators import task

from datetime import datetime
import logging

# Dataset
local_file = Dataset("/tmp/my_file.txt")

# Dag
with DAG(
    dag_id = "producer",
    schedule = "@daily",
    start_date = datetime(2025,1,1),
    catchup = False
):
    
    @task(outlets=[local_file])
    def update_dataset():
        with open(local_file.uri, '+a') as f:
            f.write(f"Producer update {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}\n")
            logging.info(f"Dataset updated.")

    # execute task
    update_dataset()