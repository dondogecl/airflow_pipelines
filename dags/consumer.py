from airflow import DAG, Dataset
from airflow.decorators import task

from datetime import datetime
import logging


# Dataset
local_file = Dataset("/tmp/my_file.txt")

# DAG
with DAG(
    dag_id = 'consumer',
    schedule = [local_file],
    start_date = datetime(2024, 1, 1),
    catchup = False
):
    
    @task
    def read_dataset():
        with open(local_file.uri, "r") as f:
            print(f.read())
            logging.info("dataset read")


    # execute
    read_dataset()