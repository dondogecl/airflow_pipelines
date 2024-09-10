import logging
from airflow import DAG
from datetime import datetime
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.http.sensors.http import HttpSensor

# Configure the DAG
with DAG('user_processing', start_date=datetime(2024,1,1), schedule_interval='@daily', catchup=False) as dag:
    
    # task to create the user table
    create_table = PostgresOperator(
        task_id='create_table',
        postgres_conn_id='awspsql',
        sql='''
            CREATE TABLE IF NOT EXISTS users(
                firstname TEXT NOT NULL,
                lastname TEXT NOT NULL,
                country TEXT NOT NULL,
                username TEXT NOT NULL,
                password TEXT NOT NULL,
                email TEXT NOT NULL
            );
        '''
    )

    # sensor that checks if an API is available
    is_api_available = HttpSensor(
        task_id='is_api_available',
        http_conn_id='user_api',
        endpoint='api/'
    )