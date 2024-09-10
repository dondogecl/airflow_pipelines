import logging
from datetime import datetime
import json
from pandas import json_normalize
import os

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

# Utility functions

def _process_user(ti):
    """ Function that handles user data that comes in JSON format """
    user = ti.xcom_pull(task_ids="extract_user")
    if 'results' in user:
        user = user['results'][0]
        processed_user = json_normalize({
            'firstname' : user['name']['first'],
            'lastname' : user['name']['last'],
            'country' : user['location']['country'],
            'username' : user['login']['username'],
            'password' : user['login']['password'],
            'email' : user['email']
        })
        processed_user.to_csv('/tmp/processed_user.csv', index=None, header=False)
        logging.info(f"Processed user: {user['login']['username']}")
    else:
        logging.error("User data was not received correctly")
        raise ValueError("JSON Data is incomplete or format is incorrect")

def _store_user():
    """ Saves the retrieved data to the psql Database using the psql hook """
    hook = PostgresHook(postgres_conn_id='awspsql')
    if os.path.exists('/tmp/processed_user.csv'):
        hook.copy_expert(sql="COPY users FROM stdin WITH DELIMITER AS ','",
                        filename='/tmp/processed_user.csv')
        logging.info('Saved data to DB')
        os.remove('/tmp/processed_user.csv')
        logging.info('Removed temporary files')
        

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

    # Extract a random user from the API and also output to the logs
    extract_user = SimpleHttpOperator(
        task_id = 'extract_user',
        http_conn_id = 'user_api',
        endpoint = 'api/',
        method = 'GET',
        response_filter = lambda response: json.loads(response.text),
        log_response=True
    )

    # handling the received user data
    process_user = PythonOperator(
        task_id = 'process_user',
        python_callable = _process_user
    )

    # save data to our DB
    store_user = PythonOperator(
        task_id = 'store_user',
        python_callable = _store_user
    )

    # dependencies
    create_table >> is_api_available >> extract_user >> process_user >> store_user