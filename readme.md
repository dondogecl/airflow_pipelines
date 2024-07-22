create the following in the environment variables:

`
AIRFLOW_IMAGE_NAME=apache/airflow:2.4.2
AIRFLOW_UID=50000
POSTGRES_USER=<your password>
POSTGRES_PASSWORD=<your password>
POSTGRES_DB=<your password>
`

- test: .env file

- prod: secrets