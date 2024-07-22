# Setup and installation instructions

## first time: folders

Run just one, to make sure we don't create the folders with root privileges and other issues:

```
mkdir -p ./dags ./logs ./plugins ./config
echo -e "AIRFLOW_UID=$(id -u)" > .env
```

## create the following in the environment variables:

Make sure to create the following ENV variables/secrets before executing or creating the containers:

```
AIRFLOW_IMAGE_NAME=apache/airflow:2.4.2   
AIRFLOW_UID=50000   
_AIRFLOW_WWW_USER_USERNAME=<your username>      
_AIRFLOW_WWW_USER_PASSWORD=<your password>      
POSTGRES_USER=<your username>   
POSTGRES_PASSWORD=<your password>   
POSTGRES_DB=<your password>   
```

## Where to store the variables

- test: .env file

- prod: secrets

# Running Airflow

## Build the docker images

Make sure to always use --no-cache when changing any dependencies or packages.

```docker compose build --no-cache```

## Run the containers
```docker compose up```


# Known issues

- Make sure to add each Azure dependency individually, adding the package "azure" to the python packages to install has been deprecated.
- Port conflict: sometimes the Postgres or Airlow webserver ports are already in use, in that case change it accordingly to one that is available in the machine where they will run. I have on purpose changed the default `:8080` to `:8081`