# Environment Variables

This document outlines the environment variables required for running the Spark job.

## Required Environment Variables

| Variable Name          | Description                                               | Example Value                                          |
|------------------------|-----------------------------------------------------------|-------------------------------------------------------|
| `LOG4J_PROPIERTIES`    | Path to the Log4J properties file                         | `./log4j.PROPIERTIES`                                |
| `REQUIREMENTS`         | Path to the Python requirements file                      | `./requirements.txt`                                 |
| `BUCKET`               | Name of the Google Cloud Storage bucket                   | `test`                                               |
| `DATASET_PATH`         | Path to the dataset directory                              | `../datasets/kaggle_anime-recommendation-db`     |
| `DATASET_ANIMES`       | Name of the anime dataset file                             | `anime.csv`                                         |
| `OUTPUT_DIRECTORY`      | Directory for saving output files                         | `./output`                                          |
| `DEBUG`                | Set to `true` to enable debug mode and `false` to deploy   | `true`                                              |

## Note
The values above are for local development. When running in the cloud, use appropriate values for your environment.

## Running

When testing locally `run_local_animes_job.sh` can be used to hide Spark/Java logs of INFO or lower level.