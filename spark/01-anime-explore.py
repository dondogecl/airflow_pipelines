import os, sys
import datetime
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
import logging
from google.cloud import storage

def verify_path_gcs(bucket_id: str, path: str) -> bool:
    """Verifies if the provided bucket and path exist.
    
    Args:
        bucket_id (str): The ID of the GCS bucket
        path (str): location of the files inside the bucket (folders or path)
    Returns:
        bool : True only if the location is valid."""
    client = storage.Client()
    bucket_name = bucket_id
    try:
        bucket = client.get_bucket(bucket_name)
    except Exception as e:
        logging.error(f'Bucket name: - {bucket_id} - was not found or cannot correct. Details:\n{e}')
        return False
    # get blobs
    files = list(bucket.list_blobs(prefix=path))
    # validate if the file exists in the bucket
    if not files:
        logging.error(f"No files found in the GCS provided - {bucket_id} / {path} -")
        return False
    logging.info('File found at the specified GCS location: {path}')
    return True
    
        


if __name__ == "__main__":
    load_dotenv()
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    # check if datasets are available
    dataset_path = os.getenv('DATASET_PATH')
    filename = os.getenv('DATASET_ANIMES')
    log4j_properties_path = os.getenv('LOG4J_PROPIERTIES')
    output_path = os.getenv('OUTPUT_DIRECTORY')
    debug = os.getenv('DEBUG', 'false').lower() in ('true', '1')

    if debug:
        logging.info(f"DEBUG MODE: ON")

    # check if the dataset is accessible
    if not os.path.exists(dataset_path):
        logging.error(f"Requested dataset path - {dataset_path} - was not found.")
        sys.exit(1)
    elif filename not in os.listdir(dataset_path):
        logging.error(f"Requested file - {filename} - was not found at the location: {dataset_path}")
        sys.exit(1)
    logging.info(f"datasets available: {os.listdir(dataset_path)}")
    
    try:
        spark = SparkSession.builder.\
            appName('pyspark').\
                config("spark.driver.extraJavaOptions", f"-Dlog4j.configuration=file:{log4j_properties_path}")\
                    .getOrCreate()
        # Set log level to WARN
        #spark.sparkContext.setLogLevel("WARN")
        logging.info('Spark Session created successfully.')
    except Exception as e:
        logging.error(f"Problem loading Spark, details:\n{e}")
        logging.warning("Shutting down...")
        sys.exit(1)


    # define schema of data to be read
    animes_schema = '`MAL_ID` INT, `Name` STRING, `Score` DOUBLE, `Genres` STRING, `English name` STRING,\
        `Japanese name` STRING, `Type` STRING, `Episodes` INT, `Aired` STRING, `Premiered` STRING,\
            `Producers` STRING, `Licensors` STRING, `Studios` STRING, `Source` STRING, `Duration` STRING,\
                `Rating` STRING, `Ranked` DOUBLE, `Popularity` INT, `Members` INT, `Favorites` INT,\
                    `Watching` INT, `Completed` INT, `On-Hold` INT, `Dropped` INT, `Plan to Watch` INT,\
                         `Score-10` INT, `Score-9` INT, `Score-8` INT, `Score-7` INT, `Score-6` INT,\
                             `Score-5` INT, `Score-4` INT, `Score-3` INT, `Score-2` INT, `Score-1` INT'
    
    # read dataframe data
    df = spark.read.format('csv')\
    .option('header', 'true') \
    .schema(animes_schema) \
    .load(os.path.join(dataset_path, filename))

    # Convert the 'Score' column to double, setting non-numeric values to null
    #df = df.withColumn("Score", when(col("Score").rlike("^\d+(\.\d+)?$"), col("Score").cast("double")).otherwise(None))

    # group by anime and count records
    count_animes = (df
                .select('MAL_ID', 'Name', 'Score', 'Episodes', 'Members', 'Completed', 'Dropped')
                .orderBy('Score', ascending=False)
                )
    
    # enable to test/preview the processed data
    #count_animes.show(n=10, truncate=False)

    output_path = os.path.join(output_path,'result_count_animes')
    
    count_animes.write\
        .format('csv')\
        .option('header', 'true')\
        .mode('overwrite')\
        .save(output_path)

    # End the Spark Session
    try:
        spark.stop()
        logging.info("Finalized Spark Session.")
    except Exception as e:
        logging.warning(f"Problem closing the Spark Session, details:\n{e}")