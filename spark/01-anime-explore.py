import os, sys
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when


if __name__ == "__main__":
    load_dotenv()
    # check if datasets are available
    animes = os.getenv('DATASET_ANIMES')
    log4j_properties_path = os.getenv('LOG4J_PROPIERTIES')
    output_path = os.getenv('OUTPUT_DIRECTORY')


    print(f"datasets available: {os.listdir('../../datasets/kaggle_anime-recommendation-db')}")
    
    try:
        spark = SparkSession.builder.\
            appName('pyspark').\
                config("spark.driver.extraJavaOptions", f"-Dlog4j.configuration=file:{log4j_properties_path}")\
                    .getOrCreate()
        # Set log level to WARN
        #spark.sparkContext.setLogLevel("WARN")
    except Exception as e:
        print(f"Problem loading Spark, details:\n{e}")
        print("Shutting down...")
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
    .load(animes)

    # Convert the 'Score' column to double, setting non-numeric values to null
    df = df.withColumn("score", when(col("score").rlike("^\d+(\.\d+)?$"), col("score").cast("double")).otherwise(None))

    # group by anime and count records
    count_animes = (df
                .select('MAL_ID', 'name', 'score')
                .groupBy('name')
                .avg('score')
                .orderBy('avg(score)', ascending=False)
                )
    
    count_animes.show(n=10, truncate=False)

    output_path = os.path.join(output_path,'result_count_animes')
    
    count_animes.write\
        .format('csv')\
        .option('header', 'true')\
        .mode('overwrite')\
        .save(output_path)

    # End the Spark Session
    try:
        spark.stop()
        print("Stopped Spark Session")
    except Exception as e:
        print(f"Problem closing the Spark Session, details:\n{e}")