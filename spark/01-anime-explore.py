import os, sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when


if __name__ == "__main__":
    # check if datasets are available
    print(f"datasets available: {os.listdir('../../datasets/kaggle_anime-recommendation-db')}")
    
    try:
        spark = SparkSession.builder.appName('pyspark').getOrCreate()
        # Set log level to WARN
        spark.sparkContext.setLogLevel("WARN")
    except Exception as e:
        print(f"Problem loading Spark, details:\n{e}")
        print("Shutting down...")
        sys.exit(1)


    # read dataframe data
    animes = '../../datasets/kaggle_anime-recommendation-db/anime.csv'
    df = spark.read.format('csv')\
    .option('header', 'true')\
    .option('inferSchema', 'true')\
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

    # End the Spark Session
    try:
        spark.stop()
        print("Stopped Spark Session")
    except Exception as e:
        print(f"Problem closing the Spark Session, details:\n{e}")