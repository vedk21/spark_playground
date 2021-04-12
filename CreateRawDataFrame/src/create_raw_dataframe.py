from pyspark.sql import SparkSession
from pyspark.sql.functions import avg

if __name__ == '__main__':
  # Create SparkSession
  spark = SparkSession
    .builder
    .appName('CreateDataFrame')
    .getOrCreate()
  
  # Create a DataFrame from raw data
  raw_df = spark.createDataFrame([('Don', 4), ('Test', 15), ('Don', 8), ('Jules', 9)], ['Name', 'Score'])

  # derive avg score for user
  avg_score_df = raw_df
    .groupBy('Name')
    .agg(avg('Score').alias('Avg Score'))
    .show(n=20, truncate=False)

  # Stop SparkSession
  spark.stop()
