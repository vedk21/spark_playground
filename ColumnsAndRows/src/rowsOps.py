import sys

from pyspark.sql import SparkSession, Row

if __name__ == '__main__':

  # Create SparkSession
  spark = (SparkSession
    .builder
    .appName('RowsOperations')
    .getOrCreate())

  # Create Row object
  data_rows = [Row('Sheldon Cooper', 31), Row('Howard Wolowitz', 32)]

  # Read JSON file into DataFrame
  user_df = (spark.createDataFrame(data_rows, ['Name', 'Age']))

  # Column expr use
  user_df.show(n=20, truncate=False)

  # Stop spark session
  spark.stop()
