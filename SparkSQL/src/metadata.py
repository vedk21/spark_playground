import sys

from pyspark.sql import SparkSession

if __name__ == '__main__':
  if len(sys.argv) != 2:
    print('Invalid inputs')
    sys.exit(-1)

  file_path = sys.argv[1]

  # Create SparkSession
  spark = (SparkSession
    .builder
    .appName('SparkMetadata')
    .getOrCreate())

  # Read csv into a DataFrame
  airports_df = (spark.read.format('csv')
    .option('header', 'true')
    .option('samplingRatio', 0.0001)
    .load(file_path))

  # Create database and use it
  spark.sql("CREATE DATABASE IF NOT EXISTS air_db LOCATION '/user/maria_dev/hive/air.db'")
  spark.sql("USE air_db")

  # List databsaes, tables and columns for it
  print(spark.catalog.listDatabases())
  print(spark.catalog.listTables())
  print(spark.catalog.listColumns('airports_df_table'))

  # Cache tables
  # With spark 3.x, we can use LAZY option with cache - it caches lazily when table is first reffered
  spark.sql("CACHE TABLE airports_df_table")
  spark.sql("UNCACHE TABLE airports_df_table")

  # Stop spark session
  spark.stop()