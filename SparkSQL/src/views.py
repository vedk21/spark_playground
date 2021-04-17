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
    .appName('views')
    .getOrCreate())

  # Read csv into a DataFrame
  airports_df = (spark.read.format('csv')
    .option('header', 'true')
    .option('samplingRatio', 0.0001)
    .load(file_path))

  # Create database and use it
  spark.sql("CREATE DATABASE IF NOT EXISTS air_db LOCATION '/user/maria_dev/hive/air.db'")
  spark.sql("USE air_db")

  # Save DataFrame as table
  airports_df.write.mode('overwrite').saveAsTable('airports_df_table')

  # Create views using SQL
  # Global - Across all spark sessions
  spark.sql("CREATE OR REPLACE GLOBAL TEMP VIEW global_airports_view AS SELECT city, name FROM airports_df_table WHERE state = 'TX'")
  # Local - Only within the current spark session
  spark.sql("CREATE OR REPLACE TEMP VIEW airports_view AS SELECT airport_id, name FROM airports_df_table WHERE state = 'CA'")

  # Create views from DataFrame
  # Global - Across all spark session
  tx_airports_df = spark.sql("SELECT city, name FROM airports_df_table WHERE state = 'TX'")
  tx_airports_df.createOrReplaceGlobalTempView('global_airports_view_df')
  # Local - Only within the current spark session
  ca_airports_df = spark.sql("SELECT airport_id, name FROM airports_df_table WHERE state = 'CA'")
  ca_airports_df.createOrReplaceTempView('airports_view_df')


  # Show global views
  # Using SQL
  spark.sql("SELECT * FROM global_temp.global_airports_view").show(n=10, truncate=False)
  # Using read.table API on DataFrame
  spark.read.table('global_temp.global_airports_view_df').show(n=10, truncate=False)

  # Show local views
  # Using SQL
  spark.sql("SELECT * FROM airports_view_df").show(n=10, truncate=False)
  # Using read.table API on DataFrame
  spark.read.table('airports_view').show(n=10, truncate=False)

  # Drop views
  spark.sql("DROP VIEW IF EXISTS global_airports_view")
  spark.sql("DROP VIEW IF EXISTS airports_view_df")
  # Using spark.catalog
  spark.catalog.dropGlobalTempView('global_airports_view_df')
  spark.catalog.dropTempView('airports_view')

  # Stop spark session
  spark.stop()
