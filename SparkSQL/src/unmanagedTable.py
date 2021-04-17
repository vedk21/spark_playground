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
    .appName('UnManagedTable')
    .getOrCreate())

  # Create database and use it
  spark.sql("CREATE DATABASE IF NOT EXISTS unmanaged_air_db LOCATION '/user/maria_dev/hive/unmanaged_air.db'")
  spark.sql("USE unmanaged_air_db")

  # Create managed table using spark.sql
  spark.sql("DROP TABLE IF EXISTS unmanaged_airports")
  spark.sql("CREATE TABLE unmanaged_airports(AirportId INT, City STRING, State STRING, Name STRING) USING CSV OPTIONS(PATH '/user/maria_dev/playground/spark/SparkSQL/data/airports.csv', HEADER 'true')")

  # Create managed table using DataFrame
  airportSchema = 'AirportId INT, City STRING, State STRING, Name STRING'
  # Get input dataset into DataFrame
  airports_df = (spark.read.format('csv')
    .schema(airportSchema)
    .option('header', 'true')
    .load(file_path))

  # Save DataFrame as table
  (airports_df.write.mode('overwrite')
    .option('path', '/user/maria_dev/hive/unmanaged_airports_df_table')
    .saveAsTable('unmanaged_airports_df_table'))

  # Query spark SQL table
  spark.sql("SELECT * FROM unmanaged_airports").show(n=20, truncate=False)

  # Query DataFrame table
  spark.sql("SELECT * FROM unmanaged_airports_df_table").show(n=20, truncate=False)

  # Stop spark session
  spark.stop()

  # To Run use this option --conf spark.sql.catalogImplementation=hive 
