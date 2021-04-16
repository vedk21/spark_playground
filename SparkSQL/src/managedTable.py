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
    .appName('ManagedTable')
    .getOrCreate())

  # Create database and use it
  spark.sql("CREATE DATABASE IF NOT EXISTS air_db LOCATION '/user/maria_dev/hive/air.db'")
  spark.sql("USE air_db")

  # Create managed table using spark.sql
  spark.sql("DROP TABLE IF EXISTS airports")
  spark.sql("CREATE TABLE airports(AirportId INT, City STRING, State STRING, Name STRING)")
  spark.sql("INSERT INTO airports VALUES(1234, 'Bethel', 'AK', 'Bethel Airport'), (4567, 'Dallas', 'TX', 'Dallas Love Field')")

  # Create managed table using DataFrame
  airportSchema = 'AirportId INT, City STRING, State STRING, Name STRING'
  # Get input dataset into DataFrame
  airports_df = (spark.read.format('csv')
    .schema(airportSchema)
    .option('header', 'true')
    .load(file_path))

  # Save DataFrame as table
  airports_df.write.mode('overwrite').saveAsTable('airports_df_table')

  # Query spark SQL table
  spark.sql("SELECT * FROM airports").show(n=20, truncate=False)

  # Query DataFrame table
  spark.sql("SELECT * FROM airports_df_table").show(n=20, truncate=False)

  # stop spark session
  spark.stop()
