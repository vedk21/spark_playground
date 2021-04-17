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
    .appName('ParquetReadWrite')
    .getOrCreate())

  # Create database and use it
  spark.sql("CREATE DATABASE IF NOT EXISTS built_in_data_sources_db LOCATION '/user/maria_dev/hive/built_in_data_sources.db'")
  spark.sql("USE built_in_data_sources_db")

  # Schema for given parquet file
  airportSchema = 'AirportId INT, City STRING, State STRING, Name STRING'

  # Read parquet file into DataFrame
  parquet_df = (spark.read.format('parquet')
    .schema(airportSchema)
    .load(file_path))

  # Read parquet file into SQL table or view
  spark.sql("CREATE OR REPLACE TEMP VIEW airports_parquet_view USING parquet OPTIONS(PATH '/user/maria_dev/hive/unmanaged_airports_df_table')")

  # Show parquet data
  parquet_df.show(n=10, truncate=False)
  spark.sql("SELECT * FROM airports_parquet_view").show(n=10, truncate=False)

  # Write DataFrame to parquet file
  (parquet_df.write.mode('overwrite')
    .option('compression', 'snappy')
    .save('/user/maria_dev/playground/spark/BuiltInDataSources/data/output/parquet'))

  # Write DataFrame to table
  (parquet_df.write.mode('overwrite')
    .saveAsTable('parquet_table'))

  # Stop spark session
  spark.stop()