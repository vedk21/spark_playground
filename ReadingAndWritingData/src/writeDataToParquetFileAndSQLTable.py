import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

if __name__ == '__main__':
  if len(sys.argv) != 2:
    print('Invalid inputs')
    sys.exit(-1)
  
  # Input data path
  data_file = sys.argv[1]

  # Create SparkSession
  spark = (SparkSession
    .builder
    .appName('ReadData')
    .getOrCreate())

  # DDL schema declaration
  DDLSchema = "MovieID INT, Title STRING, Genres STRING"

  # Read movies data into DataFrame
  movies_df = (spark.read.schema(DDLSchema)
    .format('csv')
    .option('header', 'true')
    .load(data_file))

  # Filtered data
  filtered_movies = movies_df.filter(col('MovieID') < 10)

  # Write to parquet file
  file_path_to_write = 'playground/spark/ReadAndWriteData/data/output'
  filtered_movies.write.format('parquet').save(file_path_to_write)

  # Save DataFrame as table
  filtered_movies.write.format('parquet').saveAsTable('filtered_spark_movies')

  # Read from saved table
  spark.sql('SELECT * FROM filtered_spark_movies').show(n=10, truncate=False)

  # Stop spark session
  spark.stop()
