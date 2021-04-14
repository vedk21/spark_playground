import sys

from pyspark.sql import SparkSession

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

  # Show data
  movies_df.show(n=20, truncate=False)

  # Stop spark session
  spark.stop()
