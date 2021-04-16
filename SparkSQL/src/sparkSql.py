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
    .appName('SparkSQL')
    .getOrCreate())

  # Get input dataset into DataFrame
  airports_df = (spark.read.format('csv')
    .option('header', 'true')
    .option('samplingRatio', 0.0001)
    .load(file_path))

  # Create temp view
  airports_df.createOrReplaceTempView('airportsView')

  # Spark SQL queries
  spark.sql("SELECT airport_id, city FROM airportsView").show(n=20, truncate=False)

  # Stop spark session
  spark.stop()
