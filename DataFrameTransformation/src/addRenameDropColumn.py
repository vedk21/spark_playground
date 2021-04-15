import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, year

if __name__ == '__main__':
  # Check for valid inputs
  if len(sys.argv) != 2:
    print('Invalid inputs')
    sys.exit(-1)

  # Create SparkSession
  spark = (SparkSession
    .builder
    .appName('ProjectionsAndFilters')
    .getOrCreate())

  # Input data file path
  file_path = sys.argv[1]

  # Read csv to DataFrame
  sf_fire_df = (spark.read.format('csv')
    .option('header', 'true')
    .option('samplingRatio', '0.001')
    .load(file_path))

  # Rename the existing column
  (sf_fire_df.withColumnRenamed('Delay', 'ResponseDelayedInMins')
    .select('ResponseDelayedInMins')
    .show(n=10, truncate=False))

  # Add new columns and drop existing
  sf_fire_date_df = (sf_fire_df.withColumn('IncidentDate', to_timestamp(col('callDate'), 'MM/dd/yyyy'))
    .drop('CallDate')
    .select('IncidentNumber', 'CallType', 'IncidentDate'))

  sf_fire_date_df.show(n=10, truncate=False)

  # Use inbuilt data functions on new date columns
  (sf_fire_date_df.select('IncidentNumber', 'CallType', year('IncidentDate'))
    .distinct()
    .orderBy(year('IncidentDate').desc)
    .show(n=10, truncate=False))

  # Stop spark session
  spark.stop()