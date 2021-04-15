import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

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

  # Project and filter using where and filter
  (sf_fire_df.select('IncidentNumber', 'CallType', 'StationArea')
    .filter(col('CallType') != 'Structure Fire')
    .show(n=10, truncate=False))

  (sf_fire_df.select('IncidentNumber', 'CallType', 'StationArea')
    .where("CallType != 'Structure Fire'")
    .show(n=10, truncate=False))

  # Stop spark session
  spark.stop()
