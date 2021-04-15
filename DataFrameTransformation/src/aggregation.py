import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, sum, min, max

if __name__ == '__main__':
  # Check for valid inputs
  if len(sys.argv) != 2:
    print('Invalid inputs')
    sys.exit(-1)

  # Create SparkSession
  spark = (SparkSession
    .builder
    .appName('AggregationDataFrame')
    .getOrCreate())

  # Input data file path
  file_path = sys.argv[1]

  # Read csv to DataFrame
  sf_fire_df = (spark.read.format('csv')
    .option('header', 'true')
    .option('samplingRatio', '0.001')
    .load(file_path))

  # Aggregation on DataFrame
  (sf_fire_df.select('CallType')
    .filter(col('CallType').isNotNull())
    .groupBy('CallType')
    .count()
    .orderBy('count', ascending=False)
    .show(n=15, truncate=False))

  # Other common aggregation operations
  (sf_fire_df.select(sum('NumAlarms'), avg('Delay'), min('Delay'), max('Delay'))
    .show(n=15, truncate=False))

  # Stop spark session
  spark.stop()