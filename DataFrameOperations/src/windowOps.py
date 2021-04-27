import sys

from pyspark.sql import SparkSession
from pyspark.sql import Window
from pyspark.sql.functions import sum, col, dense_rank

if __name__ == '__main__':
  if len(sys.argv) != 2:
    print('Invalid input')
    sys.exit(-1)

  # Create spark session
  spark = (SparkSession
    .builder
    .appName('UnionJoinWindowOps')
    .getOrCreate())

  flights_data_path = sys.argv[1]

  flights_df = (spark.read.format('csv')
    .option('inferSchema', 'true')
    .option('header', 'true')
    .load(flights_data_path))

  flights_df.show(truncate=False)

  # Find 2 Origin airports that experienced most delays flying to ROS and MNO Destinations,
  # without using window and dense rank
  (flights_df.select('Origin', 'Destination', 'Delay')
    .filter(col('Destination') == 'ROS')
    .groupBy('Origin', 'Destination')
    .agg(sum('Delay').alias('TotalDelay'))
    .orderBy('TotalDelay', ascending=False)
    .show(n=2, truncate=False))

  (flights_df.select('Origin', 'Destination', 'Delay')
    .filter(col('Destination') == 'MNO')
    .groupBy('Origin', 'Destination')
    .agg(sum('Delay').alias('TotalDelay'))
    .orderBy('TotalDelay', ascending=False)
    .show(n=2, truncate=False))

  # Now using window and dense rank functions we can find this in single query
  # Group by flights with origin and destination and find total delay
  flights_grouped_by = (flights_df.select('Origin', 'Destination', 'Delay')
    .filter(col('Destination').isin(['ROS', 'MNO']))
    .groupBy('Origin', 'Destination')
    .agg(sum('Delay').alias('TotalDelay')))
  
  # Create window for partion against destination and order by total delay
  flights_window = (Window.partitionBy(flights_grouped_by.Destination)
    .orderBy(col('TotalDelay').desc()))
  
  # Find dense rank over create window
  (flights_grouped_by.withColumn('Rank', dense_rank().over(flights_window))
    .filter(col('Rank') <= 2)
    .show(truncate=False))

  # Stop spark session
  spark.stop()