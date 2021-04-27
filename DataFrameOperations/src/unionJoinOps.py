import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

if __name__ == '__main__':
  if len(sys.argv) != 3:
    print('Invalid input')
    sys.exit(-1)

  # Create spark session
  spark = (SparkSession
    .builder
    .appName('UnionJoinWindowOps')
    .getOrCreate())

  airports_data_path = sys.argv[1]
  flights_data_path = sys.argv[2]

  airports_df = (spark.read.format('csv')
    .option('inferSchema', 'true')
    .option('header', 'true')
    .load(airports_data_path))

  airports_df.show(truncate=False)

  flights_df = (spark.read.format('csv')
    .option('inferSchema', 'true')
    .option('header', 'true')
    .load(flights_data_path))

  flights_df.show(truncate=False)

  # Select portion of airports dataframe
  nm_aiports_df = (airports_df.filter(col('State') == 'NM'))
  nm_aiports_df.show(truncate=False)

  # Union two dataframes with similar schema
  (airports_df.union(nm_aiports_df)).show(truncate=False)

  # Join dataframes
  (airports_df.join(flights_df, airports_df.IATA == flights_df.Destination)
    .select('City', 'State', 'Delay', 'Origin')
    .show(truncate=False))

  # Stop spark session
  spark.stop()
