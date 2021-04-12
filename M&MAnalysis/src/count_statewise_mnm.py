import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, col

if __name__ == '__main__':
  if len(sys.argv) != 2:
    print('Invalid inputs')
    sys.exit(-1)

  # Build SparkSession
  spark = (SparkSession
    .builder
    .appName('MNStatewiseCount')
    .getOrCreate())
  
  mnm_file = sys.argv[1]
  # Read mnm file into dataframe using csv options
  mnm_df = (spark.read.format('csv')
    .option('header', 'true')
    .option('inferSchema', 'true')
    .load(mnm_file))

  # Derive statewise count of mnm
  count_mnm_df = (mnm_df
    .groupBy('State', 'Color')
    .agg(sum('Count').alias('TotalCount'))
    .orderBy('TotalCount', ascending=False))
  
  # Show the result
  count_mnm_df.show(n=30, truncate=False)

  # Filter only the results of state='CA'
  ca_count_mnm_df = (count_mnm_df
    .where(col('State') == 'CA'))

  # Show the result
  ca_count_mnm_df.show(n=30, truncate=False)

  # Stop the SparkSession
  spark.stop()
