import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, year, month, avg

if __name__ == '__main__':
  # Check for valid inputs
  if len(sys.argv) != 2:
    print('Invalid inputs')
    sys.exit(-1)

  # Create SparkSession
  spark = (SparkSession
    .builder
    .appName('AssignmentSFFireCalls')
    .getOrCreate())

  # Input data file path
  file_path = sys.argv[1]

  # Read csv to DataFrame
  sf_fire_df = (spark.read.format('csv')
    .option('header', 'true')
    .option('samplingRatio', '0.001')
    .load(file_path))

  # Format date to spark timestamps
  sf_formatted_date = (sf_fire_df.withColumn('IncidentDate', to_timestamp('CallDate', 'MM/dd/yyyy'))
    .drop('CallDate'))

  # Q1: What were all the different types of fire calls in 2018?
  (sf_formatted_date.select('CallType', year('IncidentDate').alias('IncidentYear'))
    .filter((col('IncidentYear') == 2018) & col('CallType').isNotNull())
    .distinct()
    .show(truncate=False))

  # Q2: What months within the year 2018 saw the highest number of fire calls?
  (sf_formatted_date.select('CallType', month('IncidentDate').alias('IncidentMonth'), year('IncidentDate').alias('IncidentYear'))
    .filter((col('IncidentYear') == 2018) & (col('CallType').isNotNull()))
    .groupBy('IncidentMonth')
    .count()
    .orderBy('count', ascending=False)
    .show(truncate=False))

  # Q3: Which neighborhood in SF generated the most fire calls in 2018?
  (sf_formatted_date.select('CallType', 'Neighborhood', year('IncidentDate').alias('IncidentYear'))
    .filter((col('IncidentYear') == 2018) & (col('CallType').isNotNull()) & (col('Neighborhood').isNotNull()))
    .groupBy('Neighborhood')
    .count()
    .orderBy('count', ascending=False)
    .show(truncate=False))

  # Q4: Which neighborhood had the worst response times to fire calls in 2018?
  (sf_formatted_date.select('Delay', 'Neighborhood', year('IncidentDate').alias('IncidentYear'))
    .filter((col('IncidentYear') == 2018) & (col('CallType').isNotNull()) & (col('Neighborhood').isNotNull()))
    .groupBy('Neighborhood')
    .agg(avg('Delay'))
    .orderBy(avg('Delay'), ascending=False)
    .show(truncate=False))

  # Q5: Is there a correlation between neighborhood, zip code and number of fire calls?
  (sf_formatted_date.select('Zipcode', 'Neighborhood')
    .filter((col('CallType').isNotNull()) & (col('Neighborhood').isNotNull()) & (col('Zipcode').isNotNull()))
    .groupBy('Zipcode', 'Neighborhood')
    .count()
    .orderBy('count', ascending=False)
    .show(truncate=False))

  # Stop spark session
  spark.stop()
