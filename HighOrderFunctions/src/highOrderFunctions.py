from pyspark.sql import SparkSession
from pyspark.sql.functions import transform, filter, exists, size

if __name__ == '__main__':

  # Create SparkSession
  spark = (SparkSession
    .builder
    .appName('HighOrderFunctions')
    .getOrCreate())

  # Create sample DataFrame
  schema = 'celsius Array<INT>'
  tempList = [[12,45,23,36,22,47]], [[8,34,45,12,19,4]]
  temp_df = spark.createDataFrame(tempList, schema)
  temp_df.createOrReplaceTempView('temp_view')

  temp_df.show(truncate=False)

  # Use of transform function
  spark.sql("SELECT celsius, transform(celsius, t -> ((t * 9) div 5) + 32) AS fahrenheit FROM temp_view").show(truncate=False)

  # Use of filter function
  spark.sql("SELECT celsius, filter(celsius, t -> (t > 38)) AS high FROM temp_view").show(truncate=False)

  # Use of exists function
  spark.sql("SELECT celsius, exists(celsius, t -> (t = 23)) AS threshold FROM temp_view").show(truncate=False)

  # Use of size function
  spark.sql("SELECT celsius, size(celsius) AS total FROM temp_view").show(truncate=False)

  # Stop spark session
  spark.stop()
