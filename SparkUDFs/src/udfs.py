from pyspark.sql import SparkSession
from pyspark.sql.types import LongType

if __name__ == '__main__':
  
  # Create SparkSession
  spark = (SparkSession
    .builder
    .appName('UDFs')
    .getOrCreate())

  # Write UDF function
  def cubed(id):
    return id * id * id
  
  # Register the UDF with spark session
  spark.udf.register('cubed', cubed, LongType())

  # Create a raw view
  spark.range(1,9).createOrReplaceTempView('ranged_ids')

  # Query the view using UDF
  spark.sql("SELECT id, cubed(id) AS cubed_id FROM ranged_ids").show(truncate=False)

  # Stop spark session
  spark.stop()
