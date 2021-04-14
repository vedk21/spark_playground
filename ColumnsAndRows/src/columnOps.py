import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import expr, col

if __name__ == '__main__':
  if len(sys.argv) != 2:
    print('Invalid inputs')
    sys.exit(-1)
  
  # Input data path
  data_file = sys.argv[1]

  # Create SparkSession
  spark = (SparkSession
    .builder
    .appName('ColumnOperations')
    .getOrCreate())

  # DDL schema declaration
  DDLSchema = "Name STRING, Age INT, Rating FLOAT, Skills ARRAY<STRING>"

  # Read JSON file into DataFrame
  user_df = (spark.read.schema(DDLSchema).format('json')
    .load(data_file))

  # Column expr use
  (user_df.filter(expr('Age < 30'))
    .select('Name', 'Age')
    .show(n=20, truncate=False))

  # Column col use
  (user_df.filter(col('Age') > 30)
    .select('Name', 'Age')
    .show(n=20, truncate=False))

  # Column withColumn use
  (user_df.withColumn('Above 30', expr('Age > 30'))
    .show(n=20, truncate=False))

  # Stop spark session
  spark.stop()
