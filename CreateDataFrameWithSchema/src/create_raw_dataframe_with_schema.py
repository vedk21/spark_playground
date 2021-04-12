from pyspark.sql import SparkSession
from pyspark.sql.types import *

if __name__ == '__main__':

  # create SparkSession
  spark = (SparkSession
    .builder
    .appName('CreateRawDataFrameWithSchema')
    .getOrCreate())

  # Raw data
  raw_data = [
    ('Stephan Smith', 26, 3.5, ['Python', 'Spark']),
    ('Jules Morgan', 23, 2.2, ['Java', 'Hive']),
    ('Ketty Lopez', 39, 4.0, ['Scala', 'Hive'])
  ]

  # programatic schema declaration
  programaticSchema = StructType([StructField('Name', StringType(), False),
    StructField('Age', IntegerType(), False),
    StructField('Rating', FloatType(), False),
    StructField('Skills', ArrayType(StringType(), False), False)])

  # Create DataFrame using programatic schema
  programatic_df = spark.createDataFrame(raw_data, programaticSchema)

  # DDL schema declaration
  DDLSchema = "Name STRING, Age INT, Rating FLOAT, Skills ARRAY<STRING>"

  # Create DataFrame using DDL schema
  DDL_df = spark.createDataFrame(raw_data, DDLSchema)

  # Show created dataframe
  programatic_df.show(n=20, truncate=False)
  DDL_df.show(n=20, truncate=False)

  # Print schema
  print(programatic_df.printSchema())
  print(DDL_df.printSchema())
