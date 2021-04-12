import sys

from pyspark.sql import SparkSession
from pyspark.sql.types import *

if __name__ == '__main__':

  if len(sys.argv) != 2:
    print('Invalid inputs')
    sys.exit(-1)

  # create SparkSession
  spark = (SparkSession
    .builder
    .appName('CreateRawDataFrameWithSchema')
    .getOrCreate())

  # JSON file path
  jsonFile = sys.argv[1]

  # programatic schema declaration
  programaticSchema = StructType([StructField('Name', StringType(), False),
    StructField('Age', IntegerType(), False),
    StructField('Rating', FloatType(), False),
    StructField('Skills', ArrayType(StringType(), False), False)])

  # Create DataFrame using programatic schema
  programatic_df = spark.read.schema(programaticSchema).json(jsonFile)

  # DDL schema declaration
  DDLSchema = "Name STRING, Age INT, Rating FLOAT, Skills ARRAY<STRING>"

  # Create DataFrame using DDL schema
  DDL_df = spark.read.schema(DDLSchema).json(jsonFile)

  # Show created dataframe
  programatic_df.show(n=20, truncate=False)
  DDL_df.show(n=20, truncate=False)

  # Print schema
  print(programatic_df.printSchema())
  print(DDL_df.printSchema())
