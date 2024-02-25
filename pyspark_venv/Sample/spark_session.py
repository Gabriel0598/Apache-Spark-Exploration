from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType

spark = (SparkSession.builder()
            .master("local")
            .appName("spark basic")
            .getOrCreate())

emptyRDD = spark.sparkContext.emptyRDD()
print(emptyRDD)

schema = StructType([
  StructField('firstname', StringType(), True),
  StructField('middlename', StringType(), True),
  StructField('lastname', StringType(), True)
  ])

df = spark.createDataframe(emptyRDD, schema)
df.printSchema()
