from pyspark.sql import SparkSession
from pyspark.sql.functions import size

# Create a SparkSession
spark = (SparkSession
            .builder
                .master("local[*]")
                    .appName("Get length from a column")
                        .getOrCreate())

# Source dataframe.
df_source = spark.read.table("db_src.tb_src")

df_length = df_source.withColumn("length_col", size(df_source['col_src']))
df_length.printSchema()
