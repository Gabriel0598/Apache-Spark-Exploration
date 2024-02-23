from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark_venv.Sample.common import create_spark_session

data_jan_24 = "Data/jan_24/*.parquet"
data_set_23 = "Data/set_23/*.parquet"

def analyze_data(spark: SparkSession):
    data = spark.read.parquet(data_set_23).repartition(4)

    print(f"The data contains: {data.count()} rows")

    aggregted_by_pickup_location = (
        data.groupBy("PULocationID")
        .agg(
            F.count(F.lit(1)).alias("num_rows"),
            F.avg("tip_amount").alias("avg_tip")
        )
        .filter(F.col("num_rows") > 20)
    )

    aggregted_by_pickup_location.sort(F.col("avg_tip").desc()).show(truncate = False, n=10)
    aggregted_by_pickup_location.write.option("header", "true").csv("output")

if __name__ == '__main__':
    spark = create_spark_session("test job")
    analyze_data(spark=spark)
