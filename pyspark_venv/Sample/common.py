from pyspark import SparkConf
from pyspark.sql import SparkSession

def create_spark_session(appname: str) -> SparkSession:
    conf = (
        SparkConf()
            .set("spark.driver.memory", "8g")
            .set("spark.sql.session.timezone", "UTC")
    )

    spark_session = (
        SparkSession
            .builder()
                .master("local[4]")
                    .config(conf=conf)
                        .appName(appname)
                            .getOrCreate()
    )

    return spark_session
