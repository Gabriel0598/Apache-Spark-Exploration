from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import lit, size, col

# Create a SparkSession
spark = (SparkSession
            .builder
                .master("local[*]")
                    .appName("Create Item Columns")
                        .getOrCreate())

# Function to add columns with array items
def Add_Item_Columns(df: DataFrame, column: str) -> DataFrame:
    """
    Add a new column for each item in the array column.

    :param df: Source DataFrame
    :param column: Column with the array
    :return: DataFrame with new columns
    """
    array_size = df.select(size(col(column)).alias("array_size")).rdd.max()["array_size"]
    for i in range(array_size):
        df = df.withColumn(f"{column}_item_{i}", col(column).getItem(i))
    return df

# Source dataframe that will be extracted, could be read from table, parque, avro, etc...
df_source = spark.read.table("db_src.tb_src")

# It's necessary convert source dataframe to df in order to apply in function
df = df_source

# Apply the function to each column
columns_to_transform = ['col1', 'col2', 'col3', 'etc']
for column in columns_to_transform:
    df = Add_Item_Columns(df, column)

# This new schema should have the new columns
df.printSchema()

# Get positions from index
def Index_Item_Columns(df: DataFrame, column: str) -> DataFrame:
    """
    Add new columns for each item in the array column and its position index.

    :param df: Source DataFrame
    :param column: Column with the array
    :return: DataFrame with new columns
    """
    array_size = df.select(size(col(column)).alias("array_size")).rdd.max()["array_size"]
    for i in range(array_size):
        df = df.withColumn(f"{column}_item_{i}", col(column).getItem(i))
        df = df.withColumn(f"position_index_{i}", lit(i))
    return df

