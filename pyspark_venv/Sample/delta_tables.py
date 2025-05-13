from delta.tables import *
from pyspark.sql.functions import col, expr
from pyspark import SparkConf
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder.master("local[4]")
    .config("spark.driver.memory", "8g")
    .config("spark.sql.session.timezone", "UTC")
    .appName("DeltaTableExample")
    .getOrCreate()
)

# Disable Optimize Write at the Spark session level
spark.conf.set("spark.microsoft.delta.optimizeWrite.enabled", False)

# Enable Optimize Write at the Spark session level
spark.conf.set("spark.microsoft.delta.optimizeWrite.enabled", True)

print(spark.conf.get("spark.microsoft.delta.optimizeWrite.enabled"))

# Load a file into a dataframe
df = spark.read.load("Files/mydata.csv", format="csv", header=True)

# Save the dataframe as a Delta table
df.write.format("delta").saveAsTable("my_delta_table")
df.write.format("delta").saveAsTable("my_delta_table", path="Files/my_delta_table")
df.write.format("delta").saveAsTable(
    "my_delta_table",
    path="abfss://<container_name>@<storage_account_name>.dfs.core.windows.net/<path>",
)

DeltaTable = df

(
    DeltaTable.create(spark)
    .tableName("products")
    .addColumn("Productid", "INT")
    .addColumn("ProductName", "STRING")
    .addColumn("Category", "STRING")
    .addColumn("Price", "FLOAT")
    .execute()
)

spark.sql(
    """CREATE TABLE salesorders
          (OrderID INT NOT NULL
          ,OrderDate TIMESTAMP NOT NULL
          ,CustomerName STRING
          ,SalesTotal FLOAT NOT NULL)
          USING DELTA"""
)

spark.sql(
    """CREATE TABLE MyExternalTable
          USING DELTA
          LOCATION 'abfss://<container_name>@<storage_account_name>.dfs.core.windows.net/<path>'
          """
)

delta_path = (
    "abfss://<container_name>@<storage_account_name>.dfs.core.windows.net/<path>"
)
df.write.format("delta").save(delta_path)

new_rows_df = df.withColumn("unit_price", df["Price"] * 1.1)
new_rows_df.write.format("delta").mode("append").save(delta_path)

# Clean up logs
spark.sql("VACUUM lakehouse2.products RETAIN 168 HOURS")
spark.sql("DESCRIBE HISTORY lakehouse2.products")

# Partitioning
df.write.format("delta").partitionBy("Category").saveAsTable(
    "partitioned_products", path="abfs_path/partitioned_products"
)

spark.sql(
    """
          CREATE TABLE partitioned_products (
              ProductID INTEGER,
              ProductName STRING,
              Category STRING,
              ListPrice DOUBLE
              )
            PARTITIONED BY (Category)
          """
)

# Delta tables
spark.sql("INSERT INTO products VALUES (1, 'Widget', 'Accessories', 2.99)")
spark.sql("UPDATE products SET Price = 2.49 WHERE ProductId = 1")

# Delta API
# Create delta object
delta_path = "Files/my_delta_table"
deltaTable = DeltaTable.forPath(spark, delta_path)

# Update delta table (reduce price of acessories by 10%)
deltaTable.alias("products").update(
    condition="Category = 'Accessories'", set={"Price": expr("Price * 0.9")}
)

# Time travel
spark.sql("DESCRIBE HISTORY products")
spark.sql("DESCRIBE HISTORY products 'Files/my_delta_table' ")
df = spark.read.format("delta").option("versionAsOf", 0).load(delta_path)
df = (
    spark.read.format("delta")
    .option("timestampAsOf", "2023-10-01 12:00:00")
    .load(delta_path)
)

# Streaming
spark.sql(
    """
          CREATE TABLE orders_in
          (
          OrderID INT,
          OrderDate DATE,
          Customer STRING,
          Product STRING,
          Quantity INT,
          Price DECIMAL
          ) USING DELTA
    """
)

spark.sql(
    '''
    INSERT INTO orders_in (OrderID, OrderDate, Customer, Product, Quantity, Price)
    VALUES
    (3001, '2024-09-01', 'Yang', 'Road Bike Red', 1, 1200),
    (3002, '2024-09-01', 'Carlson', 'Mountain Bike Silver', 1, 1500),
    (3003, '2024-09-02', 'Wilson', 'Road Bike Yellow', 2, 1350),
    (3004, '2024-09-02', 'Yang', 'Road Front Wheel', 1, 115),
    (3005, '2024-09-02', 'Rai', 'Mountain Bike Black', 1, NULL)
    '''
)

df = spark.read.format("delta").table("orders_in")
display(df)

# Load a streaming DataFrame from the Delta table
stream_df = spark.readStream.format("delta") \
    .option("ignoreChanges", "true") \
    .table("orders_in")

# Verify that the stream is streaming
stream_df.isStreaming

# Transform the streaming DataFrame
transformed_df = stream_df.filter(col("Price").isNotNull()) \
    .withColumn('IsBike', expr("INSTR(Product, 'Bike') > 0").cast('int')) \
    .withColumn('Total', expr("Quantity * Price").cast('decimal'))

# Write the stream to a delta table
output_table_path = 'Tables/orders_processed'
checkpointpath = 'Files/delta/checkpoint'
deltastream = transformed_df.writeStream.format("delta").option("checkpointLocation", checkpointpath).start(output_table_path)

print("Streaming to orders_processed...")

spark.sql(
    '''
    SELECT *
    FROM orders_processed
    ORDER BY OrderID
    '''
)

# Stop the streaming data to avoid excessive processing costs
deltastream.stop()