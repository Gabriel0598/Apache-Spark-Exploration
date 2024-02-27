CREATE DATABASE Sales
   COLLATE Latin1_General_100_BIN2_UTF8;
 GO;

Use Sales;
 GO;

CREATE EXTERNAL DATA SOURCE sales_data WITH (
     LOCATION = 'https://datalakexxxxxxx.dfs.core.windows.net/files/sales/'
 );
 GO;

CREATE EXTERNAL FILE FORMAT CsvFormat
     WITH (
         FORMAT_TYPE = DELIMITEDTEXT,
         FORMAT_OPTIONS(
         FIELD_TERMINATOR = ',',
         STRING_DELIMITER = '"'
         )
     );
 GO;

CREATE EXTERNAL TABLE dbo.orders
 (
     SalesOrderNumber VARCHAR(10),
     SalesOrderLineNumber INT,
     OrderDate DATE,
     CustomerName VARCHAR(25),
     EmailAddress VARCHAR(50),
     Item VARCHAR(30),
     Quantity INT,
     UnitPrice DECIMAL(18,2),
     TaxAmount DECIMAL (18,2)
 )
 WITH
 (
     DATA_SOURCE =sales_data,
     LOCATION = 'csv/*.csv',
     FILE_FORMAT = CsvFormat
 );
 GO

SELECT YEAR(OrderDate) AS OrderYear,
    SUM((UnitPrice * Quantity) + TaxAmount) AS GrossRevenue
FROM dbo.orders
GROUP BY YEAR(OrderDate)
ORDER BY OrderYear;

----------------------------------------------------------------
 -- EXTERNAL DATA
 SELECT *
 FROM
     OPENROWSET(
         BULK 'csv/*.csv',
         DATA_SOURCE = 'sales_data',
         FORMAT = 'CSV',
         PARSER_VERSION = '2.0'
     ) AS orders

      SELECT *
 FROM
     OPENROWSET(
         BULK 'parquet/year=*/*.snappy.parquet',
         DATA_SOURCE = 'sales_data',
         FORMAT='PARQUET'
     ) AS orders
 WHERE orders.filepath(1) = '2019'

 --------------------------------------------------------------------------------------------------------------------------------
%%sql

SELECT * FROM transformed_orders
WHERE Year = 2021
    AND Month = 1

%%sql

CREATE OR REPLACE TABLE MyExternalTable
USING DELTA
LOCATION '/mydata'

%%sql

CREATE TABLE ManagedSalesOrders
(
    Orderid INT NOT NULL,
    OrderDate TIMESTAMP NOT NULL,
    CustomerName STRING,
    SalesTotal FLOAT NOT NULL
)
USING DELTA

%%sql

SELECT orderid, salestotal
FROM ManagedSalesOrders

%%sql

CREATE TABLE DeviceTable
USING DELTA
LOCATION '/delta/devicetable';

SELECT device, status
FROM DeviceTable;
