//CREATE DATABASE AND FILE FORMAT
CREATE OR REPLACE DATABASE SALES_DATA_ETL;
USE DATABASE SALES_DATA_ETL;
USE SCHEMA SALES;

CREATE OR REPLACE FILE FORMAT CSV_FF
TYPE = 'CSV'
FIELD_OPTIONALLY_ENCLOSED_BY = '"'
SKIP_HEADER = 1;

//Create Internal Stage to load the data
CREATE OR REPLACE STAGE SALES_DATA_STAGE
FILE_FORMAT = 'CSV_FF';

//visualizing the data available in the stage
SELECT $1, $2, $3,$4,$5,$6,$7,$8, $9,$10, $11, $12, $13, $14, $15, $16, $17, $18
FROM @SALES_DATA_STAGE/train.csv;

//creating train_dataset table and copying data
Create or replace table sales_dataset("Row ID","Order ID","Order Date","Ship Date","Ship Mode","Customer ID","Customer Name","Segment","Country","City","State",	"Postal Code","Region","Product ID",	"Category","Sub-Category","Product Name",	"Sales")
as SELECT $1, $2, $3,$4,$5,$6,$7,$8, $9,$10, $11, $12, $13,$14,$15,$16,$17,$18 
from @SALES_DATA_STAGE/train.csv(file_format => 'CSV_FF') ;

SELECT *
FROM SALES_DATASET;

//CREATE TABLE SHIPMENT_ID_DIM AND AUTOINCREMENT VALUES IN SHIPMENT_ID
CREATE TABLE SHIPMENT_ID_DIM (
"Shipment_ID" NUMBER AUTOINCREMENT START 1000, 
"Order Date" DATE,
"Ship Date" DATE,
"Ship Mode" VARCHAR);


//COPY ROWS FROM SALES_DATASET TO SHIPMENT_ID_DIM
INSERT INTO SHIPMENT_ID_DIM ("Order Date", "Ship Mode", "Ship Date")
SELECT TO_DATE("Order Date", 'DD/MM/YYYY') AS "Order Date",
       "Ship Mode",
       TO_DATE("Ship Date", 'DD/MM/YYYY') AS "Ship Date"
FROM SALES_DATASET;

SELECT *
FROM SHIPMENT_ID_DIM;

//CREATE TABLE CUSTOMER_ID_DIM
CREATE TABLE CUSTOMER_ID_DIM("Customer ID" VARCHAR ,"Customer Name" VARCHAR);

//COPY UNIQUE RECORDS FROM SALES_DATASET TO CUSTOMER_ID_DIM
INSERT INTO CUSTOMER_ID_DIM ("Customer ID", "Customer Name")
SELECT DISTINCT "Customer ID","Customer Name"
FROM SALES_DATASET;


//Create the table LOCATION_ID_DIM
CREATE TABLE LOCATION_ID_DIM (
  "Postal_Code" STRING,
  "Region" STRING,
  "City" STRING,
  "State" STRING,
  "Country" STRING
);

//Insert distinct values into the new table
INSERT INTO LOCATION_ID_DIM ("Postal_Code", "Region", "City", "State", "Country")
SELECT DISTINCT "Postal Code", "Region", "City", "State", "Country"
FROM SALES_DATASET;


SELECT * FROM FACT_TABLE

//CREATE TABLE PRODUCT_ID_DIM
CREATE TABLE PRODUCT_ID_DIM ("Product_ID" STRING,
"Category" STRING,
"Sub_Category" STRING,
"Product_Name" STRING)

//INSERT UNIQUE RECORDS FROM SALES_DATASET INTO PRODUCT_ID_DIM
INSERT INTO PRODUCT_ID_DIM ("Product_ID",
"Category" ,
"Sub_Category" ,
"Product_Name" )
SELECT DISTINCT "Product ID", "Category", "Sub-Category", "Product Name"
FROM SALES_DATASET;

//CREATE TABLE FACT_TABLE
CREATE TABLE FACT_TABLE ("Order_ID" STRING,
"Shipment_ID" NUMBER AUTOINCREMENT START 1000,
"Customer_ID" STRING,
"Postal_Code" STRING,
"Product_ID" STRING,
"SALES" NUMBER);

//INSERT DATA FROM SALES_DATASET
INSERT INTO FACT_TABLE ("Order_ID", "Customer_ID", "Postal_Code", "Product_ID", "SALES")
SELECT "Order ID", "Customer ID", "Postal Code", "Product ID", "Sales"
FROM SALES_DATASET;


SELECT *
FROM FACT_TABLE;

//Performing LEFT JOIN TO Connect CUSTOMER_ID_DIM with FACT_TABLE
SELECT CUSTOMER_ID_DIM."Customer ID", CUSTOMER_ID_DIM."Customer Name"
FROM CUSTOMER_ID_DIM
LEFT JOIN FACT_TABLE
ON CUSTOMER_ID_DIM."Customer ID" = FACT_TABLE."Customer_ID";

//Performing LEFT JOIN TO Connect SHIPMENT_ID_DIM with FACT_TABLE
SELECT SHIPMENT_ID_DIM."Shipment_ID", SHIPMENT_ID_DIM."Order Date", SHIPMENT_ID_DIM."Ship Date", SHIPMENT_ID_DIM."Ship Mode"
FROM SHIPMENT_ID_DIM
LEFT JOIN FACT_TABLE
ON SHIPMENT_ID_DIM."Shipment_ID" = FACT_TABLE."Shipment_ID";

//Performing LEFT JOIN TO Connect LOCATION_ID_DIM with FACT_TABLE
SELECT LOCATION_ID_DIM."Postal_Code", LOCATION_ID_DIM."Region", LOCATION_ID_DIM."City", LOCATION_ID_DIM."State", LOCATION_ID_DIM."Country"
FROM LOCATION_ID_DIM
LEFT JOIN FACT_TABLE
ON LOCATION_ID_DIM."Postal_Code" = FACT_TABLE."Postal_Code";

//Performing LEFT JOIN TO Connect PRODUCT_ID_DIM with FACT_TABLE
SELECT PRODUCT_ID_DIM."Product_ID", PRODUCT_ID_DIM."Category", PRODUCT_ID_DIM."Sub_Category", PRODUCT_ID_DIM."Product_Name"
FROM PRODUCT_ID_DIM
LEFT JOIN FACT_TABLE
ON PRODUCT_ID_DIM."Product_ID" = FACT_TABLE."Product_ID";