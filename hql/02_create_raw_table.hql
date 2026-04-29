CREATE DATABASE IF NOT EXISTS olist_dw
LOCATION 'hdfs://namenode:9000/user/hive/warehouse/olist_dw';

USE olist_dw;
DROP TABLE IF EXISTS raw_olist_orders;

CREATE EXTERNAL TABLE raw_olist_orders (
    order_id STRING, order_purchase_timestamp STRING,
    customer_unique_id STRING, customer_id STRING,
    product_id STRING, product_category_name STRING,
    product_category_name_eng STRING, seller_id STRING,
    price DOUBLE, freight_value DOUBLE, item_revenue DOUBLE,
    total_payment_value DOUBLE, review_score DOUBLE,
    cust_lat DOUBLE, cust_lng DOUBLE, cust_city STRING,
    cust_state STRING, customer_city STRING, customer_state STRING,
    seller_lat DOUBLE, seller_lng DOUBLE,
    seller_city STRING, seller_state STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 'hdfs://namenode:9000/user/hive/warehouse/olist_raw'
TBLPROPERTIES ('skip.header.line.count'='1');

SET hive.exec.mode.local.auto=true;
SELECT COUNT(*) AS total_rows FROM raw_olist_orders;