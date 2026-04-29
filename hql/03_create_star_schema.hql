USE olist_dw;

DROP TABLE IF EXISTS dim_customer;
CREATE TABLE dim_customer (
    customer_key INT, customer_id STRING, customer_unique_id STRING,
    customer_city STRING, customer_state STRING,
    cust_lat DOUBLE, cust_lng DOUBLE
) STORED AS ORC
LOCATION 'hdfs://namenode:9000/user/hive/warehouse/olist_dw/dim_customer'
TBLPROPERTIES ('orc.compress'='SNAPPY');

DROP TABLE IF EXISTS dim_product;
CREATE TABLE dim_product (
    product_key INT, product_id STRING,
    product_category_name STRING, product_category_name_eng STRING
) STORED AS ORC
LOCATION 'hdfs://namenode:9000/user/hive/warehouse/olist_dw/dim_product'
TBLPROPERTIES ('orc.compress'='SNAPPY');

DROP TABLE IF EXISTS dim_seller;
CREATE TABLE dim_seller (
    seller_key INT, seller_id STRING, seller_city STRING,
    seller_state STRING, seller_lat DOUBLE, seller_lng DOUBLE
) STORED AS ORC
LOCATION 'hdfs://namenode:9000/user/hive/warehouse/olist_dw/dim_seller'
TBLPROPERTIES ('orc.compress'='SNAPPY');

DROP TABLE IF EXISTS dim_date;
CREATE TABLE dim_date (
    date_key INT, full_date STRING, year INT, quarter INT,
    month INT, month_name STRING, day INT,
    day_of_week INT, week_of_year INT
) STORED AS ORC
LOCATION 'hdfs://namenode:9000/user/hive/warehouse/olist_dw/dim_date'
TBLPROPERTIES ('orc.compress'='SNAPPY');

DROP TABLE IF EXISTS fact_sales;
CREATE TABLE fact_sales (
    date_key INT, customer_key INT, product_key INT, seller_key INT,
    order_id STRING, price DOUBLE, freight_value DOUBLE,
    item_revenue DOUBLE, total_payment_value DOUBLE, review_score DOUBLE
) STORED AS ORC
LOCATION 'hdfs://namenode:9000/user/hive/warehouse/olist_dw/fact_sales'
TBLPROPERTIES ('orc.compress'='SNAPPY');

SHOW TABLES IN olist_dw;