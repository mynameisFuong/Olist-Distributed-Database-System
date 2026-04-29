from __future__ import annotations
from datetime import datetime, timedelta
import subprocess

from airflow import DAG
from airflow.operators.python import PythonOperator

HIVE_HOST    = "hive-server"
HIVE_PORT    = 10000
NAMENODE     = "namenode"
HDFS_RAW_DIR = "/user/hive/warehouse/olist_raw"
CSV_PATH     = "/opt/airflow/etl/data/fact_olist_orders.csv"

DEFAULT_ARGS = {
    "owner": "olist",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}


# ── Task 1 ────────────────────────────────────────────────────────────
def upload_to_hdfs():
    """Upload CSV lên HDFS qua WebHDFS REST API."""
    import requests
    import os

    csv_path  = CSV_PATH
    hdfs_dir  = HDFS_RAW_DIR
    filename  = "fact_olist_orders.csv"
    webhdfs   = f"http://namenode:9870/webhdfs/v1"

    # Bước 1: Tạo thư mục trên HDFS
    requests.put(
        f"{webhdfs}{hdfs_dir}?op=MKDIRS&user.name=root"
    )

    # Bước 2: Upload file (2 bước theo WebHDFS protocol)
    # Bước 2a: Gửi request CREATE để lấy redirect URL
    r = requests.put(
        f"{webhdfs}{hdfs_dir}/{filename}?op=CREATE"
        "&user.name=root&overwrite=true",
        allow_redirects=False
    )
    # Bước 2b: Upload thực sự lên DataNode URL
    upload_url = r.headers["Location"]
    with open(csv_path, "rb") as f:
        resp = requests.put(
            upload_url,
            data=f,
            headers={"Content-Type": "application/octet-stream"}
        )
    assert resp.status_code == 201, f"Upload failed: {resp.text}"
    print(f"Upload OK -> {hdfs_dir}/{filename}")


# ── Task 2 ────────────────────────────────────────────────────────────
def create_raw_table():
    from pyhive import hive
    conn = hive.Connection(
        host=HIVE_HOST, port=HIVE_PORT,
        username="root", auth="NONE"
    )
    cur = conn.cursor()

    # Local mode
    for cfg in [
        "SET mapreduce.framework.name=local",
        "SET hive.exec.mode.local.auto=true",
        "SET hive.exec.mode.local.auto.inputbytes.max=1073741824",
    ]:
        cur.execute(cfg)

    cur.execute(
        "CREATE DATABASE IF NOT EXISTS olist_dw "
        "LOCATION 'hdfs://namenode:9000/user/hive/warehouse/olist_dw'"
    )
    cur.execute("DROP TABLE IF EXISTS olist_dw.raw_olist_orders")
    cur.execute("""
        CREATE EXTERNAL TABLE olist_dw.raw_olist_orders (
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
        TBLPROPERTIES ('skip.header.line.count'='1')
    """)

    # Dùng LIMIT thay COUNT để tránh MapReduce job
    cur.execute("SELECT order_id FROM olist_dw.raw_olist_orders LIMIT 1")
    row = cur.fetchone()
    assert row is not None, "Raw table rỗng!"
    print(f"Raw table OK - sample order_id: {row[0]}")

    cur.close()
    conn.close()


# ── Task 3 ────────────────────────────────────────────────────────────
def create_star_schema():
    from pyhive import hive
    conn = hive.Connection(
        host=HIVE_HOST, port=HIVE_PORT,
        username="root", database="olist_dw", auth="NONE"
    )
    cur = conn.cursor()
    ddls = {
        "dim_customer": """
            CREATE TABLE IF NOT EXISTS olist_dw.dim_customer (
                customer_key INT, customer_id STRING,
                customer_unique_id STRING, customer_city STRING,
                customer_state STRING, cust_lat DOUBLE, cust_lng DOUBLE
            ) STORED AS ORC
            LOCATION 'hdfs://namenode:9000/user/hive/warehouse/olist_dw/dim_customer'
            TBLPROPERTIES ('orc.compress'='SNAPPY')
        """,
        "dim_product": """
            CREATE TABLE IF NOT EXISTS olist_dw.dim_product (
                product_key INT, product_id STRING,
                product_category_name STRING, product_category_name_eng STRING
            ) STORED AS ORC
            LOCATION 'hdfs://namenode:9000/user/hive/warehouse/olist_dw/dim_product'
            TBLPROPERTIES ('orc.compress'='SNAPPY')
        """,
        "dim_seller": """
            CREATE TABLE IF NOT EXISTS olist_dw.dim_seller (
                seller_key INT, seller_id STRING, seller_city STRING,
                seller_state STRING, seller_lat DOUBLE, seller_lng DOUBLE
            ) STORED AS ORC
            LOCATION 'hdfs://namenode:9000/user/hive/warehouse/olist_dw/dim_seller'
            TBLPROPERTIES ('orc.compress'='SNAPPY')
        """,
        "dim_date": """
            CREATE TABLE IF NOT EXISTS olist_dw.dim_date (
                date_key INT, full_date STRING, year INT, quarter INT,
                month INT, month_name STRING, day INT,
                day_of_week INT, week_of_year INT
            ) STORED AS ORC
            LOCATION 'hdfs://namenode:9000/user/hive/warehouse/olist_dw/dim_date'
            TBLPROPERTIES ('orc.compress'='SNAPPY')
        """,
        "fact_sales": """
            CREATE TABLE IF NOT EXISTS olist_dw.fact_sales (
                date_key INT, customer_key INT, product_key INT,
                seller_key INT, order_id STRING, price DOUBLE,
                freight_value DOUBLE, item_revenue DOUBLE,
                total_payment_value DOUBLE, review_score DOUBLE
            ) STORED AS ORC
            LOCATION 'hdfs://namenode:9000/user/hive/warehouse/olist_dw/fact_sales'
            TBLPROPERTIES ('orc.compress'='SNAPPY')
        """,
    }
    for name, ddl in ddls.items():
        cur.execute(ddl.strip())
        print(f"Created: {name}")
    cur.close(); conn.close()


# ── Task 4 ────────────────────────────────────────────────────────────
def run_etl():
    import sys
    sys.path.insert(0, "/opt/airflow/etl")
    from etl_load import run_etl as _etl
    _etl()


# ── Task 5 ────────────────────────────────────────────────────────────
def verify_data():
    from pyhive import hive
    conn = hive.Connection(
        host=HIVE_HOST, port=HIVE_PORT,
        username="root", database="olist_dw", auth="NONE"
    )
    cur = conn.cursor()

    for cfg in [
        "SET mapreduce.framework.name=local",
        "SET hive.exec.mode.local.auto=true",
        "SET hive.exec.mode.local.auto.inputbytes.max=1073741824",
    ]:
        cur.execute(cfg)

    # Dùng LIMIT 1 thay COUNT(*) để tránh MapReduce
    tables = ["dim_date", "dim_customer", "dim_product", "dim_seller", "fact_sales"]
    for table in tables:
        cur.execute(f"SELECT * FROM olist_dw.{table} LIMIT 1")
        row = cur.fetchone()
        status = "OK" if row else "FAIL"
        print(f"[{status}] {table}: {'has data' if row else 'EMPTY!'}")
        assert row is not None, f"{table} rỗng!"

    cur.close()
    conn.close()
    print("Verify OK!")


# ── DAG ───────────────────────────────────────────────────────────────
with DAG(
    dag_id="olist_etl_pipeline",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2024, 1, 1),
    schedule_interval="0 2 * * *",
    catchup=False,
    tags=["olist", "etl"],
) as dag:

    t1 = PythonOperator(task_id="upload_to_hdfs",    python_callable=upload_to_hdfs)
    t2 = PythonOperator(task_id="create_raw_table",  python_callable=create_raw_table)
    t3 = PythonOperator(task_id="create_star_schema",python_callable=create_star_schema)
    t4 = PythonOperator(task_id="run_etl",           python_callable=run_etl,
                        execution_timeout=timedelta(hours=1))
    t5 = PythonOperator(task_id="verify_data",       python_callable=verify_data)

    t1 >> t2 >> t3 >> t4 >> t5