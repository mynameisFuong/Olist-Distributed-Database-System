from pyhive import hive
import logging
import os

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)

HIVE_HOST = os.getenv("HIVE_HOST", "hive-server")  # đọc từ env, mặc định hive-server
HIVE_PORT = int(os.getenv("HIVE_PORT", "10000"))

def get_conn():
    return hive.Connection(
        host=HIVE_HOST, port=HIVE_PORT,
        username="root", database="olist_dw", auth="NONE"
    )


def run(cur, sql, label):
    log.info(f">> {label}")
    cur.execute(sql)
    log.info(f"   Done: {label}")


ETL_STEPS = [
    ("Load dim_date", """
        INSERT OVERWRITE TABLE dim_date
        SELECT DISTINCT
            CAST(DATE_FORMAT(TO_DATE(order_purchase_timestamp),'yyyyMMdd') AS INT),
            TO_DATE(order_purchase_timestamp),
            YEAR(order_purchase_timestamp), QUARTER(order_purchase_timestamp),
            MONTH(order_purchase_timestamp),
            DATE_FORMAT(order_purchase_timestamp,'MMMM'),
            DAY(order_purchase_timestamp), DAYOFWEEK(order_purchase_timestamp),
            WEEKOFYEAR(order_purchase_timestamp)
        FROM raw_olist_orders
        WHERE order_purchase_timestamp IS NOT NULL
          AND order_purchase_timestamp != ''
    """),
    ("Load dim_customer", """
        INSERT OVERWRITE TABLE dim_customer
        SELECT ROW_NUMBER() OVER (ORDER BY customer_id),
               customer_id, customer_unique_id, customer_city,
               customer_state, cust_lat, cust_lng
        FROM (
            SELECT customer_id, customer_unique_id, customer_city,
                   customer_state, cust_lat, cust_lng,
                   ROW_NUMBER() OVER (PARTITION BY customer_id
                       ORDER BY order_purchase_timestamp DESC) AS rn
            FROM raw_olist_orders
            WHERE customer_id IS NOT NULL AND customer_id != ''
        ) t WHERE rn = 1
    """),
    ("Load dim_product", """
        INSERT OVERWRITE TABLE dim_product
        SELECT ROW_NUMBER() OVER (ORDER BY product_id),
               product_id, product_category_name, product_category_name_eng
        FROM (
            SELECT product_id, product_category_name, product_category_name_eng,
                   ROW_NUMBER() OVER (PARTITION BY product_id
                       ORDER BY order_purchase_timestamp DESC) AS rn
            FROM raw_olist_orders
            WHERE product_id IS NOT NULL AND product_id != ''
        ) t WHERE rn = 1
    """),
    ("Load dim_seller", """
        INSERT OVERWRITE TABLE dim_seller
        SELECT ROW_NUMBER() OVER (ORDER BY seller_id),
               seller_id, seller_city, seller_state, seller_lat, seller_lng
        FROM (
            SELECT seller_id, seller_city, seller_state, seller_lat, seller_lng,
                   ROW_NUMBER() OVER (PARTITION BY seller_id
                       ORDER BY order_purchase_timestamp DESC) AS rn
            FROM raw_olist_orders
            WHERE seller_id IS NOT NULL AND seller_id != ''
        ) t WHERE rn = 1
    """),
    ("Load fact_sales", """
        INSERT OVERWRITE TABLE fact_sales
        SELECT
            CAST(DATE_FORMAT(TO_DATE(r.order_purchase_timestamp),'yyyyMMdd') AS INT),
            dc.customer_key, dp.product_key, ds.seller_key,
            r.order_id, r.price, r.freight_value, r.item_revenue,
            r.total_payment_value, r.review_score
        FROM raw_olist_orders r
        JOIN dim_customer dc ON dc.customer_id = r.customer_id
        JOIN dim_product  dp ON dp.product_id  = r.product_id
        JOIN dim_seller   ds ON ds.seller_id   = r.seller_id
        WHERE r.order_id IS NOT NULL
          AND r.order_purchase_timestamp IS NOT NULL
    """),
]


def run_etl():
    log.info("=== ETL bắt đầu ===")
    conn = get_conn()
    cur = conn.cursor()
    try:
        for cfg in [
            "SET mapreduce.framework.name=local",
            "SET hive.exec.mode.local.auto=true",
            "SET hive.exec.mode.local.auto.inputbytes.max=1073741824",
            "SET hive.exec.mode.local.auto.input.files.max=10",
            "SET hive.exec.dynamic.partition=true",
            "SET hive.exec.dynamic.partition.mode=nonstrict",
            "SET hive.vectorized.execution.enabled=true",
            "SET hive.auto.convert.join=true",
        ]:
            cur.execute(cfg)

        for label, sql in ETL_STEPS:
            run(cur, sql.strip(), label)

        # Verify
        cur.execute("""
            SELECT 'dim_date', COUNT(*) FROM dim_date UNION ALL
            SELECT 'dim_customer', COUNT(*) FROM dim_customer UNION ALL
            SELECT 'dim_product', COUNT(*) FROM dim_product UNION ALL
            SELECT 'dim_seller', COUNT(*) FROM dim_seller UNION ALL
            SELECT 'fact_sales', COUNT(*) FROM fact_sales
        """)
        log.info("=== Kết quả ===")
        for row in cur.fetchall():
            log.info(f"  {row[0]:<15}: {row[1]:>8} rows")

        log.info("=== ETL hoàn thành ===")
    except Exception as e:
        log.error(f"ETL lỗi: {e}")
        raise
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    run_etl()