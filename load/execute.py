import os
import sys
import time
import psycopg2
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from utility.utility import setup_logging, format_time

logger = setup_logging("load.log")

# ================== Spark Session ==================
def create_spark_session():
    """Initialize Spark session with PostgreSQL JDBC driver."""
    try:
        return SparkSession.builder \
            .appName("InstacartDataLoad") \
            .config(
                "spark.jars",
                "/home/sabin-adhikari/Workspace/pyspark/venv/lib/python3.12/site-packages/pyspark/jars/postgresql-42.7.7.jar"
            ) \
            .getOrCreate()
    except Exception as e:
        logger.error(f"Failed to create Spark session: {e}")
        raise

# ================== Create Tables ==================
def create_postgres_tables(pg_un, pg_pw):
    """Create PostgreSQL tables matching the transformed Parquet schema."""
    try: 
        conn = psycopg2.connect(
            dbname="instacart",
            user=pg_un,
            password=pg_pw,
            host="localhost",
            port="5432"
        )
        conn.autocommit = True
        
        with conn.cursor() as cursor:
            create_table_queries = [
                """CREATE TABLE IF NOT EXISTS orders_fact (
                    order_id INT,
                    user_id INT,
                    eval_set VARCHAR(20),
                    order_number INT,
                    order_dow INT,
                    order_hour_of_day INT,
                    days_since_prior_order FLOAT,
                    product_id INT,
                    add_to_cart_order INT,
                    reordered INT,
                    product_name TEXT,
                    aisle_id INT,
                    department_id INT,
                    aisle VARCHAR(255),
                    department VARCHAR(255)
                );""",
                """CREATE TABLE IF NOT EXISTS products_dim (
                    product_id INT PRIMARY KEY,
                    product_name TEXT,
                    aisle_id INT,
                    department_id INT,
                    aisle VARCHAR(255),
                    department VARCHAR(255)
                );""",
                """CREATE TABLE IF NOT EXISTS aisles_dim (
                    aisle_id INT PRIMARY KEY,
                    aisle VARCHAR(255)
                );""",
                """CREATE TABLE IF NOT EXISTS departments_dim (
                    department_id INT PRIMARY KEY,
                    department VARCHAR(255)
                );"""
            ]

            for query in create_table_queries:
                cursor.execute(query)
            logger.info("Tables created successfully")
    except Exception as e:
        logger.error(f"Error creating tables: {e}")
        raise
    finally:
        if conn:
            conn.close()

# ================== Load Parquet Data ==================
def load_to_postgres(spark, input_dir, pg_un, pg_pw):
    """Load Parquet files into PostgreSQL via JDBC."""
    jdbc_url = "jdbc:postgresql://localhost:5432/instacart"
    connection_properties = {
        "user": pg_un,
        "password": pg_pw,
        "driver": "org.postgresql.Driver"
    }

    tables = [
        ("orders_fact", "orders_fact"),
        ("products_dim", "products_dim"),
        ("aisles_dim", "aisles_dim"),
        ("departments_dim", "departments_dim")
    ]

    for parquet_name, table_name in tables:
        try:
            full_path = os.path.join(input_dir, parquet_name)
            if not os.path.exists(full_path):
                logger.warning(f"File not found: {full_path}")
                continue

            df = spark.read.parquet(full_path)
            df.write \
                .mode("overwrite") \
                .jdbc(url=jdbc_url, table=table_name, properties=connection_properties)
            logger.info(f"Successfully loaded {table_name}")
        except Exception as e:
            logger.error(f"Error loading {table_name}: {str(e)}")
    return True

# ================== Main ==================
if __name__ == "__main__":
    try:
        if len(sys.argv) < 2:
            logger.error("Usage: python load/execute.py <input_dir> [pg_un] [pg_pw]")
            sys.exit(1)

        input_dir = sys.argv[1]
        pg_un = sys.argv[2] if len(sys.argv) > 2 else "postgres"
        pg_pw = sys.argv[3] if len(sys.argv) > 3 else "postgres"

        if not os.path.isdir(input_dir):
            logger.error(f"Invalid directory: {input_dir}")
            sys.exit(1)
        
        logger.info("ETL Load process started")
        start_time = time.time()

        spark = create_spark_session()
        
        create_postgres_tables(pg_un, pg_pw)
        load_to_postgres(spark, input_dir, pg_un, pg_pw)
            
        logger.info(f"\nETL Load completed in {format_time(time.time() - start_time)}")

    except Exception as e:
        logger.error(f"ETL Load failed: {str(e)}")
        sys.exit(1)
    finally:
        if 'spark' in locals():
            spark.stop()
            logger.info("Spark stopped")