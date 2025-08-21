import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import pyspark.sql.types as T

# ================== Spark Session ==================
def create_spark_session():
    return SparkSession.builder \
        .appName("InstacartTransform") \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "8g") \
        .getOrCreate()

# ================== Load and Transform ==================
def transform_data(spark, input_dir, output_dir):
    raw_dir = input_dir
    os.makedirs(output_dir, exist_ok=True)

    # ===== Schemas =====
    orders_schema = T.StructType([
        T.StructField("order_id", T.IntegerType(), True),
        T.StructField("user_id", T.IntegerType(), True),
        T.StructField("eval_set", T.StringType(), True),
        T.StructField("order_number", T.IntegerType(), True),
        T.StructField("order_dow", T.IntegerType(), True),
        T.StructField("order_hour_of_day", T.IntegerType(), True),
        T.StructField("days_since_prior_order", T.FloatType(), True)
    ])

    order_products_schema = T.StructType([
        T.StructField("order_id", T.IntegerType(), True),
        T.StructField("product_id", T.IntegerType(), True),
        T.StructField("add_to_cart_order", T.IntegerType(), True),
        T.StructField("reordered", T.IntegerType(), True)
    ])

    products_schema = T.StructType([
        T.StructField("product_id", T.IntegerType(), True),
        T.StructField("product_name", T.StringType(), True),
        T.StructField("aisle_id", T.IntegerType(), True),
        T.StructField("department_id", T.IntegerType(), True)
    ])

    aisles_schema = T.StructType([
        T.StructField("aisle_id", T.IntegerType(), True),
        T.StructField("aisle", T.StringType(), True)
    ])

    departments_schema = T.StructType([
        T.StructField("department_id", T.IntegerType(), True),
        T.StructField("department", T.StringType(), True)
    ])

    # ===== Load CSVs =====
    orders = spark.read.csv(os.path.join(raw_dir, "orders.csv"), header=True, schema=orders_schema)
    order_products_prior = spark.read.csv(os.path.join(raw_dir, "order_products__prior.csv"), header=True, schema=order_products_schema)
    order_products_train = spark.read.csv(os.path.join(raw_dir, "order_products__train.csv"), header=True, schema=order_products_schema)
    products = spark.read.csv(os.path.join(raw_dir, "products.csv"), header=True, schema=products_schema)
    aisles = spark.read.csv(os.path.join(raw_dir, "aisles.csv"), header=True, schema=aisles_schema)
    departments = spark.read.csv(os.path.join(raw_dir, "departments.csv"), header=True, schema=departments_schema)

    # ===== Combine prior + train order products =====
    order_products = order_products_prior.union(order_products_train)

    # ===== Join Orders with Order Products =====
    orders_fact = order_products.join(orders, on="order_id", how="inner")

    # ===== Join with Products =====
    orders_fact = orders_fact.join(products, on="product_id", how="inner")

    # ===== Join with Aisles & Departments =====
    orders_fact = orders_fact.join(aisles, on="aisle_id", how="left") \
                             .join(departments, on="department_id", how="left")

    # ===== Create Dimension Tables =====
    products_dim = products.join(aisles, on="aisle_id", how="left") \
                           .join(departments, on="department_id", how="left")

    aisles_dim = aisles
    departments_dim = departments

    # ===== Save as Parquet =====
    orders_fact.write.mode("overwrite").parquet(os.path.join(output_dir, "orders_fact"))
    products_dim.write.mode("overwrite").parquet(os.path.join(output_dir, "products_dim"))
    aisles_dim.write.mode("overwrite").parquet(os.path.join(output_dir, "aisles_dim"))
    departments_dim.write.mode("overwrite").parquet(os.path.join(output_dir, "departments_dim"))

    print("Transformation Completed: Parquet files created.")

# ================== Main ==================
if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python transform/execute.py <input_dir> <output_dir>")
        sys.exit(1)

    input_dir = sys.argv[1]
    output_dir = sys.argv[2]

    spark = create_spark_session()
    transform_data(spark, input_dir, output_dir)
    spark.stop()
