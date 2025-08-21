from flask import Flask, render_template, jsonify
import psycopg2

app = Flask(__name__)

def get_db_connection():
    conn = psycopg2.connect(
        dbname="instacart",
        user="postgres",
        password="postgres",
        host="localhost",
        port="5432"
    )
    return conn

@app.route("/hello")
def index():
    return render_template("index.html")

@app.route("/api/orders_per_department")
def orders_per_department():
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT department, COUNT(*) as total_orders
        FROM orders_fact
        GROUP BY department
        ORDER BY total_orders DESC
    """)
    rows = cur.fetchall()
    cur.close()
    conn.close()

    labels = [row[0] for row in rows]
    values = [row[1] for row in rows]
    return jsonify({"labels": labels, "values": values})

@app.route("/api/top_products")
def top_products():
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT product_name, COUNT(*) as order_count
        FROM orders_fact
        GROUP BY product_name
        ORDER BY order_count DESC
        LIMIT 10
    """)
    rows = cur.fetchall()
    cur.close()
    conn.close()

    labels = [row[0] for row in rows]
    values = [row[1] for row in rows]
    return jsonify({"labels": labels, "values": values})

@app.route("/api/orders_per_day")
def orders_per_day():
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT order_dow, COUNT(*) as total_orders
        FROM orders_fact
        GROUP BY order_dow
        ORDER BY order_dow
    """)
    rows = cur.fetchall()
    cur.close()
    conn.close()

    labels = [str(row[0]) for row in rows]
    values = [row[1] for row in rows]
    return jsonify({"labels": labels, "values": values})

if __name__ == "__main__":
    app.run(debug=True, port=5001)

