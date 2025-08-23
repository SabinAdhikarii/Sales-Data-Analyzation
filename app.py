from flask import Flask, render_template, jsonify, request
import os
import re
import psycopg2
from datetime import datetime
from pathlib import Path

app = Flask(__name__)

# ----------------------------- Config -----------------------------
PG_DB = os.getenv("PG_DB", "instacart")
PG_USER = os.getenv("PG_USER", "postgres")
PG_PW = os.getenv("PG_PW", "postgres")
PG_HOST = os.getenv("PG_HOST", "localhost")
PG_PORT = os.getenv("PG_PORT", "5432")

LOGS_DIR = Path(os.getenv("LOGS_DIR", "logs"))
EXTRACT_TAG = os.getenv("EXTRACT_LOG", "extract.log")
TRANSFORM_TAG = os.getenv("TRANSFORM_LOG", "transform.log")
LOAD_TAG = os.getenv("LOAD_LOG", "load.log")

# Data dirs used by your ETL steps
EXTRACT_OUT_DIR = Path(os.getenv("EXTRACT_OUT_DIR", "/home/sabin-adhikari/Project-Data/extraction/raw"))
TRANSFORM_OUT_DIR = Path(os.getenv("TRANSFORM_OUT_DIR", "/home/sabin-adhikari/Project-Data/transform"))

# ----------------------------- DB Helpers -----------------------------
def get_db_connection():
    return psycopg2.connect(
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PW,
        host=PG_HOST,
        port=PG_PORT
    )

def query_one(sql, params=None, default=None):
    try:
        with get_db_connection() as conn, conn.cursor() as cur:
            cur.execute(sql, params or [])
            row = cur.fetchone()
            return row[0] if row else default
    except Exception:
        return default

def query_rows(sql, params=None):
    try:
        with get_db_connection() as conn, conn.cursor() as cur:
            cur.execute(sql, params or [])
            return cur.fetchall()
    except Exception:
        return []

# ----------------------------- ETL Status -----------------------------
def _read_last_line(p: Path):
    try:
        with p.open("rb") as f:
            f.seek(0, os.SEEK_END)
            size = f.tell()
            block = 1024
            data = b""
            while size > 0:
                step = min(block, size)
                f.seek(-step, os.SEEK_CUR)
                data = f.read(step) + data
                f.seek(-step, os.SEEK_CUR)
                size -= step
                if b"\n" in data:
                    break
            line = data.splitlines()[-1].decode("utf-8", "ignore")
            return line.strip()
    except Exception:
        return None

def _parse_time_from_line(line: str):
    # Expecting something like: "2025-08-17 10:32:18,638 [INFO] ..."
    try:
        ts_match = re.match(r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}),", line)
        if ts_match:
            return datetime.strptime(ts_match.group(1), "%Y-%m-%d %H:%M:%S")
    except Exception:
        pass
    return None

def step_status_from_log(filename_hint: str):
    """
    Try hard to find the log:
      1) logs/<hint>
      2) logs/<hint>.log
      3) any file in logs starting with the hint
    Returns dict with status, last_line, last_time, log_path (if exists)
    """
    LOGS_DIR.mkdir(exist_ok=True)

    candidates = [
        LOGS_DIR / filename_hint,
        LOGS_DIR / f"{filename_hint}.log"
    ] + sorted([p for p in LOGS_DIR.glob(f"{filename_hint}*") if p.is_file()])

    target = None
    for p in candidates:
        if p.exists() and p.is_file():
            target = p
            break

    if not target:
        return {"status": "unknown", "last_line": None, "last_time": None, "log_path": None}

    last_line = _read_last_line(target) or ""
    lower = last_line.lower()

    if "error" in lower or "failed" in lower:
        status = "error"
    elif "completed" in lower or "success" in lower:
        status = "success"
    else:
        status = "running" if "start" in lower or "started" in lower else "unknown"

    return {
        "status": status,
        "last_line": last_line,
        "last_time": _parse_time_from_line(last_line),
        "log_path": str(target)
    }

def etl_artifact_checks():
    """Supplement log-based status with artifact presence checks."""
    extract_ok = EXTRACT_OUT_DIR.exists() and any(EXTRACT_OUT_DIR.glob("*.csv"))
    transform_ok = TRANSFORM_OUT_DIR.exists() and any((TRANSFORM_OUT_DIR / "orders_fact").glob("*.parquet"))
    # For load, we check table presence
    load_ok = query_one("SELECT to_regclass('public.orders_fact') IS NOT NULL;") is True
    return {"extract_ok": extract_ok, "transform_ok": transform_ok, "load_ok": load_ok}

@app.route("/api/etl/status")
def etl_status():
    extract = step_status_from_log(EXTRACT_TAG)
    transform = step_status_from_log(TRANSFORM_TAG)
    load = step_status_from_log(LOAD_TAG)

    artifacts = etl_artifact_checks()
    # Blend the statuses: if artifacts exist, upgrade to success
    for step, ok in [("extract", artifacts["extract_ok"]),
                     ("transform", artifacts["transform_ok"]),
                     ("load", artifacts["load_ok"])]:
        obj = {"extract": extract, "transform": transform, "load": load}[step]
        if ok and obj["status"] not in ("success", "error"):
            obj["status"] = "success"

    # Format times for UI
    def fmt(dt): 
        return dt.strftime("%Y-%m-%d %H:%M:%S") if isinstance(dt, datetime) else None

    payload = {
        "extract": {**extract, "last_time": fmt(extract["last_time"])},
        "transform": {**transform, "last_time": fmt(transform["last_time"])},
        "load": {**load, "last_time": fmt(load["last_time"])},
        "artifacts": artifacts
    }
    return jsonify(payload)

# ----------------------------- KPIs -----------------------------
@app.route("/api/kpis")
def api_kpis():
    total_orders = query_one("SELECT COUNT(*) FROM orders_fact;", default=0)
    unique_users = query_one("SELECT COUNT(DISTINCT user_id) FROM orders_fact;", default=0)
    unique_products = query_one("SELECT COUNT(*) FROM products_dim;", default=0)
    aisles = query_one("SELECT COUNT(*) FROM aisles_dim;", default=0)
    departments = query_one("SELECT COUNT(*) FROM departments_dim;", default=0)

    # Optional: filter by day of week via ?dow=0..6
    dow = request.args.get("dow")
    orders_today = None
    if dow is not None and dow.isdigit():
        orders_today = query_one(
            "SELECT COUNT(*) FROM orders_fact WHERE order_dow = %s;",
            [int(dow)], default=0
        )

    return jsonify({
        "total_orders": total_orders or 0,
        "unique_users": unique_users or 0,
        "unique_products": unique_products or 0,
        "aisles": aisles or 0,
        "departments": departments or 0,
        "orders_for_dow": orders_today
    })

# ----------------------------- Charts -----------------------------
@app.route("/api/orders_per_department")
def orders_per_department():
    rows = query_rows("""
        SELECT COALESCE(department, 'Unknown') AS department, COUNT(*) AS total_orders
        FROM orders_fact
        GROUP BY department
        ORDER BY total_orders DESC
        LIMIT 30
    """)
    labels = [r[0] for r in rows]
    values = [r[1] for r in rows]
    return jsonify({"labels": labels, "values": values})

@app.route("/api/top_products")
def top_products():
    limit = request.args.get("limit", "10")
    if not limit.isdigit():
        limit = "10"
    rows = query_rows(f"""
        SELECT product_name, COUNT(*) AS order_count
        FROM orders_fact
        GROUP BY product_name
        ORDER BY order_count DESC
        LIMIT {limit}
    """)
    labels = [r[0] for r in rows]
    values = [r[1] for r in rows]
    return jsonify({"labels": labels, "values": values})

@app.route("/api/orders_per_day")
def orders_per_day():
    rows = query_rows("""
        SELECT order_dow, COUNT(*) AS total_orders
        FROM orders_fact
        GROUP BY order_dow
        ORDER BY order_dow
    """)
    labels = [str(r[0]) for r in rows]
    values = [r[1] for r in rows]
    return jsonify({"labels": labels, "values": values})

# ----------------------------- Views -----------------------------
@app.route("/")
def index():
    return render_template("index.html")

# ----------------------------- Main -----------------------------
if __name__ == "__main__":
    # Run on 5001 as you had before
    app.run(debug=True, port=5001)