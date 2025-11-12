import duckdb
from datetime import datetime
import os
import shutil

# con = duckdb.connect('md:mdb_timestock', config={"motherduck_token": MOTHERDUCK_TOKEN})
# con = duckdb.connect('backend/db_timestock')

REPO_DB_PATH = "backend/db_timestock1"

# If running locally, use a local file
if os.environ.get("RAILWAY") == "1":
    # Production (Railway) path: the mounted volume
    DB_PATH = "/data/db_timestock1"
else:
    # Local path
    DB_PATH = "backend/db_timestock1"

os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

# Copy starter DB if it doesn't exist yet
if not os.path.exists(DB_PATH):
    if os.path.exists(REPO_DB_PATH):
        shutil.copy(REPO_DB_PATH, DB_PATH)
        print(f"Copied starter DB to {DB_PATH}")
    else:
        print(f"No starter DB found at {REPO_DB_PATH}. A new DB will be created.")


# Connect to DuckDB
con = duckdb.connect(DB_PATH)
print(f"Connected to DB at {DB_PATH}")

# Alerts
def get_minimum_stock_alerts():
    query = """
        SELECT 
            m.current_stock,
            m.minimum_stock,
            i.item_name
        FROM materials m
        JOIN items i ON m.item_id = i.id
    """

    with duckdb.connect(DB_PATH) as conn:
    # with duckdb.connect('md:mdb_timestock', config={"motherduck_token": MOTHERDUCK_TOKEN}) as conn:
        df = conn.execute(query).fetchdf()

    alerts = []
    for _, row in df.iterrows():
        stock = row['current_stock']
        minimum = row['minimum_stock']
        item = row['item_name']
        threshold = minimum * 1.2  # 20% buffer zone

        if stock < minimum:
            alerts.append(f"⚡️ {item}: Stock is {stock}, below minimum of {minimum} – Stocking is needed.")
        elif stock < threshold:
            alerts.append(f"🔶 {item}: Stock is {stock}, nearing minimum ({minimum}) – Monitor.")
    return alerts


def get_low_stock_alerts():
    return con.execute("""
        SELECT 
            i.id AS item_id,
            i.item_name,
            i.item_decription,
            m.current_stock,
            m.minimum_stock,
            m.unit_measurement,
            m.supplier_id
        FROM materials m
        JOIN items i ON m.item_id = i.id
        WHERE m.current_stock <= m.minimum_stock
    """).fetchdf().to_dict(orient="records")

# Total number of materials
def get_total_materials():
    return con.execute("SELECT COUNT(*) AS total_materials FROM materials").fetchone()[0]

# Total number of products
def get_total_products():
    return con.execute("SELECT COUNT(*) AS total_products FROM products").fetchone()[0]

# Materials below minimum stock
def get_low_stock_materials():
    return con.execute("""
        SELECT COUNT(*) AS low_stock_materials 
        FROM materials 
        WHERE current_stock < minimum_stock
    """).fetchone()[0]

# Materials out of stock
def get_out_of_stock_materials():
    return con.execute("""
        SELECT COUNT(*) AS out_of_stock 
        FROM materials 
        WHERE current_stock <= 0
    """).fetchone()[0]

# Total inventory value
def get_total_inventory_value():
    return con.execute("""
        SELECT SUM(current_stock * material_cost) AS total_inventory_value
        FROM materials
    """).fetchone()[0]

# Top 5 used materials in the last 30 days
def get_top_used_materials():
    return con.execute("""
        SELECT 
            i.item_name,
            SUM(oi.quantity * pm.used_quantity) AS total_used
        FROM order_items oi
        JOIN product_materials pm ON oi.product_id = pm.product_id
        JOIN materials m ON pm.material_id = m.id
        JOIN items i ON m.item_id = i.id
        JOIN order_transactions ot ON ot.id = oi.order_id
        WHERE ot.date_created >= NOW() - INTERVAL 30 DAY
        GROUP BY i.item_name
        ORDER BY total_used DESC
        LIMIT 5
    """).fetchdf()

# Material category distribution
def get_material_category_distribution():
    return con.execute("""
        SELECT 
            c.category_name,
            COUNT(m.id) AS material_count
        FROM materials m
        JOIN items i ON m.item_id = i.id
        JOIN material_categories c ON i.category_id = c.id
        GROUP BY c.category_name
    """).fetchdf()

def get_total_orders() -> int:
    return con.execute("SELECT COUNT(*) FROM order_transactions").fetchone()[0]

def get_total_sales() -> int:
    return con.execute("SELECT SUM(quantity) FROM order_items").fetchone()[0] or 0

def get_total_revenue() -> float:
    return con.execute("SELECT SUM(total_amount) FROM order_transactions").fetchone()[0] or 0.0

def get_all_time_metrics(): 
    con = duckdb.connect(DB_PATH)
    # con = duckdb.connect('md:mdb_timestock', config={"motherduck_token": MOTHERDUCK_TOKEN})

    query = """
    SELECT 
        COUNT(DISTINCT ot.id) AS total_orders,
        SUM(oi.quantity) AS total_sales,
        SUM(ot.total_amount) AS total_revenue
    FROM order_transactions ot
    LEFT JOIN order_items oi ON ot.id = oi.order_id
    WHERE ot.status_id = 'OS005'
    """
    with duckdb.connect(DB_PATH) as conn:
    # with duckdb.connect('md:mdb_timestock', config={'motherduck_token': MOTHERDUCK_TOKEN}) as conn:
        result = con.execute(query).fetchone()

    return {
        "total_orders": result[0],
        "total_sales": result[1],
        "total_revenue": result[2]
    }


#SUMMARIES
def get_sales_summary():
    return {
        "total_orders": get_total_orders(),
        "total_sales": get_total_sales(),
        "total_revenue": get_total_revenue()
    }

def get_inventory_summary():
    return {
        "total_materials": get_total_materials(),
        "total_products": get_total_products(),
        "low_stock_materials": get_low_stock_materials(),
        "out_of_stock_materials": get_out_of_stock_materials(),
        "total_inventory_value": get_total_inventory_value(),
        "top_used_materials": get_top_used_materials().to_dict(orient="records"),
        "material_category_distribution": get_material_category_distribution().to_dict(orient="records")
    }

def get_fast_moving_ratings_map():
    query = """
    SELECT 
        i.item_name,
        ROUND(
            100.0 * SUM(oi.quantity * pm.used_quantity) / MAX(SUM(oi.quantity * pm.used_quantity)) OVER (),
            2
        ) AS fast_moving_rating
    FROM order_items oi
    JOIN products p ON p.id = oi.product_id
    JOIN product_materials pm ON pm.product_id = p.id
    JOIN materials m ON m.id = pm.material_id
    JOIN items i ON i.id = m.item_id
    JOIN order_transactions ot ON ot.id = oi.order_id
    WHERE ot.date_created >= DATE_TRUNC('month', CURRENT_DATE - INTERVAL 3 MONTH)
    GROUP BY i.item_name
    """
    with duckdb.connect(DB_PATH) as conn:
    # with duckdb.connect('md:mdb_timestock', config={"motherduck_token": MOTHERDUCK_TOKEN}) as conn:
        df = conn.execute(query).fetchdf()
    
    return {row["item_name"]: row["fast_moving_rating"] for _, row in df.iterrows()}


def get_total_products():
    return con.execute("SELECT COUNT(*) FROM products").fetchone()[0] or 0

def get_most_used_product():
    row = con.execute("""
        SELECT
            i.item_name,
            SUM(oi.quantity) AS total_used
        FROM order_items oi
        JOIN products p ON oi.product_id = p.id
        JOIN items AS i ON p.item_id = i.id
        JOIN order_transactions ot ON ot.id = oi.order_id
        WHERE ot.date_created >= NOW() - INTERVAL 30 DAY
        GROUP BY i.item_name
        ORDER BY total_used DESC
        LIMIT 1
    """).fetchone()
    return {"item_name": row[0], "total_sold": row[1]} if row else {"item_name": None, "total_sold": 0}

def get_highest_revenue_product():
    row = con.execute("""
        SELECT
            i.item_name,
            SUM(oi.quantity * oi.unit_price) AS revenue
        FROM order_items AS oi
        JOIN products p ON oi.product_id = p.id
        JOIN items i ON p.item_id = i.id
        JOIN order_transactions ot ON oi.order_id= ot.id
        JOIN order_statuses os ON ot.status_id = os.id
        WHERE ot.date_created >= NOW() - INTERVAL 30 DAY AND os.status_code = 'completed'
        GROUP BY p.id, i.item_name
        ORDER BY revenue DESC
        LIMIT 1;
    """).fetchone()
    return {"item_name": row[0], "revenue": row[1]} if row else {"item_name": None, "revenue": 0}

def get_total_in_production():
    return con.execute("""
        SELECT COUNT(*) 
        FROM order_transactions ot
        JOIN order_statuses os ON os.id = ot.status_id
        WHERE os.status_code = 'in_production'
    """).fetchone()[0] or 0


def get_product_usage_summary():
    return {
        "total_product_quantity": get_total_products(),
        "most_used_product": get_most_used_product(),
        "highest_revenue_product": get_highest_revenue_product(),
        "in_production_count": get_total_in_production()

    }


def get_stock_summary():
    con = duckdb.connect(DB_PATH)
    # con = duckdb.connect('md:mdb_timestock', config={"motherduck_token": MOTHERDUCK_TOKEN})

    query = """
        WITH stock_totals AS (
            SELECT 
                stt.type_code,
                SUM(sti.quantity) AS total_qty
            FROM stock_transaction_items sti
            JOIN stock_transactions st ON st.id = sti.stock_transaction_id
            JOIN stock_transaction_types stt ON stt.id = st.stock_type_id
		        WHERE st.date_created >= CURRENT_DATE - INTERVAL '1 month'
		        AND st.date_created < CURRENT_DATE + INTERVAL '1 day'
            GROUP BY stt.type_code
        ),
        supplier_totals AS (
            SELECT 
                s.contact_name,
                SUM(sti.quantity) AS total_supplied
            FROM stock_transaction_items sti
            JOIN stock_transactions st ON st.id = sti.stock_transaction_id
            JOIN stock_transaction_types stt ON stt.id = st.stock_type_id
            JOIN suppliers s ON s.id = st.supplier_id
            WHERE stt.type_code = 'stock-in'
                AND st.date_created >= CURRENT_DATE - INTERVAL '1 month'
                AND st.date_created < CURRENT_DATE + INTERVAL '1 day'
            GROUP BY s.contact_name
            ORDER BY total_supplied DESC
            LIMIT 1
        )
        SELECT 
            COALESCE((SELECT total_qty FROM stock_totals WHERE type_code = 'stock-in'), 0) AS stock_in,
            COALESCE((SELECT total_qty FROM stock_totals WHERE type_code = 'stock-out'), 0) AS stock_out,
            COALESCE((SELECT total_qty FROM stock_totals WHERE type_code = 'stock-in'), 0) 
              - COALESCE((SELECT total_qty FROM stock_totals WHERE type_code = 'stock-out'), 0) AS net_flow,
            (SELECT contact_name FROM supplier_totals) AS top_supplier,
            (SELECT total_supplied FROM supplier_totals) AS top_supplier_total
    """
    with duckdb.connect(DB_PATH) as conn:
    # with duckdb.connect('md:mdb_timestock', config={'motherduck_token': MOTHERDUCK_TOKEN}) as conn:
        result = con.execute(query).fetchone()

    return {
        "stock_in": result[0],
        "stock_out": result[1],
        "net_flow": result[2],
        "top_supplier": result[3],
        "top_supplier_total": result[4],
    }

# Orders
def get_summary_cards(period: str):
    con = duckdb.connect(DB_PATH)
    # con = duckdb.connect('md:mdb_timestock', config={"motherduck_token": MOTHERDUCK_TOKEN})

    if period not in ('week', 'month', 'year'):
        period = 'week'

    query = f"""
        SELECT 
            COUNT(DISTINCT ot.id) AS total_orders,
            SUM(oi.quantity) AS total_sales,
            SUM(ot.total_amount) AS total_revenue
        FROM order_transactions ot
        JOIN order_items oi ON ot.id = oi.order_id
        WHERE DATE_TRUNC('{period}', ot.date_created) = DATE_TRUNC('{period}', CURRENT_DATE)
          AND ot.status_id = 'OS005'
    """

    result = con.execute(query).fetchone()

    return {
        "total_orders": int(result[0] or 0),
        "total_sales": int(result[1] or 0),
        "total_revenue": float(result[2] or 0.0),
    }


def get_total_materials():
    return con.execute ("SELECT COUNT(*) FROM materials").fetchone()[0] or 0

def get_most_used_material():
    row = con.execute("""
       SELECT
            i.item_name,
            SUM(sti.quantity) AS total_used
        FROM stock_transaction_items sti
        JOIN stock_transactions st ON sti.stock_transaction_id = st.id
        JOIN materials m ON sti.material_id = m.id
        JOIN items i ON m.item_id = i.id
        WHERE st.date_created >= CURRENT_DATE - INTERVAL '3 months'
        AND st.stock_type_id = 'STT002'  
        GROUP BY m.id, i.item_name
        ORDER BY total_used DESC
        LIMIT 1;

    """).fetchone()
    return {"item_name": row[0],"total_used": row[1]} if row else {"item_name": None, "total_used": 0}

def get_total_material_quantity():
    return con.execute ("SELECT SUM(current_stock) FROM materials").fetchone()[0] or 0

def get_material_usage_summary():
    return {
        "total_materials": get_total_materials(),
        "most_used_material": get_most_used_material(),
        "total_material_quantity": get_total_material_quantity()
    }

def get_recent_order_transactions(limit=5):
    query = f"""
        SELECT 
            ot.date_created,
            CONCAT(c.firstname, ' ', c.lastname) AS customer_name,
            ot.total_amount,
            GROUP_CONCAT(DISTINCT i.item_name, ', ') AS product_names,
            os.status_code
        FROM order_transactions ot
        JOIN customers c ON ot.customer_id = c.id
        JOIN order_statuses os ON ot.status_id = os.id
        LEFT JOIN order_items oi ON ot.id = oi.order_id
        LEFT JOIN products p ON oi.product_id = p.id
        LEFT JOIN items i ON p.item_id = i.id
        GROUP BY ot.id, c.firstname, c.lastname, ot.total_amount, os.status_code, ot.date_created
        ORDER BY ot.date_created DESC
        LIMIT {limit}
    """

    df = con.execute(query).fetchdf()

    # Convert timestamps to string
    df['date_created'] = df['date_created'].astype(str)

    return df.to_dict("records")

