import duckdb
from datetime import datetime
from collections import defaultdict
from fastapi import HTTPException 
from argon2 import PasswordHasher, exceptions as argon2_ex
from argon2.exceptions import VerifyMismatchError


con = duckdb.connect('backend/db_timestock')
ph = PasswordHasher()

# Product_materials
def get_product_materials_grouped():
    df = con.execute("""
        SELECT 
            p.id AS product_id,
            i.item_name AS product_name,
            pm.material_id,
            mi.item_name AS material_name,
            pm.used_quantity,
            pm.unit_cost,
            pm.line_cost
        FROM product_materials pm
        JOIN products p ON pm.product_id = p.id
        JOIN items i ON p.item_id = i.id
        JOIN materials m ON pm.material_id = m.id
        JOIN items mi ON m.item_id = mi.id
    """).fetchdf()

    grouped = defaultdict(lambda: {"product_id": None, "product_name": None, "materials": []})

    for row in df.itertuples(index=False):
        prod_id = row.product_id
        grouped[prod_id]["product_id"] = prod_id
        grouped[prod_id]["product_name"] = row.product_name
        grouped[prod_id]["materials"].append({
            "material_id": row.material_id,
            "material_name": row.material_name,
            "used_quantity": row.used_quantity,
            "unit_cost": row.unit_cost,
            "line_cost": row.line_cost
        })

    return list(grouped.values())

def add_product_materials(data: dict):
    product_id = data['product_id']
    materials = data['materials']

    for material in materials:
        material_id = material['material_id']
        used_quantity = material['used_quantity']

        # Check for duplicates
        existing = con.execute("""
            SELECT 1 FROM product_materials
            WHERE product_id = ? AND material_id = ?
        """, (product_id, material_id)).fetchone()

        if existing:
            # Skip existing material instead of raising error
            continue

        # Use provided unit_cost if available; else fetch from DB
        unit_cost = material.get('unit_cost')
        if unit_cost is None:
            result = con.execute("""
                SELECT material_cost FROM materials WHERE id = ?
            """, (material_id,)).fetchone()

            if not result:
                raise ValueError(f"Material with ID '{material_id}' not found.")
            unit_cost = result[0]

        # Insert new material
        con.execute("""
            INSERT INTO product_materials (
                product_id, material_id, used_quantity, unit_cost
            ) VALUES (?, ?, ?, ?)
        """, (
            product_id,
            material_id,
            used_quantity,
            unit_cost
        ))

    con.commit()


def get_product_materials_by_product_id(product_id: str):
    query = """
        SELECT pm.material_id, i.item_name, m.unit_measurement, pm.used_quantity, pm.unit_cost
        FROM product_materials pm
        JOIN materials m ON pm.material_id = m.id
        JOIN items i ON m.item_id = i.id
        WHERE pm.product_id = ?
    """
    result = con.execute(query, (product_id,)).fetchall()

    return [
        {
            "material_id": row[0],
            "item_name": row[1],
            "unit_measurement": row[2],
            "used_quantity": row[3],
            "unit_cost": row[4]
        }
        for row in result
    ]


def update_product_material(product_id: str, material_id: str, used_quantity: float, unit_cost: float):
    result = con.execute("""
        UPDATE product_materials
        SET used_quantity = ?, unit_cost = ?
        WHERE product_id = ? AND material_id = ?
    """, (used_quantity, unit_cost, product_id, material_id))

    if result.rowcount == 0:
        raise ValueError("No matching product-material found to update.")

    con.commit()

def delete_product_material(product_id: str, material_id: str):
    result = con.execute("""
        DELETE FROM product_materials
        WHERE product_id = ? AND material_id = ?
    """, (product_id, material_id))

    if result.rowcount == 0:
        raise ValueError("No matching product-material found to delete.")

    con.commit()


# Product Calculation
def calculate_quote(product_id: str):
    rows = con.execute("""
        SELECT 
            pm.material_id,
            m.material_cost,
            pm.line_cost,
            i.item_name,
            i.item_decription,
            pm.unit_cost,
            pm.used_quantity,
            m.unit_measurement
        FROM product_materials pm
        JOIN materials m ON pm.material_id = m.id
        JOIN items i ON m.item_id = i.id 
        WHERE pm.product_id = ?
    """, (product_id,)).fetchdf()

    total = rows['line_cost'].sum()
    return {
        "materials": rows.to_dict(orient="records"),
        "total_cost": total
    }



# Product_categories CRUDS
def get_product_categories():
    return con.execute("SELECT * FROM product_categories").fetchdf()

def add_product_category(data: dict):
    category_name = data['category_name'].strip().title()
    description = data['description'].strip()

    # Check for duplicates
    exists = con.execute("""
        SELECT 1 FROM product_categories WHERE LOWER(TRIM(category_name)) = ?
    """, (category_name,)).fetchone()

    if exists:
        return None  # Return None explicitly to indicate duplicate

    cur = con.cursor()
    new_id = cur.execute("""
        INSERT INTO product_categories (category_name, description)
        VALUES (?, ?)
        RETURNING id
    """, (category_name, description)).fetchone()[0]

    cur.execute("""
        INSERT INTO item_categories (id, category_name, description)
        VALUES (?, ?, ?)
    """, (new_id, category_name, description))

    con.commit()
    return new_id

def update_product_category(id: str, data: dict):
    category_name = data['category_name'].strip().title()
    description = data['description'].strip()

    con.execute("""
        UPDATE product_categories SET
            category_name = ?,
            description = ?
        WHERE id = ?
    """, (category_name, description, id))

def delete_product_categories(id: str):
    con.execute("DELETE FROM product_categories WHERE id = ?;", (id,))

# Material_categories CRUD
def get_material_categories():
      with duckdb.connect('backend/db_timestock') as conn:
        return conn.execute("SELECT * FROM material_categories").fetchdf()



def add_material_category(data: dict):
    raw_name = data['category_name'].strip()
    name = raw_name.lower()
    description = data['description'].strip()

    # Case-insensitive duplicate check
    exists = con.execute("""
        SELECT 1 FROM material_categories WHERE LOWER(TRIM(category_name)) = ?
    """, (name,)).fetchone()

    if exists:
        return None  # Prevent duplicate insert

    formatted_name = raw_name.title()

    cur = con.cursor()
    new_id = cur.execute("""
        INSERT INTO material_categories (category_name, description)
        VALUES (?, ?)
        RETURNING id
    """, (formatted_name, description)).fetchone()[0]

    cur.execute("""
        INSERT INTO item_categories (id, category_name, description)
        VALUES (?, ?, ?)
    """, (new_id, formatted_name, description))

    con.commit()
    return new_id

def update_material_category(id: str, data: dict):
    category_name = data['category_name'].strip().title()
    description = data['description'].strip()

    con.execute("""
        UPDATE material_categories SET
            category_name = ?,
            description = ?
        WHERE id = ?
    """, (category_name, description, id))

def delete_material_category(id: str):
    con.execute("DELETE FROM material_categories WHERE id = ?", (id,))


#Materials CRUDS
def get_material():
    return con.execute("""
       SELECT 
            i.id AS item_id,
            i.item_name,
            i.item_decription,
            i.category_id,  -- <-- include this
            mc.category_name AS item_category_name,
            m.id AS material_id,
            m.unit_measurement,
            m.material_cost,
            m.current_stock,
            m.minimum_stock,
            m.maximum_stock,
            m.supplier_id,  -- <-- include this
            s.contact_name AS supplier_name
        FROM items i
        JOIN materials m ON i.id = m.item_id
        JOIN material_categories mc ON i.category_id = mc.id
        JOIN suppliers s ON m.supplier_id = s.id
    """).fetchdf()

def get_stock_type():
    return con.execute("""
        SELECT 
            i.id AS item_id,
            i.item_name,
            i.item_decription,
            mc.category_name AS item_category_name,
            m.id AS material_id,
            m.unit_measurement,
            m.material_cost,
            m.current_stock,
            m.minimum_stock,
            m.maximum_stock,
            s.contact_name AS supplier_name
        FROM items i
        JOIN materials m ON i.id = m.item_id
        JOIN material_categories mc ON i.category_id = mc.id
        JOIN suppliers s ON m.supplier_id = s.id
    """).fetchdf()

def update_materials(
    con,
    material_id: str,
    item_name: str,
    item_description: str,
    category_id: str,
    unit_measurement: str,
    material_cost: float,
    current_stock: float,
    minimum_stock: float,
    maximum_stock: float,
    supplier_id: str
):
    # Get the item_id linked to the material
    item_id_result = con.execute(
        "SELECT item_id FROM materials WHERE id = ?", (material_id,)
    ).fetchone()

    if item_id_result is None:
        raise ValueError(f"No material found with id {material_id}")

    item_id = item_id_result[0]

    # Check for duplicate item name (excluding this item_id)
    duplicate_check = con.execute("""
        SELECT 1 FROM items 
        WHERE item_name = ? AND id != ?
        LIMIT 1
    """, (item_name, item_id)).fetchone()

    if duplicate_check:
        raise ValueError(f"Item name '{item_name}' already exists.")

    try:
        con.execute("BEGIN")  # Start transaction

        # Update materials table
        con.execute("""
            UPDATE materials
            SET 
                unit_measurement = ?,
                material_cost = ?,
                current_stock = ?,
                minimum_stock = ?,
                maximum_stock = ?,
                supplier_id = ?,
                date_updated = CURRENT_TIMESTAMP
            WHERE id = ?
        """, (
            unit_measurement,
            material_cost,
            current_stock,
            minimum_stock,
            maximum_stock,
            supplier_id,
            material_id
        ))

        # Update items table
        con.execute("""
            UPDATE items
            SET 
                item_name = ?,
                item_decription = ?, 
                category_id = ?,
                date_updated = CURRENT_TIMESTAMP
            WHERE id = ?
        """, (
            item_name,
            item_description,
            category_id,
            item_id
        ))

        con.execute("COMMIT")  # Commit if all succeeded

    except Exception as e:
        con.execute("ROLLBACK")  # Roll back if any error occurs
        raise e


def update_order_status(transaction_id: str, new_status_code: str, con):
    # Validate status_code exists
    status_row = con.execute("""
        SELECT id FROM order_statuses WHERE status_code = ?
    """, (new_status_code,)).fetchone()
    
    if not status_row:
        return {"error": "Status code not found."}

    # Validate transaction exists
    txn_row = con.execute("""
        SELECT id FROM order_transactions WHERE id = ?
    """, (transaction_id,)).fetchone()
    
    if not txn_row:
        return {"error": "Transaction ID not found."}

    # Update order transaction
    con.execute("""
        UPDATE order_transactions
        SET status_id = ?
        WHERE id = ?
    """, (status_row[0], transaction_id))

    return {"success": True}


def get_material_by_id(material_id: str):
    return con.execute("""
        SELECT 
            i.id AS item_id,
            i.item_name,
            i.item_decription,
            mc.category_name AS item_category_name,
            m.id AS material_id,
            m.unit_measurement,
            m.material_cost,
            m.current_stock,
            m.minimum_stock,
            m.maximum_stock,
            s.contact_name AS supplier_name
        FROM items i
        JOIN materials m ON i.id = m.item_id
        JOIN material_categories mc ON i.category_id = mc.id
        JOIN suppliers s ON m.supplier_id = s.id
        WHERE m.id = ?
    """, (material_id,)).fetchone()


def add_material(data: dict):
    # Normalize item fields
    item_name = data['item_name'].strip().title()
    item_description = data['item_decription'].strip()  # Keep using 'item_decription' if that's the real column name
    category_id = data['category_id']

    # Check for existing item in the 'items' table
    cur = con.cursor()
    existing_item = cur.execute("""
        SELECT 1 FROM items WHERE item_name = ?
    """, (item_name,)).fetchone()

    if existing_item:
        raise Exception(f"Item with name '{item_name}' already exists.")

    # Insert into items table
    item_id = cur.execute("""
        INSERT INTO items (
            item_name, item_decription, category_id, date_created, date_updated
        ) VALUES (?, ?, ?, ?, ?)
        RETURNING id
    """, (item_name, item_description, category_id, datetime.utcnow(), datetime.utcnow())).fetchone()[0]

    # Normalize material fields
    unit_measurement = data['unit_measurement'].strip().lower()
    material_cost = data['material_cost']
    current_stock = data['current_stock']
    minimum_stock = data['minimum_stock']
    maximum_stock = data['maximum_stock']
    supplier_id = data['supplier_id']

    # Check for existing material with the same item_id
    existing_material = cur.execute("""
        SELECT 1 FROM materials WHERE item_id = ?
    """, (item_id,)).fetchone()

    if existing_material:
        raise Exception(f"Material with item ID '{item_id}' already exists.")

    # Insert into materials table
    cur.execute("""
        INSERT INTO materials (
            item_id, category_id, unit_measurement, material_cost,
            current_stock, minimum_stock, maximum_stock, supplier_id,
            date_created, date_updated
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        item_id, category_id, unit_measurement, material_cost,
        current_stock, minimum_stock, maximum_stock, supplier_id,
        datetime.utcnow(), datetime.utcnow()
    ))

    con.commit()
    return item_id

def stock_materials(data: dict):
    items = data.pop("items")

    try:
        con.execute("BEGIN")  # <-- Start transaction block

        # --- Step 0: Create or identify supplier
        supplier_id = data.get("supplier_id")
        if not supplier_id and "supplier" in data:
            supplier = data["supplier"]
            contact_name = supplier['contact_name'].strip().title()
            contact_number = supplier['contact_number'].strip()
            email = supplier['email'].strip()
            firstname = supplier['firstname'].strip().title()
            lastname = supplier['lastname'].strip().title()
            address = supplier['address'].strip().title()

            existing = con.execute("""
                SELECT id FROM suppliers
                WHERE LOWER(contact_name) = LOWER(?)
                LIMIT 1
            """, (contact_name,)).fetchone()

            if existing:
                raise ValueError(f"Supplier with contact name '{contact_name}' already exists.")
            else:
                supplier_id = con.execute("""
                    INSERT INTO suppliers (
                        firstname, lastname, contact_name, contact_number, email, address, date_created
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    RETURNING id
                """, (
                    firstname, lastname, contact_name, contact_number, email, address, datetime.utcnow()
                )).fetchone()[0]

        elif not supplier_id:
            raise ValueError("Either supplier_id or supplier details must be provided.")

        # --- Step 1: Determine stock_type_id
        stock_type_id = data.get("stock_type_id")
        if not stock_type_id:
            result = con.execute("""
                SELECT id FROM stock_types WHERE type_code = 'STT001'
            """).fetchone()
            if not result:
                raise ValueError("Stock type 'STT001' not found in stock_types table.")
            stock_type_id = result[0]

        # --- Step 2: Insert stock transaction
        admin_id = data.get("admin_id")
        employee_id = data.get("employee_id")

        if not admin_id and not employee_id:
            raise ValueError("Either admin_id or employee_id must be provided.")

        stock_transaction_id = con.execute("""
            INSERT INTO stock_transactions (
                stock_type_id, supplier_id, admin_id, employee_id, date_created
            ) VALUES (?, ?, ?, ?, ?)
            RETURNING id
        """, (
            stock_type_id,
            supplier_id,
            admin_id,
            employee_id,
            datetime.utcnow()
        )).fetchone()[0]

        # --- Step 3: Stock materials
        for item in items:
            material_id = item["material_id"]
            quantity = item["quantity"]

            # Insert transaction item
            con.execute("""
                INSERT INTO stock_transaction_items (
                    stock_transaction_id, material_id, quantity
                ) VALUES (?, ?, ?)
            """, (stock_transaction_id, material_id, quantity))

            # Update material stock
            con.execute("""
                UPDATE materials
                SET current_stock = current_stock + ?
                WHERE id = ?
            """, (quantity, material_id))

        con.commit()
        return {
            "transaction_id": stock_transaction_id,
            "message": "Materials successfully stocked."
        }

    except Exception as e:
        con.rollback()  # <-- undo all changes
        raise e


def get_stock_transactions_detailed():
    with duckdb.connect('backend/db_timestock') as conn:
        return conn.execute("""
            SELECT 
                st.id AS transaction_id,
                st.date_created,

                -- Stock Type
                stt.type_code,
                stt.description AS stock_type,

                -- Supplier
                CONCAT(s.firstname, ' ', s.lastname) AS supplier_name,
                s.contact_number AS supplier_contact,
                s.email AS supplier_email,

                -- Admin (may be NULL)
                CASE 
                    WHEN a.firstname IS NOT NULL THEN CONCAT(a.firstname, ' ', a.lastname)
                    ELSE NULL 
                END AS admin_name,
                a.email AS admin_email,

                -- Employee (may be NULL)
                CASE 
                    WHEN e.firstname IS NOT NULL THEN CONCAT(e.firstname, ' ', e.lastname)
                    ELSE NULL 
                END AS employee_name,
                e.email AS employee_email,

                -- Material Info
                i.item_name AS material_name,
                i.item_decription,
                um.measurement_code AS unit,
                sti.quantity

            FROM stock_transactions st
            JOIN stock_transaction_types stt ON st.stock_type_id = stt.id
            JOIN suppliers s ON st.supplier_id = s.id
            LEFT JOIN admin a ON st.admin_id = a.id
            LEFT JOIN employees e ON st.employee_id = e.id
            JOIN stock_transaction_items sti ON st.id = sti.stock_transaction_id
            JOIN materials m ON sti.material_id = m.id
            JOIN items i ON m.item_id = i.id
            JOIN unit_measurements um ON m.unit_measurement = um.measurement_code

            ORDER BY st.date_created DESC
        """).fetchdf()



def delete_material(material_id: str):
    try:
        # Get the item_id from the material first
        cursor = con.execute("SELECT item_id FROM materials WHERE id = ?", (material_id,))
        row = cursor.fetchone()
        if not row:
            return {"success": False, "message": "Material not found."}
        
        item_id = row[0]

        # Delete from referencing tables first to avoid FK constraint issues
        con.execute("DELETE FROM product_materials WHERE material_id = ?", (material_id,))
        con.execute("DELETE FROM stock_transaction_items WHERE material_id = ?", (material_id,))
        
        # Then delete the material and its item
        con.execute("DELETE FROM materials WHERE id = ?", (material_id,))
        con.execute("DELETE FROM items WHERE id = ?", (item_id,))
        
        con.commit()
        return {"success": True, "message": "Material and corresponding item deleted successfully."}
    except Exception as e:
        return {"success": False, "message": str(e)}


#Customer CRUD
def get_customers():
    return con.execute("SELECT * FROM customers").fetchdf()

def add_customer(data: dict):
    firstname = data['firstname'].strip().title()
    lastname = data['lastname'].strip().title()
    email = data['email'].strip().lower()
    address = data['address'].strip()
    contact_number = data['contact_number'].strip()

    existing = con.execute("""
        SELECT 1 FROM customers 
        WHERE firstname = ? AND lastname = ? 
        AND (contact_number = ? OR email = ?)
    """, (firstname, lastname, contact_number, email)).fetchone()

    if existing:
        return {"success": False, "message": "Customer already exists."}

    con.execute("""
        INSERT INTO customers (
            firstname, lastname, contact_number, email, address, date_created
        ) VALUES (?, ?, ?, ?, ?, ?)
    """, (firstname, lastname, contact_number, email, address, datetime.utcnow()))

    return {"success": True, "message": "Customer added successfully."}

def update_customer(id: str, data: dict):
    firstname = data['firstname'].strip().title()
    lastname = data['lastname'].strip().title()
    email = data['email'].strip().lower()
    address = data['address'].strip()
    contact_number = data['contact_number'].strip()

    con.execute("""
        UPDATE customers SET
            firstname = ?,
            lastname = ?,
            contact_number = ?,
            email = ?,
            address = ?
        WHERE id = ?
    """, (firstname, lastname, contact_number, email, address, id))

def delete_customer(id: str):
    con.execute("DELETE FROM customers WHERE id = ?", (id,))

#Products CRUD
def get_products():
    with duckdb.connect("backend/db_timestock") as conn:
        return conn.execute("""
            SELECT 
                i.id AS item_id,
                i.item_name,
                i.item_decription,
                pc.category_name AS item_category_name,  -- <-- join result
                p.id AS product_id,
                p.unit_price,
                p.materials_cost,
                p.status,
                p.date_created,
                p.date_updated
            FROM items i
            JOIN products p ON i.id = p.item_id
            JOIN product_categories pc ON i.category_id = pc.id  -- <-- join category name
        """).fetchdf()


def add_product(data: dict):
   # Normalize item fields
    item_name = data['item_name'].strip().title()
    item_description = data['item_decription'].strip()
    category_id = data['category_id']

    # Check for duplicate item name
    existing = con.execute("""
        SELECT id FROM items WHERE LOWER(TRIM(item_name)) = ?
    """, (item_name.lower(),)).fetchone()

    if existing:
        return {"success": False, "message": f"Item already exists with name: {item_name}"}

    cur = con.cursor()

    # Step 1: Insert into items first and get item_id
    item_id = cur.execute("""
        INSERT INTO items (
            item_name, item_decription, category_id, date_created, date_updated
        ) VALUES (?, ?, ?, ?, ?)
        RETURNING id
    """, (item_name, item_description, category_id, datetime.utcnow(), datetime.utcnow())).fetchone()[0]

    # Step 2: Insert into products with that item_id
    cur.execute("""
        INSERT INTO products (
            item_id, category_id, unit_price, materials_cost, status,
            date_created, date_updated
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (
        item_id,
        category_id,
        data['unit_price'],
        data['materials_cost'],
        data['status'].strip().title(),
        datetime.utcnow(),
        datetime.utcnow()
    ))

    con.commit()
    return {"success": True, "product_id": item_id, "message": "Product added successfully."}

def update_product(
    con,
    product_id: str,
    unit_price: float,
    materials_cost: float,
    status: str,
    category_id: str,
    item_name: str,
    item_description: str
):
    # Get the item_id linked to the product
    item_id_result = con.execute(
        "SELECT item_id FROM products WHERE id = ?", (product_id,)
    ).fetchone()

    if item_id_result is None:
        raise ValueError(f"No product found with id {product_id}")

    item_id = item_id_result[0]

    # Validate uniqueness of item_name (excluding current item_id)
    duplicate_check = con.execute("""
        SELECT 1 FROM items
        WHERE item_name = ? AND id != ?
        LIMIT 1
    """, (item_name, item_id)).fetchone()

    if duplicate_check:
        raise ValueError(f"Item name '{item_name}' already exists.")

    # Update products table
    con.execute("""
        UPDATE products 
        SET 
            unit_price = ?, 
            materials_cost = ?, 
            status = ?, 
            date_updated = CURRENT_TIMESTAMP
        WHERE id = ?
    """, (unit_price, materials_cost, status, product_id))

    # Update items table
    con.execute("""
        UPDATE items
        SET 
            item_name = ?, 
            item_decription = ?, 
            category_id = ?, 
            date_updated = CURRENT_TIMESTAMP
        WHERE id = ?
    """, (item_name, item_description, category_id, item_id))


def delete_product(product_id: str):
    with duckdb.connect('backend/db_timestock') as conn:
        try:
            # Get the corresponding item_id from the product
            item_result = conn.execute("SELECT item_id FROM products WHERE id = ?", (product_id,)).fetchone()
            if not item_result:
                return {"success": False, "message": "Product not found."}
            
            item_id = item_result[0]

            # Delete from referencing tables first to avoid FK constraint issues
            conn.execute("DELETE FROM product_materials WHERE product_id = ?", (product_id,))
            conn.execute("DELETE FROM order_items WHERE product_id = ?", (product_id,))
            
            # Then delete from main product and item tables
            conn.execute("DELETE FROM products WHERE id = ?", (product_id,))
            conn.execute("DELETE FROM items WHERE id = ?", (item_id,))  # Use the correct item_id
            
            con.commit()
            return {"success": True, "message": "Product, item, and all references deleted."}
        except Exception as e:
            return {"success": False, "message": str(e)}


#Suppliers CRUD
def get_suppliers():
        with duckdb.connect('backend/db_timestock') as conn:
            return conn.execute("SELECT * FROM suppliers").fetchdf()

def add_supplier(data: dict):
    # Normalize input
    firstname = data['firstname'].strip().title()
    lastname = data['lastname'].strip().title()
    contact_name = data['contact_name'].strip().title()
    contact_number = data['contact_number'].strip()
    email = data['email'].strip().lower()
    address = data['address'].strip()

    # Check for existing supplier
    existing = con.execute("""
        SELECT 1 FROM suppliers 
        WHERE firstname = ? AND lastname = ? 
        AND (contact_number = ? OR email = ?)
    """, (firstname, lastname, contact_number, email)).fetchone()

    if existing:
        return {"success": False, "message": "Supplier already exists."}

    # Insert new supplier
    con.execute("""
        INSERT INTO suppliers (
            firstname, lastname, contact_name, contact_number,
            email, address, date_created
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (firstname, lastname, contact_name, contact_number, email, address, datetime.utcnow()))

    return {"success": True, "message": "Supplier added successfully."}

    
def update_supplier(id: str, data: dict):
    firstname = data['firstname'].strip().title()
    lastname = data['lastname'].strip().title()
    contact_name = data['contact_name'].strip().title()
    contact_number = data['contact_number'].strip()
    email = data['email'].strip().lower()
    address = data['address'].strip()

    con.execute("""
        UPDATE suppliers SET
            firstname = ?,
            lastname = ?,
            contact_name = ?,
            contact_number = ?,
            email = ?,
            address = ?
        WHERE id = ?
    """, (firstname, lastname, contact_name, contact_number, email, address, id))

def delete_supplier(id: str):
    con.execute("DELETE FROM suppliers WHERE id = ?", (id,))


def create_order_transaction(data: dict):
    items = data.pop('items')
    total_amount = 0

    # Step 0: Create new customer if needed
    customer_id = data.get('customer_id')
    customer_data = data.get('customer')

    if not customer_id and customer_data:
        customer_id = con.execute("""
            INSERT INTO customers (
                firstname, lastname, contact_number, email, address, date_created
            ) VALUES (?, ?, ?, ?, ?, ?)
            RETURNING id
        """, (
            customer_data['firstname'].strip().title(),
            customer_data['lastname'].strip().title(),
            customer_data['contact_number'].strip(),
            customer_data['email'].strip(),
            customer_data['address'].strip().title(),
            datetime.utcnow()
        )).fetchone()[0]
    elif not customer_id:
        raise ValueError("Either customer_id or customer data must be provided.")

    try:
        con.execute("BEGIN")

        # Step 1: Pre-check ALL material stock
        material_requirements = {}

        for item in items:
            product_id = item['product_id']
            quantity = item['quantity']

            materials = con.execute("""
                SELECT pm.material_id, pm.used_quantity, m.current_stock, i.item_name, m.unit_measurement
                    FROM product_materials pm
                    JOIN materials m ON pm.material_id = m.id
                    JOIN items i ON m.item_id = i.id
                    WHERE pm.product_id = ?
            """, (product_id,)).fetchall()

            for material_id, used_qty, current_stock, item_name, unit in materials:
                total_needed = used_qty * quantity

                material_requirements[material_id] = {
                        "needed": 0,
                        "available": current_stock,
                        "item_name": item_name,
                        "unit": unit
                    }

                material_requirements[material_id]["needed"] += total_needed

        # Now check all at once
        lacking_materials = [
            f"{v['item_name']} (Need: {v['needed']} {v['unit']}, Available: {v['available']} {v['unit']})"
            for v in material_requirements.values() if v["needed"] > v["available"]
        ]

        if lacking_materials:
            formatted_message = "Insufficient material stock for the following materials:\n\n"
            formatted_message += "\n".join(f"• {item}" for item in lacking_materials)

            raise HTTPException(
                status_code=400,
                detail=formatted_message
            )


        # Step 2: Insert transaction
        transaction_id = con.execute("""
            INSERT INTO order_transactions (
                customer_id, status_id, admin_id, date_created, total_amount
            ) VALUES (?, ?, ?, ?, ?)
            RETURNING id
        """, (
            customer_id,
            data['status_id'],
            data['admin_id'],
            datetime.utcnow(),
            0.0
        )).fetchone()[0]

        # Step 3: Process order items
        for item in items:
            product_id = item['product_id']
            quantity = item['quantity']

            unit_price = con.execute("""
                SELECT unit_price FROM products WHERE id = ?
            """, (product_id,)).fetchone()

            if not unit_price:
                raise ValueError(f"Product ID {product_id} not found.")

            unit_price = unit_price[0]
            line_total = quantity * unit_price
            total_amount += line_total

            # Deduct material stock and log transaction
            materials = con.execute("""
                SELECT pm.material_id, pm.used_quantity
                FROM product_materials pm
                WHERE pm.product_id = ?
            """, (product_id,)).fetchall()

            for material_id, used_qty in materials:
                total_used = used_qty * quantity

                # Fetch supplier
                supplier = con.execute("""
                    SELECT supplier_id FROM materials WHERE id = ?
                """, (material_id,)).fetchone()

                if not supplier or not supplier[0]:
                    raise ValueError(f"No supplier found for material ID {material_id}.")

                supplier_id = supplier[0]

                # Create stock transaction
                stock_transaction_id = con.execute("""
                    INSERT INTO stock_transactions (
                        stock_type_id, supplier_id, admin_id, employee_id, date_created
                    ) VALUES (?, ?, ?, NULL, ?)
                    RETURNING id
                """, (
                    'STT002',
                    supplier_id,
                    data['admin_id'],
                    datetime.utcnow()
                )).fetchone()[0]

                # Log stock item
                con.execute("""
                    INSERT INTO stock_transaction_items (
                        stock_transaction_id, material_id, quantity
                    ) VALUES (?, ?, ?)
                """, (
                    stock_transaction_id,
                    material_id,
                    total_used
                ))

                # Deduct from material stock
                con.execute("""
                    UPDATE materials
                    SET current_stock = current_stock - ?
                    WHERE id = ?
                """, (total_used, material_id))

            # Add order item
            con.execute("""
                INSERT INTO order_items (
                    order_id, product_id, quantity, unit_price
                ) VALUES (?, ?, ?, ?)
            """, (
                transaction_id,
                product_id,
                quantity,
                unit_price
            ))

        # Update total amount
        con.execute("""
            UPDATE order_transactions
            SET total_amount = ?
            WHERE id = ?
        """, (total_amount, transaction_id))

        con.commit()
        return {"transaction_id": transaction_id, "message": "Order successfully placed."}
    except Exception as e:
        con.execute("ROLLBACK")
        raise e


def get_order_transactions_detailed():
    return con.execute("""
        SELECT 
            ot.id AS transaction_id,
            CONCAT(c.firstname, ' ', c.lastname) AS customer_name,
            c.contact_number,
            c.email AS customer_email,
            c.address,

            os.status_code,
            os.description AS status_description,

            CONCAT(a.firstname, ' ', a.lastname) AS admin_name,
            a.email AS admin_email,

            ot.date_created,
            ot.total_amount,

            COALESCE(SUM(oi.quantity), 0) AS total_items_ordered,

            -- Concatenate product names into a comma-separated list
            GROUP_CONCAT(DISTINCT i.item_name, ', ') AS product_names

        FROM order_transactions ot
        JOIN customers c ON ot.customer_id = c.id
        JOIN order_statuses os ON ot.status_id = os.id
        JOIN admin a ON ot.admin_id = a.id
        LEFT JOIN order_items oi ON ot.id = oi.order_id
        LEFT JOIN products p ON oi.product_id = p.id
        LEFT JOIN items i ON p.item_id = i.id

        GROUP BY 
            ot.id, customer_name, c.contact_number, c.email, c.address,
            os.status_code, os.description,
            admin_name, a.email,
            ot.date_created, ot.total_amount

        ORDER BY ot.date_created DESC
    """).fetchdf()


#Other Get/Reads
def get_unit_measurements():
    return con.execute("""
        SELECT id, measurement_code, description
        FROM unit_measurements
    """).fetchdf()

def get_stock_transaction_types():
    return con.execute("""
        SELECT id, type_code, description
        FROM stock_transaction_types
    """).fetchdf()

def get_order_statuses():
    return con.execute("""
        SELECT id, status_code, description
        FROM order_statuses
    """).fetchdf()

# Auth
def get_user_by_email(email: str):
    conn = duckdb.connect('backend/db_timestock')

    # Check admin
    admin_query = """
        SELECT id, firstname, lastname, email, password, 'admin' AS role
        FROM admin
        WHERE email = ?
        LIMIT 1
    """
    admin_result = conn.execute(admin_query, [email]).fetchone()
    if admin_result:
        columns = [desc[0] for desc in conn.description]
        conn.close()
        return dict(zip(columns, admin_result))

    # Check employee
    employee_query = """
        SELECT id, firstname, lastname, email, password, 'employee' AS role,
               contact_number, is_active
        FROM employees
        WHERE email = ?
        LIMIT 1
    """
    employee_result = conn.execute(employee_query, [email]).fetchone()
    if employee_result:
        columns = [desc[0] for desc in conn.description]
        conn.close()
        return dict(zip(columns, employee_result))

    conn.close()
    return None


def authenticate_user(email: str, password: str):
    """
    Authenticates a user from either table using Argon2 password hashing.
    Returns dict with user details if correct, else None.
    """
    user = get_user_by_email(email)
    if not user:
        return None

    try:
        ph.verify(user["password"], password)
    except VerifyMismatchError:
        return None

    return user


# Settings Functionalities
def get_employees():
    return con.execute("""
        SELECT
             id AS employee_id,
             firstname || '' || lastname AS fullname,
             email,
             contact_number,
             is_active AS status, 
             date_created,
             date_updated,
             last_login
        FROM employees
        """).fetchdf()


def add_employee(data: dict):
    firstname = data['firstname'].strip().title()
    lastname = data['lastname'].strip().title()
    email = data['email'].strip().lower()
    password = data['password'].strip()

    ph = PasswordHasher()
    pw_hash = ph.hash(password)

    contact_number = data['contact_number'].strip()

    if con.execute(
        "SELECT 1 FROM employees WHERE email = ?",
        (email,)
    ).fetchone():
        return {"success": False, "message": "Email is already registered."}

    if con.execute(
        "SELECT 1 FROM employees WHERE contact_number = ?",
        (contact_number,)
    ).fetchone():
        return {"success": False, "message": "Contact number is already in use."}

    cur = con.cursor()
    cur.execute("""
        INSERT into employees(firstname, lastname, email, password, contact_number)
        VALUES (?, ?, ?, ?, ?)
    """, (firstname, lastname, email, pw_hash, contact_number))

    con.commit()
    return {"success": True, "Message": "Employee added successfully!"}


def update_account_status(id: str, is_active: bool):
    status = con.execute(
        """
        UPDATE employees
        SET is_active = ?,
            date_updated = NOW()
        WHERE id = ?
        """, (is_active, id)
    )
    if status.rowcount == 0:
        raise HTTPException(status_code=404, detail=f"Employee {id} not found.")
    con.commit()
    return {"success": True, "id": id, "is_active": is_active}


def change_employee_password(
        admin_id: str,
        target_employee_id: str,
        new_password: str
    ) -> dict:

    ph = PasswordHasher()

    row = con.execute(
        "SELECT password FROM admin WHERE id = ?", (admin_id,)
    ).fetchone()

    if not row:
        raise HTTPException(status_code=404, detail="Admin account not found.")
    
    new_password = new_password.strip()
    if len(new_password) < 8:
        return {"success": False, "message": "New password must be at least 8 characters."}
    
    try:
        new_hash = ph.hash(new_password)
    except Exception:
        raise HTTPException(status_code=500, detail="Error hashing new password.")
    
    res = con.execute(
        """
        UPDATE employees
        SET password = ?,
            date_updated = NOW()
        WHERE id = ?
        """,
        (new_hash, target_employee_id)
    )

    if res.rowcount == 0:
        return {"success": False, "message": "Target user not found."}
    
    con.commit()
    return {"success": True, "message": "Employee password changed successfully."}


# One-time function to convert plaintext passwords stored in the database into hash:
def migrate_plaintext_passwords_to_hash():
    """
    Scan both `admin` and `employees` tables,
    hash any passwords that aren’t already Argon2 hashes,
    and update them in place.
    Returns counts of how many rows were updated.
    """
    ph = PasswordHasher()
    updated = {"admin": 0, "employees": 0}

    for table in ("admin", "employees"):
        rows = con.execute(f"SELECT id, password FROM {table}").fetchall()
        count = 0

        for _id, pw in rows:
            if isinstance(pw, str) and pw.startswith("$argon2id$"):
                continue

            try:
                new_hash = ph.hash(pw)
            except Exception:
                continue

            con.execute(
                f"UPDATE {table} SET password = ? WHERE id = ?",
                (new_hash, _id)
            )
            count += 1

        updated[table] = count

    con.commit()
    return {
        "success": True,
        "updated_admin_passwords": updated["admin"],
        "updated_employee_passwords": updated["employees"]
    }