import duckdb
from datetime import datetime, timedelta
from collections import defaultdict
from fastapi import HTTPException, Request 
from argon2 import PasswordHasher
from argon2.exceptions import VerifyMismatchError
from typing import Optional


con = duckdb.connect('backend/db_timestock')
ph = PasswordHasher()


def log_audit(
    entity: str,
    entity_id: str,
    action: str,
    details: Optional[str] = None,
    admin_id: Optional[str] = None,
    employee_id: Optional[str] = None,
    cur=None
):
    if bool(admin_id) == bool(employee_id):
        raise ValueError("Provide exactly one of admin_id or employee_id")

    conn_used = None
    own_cursor = False

    if cur is None:
        try:
            conn_used = con
        except NameError:
            raise RuntimeError("Database connection `con` is not defined in this module.")
        cur = conn_used.cursor()
        own_cursor = True
    else:
        if hasattr(cur, "cursor") and not hasattr(cur, "execute"):
            conn_used = cur
            cur = conn_used.cursor()
            own_cursor = True
        else:
            conn_used = None
            own_cursor = False

    if admin_id:
        row = cur.execute("SELECT 1 FROM admin WHERE id = ?", (admin_id,)).fetchone()
        if not row:
            raise ValueError("admin_id not found")
    if employee_id:
        row = cur.execute("SELECT 1 FROM employee WHERE id = ?", (employee_id,)).fetchone()
        if not row:
            raise ValueError("employee_id not found")

    cur.execute(
        """
        INSERT INTO auditlogs (action_time, admin_id, employee_id, entity, entity_id, action, details)
        VALUES (CURRENT_TIMESTAMP, ?, ?, ?, ?, ?, ?)
        """,
        (admin_id, employee_id, entity, entity_id, action, details)
    )

    if own_cursor and conn_used is not None:
        conn_used.commit()


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


# THIS IS DONE
def add_product_materials(
    data: dict,
    admin_id: Optional[str] = None,
    cur=None
):
    if admin_id is None:
        raise ValueError("admin_id is required for audit logging (admin only)")

    conn_used = None
    own_cursor = False
    if cur is None:
        try:
            conn_used = con
        except NameError:
            raise RuntimeError("Database connection `con` is not defined in this module.")
        cur = conn_used.cursor()
        own_cursor = True
    else:
        if hasattr(cur, "cursor") and not hasattr(cur, "execute"):
            conn_used = cur
            cur = conn_used.cursor()
            own_cursor = True

    try:
        product_id = data['product_id']
        materials = data.get('materials', [])
        if not isinstance(materials, list) or not materials:
            raise ValueError("`materials` must be a non-empty list")

        # Track counts and results
        inserted = 0
        skipped = 0
        single_material_audit_info = None

        # Iterate and insert (atomic with audit because we use same cursor)
        for material in materials:
            material_id = material.get('material_id')
            used_quantity = material.get('used_quantity')

            if not material_id:
                raise ValueError("Each material must have a 'material_id'")
            if used_quantity is None:
                raise ValueError(f"Material '{material_id}' missing 'used_quantity'")

            existing = cur.execute(
                "SELECT 1 FROM product_materials WHERE product_id = ? AND material_id = ?",
                (product_id, material_id)
            ).fetchone()
            if existing:
                skipped += 1
                continue

            unit_cost = material.get('unit_cost')
            if unit_cost is None:
                row = cur.execute("SELECT material_cost FROM materials WHERE id = ?", (material_id,)).fetchone()
                if not row:
                    raise ValueError(f"Material with ID '{material_id}' not found.")
                unit_cost = row[0]

            cur.execute(
                """
                INSERT INTO product_materials (product_id, material_id, used_quantity, unit_cost)
                VALUES (?, ?, ?, ?)
                """,
                (product_id, material_id, used_quantity, unit_cost)
            )
            inserted += 1

            # If this is a single-material call, capture info for per-item audit
            if len(materials) == 1:
                single_material_audit_info = {
                    "material_id": material_id,
                    "used_quantity": used_quantity,
                    "unit_cost": unit_cost
                }

        # Decide and write audit(s)
        if len(materials) == 1 and single_material_audit_info is not None:
            # Single add: log a per-item audit entry
            single_details = (
                f"Added material {single_material_audit_info['material_id']} "
                f"to product={product_id} used_quantity={single_material_audit_info['used_quantity']} "
                f"unit_cost={single_material_audit_info['unit_cost']}"
            )
            log_audit(
                entity="product_materials",
                entity_id=f"{product_id}:{single_material_audit_info['material_id']}",
                action="create",
                details=single_details,
                admin_id=admin_id,
                cur=cur
            )
        # commit if we opened the connection/cursor here
        if own_cursor and conn_used is not None:
            conn_used.commit()

        return {"success": True, "inserted": inserted, "skipped": skipped}

    except Exception:
        if own_cursor and conn_used is not None:
            conn_used.rollback()
        raise



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


# THIS IS DONE
def update_product_material(
    product_id: str, 
    material_id: str | None = None, 
    used_quantity: float | None = None, 
    unit_cost: float | None = None,
    admin_id: Optional[str] = None,
    cur = None
):
    if admin_id is None:
        raise ValueError("admin_id is required for audit logging (admin only)")
    
    conn_used = None
    own_cursor = False
    if cur is None:
        try:
            conn_used = con
        except NameError:
            raise RuntimeError("Database connection `con` is not defined in this module.")
        cur = conn_used.cursor()
        own_cursor = True
    else:
        if hasattr(cur, "cursor") and not hasattr(cur, "execute"):
            conn_used = cur
            cur = conn_used.cursor()
            own_cursor = True
    
    try:
        old_row = cur.execute(
            "SELECT used_quantity, unit_cost FROM product_materials WHERE product_id = ? AND material_id = ?",
            (product_id, material_id)
        ).fetchone()

        if not old_row:
            raise ValueError("No matching product-material found to update.")

        cur.execute(
            """
            UPDATE product_materials
            SET used_quantity = ?, unit_cost = ?
            WHERE product_id = ? AND material_id = ?
            """, (used_quantity, unit_cost, product_id, material_id)
        )

        # optional defensive check for affected rows
        affected = getattr(cur, "rowcount", None)
        if affected == 0:
            raise ValueError("No matching product-material found to update.") 

        details = f"Updated {material_id} with the following details: used_quantity = from '{old_row[0]}' into '{used_quantity}', unit_cost = from '{old_row[1]}' into '{unit_cost}'."
        log_audit(
            entity="product_materials",
            entity_id=f"{product_id}:{material_id}",
            action="update",   # use lowercase for consistency
            details=details,
            admin_id=admin_id,
            cur=cur
        )

        if own_cursor and conn_used is not None:
            conn_used.commit()
        return {"success": True, "updated": 1}
    except Exception:
        if own_cursor and conn_used is not None:
            conn_used.rollback()
        raise

# THIS IS DONE
def delete_product_material(
    product_id: str, 
    material_id: str,
    admin_id: Optional[str] = None,
    cur = None
):
    if admin_id is None:
        raise ValueError("admin_id is required for audit logging (admin only)")
    
    conn_used = None
    own_cursor = False
    if cur is None:
        try:
            conn_used = con
        except NameError:
            raise RuntimeError("Database connection `con` is not defined in this module.")
        cur = conn_used.cursor()
        own_cursor = True
    else:
        if hasattr(cur, "cursor") and not hasattr(cur, "execute"):
            conn_used = cur
            cur = conn_used.cursor()
            own_cursor = True

    try:
        old_row = cur.execute(
            "SELECT used_quantity, unit_cost FROM product_materials WHERE product_id = ? AND material_id = ?",
            (product_id, material_id)
        ).fetchone()

        if not old_row:
            raise ValueError("No matching product-material found to delete.")
        
        used_quantity, unit_cost = old_row[0], old_row[1]

        result = con.execute("""
            DELETE FROM product_materials
            WHERE product_id = ? AND material_id = ?
        """, (product_id, material_id))
        
        affected = getattr(result, "rowcount", None)
        if affected is None:
            affected = 1

        if affected == 0:
            raise ValueError("No matching product-material to delete.")
        
        details=f"Deleted {material_id} containing: used_quantity = '{used_quantity}', unit_cost = '{unit_cost}'."
        log_audit(
            entity="product_materials",
            entity_id=f"{product_id}:{material_id}",
            action="delete",
            details=details,
            admin_id=admin_id,
            cur=cur
        )

        if own_cursor and conn_used is not None:
            conn_used.commit()
        return {"success": True, "deleted": affected}
    except Exception:
        if own_cursor and conn_used is not None:
            conn_used.rollback()
        raise


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

# THIS IS DONE
def add_product_category(
    data: dict,
    admin_id: Optional[str] = None,
    cur = None
):
    
    if admin_id is None:
        raise ValueError("admin_id is required for audit logging (admin only)")
    
    conn_used = None
    own_cursor = False
    if cur is None:
        try:
            conn_used = con
        except NameError:
            raise RuntimeError("Database connection `con` is not defined in this module.")
        cur = conn_used.cursor()
        own_cursor = True
    else:
        if hasattr(cur, "cursor") and not hasattr(cur, "execute"):
            conn_used = cur
            cur = conn_used.cursor()
            own_cursor = True
    
    try:
        category_name = data['category_name'].strip().title()
        description = data['description'].strip()

        if not category_name:
            raise ValueError("category name cannot be empty.")

        # Check for duplicates
        exists = cur.execute("""
            SELECT 1 FROM product_categories WHERE LOWER(TRIM(category_name)) = ?
        """, (category_name,)).fetchone()

        if exists:
            return None  # Return None explicitly to indicate duplicate

        new_id = cur.execute("""
            INSERT INTO product_categories (category_name, description)
            VALUES (?, ?)
            RETURNING id
        """, (category_name, description)).fetchone()[0]

        cur.execute("""
            INSERT INTO item_categories (id, category_name, description)
            VALUES (?, ?, ?)
        """, (new_id, category_name, description))

        details = f"Added {category_name} in product_categories containing this description = '{description}'."
        log_audit(
            entity="product_categories",
            entity_id=new_id,
            action="create",
            details=details,
            admin_id=admin_id,
            cur=cur
        )

        if own_cursor and conn_used is not None:
            conn_used.commit()
        return new_id
    
    except Exception:
        if own_cursor and conn_used is not None:
            conn_used.rollback()
        raise


# THIS IS DONE
def update_product_category(
    id: str, 
    data: dict,
    admin_id: Optional[str] = None,
    cur = None
):
    if admin_id is None:
        raise ValueError("admin_id is required for audit logging (admin only)")
    
    conn_used = None
    own_cursor = False
    if cur is None:
        try:
            conn_used = con
        except NameError:
            raise RuntimeError("Database connection `con` is not defined in this module.")
        cur = conn_used.cursor()
        own_cursor = True
    else:
        if hasattr(cur, "cursor") and not hasattr(cur, "execute"):
            conn_used = cur
            cur = conn_used.cursor()
            own_cursor = True

    try:
        category_name = data['category_name'].strip().title()
        description = data['description'].strip()

        old_row = cur.execute(
            "SELECT category_name, description FROM product_categories WHERE id = ?", (id,)
        ).fetchone()

        if not old_row:
            raise ValueError("No matching ID found to update.")
        
        cur.execute("""
            UPDATE product_categories SET
                category_name = ?,
                description = ?
            WHERE id = ?
        """, (category_name, description, id))

        details = f"Updated {id} with the following details: category_name = from '{old_row[0]}' into '{category_name}', description = from '{old_row[1]}' into '{description}'."
        log_audit(
            entity="product_categories",
            entity_id=id,
            action="update",
            details=details,
            admin_id=admin_id,
            cur=cur
        )

        if own_cursor and conn_used is not None:
            conn_used.commit()
        return {"success": True, "updated": 1}
    
    except Exception:
        if own_cursor and conn_used is not None:
            conn_used.rollback()
        raise

# THIS IS DONE
def delete_product_categories(
    id: str,
    admin_id: Optional[str] = None,
    cur = None
):
    if admin_id is None:
        raise ValueError("admin_id is required for audit logging (admin only)")
    
    conn_used = None
    own_cursor = False
    if cur is None:
        try:
            conn_used = con
        except NameError:
            raise RuntimeError("Database connection 'con' is not defined in this module.")
        cur = conn_used.cursor()
        own_cursor = True
    else:
        if hasattr(cur, "cursor") and not hasattr(cur, "execute"):
            conn_used = cur
            cur = conn_used.cursor()
            own_cursor = True
    
    try:
        old_row = cur.execute(
            "SELECT category_name, description FROM product_categories WHERE id = ?", (id,)
        ).fetchone()

        if not old_row:
            raise ValueError("No matching ID found to delete.")
        
        category_name, description = old_row[0], old_row[1]

        result = cur.execute("DELETE FROM product_categories WHERE id = ?;", (id,))

        affected = getattr(result, "rowcount", None)
        if affected is None:
            affected = 1

        if affected == 0:
            raise ValueError("No matching product category to delete.")

        details = f"Deleted {id} containing: category_name = '{category_name}', description = '{description}'."
        log_audit(
            entity="product_categories",
            entity_id=id,
            action="delete",
            details=details,
            admin_id=admin_id,
            cur=cur
        )

        if own_cursor and conn_used is not None:
            conn_used.commit()
        return {"success": True, "deleted": 1}
    except Exception:
        if own_cursor and conn_used is not None:
            conn_used.rollback()
        raise


# Material_categories CRUD
def get_material_categories():
      with duckdb.connect('backend/db_timestock') as conn:
        return conn.execute("SELECT * FROM material_categories").fetchdf()


# THIS IS DONE
def add_material_category(
    data: dict,
    admin_id: Optional[str] = None,
    cur = None
):
    if admin_id is None:
        raise ValueError("admin_id is required for audit logging (admin only)")
    
    conn_used = None
    own_cursor = False
    if cur is None:
        try:
            conn_used = con
        except NameError:
            raise RuntimeError("Database connection `con` is not defined in this module.")
        cur = conn_used.cursor()
        own_cursor = True
    else:
        if hasattr(cur, "cursor") and not hasattr(cur, "execute"):
            conn_used = cur
            cur = conn_used.cursor()
            own_cursor = True

    try:
        raw_name = data['category_name'].strip()
        if not raw_name:
            raise ValueError("category_name cannot be empty")
        name = raw_name.lower()
        description = data['description'].strip()

        # Case-insensitive duplicate check
        exists = cur.execute("""
            SELECT 1 FROM material_categories WHERE LOWER(TRIM(category_name)) = ?
        """, (name,)).fetchone()

        if exists:
            return None  # Prevent duplicate insert

        formatted_name = raw_name.title()

        new_id = cur.execute("""
            INSERT INTO material_categories (category_name, description)
            VALUES (?, ?)
            RETURNING id
        """, (formatted_name, description)).fetchone()[0]

        cur.execute("""
            INSERT INTO item_categories (id, category_name, description)
            VALUES (?, ?, ?)
        """, (new_id, formatted_name, description))

        details = f"Added {formatted_name} into material_categories containing this description = '{description}'"
        log_audit(
            entity="material_categories",
            entity_id=new_id,
            action="create",
            details=details,
            admin_id=admin_id,
            cur=cur
        )

        if own_cursor and conn_used is not None:
            conn_used.commit()
        return new_id
    
    except Exception:
        if own_cursor and conn_used is not None:
            conn_used.rollback()
        raise

# THIS IS DONE
def update_material_category(
    id: str, 
    data: dict,
    admin_id: Optional[str] = None,
    cur = None
):
    if admin_id is None:
        raise ValueError("admin_id is required for audit logging (admin only)")
    
    conn_used = None
    own_cursor = False
    if cur is None:
        try:
            conn_used = con
        except NameError:
            raise RuntimeError("Database connection `con` is not defined in this module.")
        cur = conn_used.cursor()
        own_cursor = True
    else:
        if hasattr(cur, "cursor") and not hasattr(cur, "execute"):
            conn_used = cur
            cur = conn_used.cursor()
            own_cursor = True
    
    try:
        category_name = data['category_name'].strip().title()
        description = data['description'].strip()

        old_row = cur.execute(
            "SELECT category_name, description FROM material_categories WHERE id = ?", (id,)
        ).fetchone()

        if not old_row:
            raise ValueError("No matching ID found to update.")

        cur.execute("""
            UPDATE material_categories SET
                category_name = ?,
                description = ?
            WHERE id = ?
        """, (category_name, description, id))

        details = f"Updated {id} with the following details: category_name = from '{old_row[0]}' into '{category_name}', description = from '{old_row[1]}' into '{description}'."
        log_audit(
            entity="material_categories",
            entity_id=id,
            action="update",
            details=details,
            admin_id=admin_id,
            cur=cur
        )

        if own_cursor and conn_used is not None:
            conn_used.commit()
        return {"success": True, "updated": 1}
    
    except Exception:
        if own_cursor and conn_used is not None:
            conn_used.rollback()
        raise

# THIS IS DONE
def delete_material_category(
    id: str,
    admin_id: Optional[str] = None,
    cur = None
):
    if admin_id is None:
        raise ValueError("admin_id is required for audit logging (admin only)")
    
    conn_used = None
    own_cursor = False
    if cur is None:
        try:
            conn_used = con
        except NameError:
            raise RuntimeError("Database connection `con` is not defined in this module.")
        cur = conn_used.cursor()
        own_cursor = True
    else:
        if hasattr(cur, "cursor") and not hasattr(cur, "execute"):
            conn_used = cur
            cur = conn_used.cursor()
            own_cursor = True
    
    try:
        old_row = cur.execute(
            "SELECT category_name, description FROM material_categories WHERE id = ?", (id,)
        ).fetchone()

        if not old_row:
            raise ValueError("No matching ID ")
        
        category_name, description = old_row[0], old_row[1]
        
        result = cur.execute("DELETE FROM material_categories WHERE id = ?", (id,))

        affected = getattr(result, "rowcount", None)
        if affected is None:
            affected = 1

        if affected == 0:
            raise ValueError("No matching material category to delete.")

        details = f"Deleted {id} containing: category_name = {category_name}, description = {description}."
        log_audit(
            entity="material_categories",
            entity_id=id,
            action="delete",
            details=details,
            admin_id=admin_id,
            cur=cur
        )

        if own_cursor and conn_used is not None:
            conn_used.commit()
        return {"success": True, "deleted": affected}
    except Exception:
        if own_cursor and conn_used is not None:
            conn_used.rollback()
        raise


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

# THIS IS DONE
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
    supplier_id: str,
    admin_id: Optional[str] = None,
    cur=None
):
    """
    Minimal-change version that logs an audit row when admin_id is provided.
    Uses the same conn/cur detection pattern as other functions in this module.
    """
    # Get the item_id linked to the material
    # Use the provided connection `con` for initial checks (like original function)
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

    conn_used = None
    own_cursor = False
    # follow your existing pattern to accept either cur or connection
    if cur is None:
        try:
            conn_used = con
        except NameError:
            raise RuntimeError("Database connection `con` is not defined in this module.")
        cur = conn_used.cursor()
        own_cursor = True
    else:
        # if caller passed a connection object (has cursor) but not an execute method
        if hasattr(cur, "cursor") and not hasattr(cur, "execute"):
            conn_used = cur
            cur = conn_used.cursor()
            own_cursor = True
        else:
            conn_used = None
            own_cursor = False

    try:
        # read current values for audit details (before update) using cur
        old_mat = cur.execute(
            "SELECT unit_measurement, material_cost, current_stock, minimum_stock, maximum_stock, supplier_id "
            "FROM materials WHERE id = ?", (material_id,)
        ).fetchone()

        old_item = cur.execute(
            "SELECT item_name, item_decription, category_id FROM items WHERE id = ?", (item_id,)
        ).fetchone()

        # perform updates using the same cursor
        cur.execute("""
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

        cur.execute("""
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

        # write audit if admin_id provided (same cursor so atomic)
        if admin_id is not None:
            old_mat_vals = old_mat if old_mat is not None else ('', '', '', '', '', '')
            old_item_vals = old_item if old_item is not None else ('', '', '')
            details = (
                f"materials(id={material_id}): "
                f"unit_measurement '{old_mat_vals[0]}' -> '{unit_measurement}', "
                f"material_cost {old_mat_vals[1]} -> {material_cost}, "
                f"current_stock {old_mat_vals[2]} -> {current_stock}, "
                f"minimum_stock {old_mat_vals[3]} -> {minimum_stock}, "
                f"maximum_stock {old_mat_vals[4]} -> {maximum_stock}, "
                f"supplier_id {old_mat_vals[5]} -> {supplier_id}; "
                f"items(id={item_id}): item_name '{old_item_vals[0]}' -> '{item_name}', "
                f"item_decription '{old_item_vals[1]}' -> '{item_description}', "
                f"category_id {old_item_vals[2]} -> {category_id}"
            )
            log_audit(
                entity="materials",
                entity_id=str(material_id),
                action="update",
                details=details,
                admin_id=admin_id,
                cur=cur
            )

        # commit only if we opened/owned the cursor/connection here
        if own_cursor and conn_used is not None:
            conn_used.commit()

    except Exception as e:
        if own_cursor and conn_used is not None:
            conn_used.rollback()
        raise e


# THIS IS DONE
def update_order_status(
    transaction_id: str, 
    new_status_code: str, 
    con, 
    admin_id: Optional[str] = None, 
    cur=None
):
    """
    Minimal-change version that logs an audit row when admin_id is provided.
    Keeps the same semantics as your original function but accepts `cur` and `admin_id`.
    """
    conn_used = None
    own_cursor = False
    if cur is None:
        try:
            conn_used = con
        except NameError:
            raise RuntimeError("Database connection `con` is not defined in this module.")
        cur = conn_used.cursor()
        own_cursor = True
    else:
        if hasattr(cur, "cursor") and not hasattr(cur, "execute"):
            conn_used = cur
            cur = conn_used.cursor()
            own_cursor = True
        else:
            conn_used = None
            own_cursor = False

    try:
        # Validate status_code exists
        status_row = cur.execute("""
            SELECT id FROM order_statuses WHERE status_code = ?
        """, (new_status_code,)).fetchone()
        
        if not status_row:
            return {"error": "Status code not found."}

        # Validate transaction exists and get old status id
        txn_row = cur.execute("""SELECT status_id FROM order_transactions WHERE id = ?""", (transaction_id,)).fetchone()
        
        if not txn_row:
            return {"error": "Transaction ID not found."}

        old_status_id = txn_row[0]
        old_status_code_row = cur.execute("SELECT status_code FROM order_statuses WHERE id = ?", (old_status_id,)).fetchone()
        old_status_code = old_status_code_row[0] if old_status_code_row else str(old_status_id)

        # Update order transaction using cur
        cur.execute("""
            UPDATE order_transactions
            SET status_id = ?
            WHERE id = ?
        """, (status_row[0], transaction_id))

        # write audit row if admin_id provided
        if admin_id is not None:
            details = f"order_transactions(id={transaction_id}): status '{old_status_code}' -> '{new_status_code}'"
            log_audit(
                entity="order_transactions",
                entity_id=str(transaction_id),
                action="update_status",
                details=details,
                admin_id=admin_id,
                cur=cur
            )

        if own_cursor and conn_used is not None:
            conn_used.commit()

        return {"success": True}

    except Exception as e:
        if own_cursor and conn_used is not None:
            conn_used.rollback()
        raise e


# THIS IS DONE
def add_material(
    data: dict,
    admin_id: Optional[str] = None,
    cur=None
):
    # Normalize item fields
    item_name = data['item_name'].strip().title()
    item_description = data['item_decription'].strip()  # Keep using 'item_decription' if that's the real column name
    category_id = data['category_id']

    # follow conn/cur pattern so audit can be atomic
    conn_used = None
    own_cursor = False
    if cur is None:
        try:
            conn_used = con
        except NameError:
            raise RuntimeError("Database connection `con` is not defined in this module.")
        cur = conn_used.cursor()
        own_cursor = True
    else:
        if hasattr(cur, "cursor") and not hasattr(cur, "execute"):
            conn_used = cur
            cur = conn_used.cursor()
            own_cursor = True

    try:
        # Check for existing item in the 'items' table
        existing_item = cur.execute("""
            SELECT 1 FROM items WHERE item_name = ?
        """, (item_name,)).fetchone()

        if existing_item:
            raise Exception(f"Item with name '{item_name}' already exists.")

        # Insert into items table (RETURNING id so we have item_id)
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

        # Insert into materials table and capture material_id
        material_id = cur.execute("""
            INSERT INTO materials (
                item_id, category_id, unit_measurement, material_cost,
                current_stock, minimum_stock, maximum_stock, supplier_id,
                date_created, date_updated
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            RETURNING id
        """, (
            item_id, category_id, unit_measurement, material_cost,
            current_stock, minimum_stock, maximum_stock, supplier_id,
            datetime.utcnow(), datetime.utcnow()
        )).fetchone()[0]

        # write audit if admin_id provided (same cursor, atomic)
        if admin_id is not None:
            details = (
                f"Added material id={material_id} (item_id={item_id}) "
                f"unit_measurement={unit_measurement}, material_cost={material_cost}, "
                f"current_stock={current_stock}, supplier_id={supplier_id}"
            )
            log_audit(
                entity="materials",
                entity_id=str(material_id),
                action="create",
                details=details,
                admin_id=admin_id,
                cur=cur
            )

        if own_cursor and conn_used is not None:
            conn_used.commit()

        # keep previous return value (item_id) for compatibility
        return item_id

    except Exception:
        if own_cursor and conn_used is not None:
            conn_used.rollback()
        raise

# THIS IS DONE
def stock_materials(
    data: dict,
    cur=None
):
    # items is required by existing callers
    items = data.pop("items")

    conn_used = None
    own_cursor = False
    if cur is None:
        try:
            conn_used = con
        except NameError:
            raise RuntimeError("Database connection `con` is not defined in this module.")
        cur = conn_used.cursor()
        own_cursor = True
    else:
        if hasattr(cur, "cursor") and not hasattr(cur, "execute"):
            conn_used = cur
            cur = conn_used.cursor()
            own_cursor = True

    try:
        # Use cur for all DB operations so the audit can be in the same transaction
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

            existing = cur.execute("""
                SELECT id FROM suppliers
                WHERE LOWER(contact_name) = LOWER(?)
                LIMIT 1
            """, (contact_name,)).fetchone()

            if existing:
                raise ValueError(f"Supplier with contact name '{contact_name}' already exists.")
            else:
                supplier_id = cur.execute("""
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
            result = cur.execute("""
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

        stock_transaction_id = cur.execute("""
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
            cur.execute("""
                INSERT INTO stock_transaction_items (
                    stock_transaction_id, material_id, quantity
                ) VALUES (?, ?, ?)
            """, (stock_transaction_id, material_id, quantity))

            # Update material stock
            cur.execute("""
                UPDATE materials
                SET current_stock = current_stock + ?
                WHERE id = ?
            """, (quantity, material_id))

        # Audit the stock transaction (log admin_id or employee_id)
        actor_kwargs = {}
        if admin_id:
            actor_kwargs['admin_id'] = admin_id
        else:
            actor_kwargs['employee_id'] = employee_id

        # Build a compact details string
        item_summary = ", ".join(f"{it['material_id']} x{it['quantity']}" for it in items[:20])
        details = f"Stock transaction id={stock_transaction_id}, items=[{item_summary}]"

        log_audit(
            entity="stock_transactions",
            entity_id=str(stock_transaction_id),
            action="create",
            details=details,
            cur=cur,
            **actor_kwargs
        )

        if own_cursor and conn_used is not None:
            conn_used.commit()

        return {
            "transaction_id": stock_transaction_id,
            "message": "Materials successfully stocked."
        }

    except Exception as e:
        if own_cursor and conn_used is not None:
            conn_used.rollback()  # <-- undo all changes
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


# THIS IS DONE
def delete_material(
    material_id: str,
    admin_id: Optional[str] = None,
    cur=None
):
    conn_used = None
    own_cursor = False
    if cur is None:
        try:
            conn_used = con
        except NameError:
            raise RuntimeError("Database connection `con` is not defined in this module.")
        cur = conn_used.cursor()
        own_cursor = True
    else:
        if hasattr(cur, "cursor") and not hasattr(cur, "execute"):
            conn_used = cur
            cur = conn_used.cursor()
            own_cursor = True

    try:
        # Get the item_id from the material first
        row = cur.execute("SELECT item_id FROM materials WHERE id = ?", (material_id,)).fetchone()
        if not row:
            return {"success": False, "message": "Material not found."}

        item_id = row[0]

        # Delete from referencing tables first to avoid FK constraint issues
        cur.execute("DELETE FROM product_materials WHERE material_id = ?", (material_id,))
        cur.execute("DELETE FROM stock_transaction_items WHERE material_id = ?", (material_id,))

        # Then delete the material and its item
        cur.execute("DELETE FROM materials WHERE id = ?", (material_id,))
        cur.execute("DELETE FROM items WHERE id = ?", (item_id,))

        # Audit the deletion if admin_id provided
        if admin_id is not None:
            details = f"Deleted material id={material_id} and item id={item_id}"
            log_audit(
                entity="materials",
                entity_id=str(material_id),
                action="delete",
                details=details,
                admin_id=admin_id,
                cur=cur
            )

        if own_cursor and conn_used is not None:
            conn_used.commit()
        return {"success": True, "message": "Material and corresponding item deleted successfully."}
    except Exception as e:
        if own_cursor and conn_used is not None:
            conn_used.rollback()
        # keep original behavior of returning failure dict for caller handling
        # but re-raise so callers that expect exceptions still get them
        raise



#Customer CRUD
def get_customers():
    return con.execute("SELECT * FROM customers").fetchdf()

# THIS IS DONE
def add_customer(data: dict, admin_id: Optional[str] = None, cur=None):
    if admin_id is None:
        raise ValueError("admin_id is required for audit logging (admin only)")

    conn_used = None
    own_cursor = False
    if cur is None:
        try:
            conn_used = con
        except NameError:
            raise RuntimeError("Database connection `con` is not defined in this module.")
        cur = conn_used.cursor()
        own_cursor = True
    else:
        # if caller passed a connection instead of a cursor
        if hasattr(cur, "cursor") and not hasattr(cur, "execute"):
            conn_used = cur
            cur = conn_used.cursor()
            own_cursor = True

    firstname = data['firstname'].strip().title()
    lastname = data['lastname'].strip().title()
    email = data['email'].strip().lower()
    address = data['address'].strip()
    contact_number = data['contact_number'].strip()

    existing = cur.execute("""
        SELECT 1 FROM customers 
        WHERE firstname = ? AND lastname = ? 
        AND (contact_number = ? OR email = ?)
    """, (firstname, lastname, contact_number, email)).fetchone()

    if existing:
        return {"success": False, "message": "Customer already exists."}

    try:
        new_id = cur.execute("""
            INSERT INTO customers (
                firstname, lastname, contact_number, email, address, date_created
            ) VALUES (?, ?, ?, ?, ?, ?)
            RETURNING id
        """, (firstname, lastname, contact_number, email, address, datetime.utcnow())).fetchone()[0]

        details = f"Added customer {firstname} {lastname} (email={email}, contact={contact_number})"
        log_audit(
            entity="customers",
            entity_id=str(new_id),
            action="create",
            details=details,
            admin_id=admin_id,
            cur=cur
        )

        if own_cursor and conn_used is not None:
            conn_used.commit()

        return {"success": True, "message": "Customer added successfully."}
    except Exception:
        if own_cursor and conn_used is not None:
            conn_used.rollback()
        raise

# THIS IS DONE
def update_customer(id: str, data: dict, admin_id: Optional[str] = None, cur=None):
    if admin_id is None:
        raise ValueError("admin_id is required for audit logging (admin only)")

    conn_used = None
    own_cursor = False
    if cur is None:
        try:
            conn_used = con
        except NameError:
            raise RuntimeError("Database connection `con` is not defined in this module.")
        cur = conn_used.cursor()
        own_cursor = True
    else:
        if hasattr(cur, "cursor") and not hasattr(cur, "execute"):
            conn_used = cur
            cur = conn_used.cursor()
            own_cursor = True

    firstname = data['firstname'].strip().title()
    lastname = data['lastname'].strip().title()
    email = data['email'].strip().lower()
    address = data['address'].strip()
    contact_number = data['contact_number'].strip()

    try:
        old_row = cur.execute(
            "SELECT firstname, lastname, contact_number, email, address FROM customers WHERE id = ?", (id,)
        ).fetchone()

        if not old_row:
            raise ValueError("Customer not found.")

        cur.execute("""
            UPDATE customers SET
                firstname = ?,
                lastname = ?,
                contact_number = ?,
                email = ?,
                address = ?
            WHERE id = ?
        """, (firstname, lastname, contact_number, email, address, id))

        details = (
            f"Customer {id} updated: "
            f"firstname: {old_row[0]} -> {firstname}, "
            f"lastname: {old_row[1]} -> {lastname}, "
            f"contact_number: {old_row[2]} -> {contact_number}, "
            f"email: {old_row[3]} -> {email}, "
            f"address: {old_row[4]} -> {address}"
        )
        log_audit(
            entity="customers",
            entity_id=str(id),
            action="update",
            details=details,
            admin_id=admin_id,
            cur=cur
        )

        if own_cursor and conn_used is not None:
            conn_used.commit()
    except Exception:
        if own_cursor and conn_used is not None:
            conn_used.rollback()
        raise


# def delete_customer(id: str):
#     con.execute("DELETE FROM customers WHERE id = ?", (id,))

# THIS IS DONE
def delete_customer(id: str, admin_id: Optional[str] = None, cur=None):
    if admin_id is None:
        raise ValueError("admin_id is required for audit logging (admin only)")

    conn_used = None
    own_cursor = False
    if cur is None:
        try:
            conn_used = con
        except NameError:
            raise RuntimeError("Database connection `con` is not defined in this module.")
        cur = conn_used.cursor()
        own_cursor = True
    else:
        if hasattr(cur, "cursor") and not hasattr(cur, "execute"):
            conn_used = cur
            cur = conn_used.cursor()
            own_cursor = True

    try:
        old_row = cur.execute("SELECT firstname, lastname, contact_number, email, address FROM customers WHERE id = ?", (id,)).fetchone()
        if not old_row:
            raise ValueError("Customer not found.")

        cur.execute("DELETE FROM customers WHERE id = ?", (id,))

        details = (
            f"Deleted customer {id}: firstname={old_row[0]}, lastname={old_row[1]}, "
            f"contact_number={old_row[2]}, email={old_row[3]}, address={old_row[4]}"
        )
        log_audit(
            entity="customers",
            entity_id=str(id),
            action="delete",
            details=details,
            admin_id=admin_id,
            cur=cur
        )

        if own_cursor and conn_used is not None:
            conn_used.commit()
    except Exception:
        if own_cursor and conn_used is not None:
            conn_used.rollback()
        raise


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


# def add_product(data: dict):
#    # Normalize item fields
#     item_name = data['item_name'].strip().title()
#     item_description = data['item_decription'].strip()
#     category_id = data['category_id']

#     # Check for duplicate item name
#     existing = con.execute("""
#         SELECT id FROM items WHERE LOWER(TRIM(item_name)) = ?
#     """, (item_name.lower(),)).fetchone()

#     if existing:
#         return {"success": False, "message": f"Item already exists with name: {item_name}"}

#     cur = con.cursor()

#     # Step 1: Insert into items first and get item_id
#     item_id = cur.execute("""
#         INSERT INTO items (
#             item_name, item_decription, category_id, date_created, date_updated
#         ) VALUES (?, ?, ?, ?, ?)
#         RETURNING id
#     """, (item_name, item_description, category_id, datetime.utcnow(), datetime.utcnow())).fetchone()[0]

#     # Step 2: Insert into products with that item_id
#     cur.execute("""
#         INSERT INTO products (
#             item_id, category_id, unit_price, materials_cost, status,
#             date_created, date_updated
#         ) VALUES (?, ?, ?, ?, ?, ?, ?)
#     """, (
#         item_id,
#         category_id,
#         data['unit_price'],
#         data['materials_cost'],
#         data['status'].strip().title(),
#         datetime.utcnow(),
#         datetime.utcnow()
#     ))

#     con.commit()
#     return {"success": True, "product_id": item_id, "message": "Product added successfully."}

# THIS IS DONE
def add_product(data: dict, admin_id: Optional[str] = None, cur=None):
    if admin_id is None:
        raise ValueError("admin_id is required for audit logging (admin only)")

    conn_used = None
    own_cursor = False
    if cur is None:
        try:
            conn_used = con
        except NameError:
            raise RuntimeError("Database connection `con` is not defined in this module.")
        cur = conn_used.cursor()
        own_cursor = True
    else:
        if hasattr(cur, "cursor") and not hasattr(cur, "execute"):
            conn_used = cur
            cur = conn_used.cursor()
            own_cursor = True

    # Normalize item fields
    item_name = data['item_name'].strip().title()
    item_description = data['item_decription'].strip()
    category_id = data['category_id']

    # Check for duplicate item name
    existing = cur.execute("""
        SELECT id FROM items WHERE LOWER(TRIM(item_name)) = ?
    """, (item_name.lower(),)).fetchone()

    if existing:
        return {"success": False, "message": f"Item already exists with name: {item_name}"}

    try:
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

        details = f"Created product (item_id={item_id}) name={item_name}, category_id={category_id}"
        log_audit(
            entity="products",
            entity_id=str(item_id),
            action="create",
            details=details,
            admin_id=admin_id,
            cur=cur
        )

        if own_cursor and conn_used is not None:
            conn_used.commit()
        return {"success": True, "product_id": item_id, "message": "Product added successfully."}
    except Exception:
        if own_cursor and conn_used is not None:
            conn_used.rollback()
        raise


# def update_product(
#     con,
#     product_id: str,
#     unit_price: float,
#     materials_cost: float,
#     status: str,
#     category_id: str,
#     item_name: str,
#     item_description: str
# ):
#     # Get the item_id linked to the product
#     item_id_result = con.execute(
#         "SELECT item_id FROM products WHERE id = ?", (product_id,)
#     ).fetchone()

#     if item_id_result is None:
#         raise ValueError(f"No product found with id {product_id}")

#     item_id = item_id_result[0]

#     # Validate uniqueness of item_name (excluding current item_id)
#     duplicate_check = con.execute("""
#         SELECT 1 FROM items
#         WHERE item_name = ? AND id != ?
#         LIMIT 1
#     """, (item_name, item_id)).fetchone()

#     if duplicate_check:
#         raise ValueError(f"Item name '{item_name}' already exists.")

#     # Update products table
#     con.execute("""
#         UPDATE products 
#         SET 
#             unit_price = ?, 
#             materials_cost = ?, 
#             status = ?, 
#             date_updated = CURRENT_TIMESTAMP
#         WHERE id = ?
#     """, (unit_price, materials_cost, status, product_id))

#     # Update items table
#     con.execute("""
#         UPDATE items
#         SET 
#             item_name = ?, 
#             item_decription = ?, 
#             category_id = ?, 
#             date_updated = CURRENT_TIMESTAMP
#         WHERE id = ?
#     """, (item_name, item_description, category_id, item_id))

# THIS IS DONE
def update_product(
    con,
    product_id: str,
    unit_price: float,
    materials_cost: float,
    status: str,
    category_id: str,
    item_name: str,
    item_description: str,
    admin_id: Optional[str] = None,
    cur=None
):
    if admin_id is None:
        raise ValueError("admin_id is required for audit logging (admin only)")

    conn_used = None
    own_cursor = False
    if cur is None:
        conn_used = con
        cur = conn_used.cursor()
        own_cursor = True
    else:
        if hasattr(cur, "cursor") and not hasattr(cur, "execute"):
            conn_used = cur
            cur = conn_used.cursor()
            own_cursor = True

    # Get the item_id linked to the product
    item_id_result = cur.execute("SELECT item_id FROM products WHERE id = ?", (product_id,)).fetchone()
    if item_id_result is None:
        raise ValueError(f"No product found with id {product_id}")

    item_id = item_id_result[0]

    # get old values for audit
    old_item_row = cur.execute("SELECT item_name, item_decription, category_id FROM items WHERE id = ?", (item_id,)).fetchone()
    old_product_row = cur.execute("SELECT unit_price, materials_cost, status FROM products WHERE id = ?", (product_id,)).fetchone()

    # Check for duplicate item name (excluding this item_id)
    duplicate_check = cur.execute("""
        SELECT 1 FROM items
        WHERE item_name = ? AND id != ?
        LIMIT 1
    """, (item_name, item_id)).fetchone()

    if duplicate_check:
        raise ValueError(f"Item name '{item_name}' already exists.")

    try:
        # Update products table
        cur.execute("""
            UPDATE products 
            SET 
                unit_price = ?, 
                materials_cost = ?, 
                status = ?, 
                date_updated = CURRENT_TIMESTAMP
            WHERE id = ?
        """, (unit_price, materials_cost, status, product_id))

        # Update items table
        cur.execute("""
            UPDATE items
            SET 
                item_name = ?, 
                item_decription = ?, 
                category_id = ?, 
                date_updated = CURRENT_TIMESTAMP
            WHERE id = ?
        """, (item_name, item_description, category_id, item_id))

        details = (
            f"product_id={product_id} updates: "
            f"unit_price {old_product_row[0]} -> {unit_price}, "
            f"materials_cost {old_product_row[1]} -> {materials_cost}, "
            f"status {old_product_row[2]} -> {status}; "
            f"item (id={item_id}) name {old_item_row[0]} -> {item_name}, "
            f"description updated, category {old_item_row[2]} -> {category_id}"
        )

        log_audit(
            entity="products",
            entity_id=str(product_id),
            action="update",
            details=details,
            admin_id=admin_id,
            cur=cur
        )

        if own_cursor and conn_used is not None:
            conn_used.commit()
    except Exception:
        if own_cursor and conn_used is not None:
            conn_used.rollback()
        raise


# def delete_product(product_id: str):
#     with duckdb.connect('backend/db_timestock') as conn:
#         try:
#             # Get the corresponding item_id from the product
#             item_result = conn.execute("SELECT item_id FROM products WHERE id = ?", (product_id,)).fetchone()
#             if not item_result:
#                 return {"success": False, "message": "Product not found."}
            
#             item_id = item_result[0]

#             # Delete from referencing tables first to avoid FK constraint issues
#             conn.execute("DELETE FROM product_materials WHERE product_id = ?", (product_id,))
#             conn.execute("DELETE FROM order_items WHERE product_id = ?", (product_id,))
            
#             # Then delete from main product and item tables
#             conn.execute("DELETE FROM products WHERE id = ?", (product_id,))
#             conn.execute("DELETE FROM items WHERE id = ?", (item_id,))  # Use the correct item_id
            
#             con.commit()
#             return {"success": True, "message": "Product, item, and all references deleted."}
#         except Exception as e:
#             return {"success": False, "message": str(e)}

# THIS IS DONE
def delete_product(product_id: str, admin_id: Optional[str] = None, cur=None):
    if admin_id is None:
        raise ValueError("admin_id is required for audit logging (admin only)")

    conn_used = None
    own_cursor = False
    # prefer using provided cursor/conn; else create a local connection like before
    if cur is None:
        conn_used = duckdb.connect('backend/db_timestock')
        cur = conn_used.cursor()
        own_cursor = True
    else:
        if hasattr(cur, "cursor") and not hasattr(cur, "execute"):
            conn_used = cur
            cur = conn_used.cursor()
            own_cursor = True

    try:
        # Get the corresponding item_id from the product
        item_result = cur.execute("SELECT item_id FROM products WHERE id = ?", (product_id,)).fetchone()
        if not item_result:
            if own_cursor and conn_used is not None:
                conn_used.close() if hasattr(conn_used, "close") else None
            return {"success": False, "message": "Product not found."}
        
        item_id = item_result[0]

        # get snapshots for audit
        prod_row = cur.execute("SELECT unit_price, materials_cost, status FROM products WHERE id = ?", (product_id,)).fetchone()
        item_row = cur.execute("SELECT item_name, item_decription FROM items WHERE id = ?", (item_id,)).fetchone()

        # Delete from referencing tables first to avoid FK constraint issues
        cur.execute("DELETE FROM product_materials WHERE product_id = ?", (product_id,))
        cur.execute("DELETE FROM order_items WHERE product_id = ?", (product_id,))
        
        # Then delete from main product and item tables
        cur.execute("DELETE FROM products WHERE id = ?", (product_id,))
        cur.execute("DELETE FROM items WHERE id = ?", (item_id,))

        details = (
            f"Deleted product {product_id} (item_id={item_id}): "
            f"name={item_row[0]}, unit_price={prod_row[0]}, materials_cost={prod_row[1]}, status={prod_row[2]}"
        )
        log_audit(
            entity="products",
            entity_id=str(product_id),
            action="delete",
            details=details,
            admin_id=admin_id,
            cur=cur
        )

        if own_cursor and conn_used is not None:
            # commit and close local conn
            conn_used.commit()
            if hasattr(conn_used, "close"):
                conn_used.close()
        return {"success": True, "message": "Product, item, and all references deleted."}
    except Exception as e:
        if own_cursor and conn_used is not None:
            conn_used.rollback()
            if hasattr(conn_used, "close"):
                conn_used.close()
        raise



#Suppliers CRUD
def get_suppliers():
        with duckdb.connect('backend/db_timestock') as conn:
            return conn.execute("SELECT * FROM suppliers").fetchdf()

# THIS IS DONE
def add_supplier(
    data: dict,
    admin_id: Optional[str] = None,
    cur = None
):
    if admin_id is None:
        raise ValueError("admin_id is required for audit logging (admin only)")
    
    conn_used = None
    own_cursor = False

    if cur is None:
        conn_used = duckdb.connect('backend/db_timestock')
        cur = conn_used.cursor()
        own_cursor = True
    else:
        if hasattr(cur, "cursor") and not hasattr(cur, "execute"):
            conn_used = cur
            cur = conn_used.cursor()
            own_cursor = True
    
    try:
        # Normalize input
        firstname = data['firstname'].strip().title()
        lastname = data['lastname'].strip().title()
        contact_name = data['contact_name'].strip().title()
        contact_number = data['contact_number'].strip()
        email = data['email'].strip().lower()
        address = data['address'].strip()

        # Check for existing supplier
        existing = cur.execute("""
            SELECT 1 FROM suppliers 
            WHERE firstname = ? AND lastname = ? 
            AND (contact_number = ? OR email = ?)
        """, (firstname, lastname, contact_number, email)).fetchone()

        if existing:
            return {"success": False, "message": "Supplier already exists."}

        # Insert new supplier
        new_id = cur.execute("""
            INSERT INTO suppliers (
                firstname, lastname, contact_name, contact_number,
                email, address, date_created
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            RETURNING id
        """, (firstname, lastname, contact_name, contact_number, email, address, datetime.utcnow()))

        details: f"Added {contact_name} into suppliers with the following details: full name = '{lastname}, {firstname}', contact number = '{contact_number}', email = '{email}', address = '{address}'"
        log_audit(
            entity="suppliers",
            entity_id=str(new_id),
            action="create",
            details=details,
            admin_id=admin_id,
            cur=cur
        )

        if own_cursor and conn_used is not None:
            conn_used.commit()
        return {"success": True, "message": "Supplier added successfully."}
    except Exception:
        if own_cursor and conn_used is not None:
            conn_used.rollback()
        raise

# THIS IS DONE
def update_supplier(
    id: str, 
    data: dict,
    admin_id: Optional[str] = None,
    cur = None
):
    if admin_id is None:
        raise ValueError("admin_id is required for audit logging (admin only)")
    
    conn_used = None
    own_cursor = False

    if cur is None:
        conn_used = duckdb.connect('backend/db_timestock')
        cur = conn_used.cursor()
        own_cursor = True
    else:
        if hasattr(cur, "cursor") and not hasattr(cur, "execute"):
            conn_used = cur
            cur = conn_used.cursor()
            own_cursor = True

    try:
        firstname = data['firstname'].strip().title()
        lastname = data['lastname'].strip().title()
        contact_name = data['contact_name'].strip().title()
        contact_number = data['contact_number'].strip()
        email = data['email'].strip().lower()
        address = data['address'].strip()

        old_row = cur.execute(
            """
            SELECT
                firstname,
                lastname,
                contact_name,
                contact_number,
                email,
                address
            FROM suppliers
            WHERE id = ?
            """, (id,)
        ).fetchone()

        if not old_row:
            raise ValueError("Supplier information not found.")

        cur.execute("""
            UPDATE suppliers SET
                firstname = ?,
                lastname = ?,
                contact_name = ?,
                contact_number = ?,
                email = ?,
                address = ?
            WHERE id = ?
        """, (firstname, lastname, contact_name, contact_number, email, address, id))

        details = (
            f"Updated {id} with the following details: first name = '{old_row[0]}' -> '{firstname}', "
            f"last name = '{old_row[1]}' -> '{lastname}', " 
            f"contact name = '{old_row[2]}' -> '{contact_name}', " 
            f"contact number = '{old_row[3]}' -> '{contact_number}', "
            f"email = '{old_row[4]}' -> '{email}', "
            f"address = '{old_row[5]}' -> '{address}'"
        )
        log_audit(
            entity="suppliers",
            entity_id=str(id),
            action="update",
            details=details,
            admin_id=admin_id,
            cur=cur
        )

        if own_cursor and conn_used is not None:
            conn_used.commit()
        return {"success": True, "updated": 1}
    except Exception:
        if own_cursor and conn_used is not None:
            conn_used.rollback()
        raise

# THIS IS DONE
def delete_supplier(
    id: str,
    admin_id: Optional[str] =  None,
    cur = None
):
    if admin_id is None:
        raise ValueError("admin_id is required for audit logging (admin only)")
    
    conn_used = None
    own_cursor = False

    if cur is None:
        conn_used = duckdb.connect('backend/db_timestock')
        cur = conn_used.cursor()
        own_cursor = True
    else:
        if hasattr(cur, "cursor") and not hasattr(cur, "execute"):
            conn_used = cur
            cur = conn_used.cursor()
            own_cursor = True
    
    try:
        old_row = cur.execute(
            """
            SELECT
                firstname,
                lastname,
                contact_name,
                contact_number,
                email,
                address
            FROM suppliers
            WHERE id = ?
            """, (id,)
        ).fetchone()

        if not old_row:
            raise ValueError("Supplier information not found.")

        firstname, lastname, contact_name, contact_number, email, address = old_row
        cur.execute("DELETE FROM suppliers WHERE id = ?", (id,))

        details = (
            f"Deleted {id} containing the following details: first name: {firstname}, "
            f"last name: {lastname}, "
            f"contact name: {contact_name}, "
            f"contact number: {contact_number}, "
            f"email: {email}, "
            f"address: {address}"
        )
        log_audit(
            entity="suppliers",
            entity_id=str(id),
            action="delete",
            details=details,
            admin_id=admin_id,
            cur=cur
        )

        if own_cursor and conn_used is not None:
            conn_used.commit()
        return {"success": True, "deleted": 1}
    except Exception:
        if own_cursor and conn_used is not None:
            conn_used.rollback()
        raise


def create_order_transaction(data: dict, admin_id: Optional[str] = None, cur = None):
    items = data.pop('items')
    total_amount = 0

    # Step 0: Create new customer if needed
    customer_id = data.get('customer_id')
    customer_data = data.get('customer')

    # prepare cursor / connection handling so we can pass cur into log_audit
    conn_used = None
    own_cursor = False
    started_txn = False            # <--- track whether we started a txn
    if cur is None:
        try:
            conn_used = con
        except NameError:
            raise RuntimeError("Database connection con is not defined in this module.")
        cur = conn_used.cursor()
        own_cursor = True
    else:
        # if caller passed a connection object instead of a cursor
        if hasattr(cur, "cursor") and not hasattr(cur, "execute"):
            conn_used = cur
            cur = conn_used.cursor()
            own_cursor = True

    if not customer_id and customer_data:
        customer_id = cur.execute("""
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

    # Determine audit actor (prefer explicit admin_id param, else data['admin_id'])
    actor_admin = admin_id or data.get('admin_id')
    actor_employee = None if actor_admin else data.get('employee_id')

    try:
        # start transaction using the connection (only if we own cursor/conn)
        if own_cursor and conn_used is not None:
            # use the connection execute to start transaction (consistent with other code)
            conn_used.execute("BEGIN")
            started_txn = True

        # Step 1: Pre-check ALL material stock
        material_requirements = {}

        for item in items:
            product_id = item['product_id']
            quantity = item['quantity']

            materials = cur.execute("""
                SELECT pm.material_id, pm.used_quantity, m.current_stock, i.item_name, m.unit_measurement
                    FROM product_materials pm
                    JOIN materials m ON pm.material_id = m.id
                    JOIN items i ON m.item_id = i.id
                    WHERE pm.product_id = ?
            """, (product_id,)).fetchall()

            for material_id, used_qty, current_stock, item_name, unit in materials:
                total_needed = used_qty * quantity

                if material_id not in material_requirements:
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

            # raise HTTPException to let FastAPI return JSON error body
            raise HTTPException(status_code=400, detail=formatted_message)

        # Step 2: Insert transaction (use whatever admin_id is present in data)
        transaction_id = cur.execute("""
            INSERT INTO order_transactions (
                customer_id, status_id, admin_id, date_created, total_amount
            ) VALUES (?, ?, ?, ?, ?)
            RETURNING id
        """, (
            customer_id,
            data['status_id'],
            data.get('admin_id'),   # keep original behavior
            datetime.utcnow(),
            0.0
        )).fetchone()[0]

        # Step 3: Process order items
        for item in items:
            product_id = item['product_id']
            quantity = item['quantity']

            unit_price_row = cur.execute("""
                SELECT unit_price FROM products WHERE id = ?
            """, (product_id,)).fetchone()

            if not unit_price_row:
                raise HTTPException(status_code=400, detail=f"Product ID {product_id} not found.")

            unit_price = unit_price_row[0]
            line_total = quantity * unit_price
            total_amount += line_total

            # Deduct material stock and log transaction
            materials = cur.execute("""
                SELECT pm.material_id, pm.used_quantity
                FROM product_materials pm
                WHERE pm.product_id = ?
            """, (product_id,)).fetchall()

            for material_id, used_qty in materials:
                total_used = used_qty * quantity

                # Fetch supplier
                supplier = cur.execute("""
                    SELECT supplier_id FROM materials WHERE id = ?
                """, (material_id,)).fetchone()

                if not supplier or not supplier[0]:
                    raise HTTPException(status_code=400, detail=f"No supplier found for material ID {material_id}.")

                supplier_id = supplier[0]

                # Create stock transaction
                stock_transaction_id = cur.execute("""
                    INSERT INTO stock_transactions (
                        stock_type_id, supplier_id, admin_id, employee_id, date_created
                    ) VALUES (?, ?, ?, NULL, ?)
                    RETURNING id
                """, (
                    'STT002',
                    supplier_id,
                    data.get('admin_id'),
                    datetime.utcnow()
                )).fetchone()[0]

                # Log stock item
                cur.execute("""
                    INSERT INTO stock_transaction_items (
                        stock_transaction_id, material_id, quantity
                    ) VALUES (?, ?, ?)
                """, (
                    stock_transaction_id,
                    material_id,
                    total_used
                ))

                # Deduct from material stock
                cur.execute("""
                    UPDATE materials
                    SET current_stock = current_stock - ?
                    WHERE id = ?
                """, (total_used, material_id))

            # Add order item
            cur.execute("""
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
        cur.execute("""
            UPDATE order_transactions
            SET total_amount = ?
            WHERE id = ?
        """, (total_amount, transaction_id))

        # Write an auditlog entry (same cursor) — exactly one of admin_id/employee_id must be passed
        # choose actor_admin if available, otherwise actor_employee
        audit_admin = actor_admin
        audit_employee = actor_employee if not actor_admin else None

        details = f"Order created: transaction_id={transaction_id}, customer_id={customer_id}, items={len(items)}, total_amount={total_amount:.2f}"
        log_audit(
            entity="order_transactions",
            entity_id=str(transaction_id),
            action="create",
            details=details,
            admin_id=audit_admin,
            employee_id=audit_employee,
            cur=cur
        )

        # commit if we opened and started the txn here
        if own_cursor and conn_used is not None and started_txn:
            conn_used.execute("COMMIT")

        return {"transaction_id": transaction_id, "message": "Order successfully placed."}
    except HTTPException:
        # Re-raise HTTPExceptions as-is (they already contain status + detail)
        if own_cursor and conn_used is not None and started_txn:
            try:
                conn_used.execute("ROLLBACK")
            except Exception:
                # rollback failure - log if you have logger; swallow to re-raise original HTTPException
                pass
        raise
    except Exception as e:
        # rollback only if we started a txn
        if own_cursor and conn_used is not None and started_txn:
            try:
                conn_used.execute("ROLLBACK")
            except Exception:
                # avoid raising transaction rollback errors that mask original error
                pass
        # raise HTTPException so FastAPI responds with JSON instead of HTML
        raise HTTPException(status_code=500, detail=str(e))


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


# def add_employee(data: dict):
#     firstname = data['firstname'].strip().title()
#     lastname = data['lastname'].strip().title()
#     email = data['email'].strip().lower()
#     password = data['password'].strip()

#     ph = PasswordHasher()
#     pw_hash = ph.hash(password)

#     contact_number = data['contact_number'].strip()

#     if con.execute(
#         "SELECT 1 FROM employees WHERE email = ?",
#         (email,)
#     ).fetchone():
#         return {"success": False, "message": "Email is already registered."}

#     if con.execute(
#         "SELECT 1 FROM employees WHERE contact_number = ?",
#         (contact_number,)
#     ).fetchone():
#         return {"success": False, "message": "Contact number is already in use."}

#     cur = con.cursor()
#     cur.execute("""
#         INSERT into employees(firstname, lastname, email, password, contact_number)
#         VALUES (?, ?, ?, ?, ?)
#     """, (firstname, lastname, email, pw_hash, contact_number))

#     con.commit()
#     return {"success": True, "Message": "Employee added successfully!"}

# THIS IS DONE
def add_employee(data: dict, admin_id: Optional[str] = None, cur=None):
    """
    Add an employee. Requires admin_id for audit logging.
    Audit details mask email and contact number (no passwords logged).
    """
    if admin_id is None:
        raise ValueError("admin_id is required for audit logging (admin only)")

    conn_used = None
    own_cursor = False
    if cur is None:
        try:
            conn_used = con
        except NameError:
            raise RuntimeError("Database connection `con` is not defined in this module.")
        cur = conn_used.cursor()
        own_cursor = True
    else:
        if hasattr(cur, "cursor") and not hasattr(cur, "execute"):
            conn_used = cur
            cur = conn_used.cursor()
            own_cursor = True

    def _mask_email(e: str) -> str:
        try:
            local, domain = e.split("@", 1)
            if len(local) <= 1:
                return f"*@{domain}"
            return f"{local[0]}***@{domain}"
        except Exception:
            return "****"

    def _mask_phone(p: str) -> str:
        p = p.strip()
        if len(p) <= 4:
            return "****"
        return f"****{p[-4:]}"

    try:
        firstname = data['firstname'].strip().title()
        lastname = data['lastname'].strip().title()
        email = data['email'].strip().lower()
        password = data['password'].strip()

        ph = PasswordHasher()
        pw_hash = ph.hash(password)

        contact_number = data['contact_number'].strip()

        # uniqueness checks (use cur so inside same cursor)
        if cur.execute("SELECT 1 FROM employees WHERE email = ?", (email,)).fetchone():
            return {"success": False, "message": "Email is already registered."}

        if cur.execute("SELECT 1 FROM employees WHERE contact_number = ?", (contact_number,)).fetchone():
            return {"success": False, "message": "Contact number is already in use."}

        cur.execute("""
            INSERT into employees(firstname, lastname, email, password, contact_number, date_created)
            VALUES (?, ?, ?, ?, ?, ?)
            RETURNING id
        """, (firstname, lastname, email, pw_hash, contact_number, datetime.utcnow()))
        new_id = cur.fetchone()[0]

        # safe audit details (mask PII)
        details = (
            f"Created employee id={new_id}, name='{firstname} {lastname}', "
            f"email={_mask_email(email)}, contact={_mask_phone(contact_number)}"
        )
        log_audit(
            entity="employees",
            entity_id=str(new_id),
            action="create",
            details=details,
            admin_id=admin_id,
            cur=cur
        )

        if own_cursor and conn_used is not None:
            conn_used.commit()
        return {"success": True, "Message": "Employee added successfully!", "employee_id": new_id}
    except Exception:
        if own_cursor and conn_used is not None:
            conn_used.rollback()
        raise


# def update_account_status(id: str, is_active: bool):
#     status = con.execute(
#         """
#         UPDATE employees
#         SET is_active = ?,
#             date_updated = NOW()
#         WHERE id = ?
#         """, (is_active, id)
#     )
#     if status.rowcount == 0:
#         raise HTTPException(status_code=404, detail=f"Employee {id} not found.")
#     con.commit()
#     return {"success": True, "id": id, "is_active": is_active}

# THIS IS DONE
def update_account_status(id: str, is_active: bool, admin_id: Optional[str] = None, cur=None):
    """
    Toggle employee active status. Requires admin_id for audit logging.
    Logs previous and new state (no sensitive data).
    """
    if admin_id is None:
        raise ValueError("admin_id is required for audit logging (admin only)")

    conn_used = None
    own_cursor = False
    if cur is None:
        try:
            conn_used = con
        except NameError:
            raise RuntimeError("Database connection `con` is not defined in this module.")
        cur = conn_used.cursor()
        own_cursor = True
    else:
        if hasattr(cur, "cursor") and not hasattr(cur, "execute"):
            conn_used = cur
            cur = conn_used.cursor()
            own_cursor = True

    try:
        # read existing state
        old_row = cur.execute("SELECT is_active FROM employees WHERE id = ?", (id,)).fetchone()
        if not old_row:
            raise HTTPException(status_code=404, detail=f"Employee {id} not found.")

        old_status = bool(old_row[0])

        res = cur.execute(
            """
            UPDATE employees
            SET is_active = ?,
                date_updated = CURRENT_TIMESTAMP
            WHERE id = ?
            """, (is_active, id)
        )

        # check affected rows: duckdb driver may provide rowcount on the result
        affected = getattr(res, "rowcount", None)
        if affected is None:
            # best effort: if no rowcount, assume success since we fetched it earlier
            affected = 1

        if affected == 0:
            raise HTTPException(status_code=404, detail=f"Employee {id} not found.")

        details = f"Admin {admin_id} changed is_active for employee {id}: {old_status} -> {bool(is_active)}"
        log_audit(
            entity="employees",
            entity_id=str(id),
            action="update_status",
            details=details,
            admin_id=admin_id,
            cur=cur
        )

        if own_cursor and conn_used is not None:
            conn_used.commit()
        return {"success": True, "id": id, "is_active": is_active}
    except Exception:
        if own_cursor and conn_used is not None:
            conn_used.rollback()
        raise

# def change_employee_password(
#         admin_id: str,
#         target_employee_id: str,
#         new_password: str
#     ) -> dict:

#     ph = PasswordHasher()

#     row = con.execute(
#         "SELECT password FROM admin WHERE id = ?", (admin_id,)
#     ).fetchone()

#     if not row:
#         raise HTTPException(status_code=404, detail="Admin account not found.")
    
#     new_password = new_password.strip()
#     if len(new_password) < 8:
#         return {"success": False, "message": "New password must be at least 8 characters."}
    
#     try:
#         new_hash = ph.hash(new_password)
#     except Exception:
#         raise HTTPException(status_code=500, detail="Error hashing new password.")
    
#     res = con.execute(
#         """
#         UPDATE employees
#         SET password = ?,
#             date_updated = NOW()
#         WHERE id = ?
#         """,
#         (new_hash, target_employee_id)
#     )

#     if res.rowcount == 0:
#         return {"success": False, "message": "Target user not found."}
    
#     con.commit()
#     return {"success": True, "message": "Employee password changed successfully."}

# THIS IS DONE
def change_employee_password(
        admin_id: str,
        target_employee_id: str,
        new_password: str,
        cur=None
    ) -> dict:
    """
    Admin changes an employee password.
    Does NOT log password content. Logs that admin performed a password change.
    """
    if admin_id is None:
        raise ValueError("admin_id is required for audit logging (admin only)")

    conn_used = None
    own_cursor = False
    if cur is None:
        try:
            conn_used = con
        except NameError:
            raise RuntimeError("Database connection `con` is not defined in this module.")
        cur = conn_used.cursor()
        own_cursor = True
    else:
        if hasattr(cur, "cursor") and not hasattr(cur, "execute"):
            conn_used = cur
            cur = conn_used.cursor()
            own_cursor = True

    ph = PasswordHasher()

    # verify admin exists
    admin_row = cur.execute("SELECT id FROM admin WHERE id = ?", (admin_id,)).fetchone()
    if not admin_row:
        raise HTTPException(status_code=404, detail="Admin account not found.")

    new_password = new_password.strip()
    if len(new_password) < 8:
        return {"success": False, "message": "New password must be at least 8 characters."}

    try:
        new_hash = ph.hash(new_password)
    except Exception:
        raise HTTPException(status_code=500, detail="Error hashing new password.")

    try:
        res = cur.execute(
            """
            UPDATE employees
            SET password = ?,
                date_updated = CURRENT_TIMESTAMP
            WHERE id = ?
            """,
            (new_hash, target_employee_id)
        )

        affected = getattr(res, "rowcount", None)
        if affected is None:
            # best-effort: check existence
            row = cur.execute("SELECT 1 FROM employees WHERE id = ?", (target_employee_id,)).fetchone()
            if not row:
                return {"success": False, "message": "Target user not found."}
            affected = 1

        if affected == 0:
            return {"success": False, "message": "Target user not found."}

        # Audit: do NOT include password or hash
        details = f"Admin {admin_id} changed password for employee {target_employee_id} (no password stored in audit)."
        log_audit(
            entity="employees",
            entity_id=str(target_employee_id),
            action="password_change",
            details=details,
            admin_id=admin_id,
            cur=cur
        )

        if own_cursor and conn_used is not None:
            conn_used.commit()
        return {"success": True, "message": "Employee password changed successfully."}
    except Exception:
        if own_cursor and conn_used is not None:
            conn_used.rollback()
        raise


def get_current_admin(request: Request):
    user = request.session.get("user")
    if not user or user.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Admin only")
    return user

# def delete_old_transactions(years: int, *, admin_id: str, dry_run: bool = False):
#     if admin_id is None:
#         raise ValueError("Error: Admin ID is required (admin only)")
    
#     if years < 2:
#         raise ValueError("Error: Cutoff year should be at least 5 years ago or older")
    
#     admin_exists = con.execute("SELECT 1 FROM admin WHERE id = ?", (admin_id,)).fetchone()
#     if not admin_exists:
#         raise ValueError("Error: Admin ID not found")

#     cutoff_date = datetime.now() - timedelta(days= years*365)
#     cutoff_param = cutoff_date.isoformat()
    
#     deleted = {
#                "old_order_items": 0,
#                "old_orders": 0,
#                "old_stock_items": 0,
#                "old_stocks": 0
#     }

#     try:
#         cur = con.cursor()

#         if dry_run:
#             deleted["old_order_items"] = cur.execute(
#                 """
#                 SELECT COUNT(*) FROM order_items
#                 WHERE order_id
#                 IN  (
#                     SELECT id 
#                     FROM order_transactions
#                     WHERE date_created < ?
#                     )
#                 """, (cutoff_param,)
#             ).fetchone()[0]
#             deleted["old_orders"] = cur.execute(
#               "SELECT COUNT(*) FROM order_transactions WHERE date_created < ?", (cutoff_param,)
#             ).fetchone()[0]

#             deleted["old_stock_items"] = cur.execute(
#                 """
#                 SELECT COUNT(*) FROM stock_transaction_items
#                 WHERE stock_transaction_id
#                 IN  (
#                     SELECT id
#                     FROM stock_transactions
#                     WHERE date_created < ?
#                     )
#                 """, (cutoff_param,)
#             ).fetchone()[0]
#             deleted["old_stocks"] = cur.execute(
#                 "SELECT COUNT(*) FROM stock_transactions WHERE date_created < ?", (cutoff_param,)
#             ).fetchone()[0]
        
#         else:
#             deleted["old_order_items"] = cur.execute(
#                 """
#                 DELETE FROM order_items
#                 WHERE order_id
#                 IN  (
#                     SELECT id 
#                    FROM order_transactions
#                    WHERE date_created < ?
#                    )
#                 """, (cutoff_param,)
#             ).rowcount
#             deleted["old_orders"] = cur.execute(
#               "DELETE FROM order_transactions WHERE date_created < ?", (cutoff_param,)
#             ).rowcount

#             deleted["old_stock_items"] = cur.execute(
#                 """
#                 DELETE FROM stock_transaction_items
#                 WHERE stock_transaction_id
#                 IN  (
#                     SELECT id
#                     FROM stock_transactions
#                     WHERE date_created < ?
#                     )
#                 """, (cutoff_param,)
#             ).rowcount
#             deleted["old_stocks"] = cur.execute(
#                 "DELETE FROM stock_transactions WHERE date_created < ?", (cutoff_param,)
#             ).rowcount

#             con.commit()

#     except Exception:
#         con.rollback()
#         raise

#     return {"success": True, "cutoff_date": cutoff_date.isoformat(), **deleted}

def delete_old_transactions(years: int, *, admin_id: str, dry_run: bool = False):
    if admin_id is None:
        raise ValueError("Error: Admin ID is required (admin only)")
    
    if years < 2:
        raise ValueError("Error: Cutoff year should be at least 5 years ago or older")
    
    admin_exists = con.execute("SELECT 1 FROM admin WHERE id = ?", (admin_id,)).fetchone()
    if not admin_exists:
        raise ValueError("Error: Admin ID not found")

    cutoff_date = datetime.now() - timedelta(days= years*365)
    cutoff_param = cutoff_date.isoformat()
    
    deleted = {
               "old_order_items": 0,
               "old_orders": 0,
               "old_stock_items": 0,
               "old_stocks": 0
    }

    try:
        cur = con.cursor()

        if dry_run:
            deleted["old_order_items"] = cur.execute(
                """
                SELECT COUNT(*) FROM order_items
                WHERE order_id
                IN  (
                    SELECT id 
                    FROM order_transactions
                    WHERE date_created < ?
                    )
                """, (cutoff_param,)
            ).fetchone()[0]
            deleted["old_orders"] = cur.execute(
              "SELECT COUNT(*) FROM order_transactions WHERE date_created < ?", (cutoff_param,)
            ).fetchone()[0]

            deleted["old_stock_items"] = cur.execute(
                """
                SELECT COUNT(*) FROM stock_transaction_items
                WHERE stock_transaction_id
                IN  (
                    SELECT id
                    FROM stock_transactions
                    WHERE date_created < ?
                    )
                """, (cutoff_param,)
            ).fetchone()[0]
            deleted["old_stocks"] = cur.execute(
                "SELECT COUNT(*) FROM stock_transactions WHERE date_created < ?", (cutoff_param,)
            ).fetchone()[0]
        
        else:
            deleted["old_order_items"] = cur.execute(
                """
                DELETE FROM order_items
                WHERE order_id
                IN  (
                    SELECT id 
                   FROM order_transactions
                   WHERE date_created < ?
                   )
                """, (cutoff_param,)
            ).rowcount
            deleted["old_orders"] = cur.execute(
              "DELETE FROM order_transactions WHERE date_created < ?", (cutoff_param,)
            ).rowcount

            deleted["old_stock_items"] = cur.execute(
                """
                DELETE FROM stock_transaction_items
                WHERE stock_transaction_id
                IN  (
                    SELECT id
                    FROM stock_transactions
                    WHERE date_created < ?
                    )
                """, (cutoff_param,)
            ).rowcount
            deleted["old_stocks"] = cur.execute(
                "DELETE FROM stock_transactions WHERE date_created < ?", (cutoff_param,)
            ).rowcount

            # write audit only when something was deleted
            total_deleted = (
                (deleted.get("old_order_items") or 0)
                + (deleted.get("old_orders") or 0)
                + (deleted.get("old_stock_items") or 0)
                + (deleted.get("old_stocks") or 0)
            )
            if total_deleted > 0:
                details = (
                    f"Admin {admin_id} purged records older than {cutoff_date.isoformat()}: "
                    f"orders={deleted['old_orders']}, order_items={deleted['old_order_items']}, "
                    f"stocks={deleted['old_stocks']}, stock_items={deleted['old_stock_items']}"
                )
                # log atomically with the same cursor
                log_audit(
                    entity="maintenance",
                    entity_id=cutoff_date.isoformat(),
                    action="delete_old_transactions",
                    details=details,
                    admin_id=admin_id,
                    cur=cur
                )

            con.commit()

    except Exception:
        con.rollback()
        raise

    return {"success": True, "cutoff_date": cutoff_date.isoformat(), **deleted}


# database.py (add/replace this function)
import duckdb
from typing import List, Dict, Any, Optional

def get_audit_logs(limit: int = 100, offset: int = 0, cur=None) -> List[Dict[str, Any]]:
    """
    Return recent audit log rows as list of dicts.
    Minimal, defensive: opens its own connection if none provided.
    """
    conn_used = None
    own_cursor = False

    if cur is None:
        # use a short-lived connection so callers don't need to pass one
        conn_used = duckdb.connect('backend/db_timestock')
        cur = conn_used.cursor()
        own_cursor = True
    else:
        # caller passed either a connection or a cursor
        if hasattr(cur, "cursor") and not hasattr(cur, "execute"):
            conn_used = cur
            cur = conn_used.cursor()
            own_cursor = True

    try:
        # column names chosen to match your log_audit schema
        rows = cur.execute(
            """
            SELECT id, entity, entity_id, action, details, admin_id, employee_id, action_time
            FROM auditlogs
            ORDER BY action_time DESC
            LIMIT ? OFFSET ?
            """, (limit, offset)
        ).fetchall()

        # duckdb cursor.description gives tuples like (name, ...)
        cols = [d[0] for d in (cur.description or [])]
        result = [dict(zip(cols, row)) for row in rows]
        return result
    finally:
        # close local connection if we opened it
        if own_cursor and conn_used is not None:
            try:
                conn_used.close()
            except Exception:
                pass



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

