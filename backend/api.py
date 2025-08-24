from typing import List, Any, Optional
from fastapi import APIRouter, Depends, HTTPException, Header,Request
from fastapi.responses import JSONResponse, FileResponse
from tempfile import NamedTemporaryFile
from datetime import datetime, timedelta
import pandas as pd
import uuid
import os, json

from backend.auth import get_current_user, verify_token
from .app_schemas import (
    ProductCategoryCreate, ProductCategoryUpdate, EmployeeStatusUpdate,
    MaterialCategoryCreate, MaterialCategoryUpdate, ChangeEmployeePassword,
    MaterialCreate, MaterialUpdate, OrderStatusUpdate, EmployeeCreate,
    CustomerCreate, CustomerUpdate, ReceiptRequest, QuotationRequest,
    ProductCreate, ProductUpdate,StockTransactionCreate,ProductMaterialBulkCreate,
    SupplierCreate, SupplierUpdate, ProductMaterialCreate, OrderTransactionCreate
)

from backend import database, reciept, graphs, analytics
router = APIRouter()


    
@router.put("/products/update") # DONE
def update_product_data(request: Request, product: ProductUpdate):
    user = request.session.get("user")
    if not user or user.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Admin privileges required")

    try:
        database.update_product(
            con=database.con,
            product_id=product.id,
            unit_price=product.unit_price,
            materials_cost=product.materials_cost,
            status=product.status,
            category_id=product.category_id,
            item_name=product.item_name,
            item_description=product.item_description,
            admin_id=user["id"]
        )
        return {"message": "Product updated successfully"}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.put("/material/update") # DONE
def update_material_api(request: Request, material: MaterialUpdate) -> Any:
    user = request.session.get("user")
    if not user or user.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Admin privileges required")

    try:
        database.update_materials(
            con=database.con,
            material_id=material.material_id,
            item_name=material.item_name,
            item_description=material.item_description,
            category_id=material.category_id,
            unit_measurement=material.unit_measurement,
            material_cost=material.material_cost,
            current_stock=material.current_stock,
            minimum_stock=material.minimum_stock,
            maximum_stock=material.maximum_stock,
            supplier_id=material.supplier_id,
            admin_id=user["id"]
        )
        return {"message": "Material updated successfully"}
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        raise HTTPException(status_code=500, detail="Internal server error: " + str(e))



# ---- Alerts ---
CACHE_FILE = "alert_cache.json"

# Load cache from file
if os.path.exists(CACHE_FILE):
    with open(CACHE_FILE, "r") as f:
        alert_cache = json.load(f)
        # Only convert to pd.Timestamp for Reorder/Minimum Stock, NOT Turnover
        for cat in ["Reorder", "Minimum Stock"]:
            for key, ts in alert_cache.get(cat, {}).items():
                try:
                    alert_cache[cat][key] = pd.Timestamp(ts)
                except:
                    alert_cache[cat][key] = pd.Timestamp.now()
else:
    alert_cache = {"Turnover": {}, "Reorder": {}, "Minimum Stock": {}}

@router.get("/all-alerts")
def get_all_alerts():
    now = pd.Timestamp.now()
    alerts = {"Turnover": [], "Reorder": [], "Minimum Stock": []}

    # --- Turnover Alerts ---
    _, turnover_df, _ = graphs.get_turnover_combined_graph()
    turnover_msgs = set()
    if "label" in turnover_df.columns:
        turnover_df['label_date'] = pd.to_datetime(turnover_df['label'], format='%Y-%m')
        recent_df = turnover_df[turnover_df['label_date'] >= (now - pd.DateOffset(months=1))]

        for _, row in recent_df.iterrows():
            label = row['label']
            rate = row['turnover_rate']

            if rate < 1:
                msg = f"⚠️ {label}: Low turnover rate ({rate}) – Review excess stock."
            elif rate > 5:
                msg = f"⚠️ {label}: High turnover rate ({rate}) – Stock might run out fast."
            else:
                msg = f"✅ {label}: Turnover rate is normal ({rate})."

            formatted_timestamp = row['label_date'].strftime("%b %Y")
            timestamp = alert_cache["Turnover"].get(msg, formatted_timestamp)
            alert_cache["Turnover"][msg] = timestamp

            alerts["Turnover"].append({
                "message": msg,
                "timestamp": timestamp,
            })
            turnover_msgs.add(msg)

    # Invalidate old Turnover cache entries not present now
    alert_cache["Turnover"] = {k: v for k, v in alert_cache["Turnover"].items() if k in turnover_msgs}

    # --- Reorder Alerts ---
    reorder_msgs = set()
    reorder_df = graphs.get_reorder_point_chart(return_df=True)
    if reorder_df is not None:
        for _, row in reorder_df.iterrows():
            if row['reorder_status'] == '⚠️ Reorder Needed':
                msg = f"⚠️ {row['item_name']}: Stock is low ({row['current_stock']}) – Reorder point is {row['reorder_point']}"
                timestamp = alert_cache["Reorder"].get(msg, now)
                alert_cache["Reorder"][msg] = timestamp

                alerts["Reorder"].append({
                    "message": msg,
                    "timestamp": timestamp.strftime("%Y-%m-%d %H:%M:%S"),
                    "display_time": timestamp.strftime("%b %d %Y %H:%M")
                })
                reorder_msgs.add(msg)

    alert_cache["Reorder"] = {k: v for k, v in alert_cache["Reorder"].items() if k in reorder_msgs}

    # --- Minimum Stock Alerts ---
    minstock_msgs = set()
    min_stock_alerts = analytics.get_minimum_stock_alerts()
    for item in min_stock_alerts:
        msg = str(item[1]) if isinstance(item, tuple) and len(item) == 2 else str(item)
        timestamp = alert_cache["Minimum Stock"].get(msg, now)
        alert_cache["Minimum Stock"][msg] = timestamp

        alerts["Minimum Stock"].append({
            "message": msg,
            "timestamp": timestamp.strftime("%Y-%m-%d %H:%M:%S"),
            "display_time": timestamp.strftime("%b %d %Y %H:%M")
        })
        minstock_msgs.add(msg)

    alert_cache["Minimum Stock"] = {k: v for k, v in alert_cache["Minimum Stock"].items() if k in minstock_msgs}

    # --- Persist cache ---
    with open(CACHE_FILE, "w") as f:
        json.dump({cat: {k: str(v) for k,v in alert_cache[cat].items()} for cat in alert_cache}, f)

    return {"alerts": alerts}

@router.get("/low-stock-alerts")
def low_stock_alerts():
    alerts = analytics.get_low_stock_alerts()
    return {"alerts": alerts}

# ---- Dashboard Summary ---
@router.get("/dashboard/summary")
def get_inventory_dashboard_summary():
    return analytics.get_inventory_summary()

@router.get("/sales-summary")
def sales_summary():
    return analytics.get_sales_summary()
#---- Product Materials ---

@router.get("/product-materials")
def read_product_materials():
    return database.get_product_materials_grouped()

@router.post("/product-materials/add") # DONE
def create_product_materials(data: ProductMaterialBulkCreate, request: Request):
    user = request.session.get("user")
    if not user or user.get("role") != "admin":
        raise HTTPException(status_code=401, detail="Unauthorized")
    try:
        # pass admin_id so DB can audit the action
        result = database.add_product_materials(data.dict(), admin_id=user["id"])
        # return success plus the insert/skip counts from DB function
        return {"message": "Product materials added successfully.", **(result or {})}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception:
        # keep generic 500 to avoid leaking internals
        raise HTTPException(status_code=500, detail="Internal server error.")
    
@router.get("/product-materials/{product_id}")
def api_get_product_materials(product_id: str):
    return database.get_product_materials_by_product_id(product_id)


@router.put("/product-materials/update") # DONE
def api_update_product_material(payload: dict, request: Request):
    user = request.session.get("user")
    if not user or user.get("role") != "admin":
        raise HTTPException(status_code=401, detail="Unauthorized")

    admin_id = user["id"]

    # Basic validation
    if "product_id" not in payload:
        raise HTTPException(status_code=400, detail="product_id is required")

    try:
        # single update
        missing = [k for k in ("material_id", "used_quantity", "unit_cost") if k not in payload]
        if missing:
            raise HTTPException(status_code=400, detail=f"Missing fields: {', '.join(missing)}")
        result = database.update_product_material(
            product_id=payload["product_id"],
            material_id=payload["material_id"],
            used_quantity=float(payload["used_quantity"]),
            unit_cost=float(payload["unit_cost"]),
            admin_id=admin_id
        )
        return result or {"success": True}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail="Internal server error.")

@router.delete("/product-materials/delete") # DONE
def api_delete_product_material(request: Request):
    data = request.query_params
    user = request.session.get("user")
    if not user or user.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Admin privileges required")

    try:
        database.delete_product_material(
            product_id=data['product_id'],
            material_id=data['material_id'],
            admin_id=user["id"]
        )
        return {"message": "Material deleted successfully"}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


# --- Product Categories ---
@router.get("/product-categories", response_model=List[dict])
def get_categories():
    try:
        categories = database.get_product_categories()
        return categories.to_dict(orient="records")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/product-categories") # DONE
def create_product_category(request: Request, data: ProductCategoryCreate):
    user = request.session.get("user")
    if not user or user.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Admin privileges required")

    new_id = database.add_product_category(data.dict(), admin_id=user["id"])
    if new_id is None:
        raise HTTPException(status_code=400, detail="Product Category already exists")
    return {"id": new_id}


# --- Material Categories ---
@router.get("/material-categories")
def get_material_categories():
    return database.get_material_categories().to_dict(orient="records")

@router.post("/material-categories") # DONE
def create_material_category(request: Request, data: MaterialCategoryCreate):
    user = request.session.get("user")
    if not user or user.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Admin privileges required")

    new_id = database.add_material_category(data.dict(), admin_id=user["id"])
    if new_id is None:
        raise HTTPException(status_code=400, detail="Category already exists")
    return {"id": new_id}

@router.put("/material-categories/{id}") # DONE
def update_material_category(request: Request, id: str, data: MaterialCategoryUpdate):
    user = request.session.get("user")
    if not user or user.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Admin privileges required")

    database.update_material_category(id, data.dict(), admin_id=user["id"])
    return {"message": "Updated successfully"}

@router.delete("/material-categories/{id}") # DONE
def delete_material_category(id: str, request: Request):
    user = request.session.get("user")
    if not user or user.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Admin privileges required")

    database.delete_material_category(id, admin_id=user["id"])
    return {"message": "Deleted successfully"}


# --- Materials ---

@router.post("/stock-materials") # DONE
def stock_materials_endpoint(
    request: Request,
    data: StockTransactionCreate,
    authorization: Optional[str] = Header(default=None)
):
    # Try JWT first
    user = None
    if authorization and authorization.startswith("Bearer "):
        token = authorization.split(" ")[1]
        user = verify_token(token)

    # Fallback to session
    if not user:
        user = request.session.get("user")

    if not user:
        raise HTTPException(status_code=401, detail="Unauthorized")

    data_dict = data.dict()
    if user['role'] == 'admin':
        data_dict['admin_id'] = user['id']
    elif user['role'] == 'employee':
        data_dict['employee_id'] = user['id']
    else:
        raise HTTPException(status_code=403, detail="Invalid user role")

    return database.stock_materials(data_dict)


@router.get("/stock-transactions")
def read_stock_transactions():
    return database.get_stock_transactions_detailed().to_dict(orient="records")

@router.get("/materials")
def get_materials():
    # Get materials as a list of dicts
    materials_df = database.get_material()
    materials = materials_df.to_dict(orient="records")

    # Get the fast moving ratings by item_name
    ratings_map = analytics.get_fast_moving_ratings_map()  # function defined below

    # Add the rating to each material
    for mat in materials:
        item_name = mat.get("item_name")
        mat["fast_moving_rating"] = ratings_map.get(item_name, 0.0)

    return materials
 


@router.post("/materials") # DONE
def create_material(request: Request, data: MaterialCreate):
    user = request.session.get("user")
    if not user or user.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Admin privileges required")

    try:
        # pass admin_id for audit logging (DB function expected to accept it)
        item_id = database.add_material(data.dict(), admin_id=user["id"])
        return {"message": "Material and item added", "item_id": item_id}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.delete("/materials/{id}")
def delete_material(id: str, request: Request):
    user = request.session.get("user")
    if not user or user.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Admin privileges required")

    database.delete_material(id, admin_id=user["id"])
    return {"message": "Deleted successfully"}


# --- Customers ---
@router.get("/customers")
def get_customers():
    return database.get_customers().to_dict(orient="records")

@router.post("/customers") # DONE
def create_customer(request: Request, data: CustomerCreate):
    user = request.session.get("user")
    if not user or user.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Admin privileges required")

    result = database.add_customer(data.dict(), admin_id=user["id"])
    if not result["success"]:
        raise HTTPException(status_code=400, detail=result["message"])
    return {"message": result["message"]}


@router.put("/customers/{id}") # DONE
def update_customer(request: Request, id: str, data: CustomerUpdate):
    user = request.session.get("user")
    if not user or user.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Admin privileges required")

    database.update_customer(id, data.dict(), admin_id=user["id"])
    return {"message": "Updated successfully"}


@router.delete("/customers/{id}") # DONE
def delete_customer(request: Request, id: str):
    user = request.session.get("user")
    if not user or user.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Admin privileges required")

    database.delete_customer(id, admin_id=user["id"])
    return {"message": "Deleted successfully"}


# --- Products ---
@router.get("/products/{product_id}/quote")
def get_product_quote(product_id: str):
    try:
        quote = database.calculate_quote(product_id)
        return quote
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
        
@router.get("/products")
def get_products():
    return database.get_products().to_dict(orient="records")

@router.post("/products") # DONE
def create_product(request: Request, data: ProductCreate):
    user = request.session.get("user")
    if not user or user.get("role") != "admin":
        raise HTTPException(status_code=401, detail="Unauthorized")

    try:
        # pass admin_id for audit logging (DB function expected to accept it)
        result = database.add_product(data.dict(), admin_id=user["id"])
    except TypeError:
        # fallback if DB signature wasn't changed
        result = database.add_product(data.dict())

    if not result["success"]:
        raise HTTPException(status_code=400, detail=result["message"])
    return result

@router.delete("/products/{id}") # DONE
def delete_product(request: Request, id: str):
    user = request.session.get("user")
    if not user or user.get("role") != "admin":
        raise HTTPException(status_code=401, detail="Unauthorized")

    try:
        # try with admin_id if DB was updated
        try:
            return database.delete_product(id, admin_id=user["id"])
        except TypeError:
            return database.delete_product(id)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


# --- Suppliers ---
@router.get("/suppliers")
def get_suppliers():
    return database.get_suppliers().to_dict(orient="records")

@router.post("/suppliers") # DONE
def create_supplier(request: Request, data: SupplierCreate):
    user = request.session.get("user")
    if not user or user.get("role") != "admin":
        raise HTTPException(status_code=401, detail="Unauthorized")

    try:
        try:
            result = database.add_supplier(data.dict(), admin_id=user["id"])
        except TypeError:
            result = database.add_supplier(data.dict())

        if not result["success"]:
            raise HTTPException(status_code=400, detail=result["message"])
        return {"message": result["message"]}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@router.put("/suppliers/{id}") # DONE
def update_supplier(request: Request, id: str, data: SupplierUpdate):
    user = request.session.get("user")
    if not user or user.get("role") != "admin":
        raise HTTPException(status_code=401, detail="Unauthorized")

    try:
        try:
            database.update_supplier(id, data.dict(), admin_id=user["id"])
        except TypeError:
            database.update_supplier(id, data.dict())
        return {"message": "Updated successfully"}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@router.delete("/suppliers/{id}") # DONE
def delete_supplier(request: Request, id: str):
    user = request.session.get("user")
    if not user or user.get("role") != "admin":
        raise HTTPException(status_code=401, detail="Unauthorized")

    try:
        try:
            database.delete_supplier(id, admin_id=user["id"])
        except TypeError:
            database.delete_supplier(id)
        return {"message": "Deleted successfully"}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
    
# ----- Ordering Transaction ----

@router.post("/orders") # DONE
def place_order(request: Request, order: OrderTransactionCreate):
    user = request.session.get("user")
    if not user or user.get("role") != "admin":
        raise HTTPException(status_code=401, detail="Unauthorized")

    admin_id = user["id"]

    # Include admin_id in the order object before saving
    order_data = order.dict()
    order_data["admin_id"] = admin_id

    result = database.create_order_transaction(order_data)
    return result

@router.get("/order-statuses")
def order_statuses():
    result = database.get_order_statuses()
    return result.to_dict(orient="records")

@router.put("/orders/update-status") # DONE
def update_order_transaction_status(request: Request, data: OrderStatusUpdate):
    user = request.session.get("user")
    if not user or user.get("role") != "admin":
        raise HTTPException(status_code=401, detail="Unauthorized")

    # keep DB call same as before (minimal change). If DB was updated to accept admin_id,
    # we can add it here similarly to the other endpoints.
    result = database.update_order_status(data.transaction_id, data.status_code, database.con)

    if "error" in result:
        raise HTTPException(status_code=404, detail=result["error"])
    
    return {"message": "Order status updated successfully."}

@router.get("/order-transactions")
def read_order_transactions():
    return database.get_order_transactions_detailed().to_dict(orient="records")

# ----- Other Read/Get -----

@router.get("/unit-measurements")
def read_unit_measurements():
    return database.get_unit_measurements().to_dict(orient="records")

@router.get("/stock-transaction-types")
def read_stock_transaction_types():
    return database.get_stock_transaction_types().to_dict(orient="records")

@router.get("/order-statuses")
def read_order_statuses():
    return database.get_order_statuses().to_dict(orient="records")

@router.get("/employees")
def read_employees():
    return database.get_employees().to_dict(orient="records")

@router.get("/admins")
def read_admins():
    return database.get_admins().to_dict(orient="records")


# ---------- ANALYTICS -----------

@router.get("/analytics/order-rev-sales")
def get_inventory_dashboard_summary(request: Request, period: str = "week"):
    summary = analytics.get_summary_cards(period)
    return JSONResponse(content=summary)

@router.get("/summary/products")
def product_summary():
    return analytics.get_product_usage_summary()

@router.get("/summary/materials")
def material_summary():
    return analytics.get_material_usage_summary()

@router.get("/recent-transactions")
def recent_transactions_api():
    try:
        data = analytics.get_recent_order_transactions()
        return JSONResponse(content={"transactions": data})
    except Exception as e:
        import traceback
        traceback.print_exc()
        return JSONResponse(content={"error": str(e)}, status_code=500)
    
@router.get("/dashboard/metrics")
def dashboard_metrics():
    return analytics.get_all_time_metrics()

@router.get("/dashboard/stock-flow")
def stock_flow_summary():
    try:
        result = analytics.get_stock_summary()
        return JSONResponse(content=result)
    except Exception as e:
        import traceback
        traceback.print_exc()
        return JSONResponse(content={"error": str(e)}, status_code=500)

# ------------ Receipt and Quote -----------

@router.post("/generate-receipt")
def generate_receipt(req: ReceiptRequest):
    # Ensure pdf_container exists
    output_dir = os.path.join(os.path.dirname(__file__), "..", "pdf_container")
    os.makedirs(output_dir, exist_ok=True)

    # Clean old files
    reciept.cleanup_old_pdfs(output_dir, max_age_minutes=10)

    # Validate down payment
    grand_total = sum(item.quantity * item.unit_price for item in req.items)
    if req.down_payment > grand_total:
        return {"error": "Down payment cannot exceed the total product cost."}

    # Create receipt filename
    filename = os.path.join(output_dir, f"receipt_{uuid.uuid4().hex}.pdf")

    # Generate PDF
    reciept.generate_unofficial_receipt(
        filename=filename,
        company_name="Times Stock Aluminum & Glass",
        customer_name=req.customer_name,
        address=req.address,
        phone=req.phone,
        items=[item.dict() for item in req.items],
        down_payment=req.down_payment
    )

    return FileResponse(filename, media_type="application/pdf", filename="receipt.pdf")

@router.post("/generate-quotation")
def generate_quotation(data: QuotationRequest):
    temp_file = NamedTemporaryFile(delete=False, suffix=".pdf")
    filename = temp_file.name

    reciept.generate_modern_quotation_pdf(
        filename=filename,
        client_name=data.client_name,
        client_address=data.client_address,
        items_quote=[item.dict() for item in data.items_quote]
    )

    return FileResponse(filename, media_type="application/pdf", filename="quotation.pdf")

@router.get("/reports/pdf")
def generate_report_pdf_endpoint(year: int, month: int = None, user: dict = Depends(get_current_user)):
    if not user:
        return {"error": "Unauthorized"}

    # Gather reports
    report_text = graphs.get_text_report_for_month(year, month)
    turnover_report = graphs.get_turnover_text_report_for_month(year, month)
    stl_report = graphs.get_stl_text_report_for_month(year, month)
    moving_avg_report = graphs.get_sales_moving_average_text_report(month=month, year=year)
    stock_movement_report = graphs.get_stock_movement_report_for_month(year, month)  
    products_sold_report = graphs.get_products_sold_for_month(year, month)

    # Generate PDF
    filepath = reciept.generate_report_pdf(report_text, turnover_report, stl_report, moving_avg_report,stock_movement_report,products_sold_report, year, month)

    return FileResponse(filepath, media_type="application/pdf", filename=filepath.split("/")[-1])


# ------------ SETTINGS -------------

@router.post("/settings/add-employees", status_code=201) # DONE
def api_add_employee(
    payload: EmployeeCreate,
    request: Request
):
    # require admin to add employees
    user = request.session.get("user")
    if not user or user.get("role") != "admin":
        raise HTTPException(status_code=401, detail="Unauthorized")

    data = payload.model_dump()
    try:
        # prefer new signature with admin_id if DB supports it
        try:
            result = database.add_employee(data, admin_id=user["id"])
        except TypeError:
            result = database.add_employee(data)

        if not result.get("success", False):
            raise HTTPException(status_code=400, detail=result["message"])
        return result
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.put("/settings/{id}/status") # DONE
def api_update_employee_status(
    id: str,
    payload: EmployeeStatusUpdate,
    request: Request
):
    # require admin to change employee status
    user = request.session.get("user")
    if not user or user.get("role") != "admin":
        raise HTTPException(status_code=401, detail="Unauthorized")

    try:
        try:
            # if DB update_account_status now accepts admin_id (audit), pass it
            result = database.update_account_status(id, payload.is_active, admin_id=user["id"])
        except TypeError:
            # fallback to original signature
            result = database.update_account_status(id, payload.is_active)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    return result


@router.post("/settings/change-employee-password", status_code=200) # DONE
def api_change_employee_password(
    payload: ChangeEmployeePassword,
    request: Request
):
    # Pull current user from session
    user = request.session.get("user")

    # If no session or not admin, block access
    if not user or user.get("role") != "admin":
        raise HTTPException(status_code=401, detail="Unauthorized")

    # Call DB function using session admin ID
    result = database.change_employee_password(
        user["id"],  # taken from logged-in admin session
        payload.target_employee_id,
        payload.new_password
    )

    if not result.get("success", False):
        raise HTTPException(status_code=400, detail=result["message"])

    return result


@router.get("/maintenance/preview-delete/{years}")
def preview_transactions_to_delete(years: int, current_admin = Depends(database.get_current_admin)):
    result = database.delete_old_transactions(years, admin_id=current_admin["id"], dry_run=True)
    total = (
        result.get("old_order_items", 0)
        + result.get("old_orders", 0)
        + result.get("old_stock_items", 0)
        + result.get("old_stocks", 0)
    )
    if total == 0:
        return {"message": f"Data still hasn't reached {years} years old", "cutoff_date": result.get("cutoff_date")}
    return result

@router.delete("/maintenance/delete-old-transactions/{years}") # DONE
def perform_delete_old_transactions(years: int, current_admin = Depends(database.get_current_admin)):
    try:
        admin_id = current_admin["id"]
        result = database.delete_old_transactions(years, admin_id=admin_id, dry_run=False)
        return result
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

@router.get("/audit-logs")
def fetch_audit_logs(limit: int = 100, offset: int = 0, request: Request = None):
    # admin-only
    user = request.session.get("user") if request else None
    if not user or user.get("role") != "admin":
        raise HTTPException(status_code=401, detail="Unauthorized")

    try:
        logs = database.get_audit_logs(limit=limit, offset=offset)
        return {"logs": logs}
    except Exception as e:
        # return a helpful message during development; you can remove detail in production
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/settings/migrate-hashes")    
def api_migrate_password_hashes():
    return database.migrate_plaintext_passwords_to_hash()

