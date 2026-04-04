from duckdb import df
from fastapi import FastAPI, HTTPException, Header, Request, Depends
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.responses import FileResponse, HTMLResponse, RedirectResponse
from fastapi.middleware.cors import CORSMiddleware
from starlette.middleware.sessions import SessionMiddleware
from datetime import datetime, timedelta
from .api import router as api_router
from .auth import router as auth_router, get_current_user
import os
from pathlib import Path
import asyncio
from backend import graphs

app = FastAPI(title="TimeStock Inventory API")

ENV = os.getenv("APP_ENV", "development").lower()
SESSION_SECRET = os.getenv("SESSION_SECRET")
if not SESSION_SECRET:
    raise RuntimeError("SESSION_SECRET is not set")

if ENV == "production":
    allowed_origins = [
        "https://timestock-ims.online",
    ]
    https_only = True
else:
    allowed_origins = [
        "http://127.0.0.1:8000",
        "http://localhost:8000",
    ]
    https_only = False

app.add_middleware(
    CORSMiddleware,
    allow_origins=allowed_origins,
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
    allow_headers=["Authorization", "Content-Type"],
)

app.add_middleware(
    SessionMiddleware,
    secret_key=SESSION_SECRET,
    session_cookie="session",
    same_site="lax",
    https_only=https_only,
    max_age=60 * 60 * 8,
)

app.include_router(api_router, prefix="/api")
app.include_router(auth_router)


@app.middleware("http")
async def no_cache_headers(request: Request, call_next):
    response = await call_next(request)
    response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    response.headers["Pragma"] = "no-cache"
    response.headers["Expires"] = "0"
    return response

# Set up Jinja templates directory
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
templates = Jinja2Templates(directory=os.path.join(BASE_DIR, "../templates/html"))
app.mount("/css", StaticFiles(directory=os.path.join(BASE_DIR, "../templates/css")), name="css")
app.mount("/images", StaticFiles(directory=os.path.join(BASE_DIR, "../templates/images")), name="images")

# Home route
@app.get("/", response_class=HTMLResponse)
@app.get("/Home.html", response_class=HTMLResponse, include_in_schema=False)
def index(request: Request, user: dict = Depends(get_current_user)):
    if not user:
        return RedirectResponse(url="/login", status_code=302)

    context = {
        "request": request,
        "user": user,
        "fastest_moving_html": graphs.get_fastest_moving_materials_chart(),
        "reorder_point_html": graphs.get_reorder_point_chart(),
    }

    return templates.TemplateResponse("Home.html", context)


@app.get("/product.html", response_class=HTMLResponse)
def product_page(request: Request, user: dict = Depends(get_current_user)):
    if not user:
        return RedirectResponse(url="/login")
    return templates.TemplateResponse("product.html", {"request": request, "user": user})

@app.get("/Materials.html", response_class=HTMLResponse)
def materials_page(request: Request, user: dict = Depends(get_current_user)):
    if not user:
        return RedirectResponse(url="/login")
    return templates.TemplateResponse("Materials.html", {"request": request, "user": user})

async def get_all_graphs():
    tasks = [
        asyncio.to_thread(graphs.get_graph_html),
        asyncio.to_thread(graphs.get_turnover_combined_graph),
        asyncio.to_thread(graphs.get_stl_decomposition_graph),
        asyncio.to_thread(graphs.get_sales_moving_average_chart),
    ]
    results = await asyncio.gather(*tasks)
    return results

@app.get("/Analytics.html", response_class=HTMLResponse)
def analytics_page(request: Request, user: dict = Depends(get_current_user)):
    if not user:
        return RedirectResponse(url="/login")

    # Basic sales and turnover charts
    chart_html, chart_report = graphs.get_graph_html()
    turnover_combined_html, _, summary_html = graphs.get_turnover_combined_graph()

    # STL Decomposition

    stl_html, _, df, result, top_products_df = graphs.get_stl_decomposition_graph()
    stl_report = graphs.get_stl_decomposition_report(df, result)
    stl_recommendation_flat, stl_recommendation_grouped, stl_confidence = \
        graphs.generate_recommendations_from_stl(df, result, top_products_df)

    # Moving Average Chart & Recommendations
    ma_chart_html, ma_df = graphs.get_sales_moving_average_chart()
    ma_report = graphs.generate_sales_moving_average_report(ma_df)
    ma_recommendation = graphs.generate_moving_average_recommendations(ma_df) 

    return templates.TemplateResponse("Analytics.html", {
        "request": request,
        "user": user,
        "chart_html": chart_html,
        "chart_report": chart_report,
        "turnover_combined_html": turnover_combined_html,
        "summary": summary_html,

        "stl_html": stl_html,
        "stl_report": stl_report,
        "stl_recommendation": stl_recommendation_flat,
        "stl_recommendation_grouped": stl_recommendation_grouped,
        "stl_confidence": stl_confidence,

        "ma_chart_html": ma_chart_html,
        "ma_report": ma_report,
        "ma_recommendation": ma_recommendation
    })



@app.get("/Order_and_Quotation.html", response_class=HTMLResponse)
def order_quotation_page(request: Request, user: dict = Depends(get_current_user)):
    if not user:
        return RedirectResponse(url="/login")
    return templates.TemplateResponse("Order_and_Quotation.html", {"request": request, "user": user})

@app.get("/Settings.html", response_class=HTMLResponse)
def settings_page(request: Request, user: dict = Depends(get_current_user)):
    if not user:
        return RedirectResponse(url="/login")
    return templates.TemplateResponse("Settings.html", {"request": request, "user": user})

@app.get("/Supplier.html", response_class=HTMLResponse)
def supplier_page(request: Request, user: dict = Depends(get_current_user)):
    if not user:
        return RedirectResponse(url="/login")
    return templates.TemplateResponse("Supplier.html", {"request": request, "user": user})


@app.get("/profile_management.html", response_class=HTMLResponse)
def profile_management_page(request: Request, user: dict = Depends(get_current_user)):
    if not user:
        return RedirectResponse(url="/login")
    return templates.TemplateResponse("profile_management.html", {"request": request, "user": user})


@app.get("/Transactions.html", response_class=HTMLResponse)
def transactions_page(request: Request, user: dict = Depends(get_current_user)):
    if not user:
        return RedirectResponse(url="/login")
    return templates.TemplateResponse("Transactions.html", {"request": request, "user": user})

@app.get("/Reports.html", response_class=HTMLResponse)
def reports_page(request: Request, user: dict = Depends(get_current_user), month: int = None, year: int = None):
    if not user:
        return RedirectResponse(url="/login")

    # Default to last month if not provided
    today = datetime.today()
    if not year or not month:
        first_of_this_month = today.replace(day=1)
        last_month_date = first_of_this_month - timedelta(days=1)
        year, month = last_month_date.year, last_month_date.month

    # Validate year/month inputs
    try:
        report_date = datetime(year=year, month=month, day=1)
        if report_date > today:
            raise ValueError("Selected month/year is in the future.")
    except ValueError as ve:
        # Return template with error message
        return templates.TemplateResponse("Reports.html", {
            "request": request,
            "user": user,
            "error_message": f"Invalid month/year: {ve}",
            "year": year,
            "month": month
        })

    # Try generating reports, catch any errors (e.g., no data)
    try:
        report_text = graphs.get_text_report_for_month(year, month)
        turnover_report = graphs.get_turnover_text_report_for_month(year, month)
        stl_report = graphs.get_stl_text_report_for_month(year, month)
        moving_avg_report = graphs.get_sales_moving_average_text_report(month=month, year=year)
        stock_movement_report = graphs.get_stock_movement_report_for_month(year, month)
        products_sold_report = graphs.get_products_sold_for_month(year, month)
    except Exception as e:
        return templates.TemplateResponse("Reports.html", {
            "request": request,
            "user": user,
            "error_message": f"No data found or an error occurred for {month}/{year}: {e}",
            "year": year,
            "month": month
        })

    return templates.TemplateResponse("Reports.html", {
        "request": request,
        "user": user,
        "report_text": report_text,
        "turnover_report": turnover_report,
        "stl_report": stl_report,
        "moving_avg_report": moving_avg_report,
        "stock_movement_report": stock_movement_report, 
        "products_sold_report": products_sold_report,
        "year": year,
        "month": month
    })


@app.get("/Customer.html", response_class=HTMLResponse)
def customer_page(request: Request, user: dict = Depends(get_current_user)):
    if not user:
        return RedirectResponse(url="/login")
    return templates.TemplateResponse("Customer.html", {"request": request, "user": user})

@app.get("/download-db")
def download_duckdb(user: dict = Depends(get_current_user)):
    if not user:
        return RedirectResponse(url="/login")
    
    # Use persistent Railway volume
    db_path = Path("/data/rdb_timestock_3") 

    if not db_path.exists():
        raise HTTPException(status_code=404, detail=f"Database file not found: {db_path}")

    return FileResponse(
        path=db_path,
        media_type="application/octet-stream",
        filename=db_path.name
    )