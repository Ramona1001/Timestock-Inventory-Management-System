from fastapi import FastAPI, Request, Depends
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.middleware.cors import CORSMiddleware
from starlette.middleware.sessions import SessionMiddleware
from datetime import datetime, timedelta
from .api import router as api_router
from .auth import router as auth_router, get_current_user
import os
import asyncio
from backend import graphs

app = FastAPI(title="TimeStock Inventory API")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.add_middleware(
    SessionMiddleware,
    secret_key="SKQ2x3IVvY3Dqnr8QXoLfnc1F9-zTj0Zu1-vO6F2b7c",
    session_cookie="session",
    same_site="lax",
    https_only=False,   # Keep True if using HTTPS, False for local HTTP
    max_age=3600 * 24, # 1 day
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
def index(request: Request, user: dict = Depends(get_current_user)):
    if not user:
        return RedirectResponse(url="/login")
    
    fastest_moving_html = graphs.get_fastest_moving_materials_chart()
    reorder_point_html = graphs.get_reorder_point_chart()
    
    return templates.TemplateResponse("index.html", {
        "request": request,
        "user": user,
        "fastest_moving_html": fastest_moving_html,
        "reorder_point_html": reorder_point_html
    })


@app.get("/index.html", response_class=HTMLResponse)
def index_page(request: Request, user: dict = Depends(get_current_user)):
    if not user:
        return RedirectResponse(url="/login")

    fastest_moving_html = graphs.get_fastest_moving_materials_chart()
    reorder_point_html = graphs.get_reorder_point_chart()

    return templates.TemplateResponse("index.html", {
        "request": request,
        "user": user,
        "fastest_moving_html": fastest_moving_html,
        "reorder_point_html": reorder_point_html
    })


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
    stl_recommendation_flat, stl_recommendation_grouped = graphs.generate_recommendations_from_stl(df, result, top_products_df)

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
