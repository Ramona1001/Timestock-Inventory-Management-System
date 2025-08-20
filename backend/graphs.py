# graph.py
import duckdb
from datetime import datetime, timedelta
import calendar
import plotly.graph_objects as go
import pandas as pd
from plotly.utils import PlotlyJSONEncoder
import plotly.subplots as sp
from statsmodels.tsa.seasonal import STL
import json


def get_graph_html(period='month'):
    con = duckdb.connect('backend/db_timestock')

    query = f"""
        SELECT 
            DATE_TRUNC('{period}', ot.date_created) AS period,
            COUNT(DISTINCT ot.id) AS total_orders,
            SUM(oi.quantity) AS total_sales,
            SUM(ot.total_amount) AS total_revenue
        FROM order_transactions ot
        JOIN order_items oi ON ot.id = oi.order_id
        GROUP BY period
        ORDER BY period;
    """
    df = con.execute(query).fetchdf()
    
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=df['period'], y=df['total_orders'], name='Total Orders', mode='lines+markers', yaxis='y1'))
    fig.add_trace(go.Scatter(x=df['period'], y=df['total_sales'], name='Total Sales', mode='lines+markers', yaxis='y1'))
    fig.add_trace(go.Scatter(x=df['period'], y=df['total_revenue'], name='Total Revenue (₱)', mode='lines+markers', yaxis='y2', line=dict(color='green')))

    fig.update_layout(
        title=f"Orders, Sales, and Revenue per {period.capitalize()}",
        xaxis=dict(title=period.capitalize()),
        yaxis=dict(title='Orders / Sales'),
        yaxis2=dict(title='Revenue (₱)', overlaying='y', side='right', showgrid=False),
        legend=dict(orientation='h', x=0, y=1.15),
        hovermode='x unified',
        template='plotly_white',
        width=600,
        height=400
    )

    report = generate_chart_report(df)
    return fig.to_html(full_html=False, config={'responsive': True}), report

def generate_chart_report(df):
    highest_revenue_row = df.loc[df['total_revenue'].idxmax()]
    max_month = highest_revenue_row['period'].strftime('%B %Y')
    max_revenue = highest_revenue_row['total_revenue']
    avg_orders = df['total_orders'].mean()
    total_sales = df['total_sales'].sum()
    avg_revenue = df['total_revenue'].mean()

    return f"""
    <strong>Summary Report:</strong><br>
    📅 Highest Revenue Month: <b>{max_month}</b><br>
    💰 Max Revenue: ₱{max_revenue:,.2f}<br>
    📦 Total Sales: {total_sales:,} units<br>
    📊 Average Orders/Month: {avg_orders:.1f}<br>
    💵 Average Monthly Revenue: ₱{avg_revenue:,.2f}
    """

def get_turnover_combined_graph():
    con = duckdb.connect('backend/db_timestock')

    df = con.execute("""
        WITH monthly_data AS (
            SELECT
                DATE_TRUNC('month', st.date_created) AS period,
                SUM(CASE WHEN stt.type_code = 'stock-in' THEN sti.quantity * m.material_cost ELSE 0 END) AS stock_in_value,
                SUM(CASE WHEN stt.type_code = 'stock-out' THEN sti.quantity * m.material_cost ELSE 0 END) AS cogs,
                SUM(m.current_stock * m.material_cost) AS ending_inventory_value
            FROM stock_transaction_items sti
            JOIN stock_transactions st ON st.id = sti.stock_transaction_id
            JOIN stock_transaction_types stt ON stt.id = st.stock_type_id
            JOIN materials m ON m.id = sti.material_id
            GROUP BY period
        ),
        turnover_calc AS (
            SELECT
                STRFTIME(period, '%Y-%m') AS label,
                cogs,
                ending_inventory_value,
                ROUND(
                    (ending_inventory_value + (cogs + stock_in_value - ending_inventory_value)) / 2.0, 2
                ) AS avg_inventory,
                ROUND(
                    CASE
                        WHEN ((ending_inventory_value + (cogs + stock_in_value - ending_inventory_value)) / 2.0) > 0
                        THEN cogs / ((ending_inventory_value + (cogs + stock_in_value - ending_inventory_value)) / 2.0)
                        ELSE 0
                    END, 2
                ) AS turnover_rate
            FROM monthly_data
        )
        SELECT label, cogs, avg_inventory, turnover_rate
        FROM turnover_calc
        ORDER BY label;
    """).fetchdf()

    

    fig = go.Figure()

    # Bar: COGS
    fig.add_trace(go.Bar(
        x=df['label'],
        y=df['cogs'],
        name='COGS',
        marker_color='salmon',
        yaxis='y'
    ))

    # Bar: Average Inventory
    fig.add_trace(go.Bar(
        x=df['label'],
        y=df['avg_inventory'],
        name='Average Inventory',
        marker_color='skyblue',
        yaxis='y'
    ))

    # Line: Turnover Rate
    fig.add_trace(go.Scatter(
        x=df['label'],
        y=df['turnover_rate'],
        name='Turnover Rate',
        mode='lines+markers',
        line=dict(color='green', width=2),
        yaxis='y2'
    ))

    fig.update_layout(
        title="COGS, Average Inventory & Turnover Rate (Monthly)",
        barmode='group',
        xaxis=dict(title='Month'),
        yaxis=dict(
            title='Amount (₱)',
            side='left',
            showgrid=False
        ),
        yaxis2=dict(
            title='Turnover Rate',
            overlaying='y',
            side='right',
            showgrid=False
        ),
        legend=dict(orientation='h', x=0, y=1.15),
        template='plotly_white',
        width=600,
        height=400
    )

    summary_html = generate_turnover_summary(df)
    return fig.to_html(full_html=False, config={'responsive': True}), df, summary_html

def generate_turnover_summary(df):
    max_turnover_row = df.loc[df['turnover_rate'].idxmax()]
    max_month = max_turnover_row['label']
    max_turnover = max_turnover_row['turnover_rate']
    avg_turnover = df['turnover_rate'].mean()
    avg_inventory = df['avg_inventory'].mean()
    total_cogs = df['cogs'].sum()

    return f"""
    <strong>Turnover Summary Report:</strong><br>
    🔄 Highest Turnover Rate Month: <b>{max_month}</b><br>
    📈 Max Turnover Rate: <b>{max_turnover:.2f}</b><br>
    💹 Average Turnover Rate: <b>{avg_turnover:.2f}</b><br>
    📦 Average Inventory Value: ₱{avg_inventory:,.2f}<br>
    💰 Total COGS: ₱{total_cogs:,.2f}
    """


def get_fastest_moving_materials_chart():
    query = """
    SELECT 
        i.item_name,
        m.unit_measurement,
        SUM(oi.quantity * pm.used_quantity) AS total_material_used
    FROM order_items oi
    JOIN products p ON oi.product_id = p.id
    JOIN product_materials pm ON p.id = pm.product_id
    JOIN materials m ON pm.material_id = m.id
    JOIN items i ON m.item_id = i.id
    JOIN order_transactions ot ON ot.id = oi.order_id
    WHERE ot.date_created >= DATE_TRUNC('month', CURRENT_DATE - INTERVAL 3 MONTH)
    AND ot.status_id = 'OS005'
    GROUP BY i.item_name, m.unit_measurement
    ORDER BY total_material_used DESC
    LIMIT 10;
    """

    with duckdb.connect("backend/db_timestock") as conn:
        df = conn.execute(query).fetchdf()

    if df.empty:
        return "<p>No data available for the past 3 months.</p>"

    df["item_name"] = df["item_name"].astype(str).str.slice(0, 25)
    df["total_material_used"] = pd.to_numeric(df["total_material_used"], errors="coerce").fillna(0)

    # Format usage with unit, e.g., "850 ft"
    df["used_with_unit"] = df["total_material_used"].round(2).astype(str) + " " + df["unit_measurement"]

    fig = go.Figure(
        data=[
            go.Bar(
                x=df["total_material_used"],
                y=df["item_name"],
                orientation="h",
                marker=dict(
                    color=df["total_material_used"],
                    colorscale="Blues",
                    line=dict(width=0.5, color="darkgray")
                ),
                text=df["used_with_unit"],  # show "850 ft"
                textposition="auto",
                hovertemplate="<b>%{y}</b><br>Used: %{text}<extra></extra>"  # use formatted string in hover
            )
        ]
    )

    fig.update_layout(
        title=dict(
            text="Top 10 Fastest Moving Materials (Last 3 Months)",
            font=dict(size=18),
            x=0.5,
            xanchor="center"
        ),
        xaxis=dict(
            title="Total Material Used",
            showgrid=True,
            gridcolor="lightgrey",
            zeroline=False
        ),
        yaxis=dict(
            title="Material",
            autorange="reversed",
            tickfont=dict(size=12)
        ),
        margin=dict(l=100, r=20, t=60, b=40),
        width=600,
        height=400,
        template="plotly_white"
    )

    return fig.to_html(full_html=False, include_plotlyjs='cdn', config={"responsive": True})

def get_reorder_point_chart(return_df=False):
    query = """
        WITH daily_usage AS (
          SELECT 
            sti.material_id,
            DATE(st.date_created) AS usage_day,
            SUM(sti.quantity) AS total_used
          FROM stock_transaction_items sti
          JOIN stock_transactions st ON sti.stock_transaction_id = st.id
          JOIN stock_transaction_types stt ON st.stock_type_id = stt.id
          WHERE stt.type_code = 'stock-out'
            AND st.date_created >= (CURRENT_DATE - INTERVAL 30 DAY)
          GROUP BY sti.material_id, DATE(st.date_created)
        ),
        average_usage AS (
          SELECT 
            material_id,
            ROUND(AVG(total_used), 2) AS avg_daily_usage
          FROM daily_usage
          GROUP BY material_id
        )
        SELECT 
          m.id AS material_id,
          i.item_name,
          m.current_stock,
          au.avg_daily_usage,
          ROUND((au.avg_daily_usage * 5 + 10), 2) AS reorder_point,
          CASE 
            WHEN m.current_stock <= (au.avg_daily_usage * 5 + 10) THEN '⚠️ Reorder Needed'
            ELSE '✅ Sufficient Stock'
          END AS reorder_status
        FROM materials m
        JOIN items i ON m.item_id = i.id
        JOIN average_usage au ON m.id = au.material_id
        ORDER BY reorder_status DESC, item_name;
    """

    with duckdb.connect("backend/db_timestock") as conn:
        df = conn.execute(query).fetchdf()

    if return_df:
        return df if not df.empty else None
    
    if df.empty:
        return "<p>No reorder data available.</p>"

    color_map = df['reorder_status'].map({
        '⚠️ Reorder Needed': 'crimson',
        '✅ Sufficient Stock': 'seagreen'
    })

    fig = go.Figure()

    fig.add_trace(go.Bar(
        x=df['item_name'],
        y=df['current_stock'],
        name='Current Stock',
        marker_color=color_map,
        text=df['reorder_status'],
        textposition='outside',
        hovertemplate=(
            "<b>%{x}</b><br>Stock: %{y}<br>ROP: %{customdata[0]}<br>Daily Usage: %{customdata[1]}<extra></extra>"
        ),
        customdata=df[['reorder_point', 'avg_daily_usage']]
    ))

    fig.add_trace(go.Scatter(
        x=df['item_name'],
        y=df['reorder_point'],
        mode='lines',
        name='Reorder Point',
        line=dict(color='orange', dash='dash')
    ))

    fig.update_layout(
        title="Reorder Point vs Current Stock",
        xaxis=dict(title="Material", tickangle=-45),
        yaxis=dict(title="Quantity"),
        barmode='group',
        legend=dict(orientation="h", x=0.5, xanchor="center", y=1.15),
        template="plotly_white",
        width=600,
        height=400,
        margin=dict(t=60, b=120)
    )

    return fig.to_html(full_html=False, include_plotlyjs='cdn', config={"responsive": True})


def get_stl_decomposition_graph():
    con = duckdb.connect('backend/db_timestock')

    # Monthly order quantity
    query = """
    SELECT 
        DATE_TRUNC('month', ot.date_created) AS order_month,
        SUM(oi.quantity) AS total_quantity
    FROM order_transactions ot
    JOIN order_items oi ON ot.id = oi.order_id
    GROUP BY order_month
    ORDER BY order_month
    """
    df = con.execute(query).fetchdf()
    df['order_month'] = pd.to_datetime(df['order_month'])

    df.set_index('order_month', inplace=True)
    df = df.asfreq('MS')
    df['total_quantity'] = df['total_quantity'].fillna(0)

    stl = STL(df['total_quantity'], period=12)
    result = stl.fit()

    # Top-selling product per month
    top_products_df = con.execute("""
        SELECT month, product_name FROM (
            SELECT 
                DATE_TRUNC('month', ot.date_created) AS month,
                i.item_name AS product_name,
                SUM(oi.quantity) AS total_qty,
                RANK() OVER (
                    PARTITION BY DATE_TRUNC('month', ot.date_created) 
                    ORDER BY SUM(oi.quantity) DESC
                ) AS rnk
            FROM order_transactions ot
            JOIN order_items oi ON ot.id = oi.order_id
            JOIN products p ON oi.product_id = p.id
            JOIN items i ON p.item_id = i.id
            GROUP BY month, product_name
        ) 
        WHERE rnk = 1
    """).fetchdf()

    top_products_df['month'] = pd.to_datetime(top_products_df['month'])
    top_products_df.rename(columns={'month': 'order_month', 'product_name': 'top_product'}, inplace=True)

    
    # Merge for hover info
    merged = result.trend.to_frame(name='trend').reset_index()
    merged = merged.merge(top_products_df, how='left', on='order_month')
    hover_text = [
        f"{m.strftime('%B %Y')}<br>Top Product: {p if pd.notna(p) else 'N/A'}"
        for m, p in zip(merged['order_month'], merged['top_product'])
    ]

    seasonal_hover = [
        f"{m.strftime('%B')}<br>Top Seasonal Product: {p if pd.notna(p) else 'N/A'}"
        for m, p in zip(result.seasonal.index, merged['top_product']) 
    ]


    fig = sp.make_subplots(rows=3, cols=1, shared_xaxes=True, subplot_titles=("Trend", "Seasonal", "Residual"))

    fig.add_trace(go.Scatter(
        x=merged['order_month'], y=merged['trend'],
        name='Trend',
        line=dict(color='blue'),
        text=hover_text,
        hoverinfo='text+y'
    ), row=1, col=1)

    fig.add_trace(go.Scatter(
        x=merged['order_month'], y=result.seasonal,
        name='Seasonal',
        line=dict(color='green'),
        text=seasonal_hover,
        hoverinfo='text+y'
    ), row=2, col=1)


    fig.add_trace(go.Scatter(
        x=result.resid.index, y=result.resid,
        name='Residual',
        line=dict(color='red')
    ), row=3, col=1)

    fig.update_layout(
        width=610,
        height=410,
        title_text="STL Decomposition of Monthly Orders",
        template="plotly_white",
        legend=dict(orientation='h', x=0, y=1.20)
    )

    # 📌 Recommendations section
    recommendations = generate_recommendations_from_stl(df, result, top_products_df)
    recommendation_html = "<ul>" + "".join(f"<li>{r}</li>" for r in recommendations) + "</ul>"
    report_html = "<hr><h4>📌 Recommendations</h4>" + recommendation_html

    return fig.to_html(full_html=False), report_html, df, result, top_products_df


def get_stl_decomposition_report(df, result):
    trend = result.trend
    seasonal = result.seasonal
    resid = result.resid

    # 📌 Exclude current month if incomplete
    today = datetime.today()
    if df.index[-1].month == today.month and df.index[-1].year == today.year:
        df = df.iloc[:-1]
        trend = trend.iloc[:-1]
        seasonal = seasonal.iloc[:-1]
        resid = resid.iloc[:-1]

    # Time range
    start = df.index.min().strftime('%B %Y')
    end = df.index.max().strftime('%B %Y')
    n_months = len(df)

    # Overall quantity
    total_quantity = df['total_quantity'].sum()
    avg_quantity = df['total_quantity'].mean()

    # Trend
    trend_diff = trend.iloc[-1] - trend.iloc[0]
    trend_direction = "increasing" if trend_diff > 0 else "decreasing" if trend_diff < 0 else "stable"
    avg_trend_change = trend.diff().mean()

    # Seasonality
    max_season = seasonal.max()
    min_season = seasonal.min()
    max_season_month = seasonal.idxmax().strftime('%B')
    min_season_month = seasonal.idxmin().strftime('%B')

    # Residuals
    resid_std = resid.std()
    high_resid = resid[abs(resid) > (2 * resid_std)]
    anomaly_months = [idx.strftime('%B %Y') for idx in high_resid.index]

    # Format report
    report = f"""
    <div class="card">
    <h2>STL Decomposition Report</h2>
    <p><strong>Time Range:</strong> {start} to {end} ({n_months} months)</p>
    <p><strong>Total Quantity Ordered:</strong> {total_quantity:.0f} units</p>
    <p><strong>Average Monthly Quantity:</strong> {avg_quantity:.2f} units</p>
    
    <h3>Trend Analysis</h3>
    <p>Trend is <strong>{trend_direction}</strong> over time, with an average monthly change of <strong>{avg_trend_change:.2f}</strong> units.</p>
    
    <h3>Seasonality</h3>
    <p>Maximum seasonal effect: <strong>+{max_season:.2f}</strong> (in {max_season_month})</p>
    <p>Minimum seasonal effect: <strong>{min_season:.2f}</strong> (in {min_season_month})</p>

    <h3>Residuals (Irregular Fluctuations)</h3>
    <p>Standard deviation of residuals: <strong>{resid_std:.2f}</strong></p>
    <p>Months with large unexpected deviations (possible anomalies): <strong>{', '.join(anomaly_months) if anomaly_months else 'None'}</strong></p>
    </div>
    """
    return report

def generate_recommendations_from_stl(df: pd.DataFrame, result, top_products_df: pd.DataFrame) -> list:
    recommendations = []

    trend = result.trend
    seasonal = result.seasonal
    resid = result.resid

    # Use current month (forecasted)
    current_date = df.index[-1]
    current_month_str = current_date.strftime('%B %Y')

    # Forecast trend for current month using last month's slope
    trend_change = trend.iloc[-2] - trend.iloc[-3]
    forecast_trend = trend.iloc[-2] + trend_change

    # ✅ Get historical seasonal values for this month (excluding current month)
    seasonal_history = seasonal[
        (seasonal.index.month == current_date.month) &
        (seasonal.index < current_date.replace(day=1))
    ]
    seasonal_effect = seasonal_history.mean()

    # Residual stats (historical)
    residual_std = resid.std()

    # Get top product from same month last year (or most recent available, excluding current month)
    past_top_product_row = top_products_df[
        (top_products_df['order_month'].dt.month == current_date.month) &
        (top_products_df['order_month'] < current_date.replace(day=1))
    ].sort_values('order_month', ascending=False).head(1)

    top_product = past_top_product_row['top_product'].values[0] if not past_top_product_row.empty else "N/A"

    # 🔍 DEBUG OUTPUT
    print("===== STL Forecast Debug =====")
    print(f"Current Month: {current_month_str}")
    print(f"Last Month Trend Value: {trend.iloc[-2]:.2f}")
    print(f"Month Before Last Trend Value: {trend.iloc[-3]:.2f}")
    print(f"Trend Change: {trend_change:.2f}")
    print(f"Forecasted Trend: {forecast_trend:.2f}")
    print("\nSeasonality History for this month (excluding current month):")
    for date, val in seasonal_history.items():
        print(f"  {date.strftime('%B %Y')}: {val:.2f}")
    print(f"Average Seasonal Effect: {seasonal_effect:.2f}")
    print(f"\nResidual STD (historical): {residual_std:.2f}")
    print(f"Top Product (historical for same month): {top_product}")
    print("================================\n")

    # Define sensitivity threshold for ignoring small trend changes
    TREND_THRESHOLD = 0.5  

    # Multi-month slope check 
    recent_trend_points = trend.iloc[-3:]
    slope = (recent_trend_points.iloc[-1] - recent_trend_points.iloc[0]) / (len(recent_trend_points) - 1)

    if trend_change > TREND_THRESHOLD:
        recommendations.append(
            f"🟢 {current_month_str} Trend Increasing (forecasted): Consider stocking more materials of <strong>{top_product}</strong>."
        )

    elif trend_change < -TREND_THRESHOLD:
        if slope < -TREND_THRESHOLD:
            recommendations.append(
                f"🔴 {current_month_str} Sustained Trend Decrease (forecasted): Monitor demand and consider reducing stock of <strong>{top_product}</strong>."
            )
        else:
            recommendations.append(
                f"🟡 {current_month_str} Minor Decline (forecasted): <strong>{top_product}</strong> still sells well — monitor but don’t reduce stock yet."
            )

    else:
        recommendations.append(
            f"⚪ {current_month_str} Trend Stable (forecasted): No major change in demand for <strong>{top_product}</strong>."
        )


    # Seasonal insight
    if seasonal_effect > 0:
        recommendations.append(
            f"🌞 Positive seasonality expected in <strong>{current_date.strftime('%B')}</strong> — anticipate higher demand."
        )
    elif seasonal_effect < 0:
        recommendations.append(
            f"🌧️ Negative seasonality expected in <strong>{current_date.strftime('%B')}</strong> — anticipate lower demand."
        )

    # Residual volatility check (historical)
    if residual_std > 0.2 * df['total_quantity'].mean():
        recommendations.append(
            "⚠️ High residual variability detected — demand is volatile, consider adding safety stock."
        )
    else:
        recommendations.append(
            "✅ Residuals show stable behavior — current forecasting approach is reliable."
        )

    return recommendations


def get_sales_moving_average_chart():
    con = duckdb.connect('backend/db_timestock')

    # Total monthly sales
    df = con.execute("""
        SELECT
            DATE_TRUNC('month', ot.date_created) AS month,
            SUM(ot.total_amount) AS total_sales
        FROM order_transactions ot
        GROUP BY month
        ORDER BY month
    """).fetchdf()
    df['month'] = pd.to_datetime(df['month'])

    # Top-selling product by quantity for each month
    top_products_df = con.execute("""
            SELECT month, product_name FROM (
                SELECT 
                    DATE_TRUNC('month', ot.date_created) AS month,
                    i.item_name AS product_name,
                    SUM(oi.quantity) AS total_qty,
                    RANK() OVER (
                        PARTITION BY DATE_TRUNC('month', ot.date_created) 
                        ORDER BY SUM(oi.quantity) DESC
                    ) AS rnk
                FROM order_transactions ot
                JOIN order_items oi ON ot.id = oi.order_id
                JOIN products p ON oi.product_id = p.id
                JOIN items i ON p.item_id = i.id
                GROUP BY month, product_name
            ) 
            WHERE rnk = 1

    """).fetchdf()
    top_products_df['month'] = pd.to_datetime(top_products_df['month'])

    # Merge with main sales df
    df = df.merge(top_products_df, on='month', how='left')
    df.rename(columns={'product_name': 'top_product'}, inplace=True)

    # Moving averages
    df['3_MA'] = df['total_sales'].rolling(window=3).mean()
    df['6_MA'] = df['total_sales'].rolling(window=6).mean()

    # Plotting
    fig = go.Figure()

    fig.add_trace(go.Scatter(
        x=df['month'], y=df['total_sales'],
        mode='lines+markers',
        name='Actual Sales',
        line=dict(color='steelblue'),
        customdata=df[['top_product']],
        hovertemplate='<b>%{x|%B %Y}</b><br>' +
                      'Sales: ₱%{y:,.2f}<br>' +
                      'Top Product: %{customdata[0]}<extra></extra>'
    ))

    fig.add_trace(go.Scatter(
        x=df['month'], y=df['3_MA'],
        mode='lines', name='3-Month MA',
        line=dict(color='orange', dash='dash')
    ))

    fig.add_trace(go.Scatter(
        x=df['month'], y=df['6_MA'],
        mode='lines', name='6-Month MA',
        line=dict(color='green', dash='dot')
    ))


    fig.update_layout(
        title="Monthly Sales with Moving Averages",
        xaxis_title="Month",
        yaxis_title="Sales (₱)",
        template="plotly_white",
        width=610,
        height=410,
        legend=dict(orientation='h', x=0, y=1.15)
    )

    html = fig.to_html(full_html=False, config={'responsive': True})
    return html, df

def generate_moving_average_recommendations(df: pd.DataFrame) -> list:
    recommendations = []

    # Ensure sorted by month
    df = df.sort_values('month')

    # Deduplicate by month (keep last entry for that month)
    df = df.drop_duplicates(subset=['month'], keep='last')

    # Remove rows with missing MA values
    df = df.dropna(subset=['3_MA', '6_MA'])

    if df.empty:
        return ["⚠️ Not enough data to generate moving average recommendations."]

    # Identify current month (to exclude from calc but still recommend for it)
    current_month = df.iloc[-1]['month']
    current_month_str = current_month.strftime('%B %Y')

    # Remove current month from analysis dataset
    analysis_df = df[df['month'] < current_month]

    if analysis_df.empty:
        return ["⚠️ Not enough past data to generate recommendation for current month."]

    # Latest complete month for trend analysis
    latest = analysis_df.iloc[-1]
    latest_month_str = latest['month'].strftime('%B %Y')

    # Debug output
    debug_df = analysis_df.tail(6).copy()
    print(f"\n[DEBUG] Generating recommendation for {current_month_str} (using past data)")
    print("[DEBUG] Last 6 past months used for moving average calculation:")
    print(debug_df[['month', 'total_sales', '3_MA', '6_MA', 'top_product']])

    # Trend analysis
    if latest['3_MA'] > latest['6_MA']:
        recommendations.append(
            f"📈 For {current_month_str}, based on past trends up to {latest_month_str}, "
            f"sales are trending upward — 3-month average is higher than 6-month. "
            f"Consider boosting inventory for in-demand products."
        )
    elif latest['3_MA'] < latest['6_MA']:
        recommendations.append(
            f"📉 For {current_month_str}, based on past trends up to {latest_month_str}, "
            f"sales are slowing — 3-month average is below 6-month. "
            f"Review marketing or run promotions to boost sales."
        )
    else:
        recommendations.append(
            f"🔄 For {current_month_str}, based on past trends up to {latest_month_str}, "
            f"sales are stable — no significant trend shift observed."
        )

    # Consecutive underperformance check — independent of trend
    underperf_df = analysis_df[
        (analysis_df['total_sales'] < analysis_df['3_MA']) &
        (analysis_df['total_sales'] < analysis_df['6_MA'])
    ]

    if len(underperf_df.tail(3)) == 3:  # last 3 months
        recommendations.append(
            f"⚠️ Sales have been below both moving averages for the last 3 months up to {latest_month_str}. "
            f"This sustained drop suggests potential demand issues."
        )



    # Most consistent top product (based on past data only)
    top_counts = analysis_df['top_product'].value_counts()
    if not top_counts.empty:
        top_product = top_counts.idxmax()
        recommendations.append(
            f"🏆 {top_product} has been the most consistent top seller up to {latest_month_str}. "
            f"Consider prioritizing its promotion or bundling."
        )

    return recommendations


def generate_sales_moving_average_report(df: pd.DataFrame) -> str:
    # Drop NA to avoid issues with early months having no MA
    df = df.dropna(subset=['3_MA', '6_MA'])

    # Identify highest sales month
    highest_row = df.loc[df['total_sales'].idxmax()]
    highest_month = highest_row['month'].strftime('%B %Y')
    highest_value = highest_row['total_sales']

    # Average monthly sales
    avg_sales = df['total_sales'].mean()
    latest_month = df['month'].max().strftime('%B %Y')
    latest_sales = df['total_sales'].iloc[-1]
    latest_3_ma = df['3_MA'].iloc[-1]
    latest_6_ma = df['6_MA'].iloc[-1]

    return f"""
    <strong>Sales Moving Average Summary:</strong><br>
    📈 Highest Sales Month: <b>{highest_month}</b><br>
    💰 Max Sales: ₱{highest_value:,.2f}<br>
    📊 Average Monthly Sales: ₱{avg_sales:,.2f}<br>
    🗓️ Latest Month: <b>{latest_month}</b><br>
    🔸 Latest Actual Sales: ₱{latest_sales:,.2f}<br>
    🔹 3-Month MA: ₱{latest_3_ma:,.2f}<br>
    🟢 6-Month MA: ₱{latest_6_ma:,.2f}
    """

# ------------ Reports -----------
def get_text_report_for_month(year: int, month: int):
    con = duckdb.connect('backend/db_timestock')

    query = f"""
        SELECT 
            DATE_TRUNC('day', ot.date_created) AS period,
            COUNT(DISTINCT ot.id) AS total_orders,
            SUM(oi.quantity) AS total_sales,
            SUM(ot.total_amount) AS total_revenue
        FROM order_transactions ot
        JOIN order_items oi ON ot.id = oi.order_id
        WHERE EXTRACT(YEAR FROM ot.date_created) = {year}
          AND EXTRACT(MONTH FROM ot.date_created) = {month}
        GROUP BY period
        ORDER BY period;
    """
    df = con.execute(query).fetchdf()

    if df.empty:
        return f"No records found for {year}-{month:02d}"

    # Monthly totals
    total_orders = int(df["total_orders"].sum())
    total_sales = int(df["total_sales"].sum())
    total_revenue = float(df["total_revenue"].sum())

    breakdown = []
    for _, row in df.iterrows():
        breakdown.append({
            "day": row['period'].strftime("%Y-%m-%d"),
            "orders": int(row['total_orders']),
            "sales": int(row['total_sales']),
            "revenue": float(row['total_revenue'])
        })

    return {
        "empty": False,
        "title": f"Report for {pd.Timestamp(year=year, month=month, day=1).strftime('%B %Y')}",
        "total_orders": total_orders,
        "total_sales": total_sales,
        "total_revenue": total_revenue,
        "breakdown": breakdown
    }

def get_turnover_text_report_for_month(year: int, month: int):
    con = duckdb.connect('backend/db_timestock')

    query = f"""
        WITH monthly_data AS (
            SELECT
                DATE_TRUNC('month', st.date_created) AS period,
                SUM(CASE WHEN stt.type_code = 'stock-in' THEN sti.quantity * m.material_cost ELSE 0 END) AS stock_in_value,
                SUM(CASE WHEN stt.type_code = 'stock-out' THEN sti.quantity * m.material_cost ELSE 0 END) AS cogs,
                SUM(m.current_stock * m.material_cost) AS ending_inventory_value
            FROM stock_transaction_items sti
            JOIN stock_transactions st ON st.id = sti.stock_transaction_id
            JOIN stock_transaction_types stt ON stt.id = st.stock_type_id
            JOIN materials m ON m.id = sti.material_id
            WHERE EXTRACT(YEAR FROM st.date_created) = {year}
              AND EXTRACT(MONTH FROM st.date_created) = {month}
            GROUP BY period
        ),
        turnover_calc AS (
            SELECT
                STRFTIME(period, '%Y-%m') AS label,
                cogs,
                ending_inventory_value,
                ROUND(
                    (ending_inventory_value + (cogs + stock_in_value - ending_inventory_value)) / 2.0, 2
                ) AS avg_inventory,
                ROUND(
                    CASE
                        WHEN ((ending_inventory_value + (cogs + stock_in_value - ending_inventory_value)) / 2.0) > 0
                        THEN cogs / ((ending_inventory_value + (cogs + stock_in_value - ending_inventory_value)) / 2.0)
                        ELSE 0
                    END, 2
                ) AS turnover_rate
            FROM monthly_data
        )
        SELECT label, cogs, avg_inventory, turnover_rate
        FROM turnover_calc
        ORDER BY label;
    """
    df = con.execute(query).fetchdf()

    if df.empty:
        return {"empty": True, "message": f"No turnover records found for {year}-{month:02d}"}

    row = df.iloc[0]  # only one row since it's month-level
    cogs = float(row["cogs"])
    avg_inventory = float(row["avg_inventory"])
    turnover_rate = float(row["turnover_rate"])

    interpretation = (
        f"Inventory turned over about {turnover_rate:.2f} times in {year}-{month:02d}."
        if turnover_rate > 0 else
        "No turnover occurred this month."
    )

    return {
        "empty": False,
        "title": f"Inventory Turnover Report for {pd.Timestamp(year=year, month=month, day=1).strftime('%B %Y')}",
        "cogs": cogs,
        "avg_inventory": avg_inventory,
        "turnover_rate": turnover_rate,
        "interpretation": interpretation
    }

def get_stl_text_report_for_month(year: int, month: int):
    con = duckdb.connect('backend/db_timestock')

    # Monthly order quantity
    query = """
    SELECT 
        DATE_TRUNC('month', ot.date_created) AS order_month,
        SUM(oi.quantity) AS total_quantity
    FROM order_transactions ot
    JOIN order_items oi ON ot.id = oi.order_id
    GROUP BY order_month
    ORDER BY order_month
    """
    df = con.execute(query).fetchdf()
    if df.empty:
        return {"empty": True, "message": f"No STL data found for {year}-{month:02d}"}

    df['order_month'] = pd.to_datetime(df['order_month'])
    df.set_index('order_month', inplace=True)
    df = df.asfreq('MS')
    df['total_quantity'] = df['total_quantity'].fillna(0)

    # Run STL decomposition
    stl = STL(df['total_quantity'], period=12)
    result = stl.fit()

    # Top-selling product for that month
    top_products_df = con.execute("""
        SELECT month, product_name FROM (
            SELECT 
                DATE_TRUNC('month', ot.date_created) AS month,
                i.item_name AS product_name,
                SUM(oi.quantity) AS total_qty,
                RANK() OVER (
                    PARTITION BY DATE_TRUNC('month', ot.date_created) 
                    ORDER BY SUM(oi.quantity) DESC
                ) AS rnk
            FROM order_transactions ot
            JOIN order_items oi ON ot.id = oi.order_id
            JOIN products p ON oi.product_id = p.id
            JOIN items i ON p.item_id = i.id
            GROUP BY month, product_name
        ) 
        WHERE rnk = 1
    """).fetchdf()

    if not top_products_df.empty:
        top_products_df['month'] = pd.to_datetime(top_products_df['month'])
        top_products_df.rename(columns={'month': 'order_month', 'product_name': 'top_product'}, inplace=True)

    # Extract just that month
    target_date = pd.Timestamp(year=year, month=month, day=1)
    if target_date not in df.index:
        return {"empty": True, "message": f"No STL data available for {year}-{month:02d}"}

    trend_val = float(result.trend.loc[target_date])
    seasonal_val = float(result.seasonal.loc[target_date])
    resid_val = float(result.resid.loc[target_date])

    top_product_row = (
        top_products_df[top_products_df['order_month'] == target_date]
        if not top_products_df.empty else None
    )
    top_product = top_product_row['top_product'].iloc[0] if top_product_row is not None and not top_product_row.empty else "N/A"

    # Interpretations
    if seasonal_val > 0:
        seasonal_text = "Seasonality boosted demand this month."
    elif seasonal_val < 0:
        seasonal_text = "Seasonality reduced demand this month."
    else:
        seasonal_text = "Neutral seasonality this month."

    if resid_val > 0:
        resid_text = "Residual suggests an unexpected demand spike."
    elif resid_val < 0:
        resid_text = "Residual suggests an unexpected drop in demand."
    else:
        resid_text = "Residual suggests stable demand."

    return {
        "empty": False,
        "title": f"STL Decomposition Report for {pd.Timestamp(year=year, month=month, day=1).strftime('%B %Y')}",
        "top_product": top_product,
        "trend": trend_val,
        "seasonal": seasonal_val,
        "residual": resid_val,
        "interpretations": [seasonal_text, resid_text]
    }

def get_sales_moving_average_text_report(year: int, month: int | None = None):
    with duckdb.connect('backend/db_timestock') as con:
        # --- get full dataset (no filtering here) ---
        df = con.execute("""
            SELECT
                DATE_TRUNC('month', ot.date_created) AS month,
                SUM(ot.total_amount) AS total_sales
            FROM order_transactions ot
            GROUP BY month
            ORDER BY month
        """).fetchdf()

        if df.empty:
            return {"empty": True, "message": "No moving average records found."}

        df['month'] = pd.to_datetime(df['month'])

        # --- top-selling product for each month ---
        top_products_df = con.execute("""
            SELECT month, product_name FROM (
                SELECT 
                    DATE_TRUNC('month', ot.date_created) AS month,
                    i.item_name AS product_name,
                    SUM(oi.quantity) AS total_qty,
                    RANK() OVER (
                        PARTITION BY DATE_TRUNC('month', ot.date_created) 
                        ORDER BY SUM(oi.quantity) DESC
                    ) AS rnk
                FROM order_transactions ot
                JOIN order_items oi ON ot.id = oi.order_id
                JOIN products p ON oi.product_id = p.id
                JOIN items i ON p.item_id = i.id
                GROUP BY month, product_name
            ) 
            WHERE rnk = 1
        """).fetchdf()

        if not top_products_df.empty:
            top_products_df['month'] = pd.to_datetime(top_products_df['month'])

        df = df.merge(top_products_df, on='month', how='left')
        df.rename(columns={'product_name': 'top_product'}, inplace=True)

        # --- moving averages ---
        df['3_MA'] = df['total_sales'].rolling(window=3).mean()
        df['6_MA'] = df['total_sales'].rolling(window=6).mean()

        # --- select target row ---
        if month is not None:
            target_date = pd.Timestamp(year=year, month=month, day=1)
            if target_date not in df['month'].values:
                return {"empty": True, "message": f"No moving average data for {year}-{month:02d}"}
            row = df[df['month'] == target_date].iloc[0]
        else:
            # last available month of that year
            year_df = df[df['month'].dt.year == year]
            if year_df.empty:
                return {"empty": True, "message": f"No moving average records found for {year}"}
            row = year_df.iloc[-1]

        total_sales = float(row['total_sales'])
        top_product = row['top_product'] if pd.notna(row['top_product']) else "No sales"
        ma3 = float(row['3_MA']) if pd.notna(row['3_MA']) else None
        ma6 = float(row['6_MA']) if pd.notna(row['6_MA']) else None

        # --- return structured data ---
        return {
            "empty": False,
            "title": f"Moving Average Report for {row['month'].strftime('%B %Y')}",
            "total_sales": total_sales,
            "top_product": top_product,
            "ma3": ma3,
            "ma6": ma6
        }
