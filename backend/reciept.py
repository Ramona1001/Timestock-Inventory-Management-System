from reportlab.lib.pagesizes import mm,letter, A4
from reportlab.pdfgen import canvas
from reportlab.lib import colors
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer, HRFlowable
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from datetime import datetime
import calendar
from xml.sax.saxutils import escape
import time, os


def cleanup_old_pdfs(directory, max_age_minutes=10):
    now = time.time()
    max_age_seconds = max_age_minutes * 60

    for filename in os.listdir(directory):
        file_path = os.path.join(directory, filename)
        if filename.endswith(".pdf") and os.path.isfile(file_path):
            file_age = now - os.path.getmtime(file_path)
            if file_age > max_age_seconds:
                try:
                    os.remove(file_path)
                except Exception as e:
                    print(f"Failed to remove old PDF: {file_path}, error: {e}")

def format_currency(value):
    return f"Php{value:,.2f}"
#Receipt
def estimate_height(num_items):
    # Base height: header + customer info + payment summary + footer
    base_height = 100  # mm — fits small receipts
    item_row_height = 6  # mm per item row
    return (base_height + num_items * item_row_height) * mm

def generate_unofficial_receipt(
    filename, company_name, customer_name, address, phone,
    items, down_payment
):
    receipt_width = 80 * mm
    receipt_height = estimate_height(len(items))

    doc = SimpleDocTemplate(filename, pagesize=(receipt_width, receipt_height),
                            rightMargin=5, leftMargin=5, topMargin=5, bottomMargin=5)

    styles = getSampleStyleSheet()
    small = ParagraphStyle(name="Small", fontSize=7.3, leading=8.5)
    bold = ParagraphStyle(name="Bold", parent=small, fontName="Helvetica-Bold")
    center = ParagraphStyle(name="Center", parent=small, alignment=1)
    center_bold = ParagraphStyle(name="CenterBold", parent=bold, alignment=1)

    elements = []

    # Header
    elements.append(Paragraph(company_name.upper(), center_bold))
    elements.append(Paragraph("UNOFFICIAL RECEIPT", center))
    elements.append(HRFlowable(width="100%", color=colors.black, thickness=0.7))
    elements.append(Spacer(1, 3))

    # Customer Info
    elements.append(Paragraph(f"<b>Customer:</b> {escape(customer_name)}", small))
    elements.append(Paragraph(f"<b>Address:</b> {escape(address)}", small))
    elements.append(Paragraph(f"<b>Phone:</b> {phone}", small))
    elements.append(Paragraph(f"<b>Date:</b> {datetime.now().strftime('%Y-%m-%d %H:%M')}", small))
    elements.append(Spacer(1, 4))
    elements.append(HRFlowable(width="100%", color=colors.black, thickness=0.5))
    elements.append(Spacer(1, 3))

    # Items Table
    data = [["UnitID", "Product", "Qty", "Total"]]
    total_price = 0

    for item in items:
        unit_id = item["unit_id"]
        name = escape(item["name"])
        qty = item["quantity"]
        total = item["unit_price"] * qty
        total_price += total
        data.append([unit_id, Paragraph(name, small), str(qty), format_currency(total)])

    table = Table(data, colWidths=[40, 90, 25, 50])
    table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), colors.whitesmoke),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.black),
        ('GRID', (0, 0), (-1, -1), 0.2, colors.grey),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, -1), 6.8),
        ('ALIGN', (2, 1), (2, -1), 'CENTER'),
        ('ALIGN', (3, 1), (3, -1), 'RIGHT'),
        ('VALIGN', (0, 0), (-1, -1), 'TOP'),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 2),
        ('TOPPADDING', (0, 0), (-1, -1), 2),
    ]))
    elements.append(table)
    elements.append(Spacer(1, 6))

    # Payment Summary
    remaining = total_price - down_payment
    summary_table = Table([
        ["Subtotal", format_currency(total_price)],
        ["Down Payment", format_currency(down_payment)],
        ["Remaining Balance", format_currency(remaining)]
    ], colWidths=[75, 60])

    summary_table.setStyle(TableStyle([
        ('FONTNAME', (0, 0), (-1, -1), 'Helvetica'),
        ('FONTNAME', (0, -1), (-1, -1), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, -1), 7.2),
        ('ALIGN', (1, 0), (-1, -1), 'RIGHT'),
        ('LINEABOVE', (0, -1), (-1, -1), 0.5, colors.black),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 2),
        ('TOPPADDING', (0, 0), (-1, -1), 2),
    ]))
    elements.append(summary_table)
    elements.append(Spacer(1, 5))
    elements.append(HRFlowable(width="100%", color=colors.black, thickness=0.5))
    elements.append(Spacer(1, 2))

    # Footer
    elements.append(Paragraph("This document is not valid for claiming input tax.", center))
    elements.append(Paragraph("Thank you for your business!", center_bold))

    doc.build(elements)
    print(f"✅ Receipt saved to: {filename}")

#Quote

def generate_modern_quotation_pdf(filename, client_name, client_address, items_quote):
    styles = getSampleStyleSheet()
    normal = styles['Normal']
    bold = ParagraphStyle(name="Bold", parent=normal, fontName="Helvetica-Bold", fontSize=10)
    title = ParagraphStyle(name="Title", fontName="Helvetica-Bold", fontSize=18, alignment=1, textColor=colors.HexColor("#1F3B4D"))
    company = ParagraphStyle(name="Company", fontName="Helvetica-Bold", fontSize=14, alignment=1, textColor=colors.HexColor("#005691"))
    small = ParagraphStyle(name="Small", fontSize=9, fontName="Helvetica")
    label = ParagraphStyle(name="Label", fontName="Helvetica-Bold", fontSize=9, textColor=colors.HexColor("#1F3B4D"))
    section_title = ParagraphStyle(name="SectionTitle", fontName="Helvetica-Bold", fontSize=11, textColor=colors.HexColor("#003e74"))

    elements = []

    # Company Header
    elements.append(Paragraph("Times Stock Aluminum & Glass Services", company))
    elements.append(Spacer(1, 6))
    elements.append(Paragraph("QUOTATION", title))
    elements.append(Spacer(1, 6))
    elements.append(HRFlowable(width="100%", color=colors.HexColor("#005691"), thickness=1))
    elements.append(Spacer(1, 10))

    # Client Info
    elements.append(Paragraph(f"<b>Date:</b> {datetime.now().strftime('%B %d, %Y')}", small))
    elements.append(Paragraph(f"<b>Client:</b> {client_name}", small))
    elements.append(Paragraph(f"<b>Address:</b> {client_address}", small))
    elements.append(Spacer(1, 10))

    # Subject and Intro Text
    elements.append(Paragraph("SUBJECT: SUPPLY & INSTALLATION OF Fixed panel with sliding door, Frame glass door, Fixed with frame glassdoor", section_title))
    elements.append(Spacer(1, 6))
    elements.append(Paragraph("Dear Ma’am/Sir,", small))
    elements.append(Spacer(1, 4))
    elements.append(Paragraph("In response to your request, we are pleased to quote you our best price offer for your perusal.", small))
    elements.append(Spacer(1, 12))

    # Pricing Table
    elements.append(Paragraph("PRICING", section_title))
    elements.append(Spacer(1, 4))

    table_data = [["#", "DESCRIPTION", "QTY", "UNIT PRICE", "TOTAL"]]
    total = 0

    for i, item in enumerate(items_quote, 1):
        line_total = item['quantity'] * item['unit_price']
        total += line_total
        table_data.append([
            str(i),
            Paragraph(item["description"], small),
            str(item["quantity"]),
            format_currency(item["unit_price"]),
            format_currency(line_total)
        ])

    table = Table(table_data, colWidths=[20, 250, 40, 80, 80])
    table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor("#d6eaff")),  # Header row
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.HexColor("#003e74")),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, -1), 9),
        ('ALIGN', (2, 1), (-1, -1), 'CENTER'),
        ('VALIGN', (0, 0), (-1, -1), 'TOP'),
        ('GRID', (0, 0), (-1, -1), 0.4, colors.grey),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 5),
        ('TOPPADDING', (0, 0), (-1, -1), 5),
    ]))
    elements.append(table)

    # Total
    elements.append(Spacer(1, 10))
    elements.append(Paragraph(f"<b>Grand Total:</b> {format_currency(total)}", bold))
    elements.append(Spacer(1, 14))

    # Materials
    elements.append(Paragraph("MATERIALS TO BE USED", section_title))
    elements.append(Spacer(1, 4))
    for item in items_quote:
        elements.append(Paragraph(f"<b>{item['short_label']}</b>", label))
        for material in item['materials']:
            elements.append(Paragraph(f"• {material}", small))
        elements.append(Spacer(1, 4))

    # Scope of Work
    elements.append(Spacer(1, 12))
    elements.append(Paragraph("SCOPE OF WORKS", section_title))
    elements.append(Paragraph("1. Supply & Installation", small))

    # Payment Terms
    elements.append(Spacer(1, 10))
    elements.append(Paragraph("TERMS OF PAYMENT", section_title))
    elements.append(Paragraph("• 50% down payment is required to start the service.", small))
    elements.append(Paragraph("• Final payment upon delivery and completion.", small))

    # Warranty
    elements.append(Spacer(1, 10))
    elements.append(Paragraph("WARRANTY", section_title))
    elements.append(Paragraph("• 3 months for hardware & accessories", small))
    elements.append(Paragraph("• 6 months for resealant & alignment", small))

    # Lead Time
    elements.append(Spacer(1, 10))
    elements.append(Paragraph("LEAD TIME", section_title))
    elements.append(Paragraph("• 10 working days", small))

    # Footer
    elements.append(Spacer(1, 20))
    elements.append(HRFlowable(width="100%", color=colors.grey, thickness=0.5))
    elements.append(Spacer(1, 12))
    elements.append(Paragraph("Thank you for giving us the opportunity to quote. Should you have any questions or suggestions, please let us know.", small))
    elements.append(Spacer(1, 24))
    elements.append(Paragraph("Sincerely,", small))
    elements.append(Spacer(1, 20))
    elements.append(Paragraph("Emma Nuelle J. Mendoza", bold))
    elements.append(Paragraph("General Manager", small))

    # Build the PDF
    doc = SimpleDocTemplate(
        filename, pagesize=letter,
        rightMargin=30, leftMargin=30, topMargin=30, bottomMargin=30
    )
    doc.build(elements)
    print(f"✅ Quotation PDF generated: {filename}")


#----------- Reports ----------

def generate_report_pdf(report_text, turnover_report, stl_report, moving_avg_report, stock_movement_report,products_sold_report, year, month=None):
    # File setup
    month_name = calendar.month_name[month] if month else "ALL"
    filename = f"report_{month_name}_{year}.pdf"
    filepath = os.path.join("reports", filename)
    os.makedirs("reports", exist_ok=True)

    doc = SimpleDocTemplate(filepath, pagesize=A4)
    story = []
    styles = getSampleStyleSheet()
    styles.add(ParagraphStyle(name="SectionTitle", fontSize=14, leading=16, spaceAfter=10, textColor=colors.darkblue))

    # Title
    story.append(Paragraph(f"Business Report - {month_name} {year}", styles["Title"]))
    story.append(Paragraph(f"Generated on {datetime.now().strftime('%B %d, %Y %H:%M:%S')}", styles["Normal"]))
    story.append(Spacer(1, 20))

    # Sales Report
    if report_text and not report_text.get("empty", False):
        story.append(Paragraph(report_text["title"], styles["SectionTitle"]))
        story.append(Paragraph(f"<b>Total Orders:</b> {report_text['total_orders']}", styles["Normal"]))
        story.append(Paragraph(f"<b>Total Sales:</b> {report_text['total_sales']}", styles["Normal"]))
        story.append(Paragraph(f"<b>Total Revenue:</b> Php{report_text['total_revenue']:,.2f}", styles["Normal"]))
        story.append(Spacer(1, 10))

        # Daily breakdown table
        data = [["Date", "Orders", "Sales", "Revenue"]]
        for row in report_text["breakdown"]:
            data.append([
                row["day"],
                row["orders"],
                row["sales"],
                f"Php{row['revenue']:,.2f}"
            ])
        table = Table(data, colWidths=[100, 80, 80, 100])
        table.setStyle(TableStyle([
            ("BACKGROUND", (0, 0), (-1, 0), colors.grey),
            ("TEXTCOLOR", (0, 0), (-1, 0), colors.whitesmoke),
            ("GRID", (0, 0), (-1, -1), 1, colors.black),
        ]))
        story.append(table)
        story.append(Spacer(1, 20))

    # Turnover Report
    if turnover_report and not turnover_report.get("empty", False):
        story.append(Paragraph(turnover_report["title"], styles["SectionTitle"]))
        story.append(Paragraph(f"<b>COGS:</b> Php{turnover_report['cogs']:,.2f}", styles["Normal"]))
        story.append(Paragraph(f"<b>Average Inventory:</b> Php{turnover_report['avg_inventory']:,.2f}", styles["Normal"]))
        story.append(Paragraph(f"<b>Turnover Rate:</b> {turnover_report['turnover_rate']:.2f} times", styles["Normal"]))
        story.append(Paragraph(f"<i>{turnover_report['interpretation']}</i>", styles["Normal"]))
        story.append(Spacer(1, 20))

    # STL Report
    if stl_report and not stl_report.get("empty", False):
        story.append(Paragraph(stl_report["title"], styles["SectionTitle"]))
        story.append(Paragraph(f"<b>Top-Ordered Product:</b> {stl_report['top_product']}", styles["Normal"]))
        story.append(Paragraph(f"<b>Trend Component:</b> {stl_report['trend']:.2f}", styles["Normal"]))
        story.append(Paragraph(f"<b>Seasonal Component:</b> {stl_report['seasonal']:.2f}", styles["Normal"]))
        story.append(Paragraph(f"<b>Residual Component:</b> {stl_report['residual']:.2f}", styles["Normal"]))
        story.append(Spacer(1, 5))

        if "interpretations" in stl_report:
            story.append(Paragraph("<b>Interpretation:</b>", styles["Normal"]))
            for interp in stl_report["interpretations"]:
                story.append(Paragraph(f"- {interp}", styles["Normal"]))
        story.append(Spacer(1, 20))

    # Moving Average Report
    if moving_avg_report and not moving_avg_report.get("empty", False):
        story.append(Paragraph(moving_avg_report["title"], styles["SectionTitle"]))
        story.append(Paragraph(f"<b>Total Sales:</b> Php{moving_avg_report['total_sales']:,.2f}", styles["Normal"]))
        story.append(Paragraph(f"<b>Top-Selling Product:</b> {moving_avg_report['top_product']}", styles["Normal"]))

        if moving_avg_report["ma3"] is not None:
            story.append(Paragraph(f"<b>3-Month Moving Average:</b> Php{moving_avg_report['ma3']:,.2f}", styles["Normal"]))
        else:
            story.append(Paragraph("<b>3-Month Moving Average:</b> Not enough data", styles["Normal"]))

        if moving_avg_report["ma6"] is not None:
            story.append(Paragraph(f"<b>6-Month Moving Average:</b> Php{moving_avg_report['ma6']:,.2f}", styles["Normal"]))
        else:
            story.append(Paragraph("<b>6-Month Moving Average:</b> Not enough data", styles["Normal"]))

        # --- Stock Movement Report (NEW) ---
    if stock_movement_report and not stock_movement_report.get("empty", False):
        story.append(Paragraph(stock_movement_report["title"], styles["SectionTitle"]))
        story.append(Paragraph(f"<b>Total Stock-In Events:</b> {stock_movement_report['total_stock_in_events']}", styles["Normal"]))
        story.append(Paragraph(f"<b>Total Stock-Out Events:</b> {stock_movement_report['total_stock_out_events']}", styles["Normal"]))
        story.append(Spacer(1, 10))

        # Material breakdown table
        data = [["Material", "Stock-In Events", "Stock-Out Events"]]
        for row in stock_movement_report["breakdown"]:
            data.append([
                row["material_name"],
                row["stock_in_events"],
                row["stock_out_events"]
            ])
        table = Table(data, colWidths=[200, 100, 100])
        table.setStyle(TableStyle([
            ("BACKGROUND", (0, 0), (-1, 0), colors.grey),
            ("TEXTCOLOR", (0, 0), (-1, 0), colors.whitesmoke),
            ("GRID", (0, 0), (-1, -1), 1, colors.black),
        ]))
        story.append(table)
        story.append(Spacer(1, 20))
    elif stock_movement_report and stock_movement_report.get("empty", False):
        story.append(Paragraph(stock_movement_report["message"], styles["Normal"]))
        story.append(Spacer(1, 20))

    # --- Products Sold Report ---
    if products_sold_report and not products_sold_report.get("empty", False):
        story.append(Paragraph(products_sold_report["title"], styles["SectionTitle"]))
        story.append(Paragraph(f"<b>Total Quantity Sold (All Products):</b> {products_sold_report['total_quantity_all']}", styles["Normal"]))
        story.append(Paragraph(f"<b>Total Sales (All Products):</b> Php{products_sold_report['total_sales_all']:,.2f}", styles["Normal"]))
        story.append(Spacer(1, 10))

        # Product breakdown table
        data = [["Product", "Quantity Sold", "Total Sales"]]
        for row in products_sold_report["breakdown"]:
            data.append([
                row["product_name"],
                row["total_quantity"],
                f"Php{row['total_sales']:,.2f}"
            ])
        table = Table(data, colWidths=[200, 100, 100])
        table.setStyle(TableStyle([
            ("BACKGROUND", (0, 0), (-1, 0), colors.grey),
            ("TEXTCOLOR", (0, 0), (-1, 0), colors.whitesmoke),
            ("GRID", (0, 0), (-1, -1), 1, colors.black),
        ]))
        story.append(table)
        story.append(Spacer(1, 20))
    elif products_sold_report and products_sold_report.get("empty", False):
        story.append(Paragraph(products_sold_report["message"], styles["Normal"]))
        story.append(Spacer(1, 20))


    # Build PDF
    doc.build(story)
    return filepath
