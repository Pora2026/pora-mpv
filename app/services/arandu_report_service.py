"""Informes verificables de Arandú en PDF y Excel."""

from io import BytesIO

from app.services.arandu_alert_service import get_period_alerts
from app.services.arandu_finance_service import (
    get_expense_breakdown,
    get_financial_period_summary,
)


def build_excel_report(start_date, end_date):
    import openpyxl
    from openpyxl.styles import Font

    summary = get_financial_period_summary(start_date, end_date)
    expenses = get_expense_breakdown(start_date, end_date, period_summary=summary)
    alerts = get_period_alerts(start_date, end_date)
    workbook = openpyxl.Workbook()
    ws = workbook.active
    ws.title = "Resumen"
    rows = [
        ("Informe CFO Arandú", None),
        ("Desde", start_date.isoformat()),
        ("Hasta", end_date.isoformat()),
        ("Ventas brutas", summary["calculated"]["gross_sales"]),
        ("Gastos pagados", summary["calculated"]["paid_expenses"]),
        ("Retención apps estimada", summary["calculated"]["apps_retention_estimate"]),
        ("Ganancia calculada", summary["calculated"]["profit"]),
        ("Ingresos cobrados", summary["liquid"]["collected_income"]),
        ("Ganancia líquida", summary["liquid"]["profit"]),
        ("Saldo real acumulado", summary["real_accumulated"]["balance"]),
        ("Saldo real al", summary["real_accumulated"]["as_of"]),
        ("Fondos reservados", summary["reserved_funds"]["balance"]),
    ]
    for row in rows:
        ws.append(row)
    ws["A1"].font = Font(bold=True, size=14)
    ws.column_dimensions["A"].width = 30
    ws.column_dimensions["B"].width = 20

    expense_ws = workbook.create_sheet("Gastos")
    expense_ws.append(("Tipo", "Categoría", "Importe", "Registros"))
    for item in expenses["items"]:
        expense_ws.append((item["kind"], item["category"], item["amount"], item["entries"]))
    for cell in expense_ws[1]:
        cell.font = Font(bold=True)
    expense_ws.column_dimensions["B"].width = 32

    alert_ws = workbook.create_sheet("Advertencias")
    alert_ws.append(("Severidad", "Título", "Detalle", "Código"))
    for item in alerts["alerts"]:
        alert_ws.append((item["severity"], item["title"], item["detail"], item["code"]))
    for cell in alert_ws[1]:
        cell.font = Font(bold=True)
    alert_ws.column_dimensions["B"].width = 35
    alert_ws.column_dimensions["C"].width = 80

    output = BytesIO()
    workbook.save(output)
    output.seek(0)
    return output


def build_pdf_report(start_date, end_date):
    from reportlab.lib import colors
    from reportlab.lib.pagesizes import A4
    from reportlab.lib.styles import getSampleStyleSheet
    from reportlab.lib.units import mm
    from reportlab.platypus import Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle

    summary = get_financial_period_summary(start_date, end_date)
    expenses = get_expense_breakdown(start_date, end_date, period_summary=summary)
    alerts = get_period_alerts(start_date, end_date)
    output = BytesIO()
    document = SimpleDocTemplate(
        output, pagesize=A4, rightMargin=18 * mm, leftMargin=18 * mm,
        topMargin=16 * mm, bottomMargin=16 * mm,
    )
    styles = getSampleStyleSheet()
    story = [
        Paragraph("Informe CFO Arandú", styles["Title"]),
        Paragraph(f"Período: {start_date.isoformat()} al {end_date.isoformat()}", styles["Normal"]),
        Spacer(1, 8),
    ]
    data = [
        ["Indicador", "Valor"],
        ["Ventas brutas", f"$ {summary['calculated']['gross_sales']:,.0f}"],
        ["Gastos pagados", f"$ {summary['calculated']['paid_expenses']:,.0f}"],
        ["Ganancia calculada", f"$ {summary['calculated']['profit']:,.0f}"],
        ["Ganancia líquida", f"$ {summary['liquid']['profit']:,.0f}"],
        ["Saldo real acumulado", "—" if summary["real_accumulated"]["balance"] is None else f"$ {summary['real_accumulated']['balance']:,.0f}"],
        ["Fondos reservados", f"$ {summary['reserved_funds']['balance']:,.0f}"],
    ]
    table = Table(data, colWidths=[95 * mm, 65 * mm])
    table.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#1f2937")),
        ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
        ("GRID", (0, 0), (-1, -1), 0.5, colors.HexColor("#d1d5db")),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("ALIGN", (1, 1), (1, -1), "RIGHT"),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
    ]))
    story.extend([table, Spacer(1, 12), Paragraph("Advertencias", styles["Heading2"])])
    for item in alerts["alerts"]:
        story.append(Paragraph(f"<b>{item['severity'].upper()} — {item['title']}</b>: {item['detail']}", styles["BodyText"]))
        story.append(Spacer(1, 4))
    story.extend([Spacer(1, 8), Paragraph("Principales gastos", styles["Heading2"])])
    for item in expenses["items"][:15]:
        story.append(Paragraph(f"{item['category']} ({item['kind']}): $ {item['amount']:,.0f}", styles["BodyText"]))
    story.append(Spacer(1, 12))
    story.append(Paragraph("Informe determinístico de apoyo gerencial. No constituye una decisión autónoma ni reemplaza la documentación contable.", styles["Italic"]))
    document.build(story)
    output.seek(0)
    return output
