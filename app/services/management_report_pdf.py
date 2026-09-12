from io import BytesIO
from math import cos, pi, sin

from reportlab.lib import colors
from reportlab.lib.pagesizes import A3, landscape
from reportlab.lib.styles import ParagraphStyle
from reportlab.lib.units import mm
from reportlab.pdfbase.pdfmetrics import stringWidth
from reportlab.pdfgen import canvas
from reportlab.platypus import Paragraph

from app.utils.money import ars


NAVY = colors.HexColor("#123F79")
BLUE = colors.HexColor("#1769C2")
LIGHT_BLUE = colors.HexColor("#EAF4FF")
GREEN = colors.HexColor("#159447")
DARK_GREEN = colors.HexColor("#076B32")
LIGHT_GREEN = colors.HexColor("#E9F8F0")
RED = colors.HexColor("#E32626")
DARK_RED = colors.HexColor("#B50F16")
LIGHT_RED = colors.HexColor("#FFF0F0")
GOLD = colors.HexColor("#B57405")
LIGHT_GOLD = colors.HexColor("#FFF7E4")
PURPLE = colors.HexColor("#7A2DC1")
LIGHT_PURPLE = colors.HexColor("#F5EEFF")
INK = colors.HexColor("#14213D")
MUTED = colors.HexColor("#61718A")
BORDER = colors.HexColor("#D9E4F2")
PANEL = colors.HexColor("#F8FBFF")
WHITE = colors.white

PIE_COLORS = [
    colors.HexColor("#F04444"),
    colors.HexColor("#F38A28"),
    colors.HexColor("#F2CA18"),
    colors.HexColor("#28A463"),
    colors.HexColor("#2695DF"),
    colors.HexColor("#8151D8"),
]


def _fmt_pct(value, signed=False, suffix="%"):
    if value is None:
        return "-"
    sign = "+" if signed and value > 0 else ""
    return f"{sign}{value:.1f}{suffix}".replace(".", ",")


def _money(value):
    return "-" if value is None else ars(value)


def _rounded(c, x, y, w, h, fill, stroke=BORDER, radius=8, width=0.6):
    c.setFillColor(fill)
    c.setStrokeColor(stroke)
    c.setLineWidth(width)
    c.roundRect(x, y, w, h, radius, fill=1, stroke=1)


def _text(c, x, y, text, size=9, color=INK, bold=False, align="left"):
    font = "Helvetica-Bold" if bold else "Helvetica"
    c.setFont(font, size)
    c.setFillColor(color)
    if align == "right":
        c.drawRightString(x, y, str(text))
    elif align == "center":
        c.drawCentredString(x, y, str(text))
    else:
        c.drawString(x, y, str(text))


def _paragraph(c, x, y_top, w, text, size=8, leading=10, color=INK, bold=False):
    style = ParagraphStyle(
        "p",
        fontName="Helvetica-Bold" if bold else "Helvetica",
        fontSize=size,
        leading=leading,
        textColor=color,
        spaceAfter=0,
        spaceBefore=0,
    )
    p = Paragraph(text, style)
    _, h = p.wrap(w, 1000)
    p.drawOn(c, x, y_top - h)
    return h


def _money_compact(value):
    if value is None:
        return "-"
    value = float(value)
    abs_value = abs(value)
    sign = "-" if value < 0 else ""
    if abs_value >= 1_000_000:
        return f"{sign}${abs_value/1_000_000:.1f}M".replace(".", ",")
    if abs_value >= 1_000:
        return f"{sign}${abs_value/1_000:.0f}K"
    return f"{sign}${abs_value:.0f}"


def _draw_header(c, W, H, data):
    x0, y0, h = 8*mm, H - 35*mm, 28*mm
    w = W - 16*mm
    # degradé azul
    strips = 80
    for i in range(strips):
        t = i / (strips - 1)
        r = int(18 + (43 - 18) * t)
        g = int(63 + (91 - 63) * t)
        b = int(121 + (127 - 121) * t)
        c.setFillColor(colors.Color(r/255, g/255, b/255))
        c.rect(x0 + w*i/strips, y0, w/strips + 1, h, fill=1, stroke=0)
    c.setStrokeColor(NAVY)
    c.roundRect(x0, y0, w, h, 8, fill=0, stroke=1)

    _text(c, x0+8*mm, y0+17*mm, "PORÁ", 27, WHITE, True)
    _text(c, x0+8*mm, y0+10*mm, "CHIPACERÍA ARTESANAL", 9.2, WHITE, False)
    c.setStrokeColor(colors.HexColor("#DCE9F7"))
    c.setLineWidth(0.8)
    c.line(x0+57*mm, y0+5*mm, x0+57*mm, y0+23*mm)

    _text(c, x0+65*mm, y0+20*mm, "Informe Gerencial", 16, WHITE, True)
    _text(c, x0+65*mm, y0+12.7*mm, data["period_label"], 21, WHITE, True)
    _text(c, x0+65*mm, y0+7.2*mm, "Resultados, liquidez y gestión del negocio", 9.0, WHITE)

    # Separador visual sobrio: mantiene el encabezado corporativo sin ilustraciones.
    c.setStrokeColor(colors.HexColor("#6FA7DA"))
    c.setLineWidth(1.2)
    c.line(W-112*mm, y0+7*mm, W-112*mm, y0+21*mm)

    # tarjeta período
    pw, ph = 47*mm, 21*mm
    px, py = W-58*mm, y0+3.5*mm
    _rounded(c, px, py, pw, ph, colors.HexColor("#F6FAFF"), colors.HexColor("#D7E4F5"), 7)
    _text(c, px+5*mm, py+15.5*mm, "Período del informe", 6.5, NAVY)
    _text(c, px+5*mm, py+10*mm, data["start"].strftime("%d/%m/%Y"), 8.5, NAVY, True)
    _text(c, px+5*mm, py+6.3*mm, "al " + data["end"].strftime("%d/%m/%Y"), 8.5, NAVY, True)
    _text(c, px+5*mm, py+2.8*mm, f"{data['days_loaded']} días cargados", 6.5, NAVY)




def _draw_icon(c, x, y, size, kind, color):
    """Iconos vectoriales simples: sin fuentes externas ni imágenes embebidas."""
    c.saveState()
    c.setStrokeColor(color)
    c.setFillColor(color)
    c.setLineWidth(max(0.8, size * 0.07))
    if kind == "income":
        bw = size * 0.16
        for i, hh in enumerate((0.34, 0.56, 0.82)):
            c.roundRect(x + i*size*0.24, y, bw, size*hh, size*0.04, fill=1, stroke=0)
    elif kind == "liquid":
        c.roundRect(x, y+size*0.12, size*0.78, size*0.58, size*0.08, fill=0, stroke=1)
        c.line(x+size*0.08, y+size*0.49, x+size*0.70, y+size*0.49)
        c.circle(x+size*0.16, y+size*0.27, size*0.035, fill=1, stroke=0)
    elif kind == "expense":
        for dx, dy in ((0.12,0.15),(0.42,0.23),(0.27,0.47)):
            c.ellipse(x+size*dx, y+size*dy, x+size*(dx+0.38), y+size*(dy+0.18), fill=0, stroke=1)
    elif kind == "result":
        pts=[(0.06,0.18),(0.30,0.44),(0.50,0.32),(0.78,0.72)]
        for (ax,ay),(bx,by) in zip(pts,pts[1:]):
            c.line(x+size*ax,y+size*ay,x+size*bx,y+size*by)
        c.line(x+size*0.64,y+size*0.72,x+size*0.78,y+size*0.72)
        c.line(x+size*0.78,y+size*0.72,x+size*0.78,y+size*0.57)
    elif kind == "margin":
        c.circle(x+size*0.23,y+size*0.62,size*0.11,fill=0,stroke=1)
        c.circle(x+size*0.62,y+size*0.22,size*0.11,fill=0,stroke=1)
        c.line(x+size*0.16,y+size*0.13,x+size*0.69,y+size*0.72)
    elif kind == "cash":
        c.roundRect(x+size*0.06,y+size*0.15,size*0.70,size*0.52,size*0.08,fill=0,stroke=1)
        c.circle(x+size*0.61,y+size*0.40,size*0.045,fill=1,stroke=0)
        c.line(x+size*0.16,y+size*0.67,x+size*0.30,y+size*0.79)
    elif kind == "reserve":
        c.roundRect(x+size*0.16,y+size*0.10,size*0.52,size*0.45,size*0.06,fill=0,stroke=1)
        c.arc(x+size*0.25,y+size*0.38,x+size*0.59,y+size*0.78,0,180)
        c.circle(x+size*0.42,y+size*0.31,size*0.035,fill=1,stroke=0)
    elif kind == "stack":
        for off in (0.0,0.16,0.32):
            path=c.beginPath()
            path.moveTo(x+size*0.10,y+size*(0.22+off))
            path.lineTo(x+size*0.42,y+size*(0.08+off))
            path.lineTo(x+size*0.74,y+size*(0.22+off))
            path.lineTo(x+size*0.42,y+size*(0.36+off))
            path.close(); c.drawPath(path,fill=0,stroke=1)
    elif kind == "calendar":
        c.roundRect(x+size*0.08,y+size*0.10,size*0.66,size*0.62,size*0.05,fill=0,stroke=1)
        c.line(x+size*0.08,y+size*0.52,x+size*0.74,y+size*0.52)
        for xx in (0.25,0.55): c.line(x+size*xx,y+size*0.67,x+size*xx,y+size*0.79)
    elif kind == "alert":
        path=c.beginPath(); path.moveTo(x+size*0.42,y+size*0.78); path.lineTo(x+size*0.08,y+size*0.12); path.lineTo(x+size*0.76,y+size*0.12); path.close(); c.drawPath(path,fill=0,stroke=1)
        c.line(x+size*0.42,y+size*0.30,x+size*0.42,y+size*0.53); c.circle(x+size*0.42,y+size*0.21,size*0.025,fill=1,stroke=0)
    elif kind == "note":
        c.roundRect(x+size*0.12,y+size*0.10,size*0.55,size*0.68,size*0.04,fill=0,stroke=1)
        for yy in (0.29,0.43,0.57): c.line(x+size*0.23,y+size*yy,x+size*0.57,y+size*yy)
    elif kind == "pie":
        c.circle(x+size*0.40,y+size*0.40,size*0.30,fill=0,stroke=1); c.line(x+size*0.40,y+size*0.40,x+size*0.40,y+size*0.70); c.line(x+size*0.40,y+size*0.40,x+size*0.68,y+size*0.40)
    else:
        c.circle(x+size*0.4,y+size*0.4,size*0.25,fill=0,stroke=1)
    c.restoreState()


def _draw_kpi(c, x, y, w, h, title, value, change=None, compare_label=None,
              fill=LIGHT_BLUE, accent=BLUE, note=None, change_suffix="%", icon=None):

    _rounded(c, x, y, w, h, fill, colors.HexColor("#DFE9F5"), 8)
    if icon:
        _draw_icon(c, x+5*mm, y+h-15*mm, 9*mm, icon, accent)
        title_x = x+17*mm
    else:
        c.setFillColor(accent)
        c.roundRect(x+5*mm, y+h-13*mm, 6*mm, 6*mm, 2, fill=1, stroke=0)
        title_x = x+14*mm
    _text(c, title_x, y+h-8.3*mm, title, 8.2, accent, True)
    _text(c, x+w/2, y+h-18*mm, value, 15, accent, True, "center")
    if note:
        _text(c, x+w/2, y+4.8*mm, note, 7.0, MUTED, False, "center")
    elif change is not None:
        trend_color = GREEN if change >= 0 else RED
        _text(c, x+w/2, y+6.2*mm, _fmt_pct(change, signed=True, suffix=change_suffix), 8.0, trend_color, True, "center")
        if compare_label:
            _text(c, x+w/2, y+2.5*mm, f"vs. {compare_label}", 6.4, MUTED, False, "center")


def _draw_section_title(c, x, y, title, subtitle=None, accent=BLUE, icon="result"):
    _draw_icon(c, x, y-5, 10, icon, accent)
    _text(c, x+14, y, title, 9.8, NAVY, True)
    if subtitle:
        _text(c, x+14, y-10, subtitle, 7.0, MUTED)


def _draw_3d_bar(c, x, y0, w, h, value, max_value, color):
    if max_value <= 0:
        return
    sign = 1 if value >= 0 else -1
    bar_h = abs(value) / max_value * h
    if bar_h < 1.5 and abs(value) > 0:
        bar_h = 1.5
    depth = min(5, w * 0.22)
    front_y = y0 if sign >= 0 else y0 - bar_h
    c.setFillColor(color)
    c.setStrokeColor(colors.Color(max(color.red-.08,0), max(color.green-.08,0), max(color.blue-.08,0)))
    c.rect(x, front_y, w, bar_h, fill=1, stroke=1)
    # cara derecha
    darker = colors.Color(max(color.red-.14,0), max(color.green-.14,0), max(color.blue-.14,0))
    c.setFillColor(darker)
    path = c.beginPath()
    if sign >= 0:
        path.moveTo(x+w, y0)
        path.lineTo(x+w+depth, y0+depth)
        path.lineTo(x+w+depth, y0+bar_h+depth)
        path.lineTo(x+w, y0+bar_h)
    else:
        path.moveTo(x+w, y0)
        path.lineTo(x+w+depth, y0+depth)
        path.lineTo(x+w+depth, y0-bar_h+depth)
        path.lineTo(x+w, y0-bar_h)
    path.close()
    c.drawPath(path, fill=1, stroke=0)
    # cara superior
    lighter = colors.Color(min(color.red+.12,1), min(color.green+.12,1), min(color.blue+.12,1))
    c.setFillColor(lighter)
    path = c.beginPath()
    top_y = y0+bar_h if sign >= 0 else y0
    path.moveTo(x, top_y)
    path.lineTo(x+depth, top_y+depth)
    path.lineTo(x+w+depth, top_y+depth)
    path.lineTo(x+w, top_y)
    path.close()
    c.drawPath(path, fill=1, stroke=0)


def _draw_monthly_chart(c, x, y, w, h, history):
    _rounded(c, x, y, w, h, WHITE, BORDER, 8)
    _draw_section_title(c, x+6*mm, y+h-8*mm, "Evolución mensual", "Ingresos líquidos, gastos y resultado líquido", icon="income")
    if not history:
        _text(c, x+w/2, y+h/2, "Sin datos históricos suficientes", 8, MUTED, False, "center")
        return

    chart_x = x+12*mm
    chart_y = y+17*mm
    chart_w = w-21*mm
    chart_h = h-35*mm
    values = []
    for row in history:
        values += [abs(row["income"] or 0), abs(row["expense"] or 0), abs(row["profit"] or 0)]
    max_val = max(values or [1.0]) * 1.12
    baseline = chart_y + 7*mm
    pos_h = chart_h - 12*mm

    c.setStrokeColor(colors.HexColor("#DDE6F1"))
    c.setLineWidth(0.35)
    for i in range(5):
        gy = baseline + pos_h*i/4
        c.line(chart_x, gy, chart_x+chart_w, gy)
    c.setStrokeColor(colors.HexColor("#9FB1C8"))
    c.line(chart_x, baseline, chart_x+chart_w, baseline)

    group_w = chart_w / len(history)
    bar_w = min(10*mm, group_w/5)
    for idx, row in enumerate(history):
        gx = chart_x + idx*group_w + group_w*0.18
        series = [
            (row["income"] or 0, GREEN),
            (row["expense"] or 0, colors.HexColor("#ED4646")),
            (row["profit"] or 0, colors.HexColor("#2388DE")),
        ]
        for sidx, (value, color) in enumerate(series):
            bx = gx + sidx*(bar_w+4)
            _draw_3d_bar(c, bx, baseline, bar_w, pos_h*0.87, value, max_val, color)
            label_y = baseline + (abs(value)/max_val*pos_h*0.87) + 7 if value >= 0 else baseline - 12
            _text(c, bx+bar_w/2, label_y, _money_compact(value), 5.4, color, True, "center")
        _text(c, gx+1.5*(bar_w+4), chart_y, row["label"], 6, INK, False, "center")

    ly = y+5*mm
    legends = [("Ingresos líquidos", GREEN), ("Gastos", colors.HexColor("#ED4646")), ("Resultado líquido", colors.HexColor("#2388DE"))]
    lx = x+14*mm
    for text, color in legends:
        c.setFillColor(color)
        c.roundRect(lx, ly, 7, 7, 1.5, fill=1, stroke=0)
        _text(c, lx+10, ly+0.5, text, 5.7, MUTED)
        lx += stringWidth(text, "Helvetica", 5.7) + 31


def _draw_margin_chart(c, x, y, w, h, history):
    _rounded(c, x, y, w, h, WHITE, BORDER, 8)
    _draw_section_title(c, x+6*mm, y+h-8*mm, "Evolución del margen líquido", "Porcentaje sobre ingresos líquidos", icon="margin")
    rows = [r for r in history if r.get("margin") is not None]
    if not rows:
        _text(c, x+w/2, y+h/2, "Sin datos", 8, MUTED, False, "center")
        return
    cx, cy = x+13*mm, y+16*mm
    cw, ch = w-22*mm, h-34*mm
    vals = [r["margin"] for r in rows]
    vmin = min(-2.0, min(vals)-1)
    vmax = max(8.0, max(vals)+1)

    c.setStrokeColor(colors.HexColor("#DDE6F1"))
    c.setLineWidth(0.35)
    for i in range(5):
        gy = cy + ch*i/4
        c.line(cx, gy, cx+cw, gy)
    pts = []
    for i, row in enumerate(rows):
        px = cx + (cw*i/(len(rows)-1) if len(rows)>1 else cw/2)
        py = cy + (row["margin"]-vmin)/(vmax-vmin)*ch
        pts.append((px, py, row))
    c.setStrokeColor(colors.HexColor("#0879E6"))
    c.setLineWidth(1.4)
    for a, b in zip(pts, pts[1:]):
        c.line(a[0], a[1], b[0], b[1])
    for px, py, row in pts:
        c.setFillColor(colors.HexColor("#0879E6"))
        c.circle(px, py, 3, fill=1, stroke=0)
        _text(c, px, py+7, _fmt_pct(row["margin"]), 6.1, NAVY, True, "center")
        _text(c, px, cy-9, row["label"], 5.8, INK, False, "center")


def _draw_pie(c, x, y, w, h, categories, total):
    _rounded(c, x, y, w, h, WHITE, BORDER, 8)
    _draw_section_title(c, x+6*mm, y+h-8*mm, "Composición de gastos", "Distribución por categoría", icon="pie")
    if not categories or not total:
        _text(c, x+w/2, y+h/2, "Sin gastos categorizados", 8, MUTED, False, "center")
        return

    cx, cy = x+38*mm, y+35*mm
    r = min(29*mm, h*0.31)
    # sombra inferior simulada
    c.setFillColor(colors.HexColor("#D4D7DC"))
    c.ellipse(cx-r, cy-r-4, cx+r, cy+r-4, fill=1, stroke=0)
    angle = 90.0
    for idx, (name, amount) in enumerate(categories):
        extent = 360.0 * amount / total
        color = PIE_COLORS[idx % len(PIE_COLORS)]
        c.setFillColor(color)
        c.setStrokeColor(WHITE)
        c.wedge(cx-r, cy-r, cx+r, cy+r, angle, -extent, fill=1, stroke=1)
        mid = (angle - extent/2) * pi/180
        pct = amount/total*100
        if pct >= 6:
            tx = cx + cos(mid)*r*0.58
            ty = cy + sin(mid)*r*0.58
            _text(c, tx, ty-2, _fmt_pct(pct), 7.0, WHITE, True, "center")
        angle -= extent

    lx = x+w*0.56
    ly = y+h-26*mm
    for idx, (name, amount) in enumerate(categories[:6]):
        color = PIE_COLORS[idx % len(PIE_COLORS)]
        c.setFillColor(color)
        c.roundRect(lx, ly, 7, 7, 1.2, fill=1, stroke=0)
        label = name if len(name) <= 24 else name[:22] + "…"
        _text(c, lx+11, ly+0.3, label, 6.4, INK)
        ly -= 8*mm


def _draw_top_expenses(c, x, y, w, h, data):
    _rounded(c, x, y, w, h, WHITE, BORDER, 8)
    _draw_icon(c, x+5*mm, y+h-13*mm, 8*mm, "expense", BLUE)
    _text(c, x+15*mm, y+h-9*mm, "Top 5 gastos del período", 9.3, NAVY, True)
    rows = data["top_categories"]
    if not rows:
        _text(c, x+w/2, y+h/2, "Sin detalle", 8, MUTED, False, "center")
        return
    tx = x+5*mm
    ty = y+h-18*mm
    cols = [0, 12*mm, w-48*mm, w-17*mm]
    c.setFillColor(colors.HexColor("#DDEEFF"))
    c.roundRect(tx, ty-8*mm, w-10*mm, 8*mm, 3, fill=1, stroke=0)
    _text(c, tx+4, ty-5.2*mm, "#", 5.8, NAVY, True)
    _text(c, tx+cols[1], ty-5.2*mm, "Categoría", 6.4, NAVY, True)
    _text(c, tx+cols[2], ty-5.2*mm, "Monto", 6.4, NAVY, True, "right")
    _text(c, tx+cols[3], ty-5.2*mm, "%", 6.4, NAVY, True, "right")
    ry = ty-13*mm
    for i, (name, amount) in enumerate(rows, 1):
        pct = amount/data["expenses"]*100 if data["expenses"] else 0
        if i % 2 == 0:
            c.setFillColor(colors.HexColor("#F7FAFE"))
            c.rect(tx, ry-4.7*mm, w-10*mm, 6.2*mm, fill=1, stroke=0)
        label = name if len(name) <= 22 else name[:20] + "…"
        _text(c, tx+4, ry, str(i), 6.2, INK, True)
        _text(c, tx+cols[1], ry, label, 6.4, INK)
        _text(c, tx+cols[2], ry, _money(amount), 6.2, INK, False, "right")
        _text(c, tx+cols[3], ry, _fmt_pct(pct), 6.2, INK, False, "right")
        ry -= 7.2*mm
    _text(c, tx+cols[1], y+6*mm, "Total gastos", 6.7, NAVY, True)
    _text(c, tx+cols[3], y+6*mm, _money(data["expenses"]), 6.7, NAVY, True, "right")


def _draw_reconciliation(c, x, y, w, h, data):
    _rounded(c, x, y, w, h, WHITE, BORDER, 8)
    _draw_icon(c, x+5*mm, y+h-13*mm, 8*mm, "liquid", BLUE)
    _text(c, x+15*mm, y+h-9*mm, "Conciliación y liquidez", 9.3, NAVY, True)
    rec = data["reconciliation"]
    rows = [
        ("Resultado calculado", data["calculated_profit"], RED if data["calculated_profit"] < 0 else GREEN),
        ("Resultado líquido", data["liquid_profit"], RED if (data["liquid_profit"] or 0) < 0 else GREEN),
        ("Saldo real al cierre", data["actual_balance"], GREEN),
        ("Fondos reservados", data["reserved_funds"], GREEN),
        ("Brecha explicada", rec.get("explained_gap"), GREEN if rec.get("explained_gap") is not None else MUTED),
        ("Desfase no explicado", rec.get("unexplained_gap"), BLUE),
    ]
    ry = y+h-19*mm
    for label, value, color in rows:
        c.setStrokeColor(colors.HexColor("#E2EAF4"))
        c.line(x+5*mm, ry-3*mm, x+w-5*mm, ry-3*mm)
        _text(c, x+6*mm, ry, label, 5.8, INK)
        _text(c, x+w-6*mm, ry, _money(value), 6.2, color, True, "right")
        ry -= 9*mm
    status = rec.get("status", {})
    status_text = status.get("label") or "Incompleto"
    pct = status.get("pct")
    _text(c, x+6*mm, y+5*mm, "Control:", 5.8, MUTED, True)
    _text(c, x+20*mm, y+5*mm, status_text + (f" · {_fmt_pct(pct)}" if pct is not None else ""), 5.8, NAVY, True)


def _build_alerts(data):
    alerts = []
    margin = data.get("liquid_margin")
    if margin is not None:
        if margin < 0:
            alerts.append((RED, f"Margen líquido negativo ({_fmt_pct(margin)})."))
        elif margin < 5:
            alerts.append((colors.HexColor("#F39C12"), f"Margen líquido bajo ({_fmt_pct(margin)})."))
        else:
            alerts.append((GREEN, f"Margen líquido positivo ({_fmt_pct(margin)})."))
    exp_change = data["changes"].get("expenses_pct")
    if exp_change is not None:
        color = RED if exp_change > 5 else GREEN
        alerts.append((color, f"Los gastos variaron {_fmt_pct(exp_change, signed=True)} vs. período anterior."))
    inc_change = data["changes"].get("liquid_income_pct")
    if inc_change is not None:
        color = GREEN if inc_change >= 0 else RED
        alerts.append((color, f"Los ingresos líquidos variaron {_fmt_pct(inc_change, signed=True)}."))
    rec_status = data["reconciliation"].get("status", {})
    alerts.append((
        GREEN if rec_status.get("label") == "Aceptable" else colors.HexColor("#F39C12"),
        f"Desfase no explicado: {rec_status.get('label', 'Incompleto')}.",
    ))
    return alerts[:4]


def _draw_alerts_observations(c, x, y, w, h, data):
    alert_h = h*0.48
    _rounded(c, x, y+h-alert_h, w, alert_h, LIGHT_RED, colors.HexColor("#F5D5D5"), 8)
    _draw_icon(c, x+5*mm, y+h-13*mm, 8*mm, "alert", RED)
    _text(c, x+15*mm, y+h-9*mm, "Alertas gerenciales", 8.2, RED, True)
    ay = y+h-18*mm
    for color, text in _build_alerts(data):
        c.setFillColor(color)
        c.circle(x+7*mm, ay+1, 2.5, fill=1, stroke=0)
        used = _paragraph(c, x+11*mm, ay+5, w-16*mm, text, 5.8, 7.2, INK)
        ay -= max(8*mm, used+3)

    obs_y = y
    obs_h = h-alert_h-4*mm
    _rounded(c, x, obs_y, w, obs_h, LIGHT_BLUE, colors.HexColor("#D4E7FA"), 8)
    _draw_icon(c, x+5*mm, obs_y+obs_h-13*mm, 8*mm, "note", BLUE)
    _text(c, x+15*mm, obs_y+obs_h-9*mm, "Observaciones", 8.2, NAVY, True)
    top_name = data.get("top_expense_name") or "Principal gasto"
    top_share = data.get("top_expense_share") or 0.0
    observations = [
        f"{top_name} representa {_fmt_pct(top_share)} del gasto del período.",
        f"Cierre operativo: {_fmt_pct(data.get('closure_pct'))} de los días cargados.",
    ]
    if data.get("actual_balance") is not None:
        observations.append(f"Saldo real disponible: {_money(data['actual_balance'])}.")
    oy = obs_y+obs_h-17*mm
    for text in observations[:3]:
        _text(c, x+7*mm, oy, "•", 7, NAVY, True)
        used = _paragraph(c, x+11*mm, oy+4, w-16*mm, text, 5.7, 7.0, INK)
        oy -= max(7*mm, used+2)


def _draw_situation(c, x, y, w, h, data):
    _rounded(c, x, y, w, h, LIGHT_BLUE, colors.HexColor("#CFE3F8"), 8)
    _draw_icon(c, x+6*mm, y+h-13*mm, 8*mm, "note", BLUE)
    _text(c, x+16*mm, y+h-9*mm, "Situación del período", 8.8, NAVY, True)
    lp = data.get("liquid_profit") or 0.0
    margin = data.get("liquid_margin")
    direction = "positivo" if lp >= 0 else "negativo"
    inc = data["changes"].get("liquid_income_pct")
    exp = data["changes"].get("expenses_pct")
    parts = [
        f"El período cerró con un resultado líquido <b>{direction}</b> de <b>{_money(lp)}</b>, equivalente a un margen de <b>{_fmt_pct(margin)}</b>.",
    ]
    if inc is not None and exp is not None:
        parts.append(
            f"Los ingresos líquidos variaron <b>{_fmt_pct(inc, signed=True)}</b> y los gastos <b>{_fmt_pct(exp, signed=True)}</b> frente a {data['comparison_label']}."
        )
    if data.get("actual_balance") is not None:
        parts.append(
            f"El saldo real disponible al cierre fue <b>{_money(data['actual_balance'])}</b> y los fondos reservados ascienden a <b>{_money(data['reserved_funds'])}</b>."
        )
    _paragraph(c, x+7*mm, y+h-15*mm, w-14*mm, " ".join(parts), 6.3, 8.2, INK)


def _draw_days_card(c, x, y, w, h, data):
    _rounded(c, x, y, w, h, WHITE, BORDER, 8)
    _draw_icon(c, x+5*mm, y+h-13*mm, 8*mm, "calendar", BLUE)
    _text(c, x+15*mm, y+h-9*mm, "Días del período", 8.2, NAVY, True)
    rows = [
        ("Días cargados", data["days_loaded"]),
        ("Días cerrados", data["days_closed"]),
        ("Días pendientes", data["days_pending"]),
    ]
    ry = y+h-18*mm
    for label, value in rows:
        _text(c, x+6*mm, ry, label, 5.8, INK)
        _text(c, x+w-6*mm, ry, str(value), 6.2, NAVY, True, "right")
        ry -= 8*mm
    _text(c, x+6*mm, y+8*mm, "% de cierre", 5.8, INK)
    bx = x+29*mm
    by = y+7.5*mm
    bw = w-39*mm
    c.setFillColor(colors.HexColor("#DCEFE2"))
    c.roundRect(bx, by, bw, 4, 2, fill=1, stroke=0)
    c.setFillColor(GREEN)
    c.roundRect(bx, by, bw*min(max(data["closure_pct"], 0), 100)/100, 4, 2, fill=1, stroke=0)
    _text(c, x+w-6*mm, y+7*mm, _fmt_pct(data["closure_pct"]), 6.2, DARK_GREEN, True, "right")


def build_management_pdf(data):
    output = BytesIO()
    page_size = landscape(A3)
    W, H = page_size
    c = canvas.Canvas(output, pagesize=page_size, pageCompression=1)
    c.setTitle("Informe Gerencial PORÁ")
    c.setAuthor("PORÁ")
    c.setFillColor(colors.HexColor("#F4F8FC"))
    c.rect(0, 0, W, H, fill=1, stroke=0)

    _draw_header(c, W, H, data)

    margin = 8*mm
    gap = 3*mm
    content_w = W - 2*margin

    # KPIs principales: 5 tarjetas
    kpi_y = H - 72*mm
    kpi_h = 31*mm
    kpi_w = (content_w - 4*gap) / 5
    kpis = [
        ("Ingresos Brutos", _money(data["gross_income"]), data["changes"].get("gross_income_pct"), LIGHT_GREEN, DARK_GREEN, None, "income"),
        ("Ingresos Líquidos", _money(data["liquid_income"]), data["changes"].get("liquid_income_pct"), LIGHT_GREEN, DARK_GREEN, None, "liquid"),
        ("Gastos Totales", _money(data["expenses"]), data["changes"].get("expenses_pct"), LIGHT_RED, RED, None, "expense"),
        ("Resultado Líquido", _money(data["liquid_profit"]), data["changes"].get("liquid_profit_pct"), LIGHT_BLUE, BLUE, None, "result"),
        ("Margen Líquido", _fmt_pct(data["liquid_margin"]), data["changes"].get("liquid_margin_pp"), LIGHT_BLUE, NAVY, None, "margin"),
    ]
    for i, (title, value, change, fill, accent, note, icon) in enumerate(kpis):
        x = margin + i*(kpi_w+gap)
        label = data["comparison_label"]
        change_suffix = " p.p." if title == "Margen Líquido" else "%"
        _draw_kpi(c, x, kpi_y, kpi_w, kpi_h, title, value, change, label, fill, accent, note, change_suffix, icon)

    # Segunda fila
    second_y = H - 106*mm
    second_h = 28*mm
    small_w = (content_w - 4*gap) * 0.16
    situation_w = content_w - 3*small_w - 3*gap
    _draw_kpi(c, margin, second_y, small_w, second_h, "Saldo Real al Cierre", _money(data["actual_balance"]), data["changes"].get("actual_balance_pct"), data["comparison_label"], LIGHT_GREEN, DARK_GREEN, icon="cash")
    _draw_kpi(c, margin+small_w+gap, second_y, small_w, second_h, "Fondos Reservados", _money(data["reserved_funds"]), data["changes"].get("reserved_pct"), data["comparison_label"], LIGHT_GOLD, GOLD, icon="reserve")
    _draw_kpi(c, margin+2*(small_w+gap), second_y, small_w, second_h, data["top_expense_name"], _money(data["top_expense_amount"]), None, None, LIGHT_PURPLE, PURPLE, f"{_fmt_pct(data['top_expense_share'])} de los gastos", icon="stack")
    _draw_situation(c, margin+3*(small_w+gap), second_y, situation_w, second_h, data)

    # Zona de gráficos
    charts_y = H - 178*mm
    charts_h = 66*mm
    monthly_w = content_w*0.51
    margin_w = content_w*0.30
    days_w = content_w - monthly_w - margin_w - 2*gap
    _draw_monthly_chart(c, margin, charts_y, monthly_w, charts_h, data["monthly_history"])
    _draw_margin_chart(c, margin+monthly_w+gap, charts_y, margin_w, charts_h, data["monthly_history"])
    _draw_days_card(c, margin+monthly_w+gap+margin_w+gap, charts_y, days_w, charts_h, data)

    # Zona inferior
    bottom_y = 18*mm
    bottom_h = charts_y - bottom_y - gap
    pie_w = content_w*0.30
    top_w = content_w*0.25
    rec_w = content_w*0.21
    alerts_w = content_w - pie_w - top_w - rec_w - 3*gap
    _draw_pie(c, margin, bottom_y, pie_w, bottom_h, data["pie_categories"], data["expenses"])
    _draw_top_expenses(c, margin+pie_w+gap, bottom_y, top_w, bottom_h, data)
    _draw_reconciliation(c, margin+pie_w+gap+top_w+gap, bottom_y, rec_w, bottom_h, data)
    _draw_alerts_observations(c, margin+pie_w+gap+top_w+gap+rec_w+gap, bottom_y, alerts_w, bottom_h, data)

    # footer
    c.setStrokeColor(colors.HexColor("#D8E5F3"))
    c.line(margin, 12*mm, W-margin, 12*mm)
    _text(c, margin, 7*mm, f"PORÁ  |  Informe Gerencial  |  {data['period_label']}", 6.2, NAVY, True)
    _text(c, W-margin, 7*mm, "Generado automáticamente por PORÁ", 5.8, MUTED, False, "right")

    c.showPage()
    c.save()
    output.seek(0)
    return output
