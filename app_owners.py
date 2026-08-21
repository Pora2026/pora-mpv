
import os
import re
import json
from datetime import date, datetime, timedelta
from io import BytesIO

import openpyxl
from flask import (
    Flask,
    render_template_string,
    request,
    redirect,
    url_for,
    flash,
    jsonify,
    send_file,
)
from flask_login import login_required
from sqlalchemy import func, case, text
from werkzeug.security import generate_password_hash

from app.config import Config, INSTANCE_DIR
from app.extensions import db, login_manager
from app.models import User, BusinessDay, ShiftRecord, ExpenseCategory, ExpenseEntry

from app.utils.money import safe_float, ars
from app.services.finance_service import (
    compute_adjusted_profit,
    compute_apps_retention_estimate,
)
from app.utils.dates import (
    is_sunday,
    parse_ymd,
    iso,
    fmt_date_ar,
    fmt_date_ar_from_iso,
    iter_workdays,
    month_range,
)

# ----------------------------
# Config básica
# ----------------------------
app = Flask(__name__)
app.config.from_object(Config)

db.init_app(app)
login_manager.init_app(app)
login_manager.login_view = "auth_bp.login_get"


# ----------------------------
# Modelos
# ----------------------------
@login_manager.user_loader
def load_user(user_id):
    return db.session.get(User, int(user_id))


# ----------------------------
# Helpers finanzas
# ----------------------------
def ensure_shifts(bday: BusinessDay):
    existing = {s.shift for s in bday.shifts}
    for sh in ("Mañana", "Tarde"):
        if sh not in existing:
            db.session.add(ShiftRecord(business_day=bday, shift=sh))


def recalc_day_status(bday: BusinessDay):
    if not bday:
        return
    ensure_shifts(bday)
    closed = [s for s in bday.shifts if bool(getattr(s, "is_closed", False))]
    bday.status = "complete" if len(closed) > 0 else "draft"


def day_totals(bday: BusinessDay) -> dict:
    income = sum(s.income or 0 for s in bday.shifts)

    if bday.expenses and len(bday.expenses) > 0:
        var_exp = sum(e.amount or 0 for e in bday.expenses if e.kind == "variable")
        fix_exp = sum(e.amount or 0 for e in bday.expenses if e.kind == "fixed")
    else:
        var_exp = sum(s.variable_expense_total or 0 for s in bday.shifts)
        fix_exp = sum(s.fixed_expense_total or 0 for s in bday.shifts)

    exp_total = var_exp + fix_exp
    profit_raw = income - exp_total

    apps_gross = float(getattr(bday, "real_apps_pending", 0.0) or 0.0)
    apps_retention_estimate = compute_apps_retention_estimate(apps_gross)
    profit_adjusted = compute_adjusted_profit(
        total_sales=income,
        paid_expenses=exp_total,
        apps_gross=apps_gross,
    )

    return {
        "income": float(income),
        "variable_expense": float(var_exp),
        "fixed_expense": float(fix_exp),
        "expense_total": float(exp_total),

        # Compatibilidad: ``profit`` conserva la fórmula histórica.
        "profit": float(profit_raw),
        "profit_raw": float(profit_raw),

        # Nueva ganancia calculada ajustada por el costo estimado de apps.
        "apps_gross": float(apps_gross),
        "apps_retention_estimate": float(apps_retention_estimate),
        "profit_adjusted": float(profit_adjusted),
    }


def margin_bucket(margin_pct):
    if margin_pct is None:
        return ("—", "pill")
    if margin_pct <= 10:
        return ("Malo", "pill bad")
    if margin_pct < 20:
        return ("Regular", "pill warn")
    return ("Bueno", "pill ok")


def ensure_admin():
    username = os.environ.get("OWNERS_ADMIN_USER", "admin")
    password = os.environ.get("OWNERS_ADMIN_PASS", "admin123")
    u = User.query.filter_by(username=username).first()
    if not u:
        from werkzeug.security import generate_password_hash
        u = User(username=username, password_hash=generate_password_hash(password), is_admin=True)
        db.session.add(u)
        db.session.commit()


def ensure_schema():
    dialect = db.engine.dialect.name

    try:
        # =====================================================
        # POSTGRESQL
        # =====================================================

        if dialect == "postgresql":

            # legacy
            db.session.execute(text(
                "ALTER TABLE business_days ADD COLUMN IF NOT EXISTS real_profit DOUBLE PRECISION;"
            ))

            # real
            db.session.execute(text(
                "ALTER TABLE business_days ADD COLUMN IF NOT EXISTS real_cash_profit DOUBLE PRECISION;"
            ))

            db.session.execute(text(
                "ALTER TABLE business_days ADD COLUMN IF NOT EXISTS real_digital_profit DOUBLE PRECISION;"
            ))

            # apps
            db.session.execute(text(
                "ALTER TABLE business_days ADD COLUMN IF NOT EXISTS real_apps_pending DOUBLE PRECISION;"
            ))

            db.session.execute(text(
                "ALTER TABLE business_days ADD COLUMN IF NOT EXISTS real_apps_collected DOUBLE PRECISION;"
            ))

            # caja legacy
            db.session.execute(text(
                "ALTER TABLE business_days ADD COLUMN IF NOT EXISTS cash_balance DOUBLE PRECISION;"
            ))

            # =====================================================
            # NUEVA ARQUITECTURA LIQUIDEZ
            # =====================================================

            db.session.execute(text(
                "ALTER TABLE business_days ADD COLUMN IF NOT EXISTS opening_cash_balance DOUBLE PRECISION;"
            ))

            db.session.execute(text(
                "ALTER TABLE business_days ADD COLUMN IF NOT EXISTS expected_cash_balance DOUBLE PRECISION;"
            ))

            db.session.execute(text(
                "ALTER TABLE business_days ADD COLUMN IF NOT EXISTS actual_cash_balance DOUBLE PRECISION;"
            ))

            db.session.execute(text(
                "ALTER TABLE business_days ADD COLUMN IF NOT EXISTS cash_difference DOUBLE PRECISION;"
            ))

            db.session.execute(text(
                "ALTER TABLE business_days ADD COLUMN IF NOT EXISTS safe_box_transfer DOUBLE PRECISION;"
            ))

            db.session.execute(text(
                "ALTER TABLE business_days ADD COLUMN IF NOT EXISTS reserved_funds_balance DOUBLE PRECISION;"
            ))

            db.session.execute(text(
                "ALTER TABLE business_days ADD COLUMN IF NOT EXISTS reserved_funds_changed_at TIMESTAMP WITHOUT TIME ZONE;"
            ))

            db.session.execute(text(
                "ALTER TABLE business_days ADD COLUMN IF NOT EXISTS operating_cash_balance DOUBLE PRECISION;"
            ))

            db.session.execute(text(
                "ALTER TABLE business_days ADD COLUMN IF NOT EXISTS daily_mercadopago DOUBLE PRECISION;"
            ))

            db.session.execute(text(
                "ALTER TABLE business_days ADD COLUMN IF NOT EXISTS daily_cash_withdrawn DOUBLE PRECISION;"
            ))

            db.session.commit()
            return

        # =====================================================
        # SQLITE
        # =====================================================

        cols = db.session.execute(
            text("PRAGMA table_info(business_days);")
        ).fetchall()

        existing = {c[1] for c in cols}

        # legacy
        if "real_profit" not in existing:
            db.session.execute(text(
                "ALTER TABLE business_days ADD COLUMN real_profit REAL;"
            ))

        # real
        if "real_cash_profit" not in existing:
            db.session.execute(text(
                "ALTER TABLE business_days ADD COLUMN real_cash_profit REAL;"
            ))

        if "real_digital_profit" not in existing:
            db.session.execute(text(
                "ALTER TABLE business_days ADD COLUMN real_digital_profit REAL;"
            ))

        # apps
        if "real_apps_pending" not in existing:
            db.session.execute(text(
                "ALTER TABLE business_days ADD COLUMN real_apps_pending REAL;"
            ))

        if "real_apps_collected" not in existing:
            db.session.execute(text(
                "ALTER TABLE business_days ADD COLUMN real_apps_collected REAL;"
            ))

        # caja legacy
        if "cash_balance" not in existing:
            db.session.execute(text(
                "ALTER TABLE business_days ADD COLUMN cash_balance REAL;"
            ))

        # =====================================================
        # NUEVA ARQUITECTURA LIQUIDEZ
        # =====================================================

        if "opening_cash_balance" not in existing:
            db.session.execute(text(
                "ALTER TABLE business_days ADD COLUMN opening_cash_balance REAL;"
            ))

        if "expected_cash_balance" not in existing:
            db.session.execute(text(
                "ALTER TABLE business_days ADD COLUMN expected_cash_balance REAL;"
            ))

        if "actual_cash_balance" not in existing:
            db.session.execute(text(
                "ALTER TABLE business_days ADD COLUMN actual_cash_balance REAL;"
            ))

        if "cash_difference" not in existing:
            db.session.execute(text(
                "ALTER TABLE business_days ADD COLUMN cash_difference REAL;"
            ))

        if "safe_box_transfer" not in existing:
            db.session.execute(text(
                "ALTER TABLE business_days ADD COLUMN safe_box_transfer REAL;"
            ))

        if "reserved_funds_balance" not in existing:
            db.session.execute(text(
                "ALTER TABLE business_days ADD COLUMN reserved_funds_balance REAL;"
            ))

        if "reserved_funds_changed_at" not in existing:
            db.session.execute(text(
                "ALTER TABLE business_days ADD COLUMN reserved_funds_changed_at DATETIME;"
            ))

        if "operating_cash_balance" not in existing:
            db.session.execute(text(
                "ALTER TABLE business_days ADD COLUMN operating_cash_balance REAL;"
            ))

        if "daily_mercadopago" not in existing:
            db.session.execute(text(
                "ALTER TABLE business_days ADD COLUMN daily_mercadopago REAL;"
            ))

        if "daily_cash_withdrawn" not in existing:
            db.session.execute(text(
                "ALTER TABLE business_days ADD COLUMN daily_cash_withdrawn REAL;"
            ))

        db.session.commit()

    except Exception as e:
        db.session.rollback()
        print("ERROR ensure_schema:", e)


def range_series(d1: date, d2: date):
    exp_sub = (
        db.session.query(
            ExpenseEntry.business_day_id.label("bdid"),
            func.count(ExpenseEntry.id).label("cnt"),
            func.coalesce(func.sum(case((ExpenseEntry.kind == "variable", ExpenseEntry.amount), else_=0.0)), 0.0).label(
                "var_cat"
            ),
            func.coalesce(func.sum(case((ExpenseEntry.kind == "fixed", ExpenseEntry.amount), else_=0.0)), 0.0).label(
                "fix_cat"
            ),
        )
        .group_by(ExpenseEntry.business_day_id)
        .subquery()
    )

    sh_sub = (
        db.session.query(
            ShiftRecord.business_day_id.label("bdid"),
            func.coalesce(func.sum(ShiftRecord.income), 0.0).label("income"),
            func.coalesce(func.sum(ShiftRecord.variable_expense_total), 0.0).label("var_sh"),
            func.coalesce(func.sum(ShiftRecord.fixed_expense_total), 0.0).label("fix_sh"),
        )
        .group_by(ShiftRecord.business_day_id)
        .subquery()
    )

    rows = (
        db.session.query(
            BusinessDay.day.label("day"),
            func.coalesce(sh_sub.c.income, 0.0).label("income"),
            func.coalesce(BusinessDay.real_apps_pending, 0.0).label("apps_pending"),
            case(
                (func.coalesce(exp_sub.c.cnt, 0) > 0, exp_sub.c.var_cat),
                else_=func.coalesce(sh_sub.c.var_sh, 0.0),
            ).label("var_exp"),
            case(
                (func.coalesce(exp_sub.c.cnt, 0) > 0, exp_sub.c.fix_cat),
                else_=func.coalesce(sh_sub.c.fix_sh, 0.0),
            ).label("fix_exp"),
        )
        .outerjoin(sh_sub, sh_sub.c.bdid == BusinessDay.id)
        .outerjoin(exp_sub, exp_sub.c.bdid == BusinessDay.id)
        .filter(BusinessDay.day >= d1, BusinessDay.day <= d2)
        .order_by(BusinessDay.day.asc())
        .all()
    )

    out = []
    for r in rows:
        if is_sunday(r.day):
            continue
        income = float(r.income or 0)
        var_exp = float(r.var_exp or 0)
        fix_exp = float(r.fix_exp or 0)
        exp_total = var_exp + fix_exp
        profit_raw = income - exp_total
        apps_gross = float(r.apps_pending or 0.0)
        apps_retention_estimate = compute_apps_retention_estimate(apps_gross)
        profit_adjusted = compute_adjusted_profit(
            total_sales=income,
            paid_expenses=exp_total,
            apps_gross=apps_gross,
        )
        out.append(
            {
                "date": r.day.isoformat(),
                "income": income,
                "variable_expense": var_exp,
                "fixed_expense": fix_exp,
                "expense_total": exp_total,

                # Compatibilidad con consumidores históricos.
                "profit": profit_raw,
                "profit_raw": profit_raw,

                "apps_gross": apps_gross,
                "apps_retention_estimate": apps_retention_estimate,
                "profit_adjusted": profit_adjusted,
            }
        )
    return out


def period_previous(d1: date, d2: date):
    days = (d2 - d1).days + 1
    prev_to = d1 - timedelta(days=1)
    prev_from = prev_to - timedelta(days=days - 1)
    return prev_from, prev_to


# ----------------------------
# Templates (inline)
# ----------------------------
BASE_HTML = """
<!doctype html>
<html lang="es">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>{{ title or "Dueños - Panel" }}</title>

  <script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.1/dist/chart.umd.min.js"></script>

  <style>
    :root{
      --bg:#f6f7fb;
      --card:#ffffff;
      --text:#111827;
      --muted:#6b7280;
      --border:#e5e7eb;
      --shadow2: 0 10px 25px rgba(0,0,0,.06);

      --green:#16a34a;
      --red:#dc2626;
      --amber:#f59e0b;
      --blue:#2563eb;

      --incomeBg: rgba(22,163,74,.10);
      --expenseBg: rgba(220,38,38,.10);
      --profitBg: rgba(37,99,235,.10);
    }
    *{ box-sizing:border-box; }
    body{
      font-family: Arial, sans-serif;
      margin:0;
      background:var(--bg);
      color:var(--text);
    }
    .wrap{
      max-width: 1180px;
      margin: 0 auto;
      padding: 22px 18px 40px;
    }
    .nav{
      display:flex;
      align-items:center;
      gap:12px;
      flex-wrap:wrap;
      padding: 12px 14px;
      background: rgba(255,255,255,.75);
      border:1px solid var(--border);
      border-radius: 14px;
      box-shadow: var(--shadow2);
      margin-bottom: 16px;
      backdrop-filter: blur(8px);
    }
    .nav a{
      text-decoration:none;
      color:var(--text);
      padding:8px 10px;
      border-radius:10px;
      white-space:nowrap;
    }
    .nav a:hover{ background:#eef2ff; }

    h1{ margin: 8px 0 14px; font-size: 30px; }
    h2{ margin: 16px 0 10px; font-size: 20px; }
    h3{ margin: 0 0 8px; font-size: 16px; }

    .card{
      background:var(--card);
      border:1px solid var(--border);
      border-radius: 16px;
      padding: 16px;
      margin: 12px 0;
      box-shadow: var(--shadow2);
    }

    .grid{ display:grid; grid-template-columns: repeat(2,minmax(0,1fr)); gap:12px; }
    .grid3{ display:grid; grid-template-columns: repeat(3,minmax(0,1fr)); gap:12px; }
    .grid4{ display:grid; grid-template-columns: repeat(4,minmax(0,1fr)); gap:12px; }
    .grid8{ display:grid; grid-template-columns: repeat(4,minmax(0,1fr)); gap:12px; }

    @media (max-width: 980px){
      .grid8{ grid-template-columns: repeat(2,minmax(0,1fr)); }
      .grid4{ grid-template-columns: repeat(2,minmax(0,1fr)); }
    }
    @media (max-width: 640px){
      .grid, .grid3, .grid4, .grid8{ grid-template-columns: 1fr; }
    }

    .kpi{ display:flex; flex-direction:column; gap:6px; border-radius: 16px; }
    .kpi .label{ color:var(--muted); font-size: 12px; text-transform: uppercase; letter-spacing:.04em; }
    .kpi .value{ font-size: 22px; font-weight: 800; }

    .kpi.income { background: var(--incomeBg); }
    .kpi.expense { background: var(--expenseBg); }
    .kpi.profit { background: var(--profitBg); }
    
    .kpi.blue {
      background: #d9e8ee;
      border: 1px solid #7aa6b8;
    }

    .muted{ color:var(--muted); font-size: 13px; }

    .btn{
      display:inline-flex;
      align-items:center;
      justify-content:center;
      gap:8px;
      padding:10px 12px;
      border:1px solid var(--border);
      border-radius: 12px;
      background:#fff;
      text-decoration:none;
      color:var(--text);
      box-shadow: var(--shadow2);
      cursor:pointer;
      white-space: nowrap;
    }
    .btn.primary{
      background: #111827;
      color: #fff;
      border-color:#111827;
    }
    .btn:hover{ transform: translateY(-1px); }
    .disabled{ opacity: .55; pointer-events: none; }

    input, select, textarea{
      width:100%;
      padding:10px 12px;
      border:1px solid var(--border);
      border-radius: 12px;
      background:#fff;
      outline:none;
    }
    input:focus, select:focus, textarea:focus{
      border-color:#c7d2fe;
      box-shadow: 0 0 0 4px rgba(99,102,241,.15);
    }

    table{ width:100%; border-collapse: collapse; }
    th, td{ border-bottom:1px solid var(--border); padding: 10px 10px; }
    th{
      text-align:left;
      color:var(--muted);
      font-weight:800;
      font-size: 12px;
      text-transform: uppercase;
      letter-spacing:.04em;
      vertical-align: bottom;
    }
    th.num{ text-align:right; }
    td.num{ text-align:right; font-variant-numeric: tabular-nums; white-space: nowrap; }
    tr:hover td{ background: rgba(99,102,241,.06); }

    .pill{
      display:inline-block;
      padding:4px 10px;
      border-radius:999px;
      font-size:12px;
      border:1px solid var(--border);
      background:#fff;
      color:var(--muted);
    }
    .pill.ok{ color: var(--green); border-color: rgba(22,163,74,.25); background: rgba(22,163,74,.10); }
    .pill.warn{ color: #b45309; border-color: rgba(245,158,11,.3); background: rgba(245,158,11,.12); }
    .pill.bad{ color: var(--red); border-color: rgba(220,38,38,.25); background: rgba(220,38,38,.10); }

    .flash-error{ background: #fee2e2; border:1px solid #fecaca; }
    .flash-ok{ background: #dcfce7; border:1px solid #bbf7d0; }

    .row-actions { display:flex; gap:10px; flex-wrap:wrap; align-items:flex-end; }
    .row-actions .field { flex: 1; min-width: 200px; }
    .chartbox { position: relative; height: 320px; }
    .monthly-chartbox { height: 430px; }

    .ranking-section{ margin-top: 12px; }
    .ranking-section + .ranking-section{
      margin-top: 18px;
      padding-top: 16px;
      border-top: 1px solid var(--border);
    }
    .ranking-section h4{
      margin: 0 0 8px;
      font-size: 14px;
      color: var(--text);
    }

    @media (max-width: 640px){
      .chartbox{ height: 280px; }
      .monthly-chartbox{ height: 360px; }
    }

    .neg { color: var(--red); font-weight: 800; }
    .inline { display:flex; gap:10px; align-items:flex-end; flex-wrap:wrap; }
    .inline .field { flex: 1; min-width: 240px; }
    .small { font-size: 12px; }

    details{
      border:1px solid var(--border);
      border-radius:14px;
      padding:10px 12px;
      background:#fff;
      box-shadow: var(--shadow2);
      margin: 12px 0;
    }
    summary{ cursor:pointer; font-weight:800; }

    .legend-row{ display:flex; gap:8px; flex-wrap:wrap; margin-top:8px; align-items:center;}
    .legend-row .muted{ margin-right:6px;}

    .legend-row.compact { margin-top:6px; gap:6px; }
    .legend-row.compact .pill{ padding:3px 8px; font-size:11px; }
    .margen-kpi{
      display:flex;
      align-items:flex-start;
      justify-content:space-between;
      gap:10px;
    }
    .margen-kpi .margen-left{
      flex:1;
      min-width: 120px;
    }
    .margen-kpi .margen-right{
      width: 132px;
      display:flex;
      flex-direction:column;
      gap:6px;
      align-items:flex-start;
    }
    .margen-kpi .margen-right .muted{
      font-size:11px;
      margin:0;
    }
    .margen-kpi .margen-right .pill{
      padding:3px 8px;
      font-size:11px;
      white-space:nowrap;
    }

    @media (max-width: 640px){
      .margen-kpi{ flex-direction:column; }
      .margen-kpi .margen-right{ width:auto; flex-direction:row; flex-wrap:wrap; }
    }
  </style>
</head>
<body>
  <div class="wrap">

  {% if show_nav %}
  <div class="nav">
    <a href="{{ url_for('home_bp.home') }}">Inicio</a>
    <a href="{{ url_for('dashboard_bp.dashboard_finanzas') }}">Panel Central</a>
    <a href="{{ url_for('io_bp.io_dashboard') }}">Gestión Ingresos y Gastos</a>
    <a href="{{ url_for('days_bp.list_days') }}">Días</a>
    <a href="{{ url_for('import_export_bp.import_balance_get') }}">Importar Balance</a>
    <a href="{{ url_for('backup_bp.backup_home') }}">Backup</a>
    <a href="{{ url_for('import_export_bp.export_get') }}">Exportar legacy</a>
    <a href="{{ url_for('auth_bp.logout') }}">Salir</a>
  </div>
  {% endif %}

  {% with messages = get_flashed_messages(with_categories=true) %}
    {% if messages %}
      {% for category, msg in messages %}
        <div class="card {{ 'flash-error' if category=='error' else 'flash-ok' }}">{{ msg }}</div>
      {% endfor %}
    {% endif %}
  {% endwith %}

  {{ body|safe }}

  </div>

  <script>
    (function(){
      try{
        document.addEventListener('submit', function(){
          localStorage.setItem('scrollY', String(window.scrollY || 0));
        }, true);

        window.addEventListener('load', function(){
          const y = localStorage.getItem('scrollY');
          if(y !== null){
            const n = parseInt(y, 10);
            if(!isNaN(n)) window.scrollTo(0, n);
            localStorage.removeItem('scrollY');
          }
        });
      }catch(e){}
    })();
  </script>

</body>
</html>
"""


def render_page(body_html, **ctx):
    return render_template_string(BASE_HTML, body=body_html, **ctx)


# Blueprints
from app.routes.auth import auth_bp
from app.routes.home import home_bp
from app.routes.dashboard import dashboard_bp
from app.routes.days import days_bp
from app.routes.io import io_bp
from app.routes.import_export import import_export_bp
from app.routes.backup import backup_bp
from app.routes.caja_api import caja_api_bp

app.register_blueprint(auth_bp)
app.register_blueprint(home_bp)
app.register_blueprint(dashboard_bp)
app.register_blueprint(days_bp)
app.register_blueprint(io_bp)
app.register_blueprint(import_export_bp)
app.register_blueprint(backup_bp)
app.register_blueprint(caja_api_bp)

with app.app_context():
    db.create_all()
    ensure_schema()
    ensure_admin()


if __name__ == "__main__":
    with app.app_context():
        db.create_all()
        ensure_schema()
        ensure_admin()
    app.run(host="127.0.0.1", port=5001, debug=True)
