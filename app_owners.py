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
from flask_login import (
    LoginManager,
    UserMixin,
    login_user,
    logout_user,
    login_required,
)
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import func, case
from werkzeug.security import generate_password_hash, check_password_hash


# ----------------------------
# Config básica
# ----------------------------
BASE_DIR = os.path.abspath(os.path.dirname(__file__))
INSTANCE_DIR = os.path.join(BASE_DIR, "instance")
os.makedirs(INSTANCE_DIR, exist_ok=True)

DB_PATH = os.path.join(INSTANCE_DIR, "owners.db")
DATABASE_URI = "sqlite:///" + DB_PATH

app = Flask(__name__)
app.config["SECRET_KEY"] = os.environ.get("SECRET_KEY", "dev-change-me")

DATABASE_URL = os.environ.get("DATABASE_URL")
if DATABASE_URL:
    # En la nube (Render)
    app.config["SQLALCHEMY_DATABASE_URI"] = DATABASE_URL.replace("postgres://", "postgresql://")
else:
    # En local (SQLite)
    app.config["SQLALCHEMY_DATABASE_URI"] = DATABASE_URI

app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False

# Recomendado para Postgres en nube: evita conexiones "muertas"
app.config["SQLALCHEMY_ENGINE_OPTIONS"] = {
    "pool_pre_ping": True,
    "pool_recycle": 280,
}

db = SQLAlchemy(app)

login_manager = LoginManager(app)
login_manager.login_view = "login_get"


# ----------------------------
# Utilidades / Formato
# ----------------------------
def ars(value) -> str:
    """$ 1.234.567 (sin decimales)"""
    try:
        n = int(round(float(value or 0)))
    except Exception:
        n = 0
    sign = "-" if n < 0 else ""
    n = abs(n)
    return f"{sign}$ " + f"{n:,}".replace(",", ".")


def fmt_date_ar(d: date) -> str:
    return d.strftime("%d-%m-%Y") if d else ""


def fmt_date_ar_from_iso(iso: str) -> str:
    try:
        return datetime.strptime(iso, "%Y-%m-%d").strftime("%d-%m-%Y")
    except Exception:
        return iso


def is_sunday(d: date) -> bool:
    # Monday=0 ... Sunday=6
    return d.weekday() == 6


def iter_dates(d1: date, d2: date):
    cur = d1
    while cur <= d2:
        yield cur
        cur += timedelta(days=1)


def iter_workdays(d1: date, d2: date):
    for d in iter_dates(d1, d2):
        if not is_sunday(d):
            yield d


def month_range(d: date):
    first = d.replace(day=1)
    if d.month == 12:
        last = date(d.year, 12, 31)
    else:
        nextm = date(d.year, d.month + 1, 1)
        last = nextm.fromordinal(nextm.toordinal() - 1)
    return first, last


def parse_ymd(s: str) -> date:
    return datetime.strptime(s, "%Y-%m-%d").date()


def iso(d: date) -> str:
    return d.isoformat()


def safe_float(v) -> float:
    """Convierte strings numéricos estilo AR a float.
    Soporta: 221223 | 221.223 | 221,223 | 221.223,50 | 221,223.50 | $ 221.223,50
    """
    if v is None:
        return 0.0
    if isinstance(v, (int, float)):
        return float(v)

    s = str(v).strip()
    if not s:
        return 0.0

    s = s.replace("$", "").replace(" ", "")
    s = re.sub(r"[^0-9,\.\-]", "", s)
    if not s or s in ("-", ",", "."):
        return 0.0

    if "," in s and "." in s:
        if s.rfind(".") > s.rfind(","):
            # 1,234.56 -> coma miles, punto decimal
            s = s.replace(",", "")
        else:
            # 1.234,56 -> punto miles, coma decimal
            s = s.replace(".", "").replace(",", ".")
        return float(s)

    if "," in s:
        # miles: 1,234,567
        if re.match(r"^-?\d{1,3}(,\d{3})+$", s):
            return float(s.replace(",", ""))
        # decimal: 123,45
        return float(s.replace(",", "."))

    if "." in s:
        # miles: 1.234.567
        if re.match(r"^-?\d{1,3}(\.\d{3})+$", s):
            return float(s.replace(".", ""))
        return float(s)

    return float(s)



# ----------------------------
# Modelos
# ----------------------------
class User(db.Model, UserMixin):
    __tablename__ = "users"
    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(80), unique=True, nullable=False)
    password_hash = db.Column(db.String(255), nullable=False)
    is_admin = db.Column(db.Boolean, default=False)


class BusinessDay(db.Model):
    __tablename__ = "business_days"
    id = db.Column(db.Integer, primary_key=True)
    day = db.Column(db.Date, unique=True, nullable=False, index=True)
    note = db.Column(db.Text, default="")
    status = db.Column(db.String(20), default="draft")  # draft|complete
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

    shifts = db.relationship("ShiftRecord", backref="business_day", cascade="all, delete-orphan")
    expenses = db.relationship("ExpenseEntry", backref="business_day", cascade="all, delete-orphan")


class ShiftRecord(db.Model):
    __tablename__ = "shift_records"
    id = db.Column(db.Integer, primary_key=True)
    business_day_id = db.Column(db.Integer, db.ForeignKey("business_days.id"), nullable=False)

    shift = db.Column(db.String(10), nullable=False)  # "Mañana" / "Tarde"

    income = db.Column(db.Float, default=0.0)

    # Legacy: compatibilidad con import Excel viejo
    variable_expense_total = db.Column(db.Float, default=0.0)
    fixed_expense_total = db.Column(db.Float, default=0.0)

    note = db.Column(db.Text, default="")
    is_closed = db.Column(db.Boolean, default=False)

    __table_args__ = (db.UniqueConstraint("business_day_id", "shift", name="uq_day_shift"),)


class ExpenseCategory(db.Model):
    __tablename__ = "expense_categories"
    id = db.Column(db.Integer, primary_key=True)
    kind = db.Column(db.String(10), nullable=False)  # "fixed" | "variable"
    name = db.Column(db.String(120), nullable=False)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

    __table_args__ = (db.UniqueConstraint("kind", "name", name="uq_kind_name"),)


class ExpenseEntry(db.Model):
    __tablename__ = "expense_entries"
    id = db.Column(db.Integer, primary_key=True)
    business_day_id = db.Column(db.Integer, db.ForeignKey("business_days.id"), nullable=False)
    kind = db.Column(db.String(10), nullable=False)  # "fixed" | "variable"
    category_id = db.Column(db.Integer, db.ForeignKey("expense_categories.id"), nullable=False)
    amount = db.Column(db.Float, default=0.0)
    note = db.Column(db.Text, default="")
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

    category = db.relationship("ExpenseCategory")


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
    """complete si hay al menos 1 turno cerrado (permite medio día)."""
    if not bday:
        return
    ensure_shifts(bday)
    closed = [s for s in bday.shifts if bool(getattr(s, "is_closed", False))]
    bday.status = "complete" if len(closed) > 0 else "draft"


def day_totals(bday: BusinessDay) -> dict:
    """Si hay ExpenseEntry -> usa categorías. Si no -> usa legacy del Excel."""
    income = sum(s.income or 0 for s in bday.shifts)

    if bday.expenses and len(bday.expenses) > 0:
        var_exp = sum(e.amount or 0 for e in bday.expenses if e.kind == "variable")
        fix_exp = sum(e.amount or 0 for e in bday.expenses if e.kind == "fixed")
    else:
        var_exp = sum(s.variable_expense_total or 0 for s in bday.shifts)
        fix_exp = sum(s.fixed_expense_total or 0 for s in bday.shifts)

    exp_total = var_exp + fix_exp
    profit = income - exp_total
    return {
        "income": float(income),
        "variable_expense": float(var_exp),
        "fixed_expense": float(fix_exp),
        "expense_total": float(exp_total),
        "profit": float(profit),
    }


def margin_bucket(margin_pct):
    # 0-20 Malo, 21-30 Regular, 31+ Bueno
    if margin_pct is None:
        return ("—", "pill")
    if margin_pct <= 20:
        return ("Malo", "pill bad")
    if margin_pct <= 30:
        return ("Regular", "pill warn")
    return ("Bueno", "pill ok")


def ensure_admin():
    username = os.environ.get("OWNERS_ADMIN_USER", "admin")
    password = os.environ.get("OWNERS_ADMIN_PASS", "admin123")
    u = User.query.filter_by(username=username).first()
    if not u:
        u = User(username=username, password_hash=generate_password_hash(password), is_admin=True)
        db.session.add(u)
        db.session.commit()


def range_series(d1: date, d2: date):
    """
    Serie agregada por día (sin domingos).
    - Si un día tiene ExpenseEntry: usa esos gastos (var/fix).
    - Si no: usa ShiftRecord legacy.
    """
    exp_sub = (
        db.session.query(
            ExpenseEntry.business_day_id.label("bdid"),
            func.count(ExpenseEntry.id).label("cnt"),
            func.coalesce(
                func.sum(case((ExpenseEntry.kind == "variable", ExpenseEntry.amount), else_=0.0)), 0.0
            ).label("var_cat"),
            func.coalesce(
                func.sum(case((ExpenseEntry.kind == "fixed", ExpenseEntry.amount), else_=0.0)), 0.0
            ).label("fix_cat"),
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
        profit = income - exp_total
        out.append(
            {
                "date": r.day.isoformat(),
                "income": income,
                "variable_expense": var_exp,
                "fixed_expense": fix_exp,
                "expense_total": exp_total,
                "profit": profit,
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
    .grid6{ display:grid; grid-template-columns: repeat(3,minmax(0,1fr)); gap:12px; }
    .grid4{ display:grid; grid-template-columns: repeat(4,minmax(0,1fr)); gap:12px; }

    @media (max-width: 980px){
      .grid6{ grid-template-columns: repeat(2,minmax(0,1fr)); }
      .grid4{ grid-template-columns: repeat(2,minmax(0,1fr)); }
    }
    @media (max-width: 640px){
      .grid, .grid3, .grid6, .grid4{ grid-template-columns: 1fr; }
    }

    .kpi{ display:flex; flex-direction:column; gap:6px; border-radius: 16px; }
    .kpi .label{ color:var(--muted); font-size: 12px; text-transform: uppercase; letter-spacing:.04em; }
    .kpi .value{ font-size: 22px; font-weight: 800; }

    .kpi.income { background: var(--incomeBg); }
    .kpi.expense { background: var(--expenseBg); }
    .kpi.profit { background: var(--profitBg); }

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
    td.num{ text-align:right; font-variant-numeric: tabular-nums; }
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
    @media (max-width: 640px){ .chartbox{ height: 280px; } }

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
    summary{
      cursor:pointer;
      font-weight:800;
    }
  </style>
</head>
<body>
  <div class="wrap">

  {% if show_nav %}
  <div class="nav">
    <a href="{{ url_for('home') }}">Inicio</a>
    <a href="{{ url_for('dashboard_finanzas') }}">Gestión Financiera</a>
    <a href="{{ url_for('io_dashboard') }}">Gestión Ingresos y Gastos</a>
    <a href="{{ url_for('list_days') }}">Días</a>
    <a href="{{ url_for('import_balance_get') }}">Importar Balance</a>
    <a href="{{ url_for('export_get') }}">Exportar</a>
    <a href="{{ url_for('logout') }}">Salir</a>
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
</body>
</html>
"""


def render_page(body_html, **ctx):
    return render_template_string(BASE_HTML, body=body_html, **ctx)


# ----------------------------
# Auth
# ----------------------------
@app.get("/login")
def login_get():
    body = """
    <h1>Ingresar</h1>
    <div class="card">
      <form method="post" action="/login">
        <label>Usuario</label>
        <input name="username" autocomplete="username" />
        <div style="height:10px;"></div>
        <label>Contraseña</label>
        <input name="password" type="password" autocomplete="current-password" />
        <div style="height:12px;"></div>
        <button class="btn primary" type="submit">Entrar</button>
      </form>
      <p class="muted" style="margin-top:12px;">Default: admin / admin123</p>
    </div>
    """
    return render_page(body, show_nav=False)


@app.post("/login")
def login_post():
    username = (request.form.get("username") or "").strip()
    password = request.form.get("password") or ""

    u = User.query.filter_by(username=username).first()
    if not u or not check_password_hash(u.password_hash, password):
        flash("Usuario o contraseña incorrectos.", "error")
        return redirect(url_for("login_get"))

    login_user(u)
    return redirect(url_for("home"))


@app.get("/logout")
@login_required
def logout():
    logout_user()
    return redirect(url_for("login_get"))


# ----------------------------
# Home / Panel
# ----------------------------
@app.get("/")
@login_required
def root():
    return redirect(url_for("home"))


@app.get("/home")
@login_required
def home():
    body = """
    <h1>Panel</h1>
    <p class="muted">Elegí un módulo.</p>

    <div class="grid3">
      <div class="card">
        <h3>Gestión Financiera</h3>
        <p class="muted">Dashboard + gráficos + alertas.</p>
        <a class="btn primary" href="/finanzas">Entrar</a>
      </div>

      <div class="card">
        <h3>Gestión Ingresos y Gastos</h3>
        <p class="muted">Promedios, comparativas, ranking de categorías, trazabilidad.</p>
        <a class="btn primary" href="/io">Entrar</a>
      </div>

      <div class="card">
        <h3>Export / Backup</h3>
        <p class="muted">Exportar a Excel o JSON para resguardar datos.</p>
        <a class="btn primary" href="/export">Entrar</a>
      </div>
    </div>
    """
    return render_page(body, show_nav=True)


# ----------------------------
# Dashboard Finanzas
# ----------------------------
@app.get("/finanzas")
@login_required
def dashboard_finanzas():
    today = date.today()
    from_str = (request.args.get("from") or "").strip()
    to_str = (request.args.get("to") or "").strip()

    if from_str and to_str:
        try:
            d1 = parse_ymd(from_str)
            d2 = parse_ymd(to_str)
        except ValueError:
            d1, d2 = month_range(today)
            from_str, to_str = iso(d1), iso(d2)
    else:
        d1, d2 = month_range(today)
        from_str, to_str = iso(d1), iso(d2)

    if d1 > d2:
        d1, d2 = d2, d1
        from_str, to_str = iso(d1), iso(d2)

    series = range_series(d1, d2)

    income = sum(x["income"] for x in series)
    expense = sum(x["expense_total"] for x in series)
    profit = income - expense

    margen_periodo = (profit / income * 100.0) if income else None
    bucket_label, bucket_class = margin_bucket(margen_periodo)
    promedio_diario = (income / len(series)) if series else 0.0

    # Sueldo Ximena (gasto fijo) en el rango seleccionado
    sueldo_ximena = (
        db.session.query(func.coalesce(func.sum(ExpenseEntry.amount), 0.0))
        .join(ExpenseCategory, ExpenseCategory.id == ExpenseEntry.category_id)
        .join(BusinessDay, BusinessDay.id == ExpenseEntry.business_day_id)
        .filter(BusinessDay.day >= d1, BusinessDay.day <= d2)
        .filter(ExpenseEntry.kind == "fixed")
        .filter(func.lower(ExpenseCategory.name) == "sueldo ximena")
        .scalar()
        or 0.0
    )

    existing_days = {parse_ymd(x["date"]) for x in series}
    missing_days = [d for d in iter_workdays(d1, d2) if d not in existing_days]

    bar_labels, bar_income, bar_expense, bar_profit = [], [], [], []
    ranked = []
    for x in series:
        day_income = x["income"]
        day_exp = x["expense_total"]
        day_profit = x["profit"]
        m = (day_profit / day_income * 100.0) if day_income else None

        ranked.append(
            {
                "date_iso": x["date"],
                "date_ar": fmt_date_ar_from_iso(x["date"]),
                "income": day_income,
                "expense": day_exp,
                "profit": day_profit,
                "margin": m,
            }
        )

        bar_labels.append(fmt_date_ar_from_iso(x["date"]))
        bar_income.append(round(day_income, 2))
        bar_expense.append(round(day_exp, 2))
        bar_profit.append(round(day_profit, 2))

    ranked_sorted = sorted(ranked, key=lambda r: r["profit"])
    worst3 = ranked_sorted[:3]
    best3 = list(reversed(ranked_sorted[-3:]))

    ALERT_EXPENSE_THRESHOLD = 500_000
    alerts_clean = []
    for r in ranked:
        if r["expense"] > ALERT_EXPENSE_THRESHOLD:
            dday = parse_ymd(r["date_iso"])
            bday = BusinessDay.query.filter_by(day=dday).first()

            detail = ""
            if bday:
                if bday.expenses and len(bday.expenses) > 0:
                    parts = []
                    for e in sorted(bday.expenses, key=lambda x: x.amount or 0, reverse=True)[:6]:
                        parts.append(f"{e.category.name}: {ars(e.amount)}")
                    detail = " | ".join(parts)
                else:
                    parts = []
                    if (bday.note or "").strip():
                        parts.append((bday.note or "").strip())
                    for s in bday.shifts:
                        if (s.note or "").strip():
                            parts.append(f"{s.shift}: {(s.note or '').strip()}")
                    detail = " | ".join(parts).strip()

            if not detail:
                detail = "Sin detalle cargado."

            alerts_clean.append({"date_ar": fmt_date_ar(dday), "expense": r["expense"], "detail": detail})

    def rank_rows(items):
        if not items:
            return "<tr><td colspan='3' class='muted'>Sin datos</td></tr>"
        out = ""
        for rr in items:
            cls = "neg" if rr["profit"] < 0 else ""
            out += (
                "<tr>"
                f"<td>{rr['date_ar']}</td>"
                f"<td class='num'>{ars(rr['income'])}</td>"
                f"<td class='num {cls}'>{ars(rr['profit'])}</td>"
                "</tr>"
            )
        return out

    best_html = rank_rows(best3)
    worst_html = rank_rows(worst3)

    rows_html = ""
    for rr in ranked:
        mlabel, mclass = margin_bucket(rr["margin"])
        profit_cls = "neg" if rr["profit"] < 0 else ""
        rows_html += (
            "<tr>"
            f"<td><a href='/days/{rr['date_iso']}'>{rr['date_ar']}</a></td>"
            f"<td class='num'>{ars(rr['income'])}</td>"
            f"<td class='num'>{ars(rr['expense'])}</td>"
            f"<td class='num {profit_cls}'>{ars(rr['profit'])}</td>"
            f"<td class='num'><span class='{mclass}'>{mlabel}</span></td>"
            "</tr>"
        )
    if not rows_html:
        rows_html = "<tr><td colspan='5' class='muted'>No hay datos en el rango seleccionado.</td></tr>"

    if not alerts_clean:
        alerts_html = "<div class='muted'>Sin alertas (no hubo días con gastos mayores a $ 500.000).</div>"
    else:
        alerts_html = "<ul style='margin:0; padding-left:18px;'>"
        for a in alerts_clean[:50]:
            alerts_html += (
                f"<li><b>{a['date_ar']}</b> — Gastos: <b>{ars(a['expense'])}</b><br/>"
                f"<span class='muted'>{a['detail']}</span></li>"
            )
        alerts_html += "</ul>"

    # Torta del período:
    # - En números, Ingresos es el 100%.
    # - Gráficamente, mostramos "Ingresos" ocupando el 50% del gráfico.
    #   El otro 50% se reparte entre Gastos y Ganancia según su % sobre Ingresos.
    if income and income > 0 and profit >= 0:
        exp_ratio = max(expense, 0) / income
        prof_ratio = max(profit, 0) / income  # margen
        # exp_ratio + prof_ratio = 1 (salvo redondeos)
        pie_labels = ["Ingresos", "Gastos", "Ganancia"]
        pie_values = [1.0, exp_ratio, prof_ratio]  # Ingresos = 50% (porque total = 2)
    elif income and income > 0 and profit < 0:
        # Caso pérdida: mantenemos Ingresos como 50% y mostramos Gastos como 50%.
        # (El déficit se ve en el margen negativo / tablas.)
        pie_labels = ["Ingresos", "Gastos"]
        pie_values = [1.0, 1.0]
    else:
        pie_labels = ["Ingresos", "Gastos", "Ganancia"]
        pie_values = [0.0, 0.0, 0.0]
    charts_payload = {
        "bar": {"labels": bar_labels, "income": bar_income, "expense": bar_expense, "profit": bar_profit},
        "pie": {"labels": pie_labels, "values": pie_values},
    }
    charts_json = json.dumps(charts_payload, ensure_ascii=False)

    if missing_days:
        options_html = "".join(f"<option value='{iso(d)}'>{fmt_date_ar(d)}</option>" for d in missing_days)
    else:
        options_html = "<option value='' disabled selected>No hay días faltantes</option>"

    body = f"""
    <h1>Gestión Financiera</h1>

    <div class="card">
      <form method="get" action="/finanzas">
        <div class="row-actions">
          <div class="field">
            <label>Desde</label>
            <input type="date" name="from" value="{from_str}" />
          </div>
          <div class="field">
            <label>Hasta</label>
            <input type="date" name="to" value="{to_str}" />
          </div>
          <div style="min-width:160px;">
            <label>&nbsp;</label>
            <button class="btn primary" type="submit" style="width:100%;">Aplicar</button>
          </div>
        </div>
        <p class="muted" style="margin-top:10px;">Rango: {fmt_date_ar(d1)} a {fmt_date_ar(d2)} (Domingos excluidos)</p>
      </form>
    </div>

    <details>
      <summary>Completar día faltante (sin domingos)</summary>
      <form method="get" action="/days/go" style="margin-top:10px;">
        <div class="inline">
          <div class="field">
            <label>Día</label>
            <select name="day" {"disabled" if not missing_days else ""}>
              {options_html}
            </select>
          </div>
          <div style="min-width:180px;">
            <label>&nbsp;</label>
            <button class="btn primary" type="submit" style="width:100%;" {"disabled" if not missing_days else ""}>Crear / Completar</button>
          </div>
        </div>
      </form>
    </details>

    <div class="grid6">
      <div class="card kpi income">
        <div class="label">Ingresos</div>
        <div class="value">{ars(income)}</div>
      </div>
      <div class="card kpi expense">
        <div class="label">Gastos</div>
        <div class="value">{ars(expense)}</div>
      </div>
      <div class="card kpi profit">
        <div class="label">Ganancia</div>
        <div class="value">{ars(profit)}</div>
      </div>
      <div class="card kpi">
        <div class="label">Margen</div>
        <div class="value">{(f"{margen_periodo:.1f}%" if margen_periodo is not None else "—")}</div>
        <div style="margin-top:6px;"><span class="{bucket_class}">{bucket_label}</span></div>
      </div>
      <div class="card kpi">
        <div class="label">Promedio diario (Ingresos)</div>
        <div class="value">{ars(promedio_diario)}</div>
      </div>
      <div class="card kpi">
        <div class="label">Sueldo Ximena</div>
        <div class="value">{ars(sueldo_ximena)}</div>
        <div class="muted">Gasto fijo en el rango seleccionado</div>
      </div>
    </div>

    <div class="grid">
      <div class="card">
        <h3>Barras diarias: Ingresos / Gastos / Ganancia</h3>
        <div class="chartbox"><canvas id="barChart"></canvas></div>
      </div>
      <div class="card">
        <h3>Torta del período</h3>
        <div class="chartbox"><canvas id="pieChart"></canvas></div>
        <p class="muted" style="margin-top:10px;">(Domingos excluidos del cálculo)</p>
      </div>
    </div>

    <div class="grid">
      <div class="card">
        <h3>Top 3 mejores días (ganancia)</h3>
        <table>
          <thead><tr><th>Fecha</th><th class="num">Ingresos</th><th class="num">Ganancia</th></tr></thead>
          <tbody>{best_html}</tbody>
        </table>
      </div>
      <div class="card">
        <h3>Top 3 peores días (ganancia)</h3>
        <table>
          <thead><tr><th>Fecha</th><th class="num">Ingresos</th><th class="num">Ganancia</th></tr></thead>
          <tbody>{worst_html}</tbody>
        </table>
      </div>
    </div>

    <div class="card">
      <h3>Alertas (Gastos &gt; {ars(500000)})</h3>
      {alerts_html}
    </div>

    <div class="card">
      <h3>Serie del período</h3>
      <table>
        <thead>
          <tr>
            <th>Fecha</th>
            <th class="num">Ingresos</th>
            <th class="num">Gastos</th>
            <th class="num">Ganancia</th>
            <th class="num">Margen</th>
          </tr>
        </thead>
        <tbody>{rows_html}</tbody>
      </table>
    </div>

    <script>
      const payload = {charts_json};

      const shadowPlugin = {{
        id: 'shadowPlugin',
        beforeDatasetDraw(chart) {{
          const ctx = chart.ctx;
          ctx.save();
          ctx.shadowColor = 'rgba(0,0,0,0.14)';
          ctx.shadowBlur = 14;
          ctx.shadowOffsetX = 0;
          ctx.shadowOffsetY = 7;
        }},
        afterDatasetDraw(chart) {{
          chart.ctx.restore();
        }}
      }};

      const piePercentPlugin = {{
        id: 'piePercentPlugin',
        afterDatasetsDraw(chart) {{
          if (chart.config.type !== 'pie') return;
          const ctx = chart.ctx;
          const dataset = chart.data.datasets[0];
          const meta = chart.getDatasetMeta(0);
          const data = dataset.data || [];
          const total = data.reduce((a,b)=>a + (Number(b)||0), 0) || 1;

          ctx.save();
          ctx.font = '800 12px Arial';
          ctx.fillStyle = '#111827';
          ctx.textAlign = 'center';
          ctx.textBaseline = 'middle';

          meta.data.forEach((arc, i) => {{
            const v = Number(data[i] || 0);
            if (!v) return;
            const pct = (v / total) * 100;
            const label = pct.toFixed(1) + '%';

            const angle = (arc.startAngle + arc.endAngle) / 2;
            const r = arc.outerRadius * 0.70;
            const x = arc.x + Math.cos(angle) * r;
            const y = arc.y + Math.sin(angle) * r;
            ctx.fillText(label, x, y);
          }});
          ctx.restore();
        }}
      }};

      function makeBarGradient(ctx, baseColor) {{
        const g = ctx.createLinearGradient(0, 0, 0, 280);
        g.addColorStop(0, baseColor.replace('0.28', '0.45').replace('0.22','0.40'));
        g.addColorStop(1, baseColor.replace('0.28', '0.15').replace('0.22','0.12'));
        return g;
      }}

      const barCanvas = document.getElementById('barChart');
      if (barCanvas) {{
        const ctx = barCanvas.getContext('2d');
        const incomeBase = 'rgba(22,163,74,0.28)';
        const expenseBase = 'rgba(220,38,38,0.22)';
        const profitBase  = 'rgba(37,99,235,0.22)';

        new Chart(barCanvas, {{
          type: 'bar',
          data: {{
            labels: payload.bar.labels,
            datasets: [
              {{
                label: 'Ingresos',
                data: payload.bar.income,
                backgroundColor: makeBarGradient(ctx, incomeBase),
                borderColor: 'rgba(22,163,74,0.55)',
                borderWidth: 1,
                borderRadius: 12
              }},
              {{
                label: 'Gastos',
                data: payload.bar.expense,
                backgroundColor: makeBarGradient(ctx, expenseBase),
                borderColor: 'rgba(220,38,38,0.55)',
                borderWidth: 1,
                borderRadius: 12
              }},
              {{
                label: 'Ganancia',
                data: payload.bar.profit,
                backgroundColor: makeBarGradient(ctx, profitBase),
                borderColor: 'rgba(37,99,235,0.55)',
                borderWidth: 1,
                borderRadius: 12
              }}
            ]
          }},
          options: {{
            responsive: true,
            maintainAspectRatio: false,
            plugins: {{
              legend: {{ position: 'bottom' }},
              tooltip: {{
                callbacks: {{
                  label: function(context) {{
                    const v = context.raw || 0;
                    const s = Math.round(v).toString().replace(/\\B(?=(\\d{{3}})+(?!\\d))/g, ".");
                    return `${{context.dataset.label}}: $ ${{s}}`;
                  }}
                }}
              }}
            }},
            scales: {{
              y: {{ beginAtZero: true }}
            }}
          }},
          plugins: [shadowPlugin]
        }});
      }}

      const pieCanvas = document.getElementById('pieChart');
      if (pieCanvas) {{
        new Chart(pieCanvas, {{
          type: 'pie',
          data: {{
            labels: payload.pie.labels,
            datasets: [
              {{
                data: payload.pie.values,
                backgroundColor: [
                  'rgba(22,163,74,0.28)',
                  'rgba(220,38,38,0.22)',
                  'rgba(37,99,235,0.22)'
                ],
                borderColor: [
                  'rgba(22,163,74,0.55)',
                  'rgba(220,38,38,0.55)',
                  'rgba(37,99,235,0.55)'
                ],
                borderWidth: 1
              }}
            ]
          }},
          options: {{
            responsive: true,
            maintainAspectRatio: false,
            plugins: {{
              legend: {{ position: 'bottom' }}
            }}
          }},
          plugins: [shadowPlugin, piePercentPlugin]
        }});
      }}
    </script>
    """
    return render_page(body, show_nav=True)


# ----------------------------
# Gestión Ingresos y Gastos
# ----------------------------
@app.get("/io")
@login_required
def io_dashboard():
    today = date.today()
    d1s = (request.args.get("from") or "").strip()
    d2s = (request.args.get("to") or "").strip()

    compare_mode = (request.args.get("compare_mode") or "prev").strip()
    c1s = (request.args.get("cfrom") or "").strip()
    c2s = (request.args.get("cto") or "").strip()

    if d1s and d2s:
        try:
            d1 = parse_ymd(d1s)
            d2 = parse_ymd(d2s)
        except ValueError:
            d1, d2 = month_range(today)
            d1s, d2s = iso(d1), iso(d2)
    else:
        d1, d2 = month_range(today)
        d1s, d2s = iso(d1), iso(d2)

    if d1 > d2:
        d1, d2 = d2, d1
        d1s, d2s = iso(d1), iso(d2)

    series = range_series(d1, d2)
    income = sum(x["income"] for x in series)
    expense = sum(x["expense_total"] for x in series)
    profit = income - expense

    # promedios semanales (por semanas con datos)
    weekly = {}
    for x in series:
        d = parse_ymd(x["date"])
        yw = d.isocalendar()[:2]  # (year, week)
        weekly.setdefault(yw, {"income": 0.0, "expense": 0.0})
        weekly[yw]["income"] += x["income"]
        weekly[yw]["expense"] += x["expense_total"]

    weekly_rows = []
    for (y, w), v in sorted(weekly.items()):
        weekly_rows.append(
            {
                "label": f"{y}-W{w:02d}",
                "income": v["income"],
                "expense": v["expense"],
                "profit": v["income"] - v["expense"],
            }
        )

        avg_week_income = (sum(r["income"] for r in weekly_rows) / len(weekly_rows)) if weekly_rows else 0.0
    avg_week_expense = (sum(r["expense"] for r in weekly_rows) / len(weekly_rows)) if weekly_rows else 0.0
    avg_week_profit = (sum(r["profit"] for r in weekly_rows) / len(weekly_rows)) if weekly_rows else 0.0

    # promedio mensual dentro del rango
    monthly = {}
    for x in series:
        d = parse_ymd(x["date"])
        key = f"{d.year}-{d.month:02d}"
        monthly.setdefault(key, {"income": 0.0, "expense": 0.0})
        monthly[key]["income"] += x["income"]
        monthly[key]["expense"] += x["expense_total"]

    monthly_rows = [
        {"label": k, "income": v["income"], "expense": v["expense"], "profit": v["income"] - v["expense"]}
        for k, v in sorted(monthly.items())
    ]
    avg_month_income = (sum(r["income"] for r in monthly_rows) / len(monthly_rows)) if monthly_rows else 0.0
    avg_month_expense = (sum(r["expense"] for r in monthly_rows) / len(monthly_rows)) if monthly_rows else 0.0

    # gastos por categoría (solo ExpenseEntry)
    cat_rows = (
        db.session.query(
            ExpenseCategory.kind,
            ExpenseCategory.name,
            func.coalesce(func.sum(ExpenseEntry.amount), 0.0).label("total"),
        )
        .join(ExpenseEntry, ExpenseEntry.category_id == ExpenseCategory.id)
        .join(BusinessDay, BusinessDay.id == ExpenseEntry.business_day_id)
        .filter(BusinessDay.day >= d1, BusinessDay.day <= d2)
        .group_by(ExpenseCategory.kind, ExpenseCategory.name)
        .order_by(func.sum(ExpenseEntry.amount).desc())
        .all()
    )

    def _cat_row_html(r):
        kind = "Fijo" if r.kind == "fixed" else "Variable"
        return f"<tr><td>{kind}</td><td>{r.name}</td><td class='num'>{ars(r.total)}</td></tr>"

    if not cat_rows:
        cat_rank_html = (
            "<div class='muted'>"
            "No hay gastos por categorías en este rango (si venís de Excel, todavía no cargaste detalles por categoría)."
            "</div>"
        )
    else:
        top = cat_rows[:3]
        rest = cat_rows[3:50]

        top_html = "".join(_cat_row_html(r) for r in top)
        rest_html = "".join(_cat_row_html(r) for r in rest)

        cat_rank_html = """
        <table>
          <thead><tr><th>Tipo</th><th>Categoría</th><th class='num'>Total</th></tr></thead>
          <tbody>{top_html}</tbody>
        </table>
        """.format(top_html=top_html)

        if rest:
            cat_rank_html += """
            <details style="margin-top:10px;">
              <summary>Ver más</summary>
              <table style="margin-top:10px;">
                <thead><tr><th>Tipo</th><th>Categoría</th><th class='num'>Total</th></tr></thead>
                <tbody>{rest_html}</tbody>
              </table>
            </details>
            """.format(rest_html=rest_html)

    # trazabilidad mensual por categoría (Top 6)
    top_cats = [(r.kind, r.name) for r in cat_rows[:6]]
    trace = {}  # month -> {catName: total}

    # buscamos IDs sin tuple_()
    top_cat_objs = []
    for kind, name in top_cats:
        c = ExpenseCategory.query.filter_by(kind=kind, name=name).first()
        if c:
            top_cat_objs.append(c)

    top_cat_ids = [c.id for c in top_cat_objs]
    top_cat_names = {c.id: c.name for c in top_cat_objs}

    # Expresión "YYYY-MM" compatible con Postgres y SQLite
    dialect = db.engine.dialect.name  # "postgresql" o "sqlite"
    if dialect == "postgresql":
        ym_expr = func.to_char(BusinessDay.day, "YYYY-MM")
    else:
        ym_expr = func.strftime("%Y-%m", BusinessDay.day)

    rows_tr = []
    if top_cat_ids:
        rows_tr = (
            db.session.query(
                ym_expr.label("ym"),
                ExpenseEntry.category_id,
                func.coalesce(func.sum(ExpenseEntry.amount), 0.0).label("total"),
            )
            .join(BusinessDay, BusinessDay.id == ExpenseEntry.business_day_id)
            .filter(BusinessDay.day >= d1, BusinessDay.day <= d2)
            .filter(ExpenseEntry.category_id.in_(top_cat_ids))
            .group_by(ym_expr, ExpenseEntry.category_id)
            .order_by(ym_expr)
            .all()
        )

    for r in rows_tr:
        trace.setdefault(r.ym, {})
        trace[r.ym][top_cat_names.get(r.category_id, str(r.category_id))] = float(r.total or 0.0)

    trace_months = sorted(trace.keys())
    trace_labels = trace_months
    trace_datasets = []
    for cid in top_cat_ids:
        name = top_cat_names[cid]
        data = []
        for m in trace_months:
            data.append(trace.get(m, {}).get(name, 0.0))
        trace_datasets.append({"label": name, "data": data})

    # comparativa
    if compare_mode == "custom" and c1s and c2s:
        try:
            cd1 = parse_ymd(c1s)
            cd2 = parse_ymd(c2s)
        except ValueError:
            cd1, cd2 = period_previous(d1, d2)
            c1s, c2s = iso(cd1), iso(cd2)
    else:
        cd1, cd2 = period_previous(d1, d2)
        c1s, c2s = iso(cd1), iso(cd2)

    if cd1 > cd2:
        cd1, cd2 = cd2, cd1
        c1s, c2s = iso(cd1), iso(cd2)

    cseries = range_series(cd1, cd2)
    cincome = sum(x["income"] for x in cseries)
    cexpense = sum(x["expense_total"] for x in cseries)
    cprofit = cincome - cexpense

    def delta(a, b):
        return a - b

    def delta_pct(a, b):
        if b == 0:
            return None
        return (a - b) / b * 100.0

    di = delta(income, cincome)
    de = delta(expense, cexpense)
    dp = delta(profit, cprofit)

    dip = delta_pct(income, cincome)
    dep = delta_pct(expense, cexpense)
    dpp = delta_pct(profit, cprofit)

    def fmt_pct(x):
        if x is None:
            return "—"
        return f"{x:+.1f}%"

    wk_html = ""
    if not weekly_rows:
        wk_html = "<tr><td colspan='4' class='muted'>Sin datos</td></tr>"
    else:
        for r in weekly_rows[-14:]:
            wk_html += (
                f"<tr><td>{r['label']}</td>"
                f"<td class='num'>{ars(r['income'])}</td>"
                f"<td class='num'>{ars(r['expense'])}</td>"
                f"<td class='num'>{ars(r['profit'])}</td></tr>"
            )

    mo_html = ""
    if not monthly_rows:
        mo_html = "<tr><td colspan='4' class='muted'>Sin datos</td></tr>"
    else:
        for r in monthly_rows:
            mo_html += (
                f"<tr><td>{r['label']}</td>"
                f"<td class='num'>{ars(r['income'])}</td>"
                f"<td class='num'>{ars(r['expense'])}</td>"
                f"<td class='num'>{ars(r['profit'])}</td></tr>"
            )

    trace_payload = {"labels": trace_labels, "datasets": trace_datasets}
    trace_json = json.dumps(trace_payload, ensure_ascii=False)

    body = f"""
    <h1>Gestión de Ingresos y Gastos</h1>

    <div class="card">
      <form method="get" action="/io">
        <div class="row-actions">
          <div class="field">
            <label>Desde</label>
            <input type="date" name="from" value="{d1s}" />
          </div>
          <div class="field">
            <label>Hasta</label>
            <input type="date" name="to" value="{d2s}" />
          </div>

          <div class="field">
            <label>Comparar contra</label>
            <select name="compare_mode">
              <option value="prev" {"selected" if compare_mode=="prev" else ""}>Período anterior (mismo largo)</option>
              <option value="custom" {"selected" if compare_mode=="custom" else ""}>Rango personalizado</option>
            </select>
          </div>

          <div class="field">
            <label>Comparar Desde</label>
            <input type="date" name="cfrom" value="{c1s}" />
          </div>
          <div class="field">
            <label>Comparar Hasta</label>
            <input type="date" name="cto" value="{c2s}" />
          </div>

          <div style="min-width:160px;">
            <label>&nbsp;</label>
            <button class="btn primary" type="submit" style="width:100%;">Aplicar</button>
          </div>
        </div>

        <p class="muted" style="margin-top:10px;">
          Rango: {fmt_date_ar(d1)} a {fmt_date_ar(d2)} (Domingos excluidos).
          Comparación: {fmt_date_ar(cd1)} a {fmt_date_ar(cd2)}.
        </p>
      </form>
    </div>

    <div class="grid6">
      <div class="card kpi income">
        <div class="label">Ingresos (rango)</div>
        <div class="value">{ars(income)}</div>
      </div>
      <div class="card kpi expense">
        <div class="label">Gastos (rango)</div>
        <div class="value">{ars(expense)}</div>
      </div>
      <div class="card kpi profit">
        <div class="label">Ganancia (rango)</div>
        <div class="value">{ars(profit)}</div>
      </div>

      <div class="card kpi">
        <div class="label">Promedio semanal (ingresos)</div>
        <div class="value">{ars(avg_week_income)}</div>
      </div>
      <div class="card kpi">
        <div class="label">Promedio semanal (gastos)</div>
        <div class="value">{ars(avg_week_expense)}</div>
      </div>
      <div class="card kpi">
        <div class="label">Promedio semanal (ganancia)</div>
        <div class="value">{ars(avg_week_profit)}</div>
        <div class="muted">En semanas con data</div>
      </div>
    </div>

    <div class="grid">
      <div class="card">
        <h3>Comparativa vs período elegido</h3>
        <table>
          <thead>
            <tr>
              <th>Métrica</th>
              <th class="num">Actual</th>
              <th class="num">Comparación</th>
              <th class="num">Δ</th>
              <th class="num">Δ%</th>
            </tr>
          </thead>
          <tbody>
            <tr>
              <td>Ingresos</td>
              <td class="num">{ars(income)}</td>
              <td class="num">{ars(cincome)}</td>
              <td class="num">{ars(di)}</td>
              <td class="num">{fmt_pct(dip)}</td>
            </tr>
            <tr>
              <td>Gastos</td>
              <td class="num">{ars(expense)}</td>
              <td class="num">{ars(cexpense)}</td>
              <td class="num">{ars(de)}</td>
              <td class="num">{fmt_pct(dep)}</td>
            </tr>
            <tr>
              <td>Ganancia</td>
              <td class="num">{ars(profit)}</td>
              <td class="num">{ars(cprofit)}</td>
              <td class="num">{ars(dp)}</td>
              <td class="num">{fmt_pct(dpp)}</td>
            </tr>
          </tbody>
        </table>
      </div>

      <div class="card">
        <h3>Ranking de categorías (gastos)</h3>
        {cat_rank_html}
        <p class="muted" style="margin-top:10px;">
          Nota: solo aparece si cargaste gastos con categorías (no alcanza con el Excel legacy).
        </p>
      </div>
    </div>

    <div class="grid">
      <div class="card">
        <h3>Ingresos/Gastos por semana (últimas 14 semanas con data)</h3>
        <table>
          <thead><tr><th>Semana</th><th class="num">Ingresos</th><th class="num">Gastos</th><th class="num">Ganancia</th></tr></thead>
          <tbody>{wk_html}</tbody>
        </table>
      </div>

      <div class="card">
        <h3>Ingresos/Gastos por mes (en rango)</h3>
        <table>
          <thead><tr><th>Mes</th><th class="num">Ingresos</th><th class="num">Gastos</th><th class="num">Ganancia</th></tr></thead>
          <tbody>{mo_html}</tbody>
        </table>
      </div>
    </div>

    <div class="card">
      <h3>Trazabilidad mensual (Top categorías)</h3>
      <div class="chartbox"><canvas id="traceChart"></canvas></div>
      <p class="muted" style="margin-top:10px;">
        Muestra hasta 6 categorías más “pesadas” del rango, mes a mes.
      </p>
    </div>

    <script>
      const trace = {trace_json};

      const shadowPlugin = {{
        id: 'shadowPlugin',
        beforeDatasetDraw(chart) {{
          const ctx = chart.ctx;
          ctx.save();
          ctx.shadowColor = 'rgba(0,0,0,0.12)';
          ctx.shadowBlur = 10;
          ctx.shadowOffsetX = 0;
          ctx.shadowOffsetY = 5;
        }},
        afterDatasetDraw(chart) {{
          chart.ctx.restore();
        }}
      }};

      function fmtMoney(v){{
        const n = Math.round(v||0);
        const s = n.toString().replace(/\\B(?=(\\d{{3}})+(?!\\d))/g, ".");
        return "$ " + s;
      }}

      const tc = document.getElementById("traceChart");
      if (tc) {{
        new Chart(tc, {{
          type: 'line',
          data: {{
            labels: trace.labels,
            datasets: trace.datasets.map((ds, idx) => {{
              return {{
                label: ds.label,
                data: ds.data,
                tension: 0.25,
                fill: false,
                borderWidth: 2,
                pointRadius: 3
              }}
            }})
          }},
          options: {{
            responsive: true,
            maintainAspectRatio: false,
            plugins: {{
              legend: {{ position: 'bottom' }},
              tooltip: {{
                callbacks: {{
                  label: function(ctx) {{
                    return `${{ctx.dataset.label}}: ${{fmtMoney(ctx.raw)}}`;
                  }}
                }}
              }}
            }},
            scales: {{
              y: {{
                beginAtZero: true,
                ticks: {{
                  callback: function(value){{ return fmtMoney(value); }}
                }}
              }}
            }}
          }},
          plugins: [shadowPlugin]
        }});
      }}
    </script>
    """
    return render_page(body, show_nav=True)


# ----------------------------
# Export (Excel + JSON)
# ----------------------------
def build_export_data(d1: date, d2: date):
    days = (
        BusinessDay.query.filter(BusinessDay.day >= d1, BusinessDay.day <= d2)
        .order_by(BusinessDay.day.asc())
        .all()
    )

    out_days = []
    out_shifts = []
    out_expenses = []
    out_categories = []

    cats = ExpenseCategory.query.order_by(ExpenseCategory.kind.asc(), ExpenseCategory.name.asc()).all()
    for c in cats:
        out_categories.append(
            {
                "id": c.id,
                "kind": c.kind,
                "name": c.name,
                "created_at": c.created_at.isoformat() if c.created_at else None,
            }
        )

    for d in days:
        if is_sunday(d.day):
            continue
        ensure_shifts(d)
        recalc_day_status(d)
        t = day_totals(d)

        out_days.append(
            {
                "date": d.day.isoformat(),
                "status": d.status,
                "note": d.note or "",
                "income": t["income"],
                "variable_expense": t["variable_expense"],
                "fixed_expense": t["fixed_expense"],
                "expense_total": t["expense_total"],
                "profit": t["profit"],
            }
        )

        for s in d.shifts:
            out_shifts.append(
                {
                    "date": d.day.isoformat(),
                    "shift": s.shift,
                    "income": float(s.income or 0),
                    "note": s.note or "",
                    "is_closed": bool(s.is_closed),
                    "legacy_variable_expense_total": float(s.variable_expense_total or 0),
                    "legacy_fixed_expense_total": float(s.fixed_expense_total or 0),
                }
            )

        for e in d.expenses:
            out_expenses.append(
                {
                    "date": d.day.isoformat(),
                    "kind": e.kind,
                    "category_id": e.category_id,
                    "category_name": e.category.name if e.category else None,
                    "amount": float(e.amount or 0),
                    "note": e.note or "",
                    "created_at": e.created_at.isoformat() if e.created_at else None,
                }
            )

    return {
        "range": {"from": d1.isoformat(), "to": d2.isoformat()},
        "generated_at": datetime.utcnow().isoformat(),
        "days": out_days,
        "shifts": out_shifts,
        "expenses": out_expenses,
        "categories": out_categories,
    }


def export_to_excel(data: dict) -> BytesIO:
    wb = openpyxl.Workbook()
    ws_sum = wb.active
    ws_sum.title = "Summary"

    d1 = data["range"]["from"]
    d2 = data["range"]["to"]

    days = data["days"]
    total_income = sum(d["income"] for d in days)
    total_exp = sum(d["expense_total"] for d in days)
    total_profit = total_income - total_exp

    ws_sum.append(["Rango", f"{d1} a {d2}"])
    ws_sum.append(["Ingresos", total_income])
    ws_sum.append(["Gastos", total_exp])
    ws_sum.append(["Ganancia", total_profit])
    ws_sum.append(["Días (sin domingos)", len(days)])
    ws_sum.append([])
    ws_sum.append(["Nota", "Excel numérico (sin formato pesos). El backup real para reimport es el JSON."])

    ws_days = wb.create_sheet("Days")
    ws_days.append(["Fecha", "Estado", "Nota", "Ingresos", "Gasto variable", "Gasto fijo", "Gasto total", "Ganancia"])
    for d in days:
        ws_days.append(
            [
                d["date"],
                d["status"],
                d["note"],
                d["income"],
                d["variable_expense"],
                d["fixed_expense"],
                d["expense_total"],
                d["profit"],
            ]
        )

    ws_exp = wb.create_sheet("Expenses")
    ws_exp.append(["Fecha", "Tipo", "Categoría", "Monto", "Nota", "Creado"])
    for e in data["expenses"]:
        ws_exp.append([e["date"], e["kind"], e.get("category_name"), e["amount"], e["note"], e.get("created_at")])

    ws_cat = wb.create_sheet("Categories")
    ws_cat.append(["ID", "Tipo", "Nombre", "Creado"])
    for c in data["categories"]:
        ws_cat.append([c["id"], c["kind"], c["name"], c.get("created_at")])

    bio = BytesIO()
    wb.save(bio)
    bio.seek(0)
    return bio


@app.get("/export")
@login_required
def export_get():
    today = date.today()
    d1, d2 = month_range(today)

    body = f"""
    <h1>Exportar / Backup</h1>

    <div class="card">
      <form method="get" action="/export/download">
        <div class="row-actions">
          <div class="field">
            <label>Desde</label>
            <input type="date" name="from" value="{iso(d1)}" />
          </div>
          <div class="field">
            <label>Hasta</label>
            <input type="date" name="to" value="{iso(d2)}" />
          </div>
          <div class="field">
            <label>Formato</label>
            <select name="fmt">
              <option value="json">JSON (reimportable / backup real)</option>
              <option value="xlsx">Excel (lectura humana)</option>
            </select>
          </div>
          <div style="min-width:180px;">
            <label>&nbsp;</label>
            <button class="btn primary" type="submit" style="width:100%;">Descargar</button>
          </div>
        </div>
        <p class="muted" style="margin-top:10px;">
          Recomendación: guardá el JSON siempre. El Excel es para mirar.
        </p>
      </form>
    </div>
    """
    return render_page(body, show_nav=True)


@app.get("/export/download")
@login_required
def export_download():
    fmt = (request.args.get("fmt") or "json").strip().lower()
    d1s = (request.args.get("from") or "").strip()
    d2s = (request.args.get("to") or "").strip()

    if not d1s or not d2s:
        flash("Falta rango de fechas.", "error")
        return redirect(url_for("export_get"))

    try:
        d1 = parse_ymd(d1s)
        d2 = parse_ymd(d2s)
    except ValueError:
        flash("Fechas inválidas.", "error")
        return redirect(url_for("export_get"))

    if d1 > d2:
        d1, d2 = d2, d1

    data = build_export_data(d1, d2)

    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    base_name = f"owners_export_{d1.isoformat()}_{d2.isoformat()}_{stamp}"

    if fmt == "xlsx":
        bio = export_to_excel(data)
        return send_file(
            bio,
            mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            as_attachment=True,
            download_name=f"{base_name}.xlsx",
        )

    bio = BytesIO(json.dumps(data, ensure_ascii=False, indent=2).encode("utf-8"))
    return send_file(
        bio,
        mimetype="application/json",
        as_attachment=True,
        download_name=f"{base_name}.json",
    )


# ----------------------------
# Días (CRUD + categorías)
# ----------------------------
@app.get("/days/go")
@login_required
def days_go():
    day = (request.args.get("day") or "").strip()
    if not day:
        return redirect(url_for("dashboard_finanzas"))
    return redirect(url_for("edit_day", day=day))


@app.get("/days")
@login_required
def list_days():
    days = BusinessDay.query.order_by(BusinessDay.day.desc()).limit(180).all()

    trs = ""
    for d in days:
        if is_sunday(d.day):
            continue
        ensure_shifts(d)
        recalc_day_status(d)
        totals = day_totals(d)

        status_pill = "<span class='pill ok'>complete</span>" if d.status == "complete" else "<span class='pill warn'>draft</span>"
        profit_cls = "neg" if totals["profit"] < 0 else ""

        trs += (
            f"<tr>"
            f"<td><a href='/days/{d.day}'>{fmt_date_ar(d.day)}</a></td>"
            f"<td class='num'>{ars(totals['income'])}</td>"
            f"<td class='num'>{ars(totals['expense_total'])}</td>"
            f"<td class='num {profit_cls}'>{ars(totals['profit'])}</td>"
            f"<td>{status_pill}</td>"
            f"</tr>"
        )

    if not trs:
        trs = "<tr><td colspan='5' class='muted'>Todavía no cargaste ningún día.</td></tr>"

    body = f"""
    <h1>Días</h1>
    <div class="card">
      <table>
        <thead>
          <tr>
            <th>Fecha</th>
            <th class="num">Ingresos</th>
            <th class="num">Gastos</th>
            <th class="num">Ganancia</th>
            <th>Estado</th>
          </tr>
        </thead>
        <tbody>{trs}</tbody>
      </table>
    </div>
    """
    db.session.commit()
    return render_page(body, show_nav=True)


@app.get("/days/<day>")
@login_required
def edit_day(day):
    try:
        d = parse_ymd(day)
    except ValueError:
        flash("Fecha inválida.", "error")
        return redirect(url_for("list_days"))

    if is_sunday(d):
        flash("Domingo: no se trabaja. No se crea día.", "error")
        return redirect(url_for("dashboard_finanzas"))

    bday = BusinessDay.query.filter_by(day=d).first()
    if not bday:
        bday = BusinessDay(day=d, note="", status="draft")
        db.session.add(bday)
        db.session.flush()
        ensure_shifts(bday)
        db.session.commit()

    ensure_shifts(bday)
    recalc_day_status(bday)
    db.session.commit()

    var_cats = ExpenseCategory.query.filter_by(kind="variable").order_by(ExpenseCategory.name.asc()).all()
    fix_cats = ExpenseCategory.query.filter_by(kind="fixed").order_by(ExpenseCategory.name.asc()).all()

    var_options = "".join(f"<option value='{c.id}'>{c.name}</option>" for c in var_cats) or "<option value='' disabled selected>Sin categorías</option>"
    fix_options = "".join(f"<option value='{c.id}'>{c.name}</option>" for c in fix_cats) or "<option value='' disabled selected>Sin categorías</option>"

    var_rows = ""
    fix_rows = ""
    for e in sorted(bday.expenses, key=lambda x: x.created_at, reverse=True):
        row = (
            "<tr>"
            f"<td>{e.category.name}</td>"
            f"<td class='num'>{ars(e.amount)}</td>"
            f"<td>{(e.note or '')}</td>"
            f"<td class='num'><form method='post' action='/days/{bday.day}/expense/{e.id}/delete' style='margin:0;'><button class='btn' type='submit'>Borrar</button></form></td>"
            "</tr>"
        )
        if e.kind == "variable":
            var_rows += row
        else:
            fix_rows += row

    if not var_rows:
        var_rows = "<tr><td colspan='4' class='muted'>Todavía no cargaste gastos variables.</td></tr>"
    if not fix_rows:
        fix_rows = "<tr><td colspan='4' class='muted'>Todavía no cargaste gastos fijos.</td></tr>"

    shifts = {s.shift: s for s in bday.shifts}
    totals = day_totals(bday)

    def v(sh, field):
        s = shifts.get(sh)
        return str(getattr(s, field) or 0) if s else "0"

    def n(sh):
        s = shifts.get(sh)
        return (s.note or "") if s else ""

    def c(sh):
        s = shifts.get(sh)
        return "checked" if (s and bool(getattr(s, "is_closed", False))) else ""

    body = f"""
    <h1>Editar día {fmt_date_ar(bday.day)}</h1>

    <div class="card">
      <form method="post" action="/days/{bday.day}/save">
        <label>Nota del día</label>
        <textarea name="note">{bday.note or ""}</textarea>

        <div class="grid" style="margin-top:12px;">
          <div class="card">
            <h3>Mañana</h3>
            <label><input type="checkbox" name="Mañana_closed" {c("Mañana")}> Turno cerrado</label>
            <div style="height:10px;"></div>

            <label>Ingreso</label>
            <input name="Mañana_income" value="{v("Mañana","income")}" />
            <div style="height:10px;"></div>

            <label>Nota turno</label>
            <textarea name="Mañana_note">{n("Mañana")}</textarea>
          </div>

          <div class="card">
            <h3>Tarde</h3>
            <label><input type="checkbox" name="Tarde_closed" {c("Tarde")}> Turno cerrado</label>
            <div style="height:10px;"></div>

            <label>Ingreso</label>
            <input name="Tarde_income" value="{v("Tarde","income")}" />
            <div style="height:10px;"></div>

            <label>Nota turno</label>
            <textarea name="Tarde_note">{n("Tarde")}</textarea>
          </div>
        </div>

        <div style="height:12px;"></div>
        <button class="btn primary" type="submit">Guardar</button>
      </form>
    </div>

    <div class="grid">
      <div class="card">
        <div style="display:flex; justify-content:space-between; align-items:center; gap:10px; flex-wrap:wrap;"><h3 style="margin:0;">Gastos Variables</h3><a class="btn" href="/categories/manage?kind=variable&day={bday.day}">Editar categorías</a></div>

        <form method="post" action="/categories/add" class="inline" style="margin-bottom:10px;">
          <input type="hidden" name="day" value="{bday.day}" />
          <input type="hidden" name="kind" value="variable" />
          <div class="field">
            <label>Nueva categoría (variable)</label>
            <input name="name" placeholder="Ej: Cangiano, Verdulería, Harina..." />
          </div>
          <div style="min-width:180px;">
            <label>&nbsp;</label>
            <button class="btn" type="submit" style="width:100%;">Agregar categoría</button>
          </div>
        </form>

        <form method="post" action="/days/{bday.day}/expense/add" class="inline">
          <input type="hidden" name="kind" value="variable" />
          <div class="field">
            <label>Categoría</label>
            <select name="category_id" {"disabled" if not var_cats else ""}>
              {var_options}
            </select>
          </div>
          <div class="field">
            <label>Monto</label>
            <input name="amount" placeholder="Ej: 250000" />
          </div>
          <div class="field">
            <label>Nota (opcional)</label>
            <input name="note" placeholder="Ej: Factura 0001-..." />
          </div>
          <div style="min-width:180px;">
            <label>&nbsp;</label>
            <button class="btn primary" type="submit" style="width:100%;" {"disabled" if not var_cats else ""}>Agregar gasto</button>
          </div>
        </form>

        <div style="height:10px;"></div>
        <table>
          <thead><tr><th>Categoría</th><th class="num">Monto</th><th>Nota</th><th class="num">Acción</th></tr></thead>
          <tbody>{var_rows}</tbody>
        </table>
      </div>

      <div class="card">
        <div style="display:flex; justify-content:space-between; align-items:center; gap:10px; flex-wrap:wrap;"><h3 style="margin:0;">Gastos Fijos</h3><a class="btn" href="/categories/manage?kind=fixed&day={bday.day}">Editar categorías</a></div>

        <form method="post" action="/categories/add" class="inline" style="margin-bottom:10px;">
          <input type="hidden" name="day" value="{bday.day}" />
          <input type="hidden" name="kind" value="fixed" />
          <div class="field">
            <label>Nueva categoría (fijo)</label>
            <input name="name" placeholder="Ej: Alquiler, Sueldo Paula, Impuestos..." />
          </div>
          <div style="min-width:180px;">
            <label>&nbsp;</label>
            <button class="btn" type="submit" style="width:100%;">Agregar categoría</button>
          </div>
        </form>

        <form method="post" action="/days/{bday.day}/expense/add" class="inline">
          <input type="hidden" name="kind" value="fixed" />
          <div class="field">
            <label>Categoría</label>
            <select name="category_id" {"disabled" if not fix_cats else ""}>
              {fix_options}
            </select>
          </div>
          <div class="field">
            <label>Monto</label>
            <input name="amount" placeholder="Ej: 500000" />
          </div>
          <div class="field">
            <label>Nota (opcional)</label>
            <input name="note" placeholder="Ej: Mes Febrero" />
          </div>
          <div style="min-width:180px;">
            <label>&nbsp;</label>
            <button class="btn primary" type="submit" style="width:100%;" {"disabled" if not fix_cats else ""}>Agregar gasto</button>
          </div>
        </form>

        <div style="height:10px;"></div>
        <table>
          <thead><tr><th>Categoría</th><th class="num">Monto</th><th>Nota</th><th class="num">Acción</th></tr></thead>
          <tbody>{fix_rows}</tbody>
        </table>
      </div>
    </div>

    <div class="card">
      <h3>Totales del día</h3>
      <div class="grid4">
        <div class="kpi income" style="padding:14px;">
          <div class="label">Ingresos</div>
          <div class="value">{ars(totals["income"])}</div>
        </div>
        <div class="kpi expense" style="padding:14px;">
          <div class="label">Gasto total</div>
          <div class="value">{ars(totals["expense_total"])}</div>
          <div class="muted">Variable: {ars(totals["variable_expense"])} · Fijo: {ars(totals["fixed_expense"])}</div>
        </div>
        <div class="kpi profit" style="padding:14px;">
          <div class="label">Ganancia</div>
          <div class="value">{ars(totals["profit"])}</div>
        </div>
        <div class="kpi" style="padding:14px;">
          <div class="label">Estado</div>
          <div class="value"><span class="pill {'ok' if bday.status=='complete' else 'warn'}">{bday.status}</span></div>
        </div>
      </div>
    </div>
    """
    return render_page(body, show_nav=True)


@app.post("/days/<day>/save")
@login_required
def save_day(day):
    d = parse_ymd(day)
    if is_sunday(d):
        flash("Domingo: no se trabaja. No se guarda día.", "error")
        return redirect(url_for("dashboard_finanzas"))

    bday = BusinessDay.query.filter_by(day=d).first()
    if not bday:
        flash("Día no encontrado.", "error")
        return redirect(url_for("list_days"))

    bday.note = (request.form.get("note") or "").strip()

    ensure_shifts(bday)

    for sh in ("Mañana", "Tarde"):
        sr = ShiftRecord.query.filter_by(business_day_id=bday.id, shift=sh).first()
        if not sr:
            sr = ShiftRecord(business_day=bday, shift=sh)
            db.session.add(sr)

        sr.income = safe_float(request.form.get(f"{sh}_income"))
        sr.note = (request.form.get(f"{sh}_note") or "").strip()
        sr.is_closed = True if request.form.get(f"{sh}_closed") == "on" else False

    recalc_day_status(bday)
    db.session.commit()

    flash("Guardado.", "ok")
    return redirect(url_for("edit_day", day=day))


@app.post("/categories/add")
@login_required
def add_category():
    kind = (request.form.get("kind") or "").strip().lower()
    name = (request.form.get("name") or "").strip()
    day = (request.form.get("day") or "").strip()

    if kind not in ("fixed", "variable"):
        flash("Tipo de categoría inválido.", "error")
        return redirect(url_for("dashboard_finanzas"))

    if not name:
        flash("Poné un nombre de categoría.", "error")
        return redirect(url_for("edit_day", day=day)) if day else redirect(url_for("dashboard_finanzas"))

    clean = re.sub(r"\s+", " ", name).strip()

    existing = ExpenseCategory.query.filter_by(kind=kind, name=clean).first()
    if existing:
        flash("Esa categoría ya existe.", "error")
    else:
        db.session.add(ExpenseCategory(kind=kind, name=clean))
        db.session.commit()
        flash("Categoría agregada.", "ok")

    if day:
        return redirect(url_for("edit_day", day=day))
    return redirect(url_for("dashboard_finanzas"))



# ----------------------------
# Categorías (Administración)
# ----------------------------
@app.get("/categories/manage")
@login_required
def manage_categories():
    kind = (request.args.get("kind") or "").strip().lower()
    day = (request.args.get("day") or "").strip()

    if kind not in ("fixed", "variable"):
        flash("Tipo de categoría inválido.", "error")
        return redirect(url_for("dashboard_finanzas"))

    cats = ExpenseCategory.query.filter_by(kind=kind).order_by(ExpenseCategory.name.asc()).all()

    # Conteo de uso por categoría (para no borrar si está en uso)
    counts = dict(
        db.session.query(ExpenseEntry.category_id, func.count(ExpenseEntry.id))
        .group_by(ExpenseEntry.category_id)
        .all()
    )

    kind_label = "Fijas" if kind == "fixed" else "Variables"

    rows = ""
    for c in cats:
        used = int(counts.get(c.id, 0))
        disabled = "disabled" if used > 0 else ""
        disabled_class = "disabled" if used > 0 else ""
        rows += f"""
        <tr>
          <td style="width:40%;">
            <form method="post" action="/categories/{c.id}/rename" class="inline" style="margin:0;">
              <input type="hidden" name="kind" value="{kind}" />
              <input type="hidden" name="day" value="{day}" />
              <div class="field" style="min-width:260px;">
                <input name="name" value="{c.name}" />
              </div>
              <div style="min-width:140px;">
                <button class="btn" type="submit" style="width:100%;">Guardar</button>
              </div>
            </form>
          </td>
          <td class="num" style="width:10%;">{used}</td>
          <td class="num" style="width:20%;">
            <form method="post" action="/categories/{c.id}/delete" style="margin:0;">
              <input type="hidden" name="kind" value="{kind}" />
              <input type="hidden" name="day" value="{day}" />
              <button class="btn {disabled_class}" type="submit" {disabled}>Borrar</button>
            </form>
          </td>
        </tr>
        """

    if not rows:
        rows = "<tr><td colspan='3' class='muted'>No hay categorías cargadas.</td></tr>"

    back_url = url_for("edit_day", day=day) if day else url_for("dashboard_finanzas")

    body = f"""
    <h1>Categorías {kind_label}</h1>
    <p class="muted">Podés renombrar. Borrar solo si no tiene gastos asociados (Uso = 0).</p>

    <div class="card">
      <a class="btn" href="{back_url}">Volver</a>
    </div>

    <div class="card">
      <table>
        <thead><tr><th>Nombre</th><th class="num">Uso</th><th class="num">Acción</th></tr></thead>
        <tbody>{rows}</tbody>
      </table>
    </div>
    """
    return render_page(body, show_nav=True)


@app.post("/categories/<int:cid>/rename")
@login_required
def rename_category(cid):
    kind = (request.form.get("kind") or "").strip().lower()
    day = (request.form.get("day") or "").strip()
    name = (request.form.get("name") or "").strip()

    c = db.session.get(ExpenseCategory, cid)
    if not c:
        flash("Categoría no encontrada.", "error")
        return redirect(url_for("manage_categories", kind=kind, day=day))

    if kind not in ("fixed", "variable"):
        kind = c.kind

    if not name:
        flash("El nombre no puede estar vacío.", "error")
        return redirect(url_for("manage_categories", kind=kind, day=day))

    clean = re.sub(r"\s+", " ", name).strip()

    exists = ExpenseCategory.query.filter_by(kind=c.kind, name=clean).first()
    if exists and exists.id != c.id:
        flash("Ya existe una categoría con ese nombre.", "error")
        return redirect(url_for("manage_categories", kind=c.kind, day=day))

    c.name = clean
    db.session.commit()
    flash("Categoría actualizada.", "ok")
    return redirect(url_for("manage_categories", kind=c.kind, day=day))


@app.post("/categories/<int:cid>/delete")
@login_required
def delete_category(cid):
    kind = (request.form.get("kind") or "").strip().lower()
    day = (request.form.get("day") or "").strip()

    c = db.session.get(ExpenseCategory, cid)
    if not c:
        flash("Categoría no encontrada.", "error")
        return redirect(url_for("manage_categories", kind=kind, day=day))

    used = (
        db.session.query(func.count(ExpenseEntry.id))
        .filter(ExpenseEntry.category_id == c.id)
        .scalar()
        or 0
    )
    if used > 0:
        flash("No se puede borrar: la categoría tiene gastos asociados.", "error")
        return redirect(url_for("manage_categories", kind=c.kind, day=day))

    db.session.delete(c)
    db.session.commit()
    flash("Categoría borrada.", "ok")
    return redirect(url_for("manage_categories", kind=c.kind, day=day))


@app.post("/days/<day>/expense/add")
@login_required
def add_expense(day):
    d = parse_ymd(day)
    if is_sunday(d):
        flash("Domingo: no se trabaja.", "error")
        return redirect(url_for("dashboard_finanzas"))

    bday = BusinessDay.query.filter_by(day=d).first()
    if not bday:
        bday = BusinessDay(day=d, note="", status="draft")
        db.session.add(bday)
        db.session.flush()
        ensure_shifts(bday)
        db.session.commit()

    kind = (request.form.get("kind") or "").strip().lower()
    cat_id = (request.form.get("category_id") or "").strip()
    amt = (request.form.get("amount") or "").strip()
    note = (request.form.get("note") or "").strip()

    if kind not in ("fixed", "variable"):
        flash("Tipo de gasto inválido.", "error")
        return redirect(url_for("edit_day", day=day))

    if not cat_id:
        flash("Elegí una categoría.", "error")
        return redirect(url_for("edit_day", day=day))

    try:
        amount = safe_float(amt) if amt else 0.0
    except ValueError:
        flash("Monto inválido.", "error")
        return redirect(url_for("edit_day", day=day))

    if amount <= 0:
        flash("El monto debe ser mayor a 0.", "error")
        return redirect(url_for("edit_day", day=day))

    cat = db.session.get(ExpenseCategory, int(cat_id))
    if not cat or cat.kind != kind:
        flash("Categoría inválida.", "error")
        return redirect(url_for("edit_day", day=day))

    db.session.add(
        ExpenseEntry(
            business_day_id=bday.id,
            kind=kind,
            category_id=cat.id,
            amount=amount,
            note=note,
        )
    )
    db.session.commit()

    flash("Gasto agregado.", "ok")
    return redirect(url_for("edit_day", day=day))


@app.post("/days/<day>/expense/<int:eid>/delete")
@login_required
def delete_expense(day, eid):
    e = db.session.get(ExpenseEntry, eid)
    if e:
        db.session.delete(e)
        db.session.commit()
        flash("Gasto borrado.", "ok")
    return redirect(url_for("edit_day", day=day))


# ----------------------------
# Import Excel (mantenido)
# ----------------------------
def _to_float_money(x) -> float:
    if x is None:
        return 0.0
    if isinstance(x, (int, float)):
        return float(x)
    s = str(x).strip()
    if not s:
        return 0.0
    s = s.replace("$", "").replace(" ", "")
    s = re.sub(r"[^0-9,.\-]", "", s)
    if not s:
        return 0.0

    if "," in s and "." in s:
        if s.rfind(".") > s.rfind(","):
            s = s.replace(",", "")
        else:
            s = s.replace(".", "").replace(",", ".")
        return float(s)

    if "," in s:
        if re.match(r"^-?\d{1,3}(,\d{3})+$", s):
            return float(s.replace(",", ""))
        return float(s.replace(",", "."))

    if "." in s:
        if re.match(r"^-?\d{1,3}(\.\d{3})+$", s):
            return float(s.replace(".", ""))

    return float(s)


def _norm_shift(s: str) -> str:
    s = (s or "").strip().lower()
    if s.startswith("ma"):
        return "Mañana"
    if s.startswith("ta"):
        return "Tarde"
    return (s.title() if s else "")


def _parse_date_cell(x):
    if isinstance(x, datetime):
        return x.date()
    if isinstance(x, date):
        return x
    s = str(x).strip()
    for fmt in ("%d/%m/%Y", "%d-%m-%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            pass
    raise ValueError(f"Fecha inválida en Excel: {x}")


def _find_header_map(ws):
    return 2, 3, 4, 5, 6


def import_balance_excel(filepath: str, sheet_names: list[str], mode: str = "skip") -> dict:
    wb = openpyxl.load_workbook(filepath, data_only=True)
    imported = 0
    skipped = 0
    replaced = 0

    for sname in sheet_names:
        if sname not in wb.sheetnames:
            continue
        ws = wb[sname]
        col_date, col_shift, col_income, col_var, col_fix = _find_header_map(ws)

        last_date = None
        for r in range(3, ws.max_row + 1):
            raw_date = ws.cell(r, col_date).value
            raw_shift = ws.cell(r, col_shift).value
            if raw_shift in (None, ""):
                continue

            if raw_date in (None, ""):
                if last_date is None:
                    continue
                d = last_date
            else:
                try:
                    d = _parse_date_cell(raw_date)
                    last_date = d
                except ValueError:
                    continue

            if is_sunday(d):
                continue

            shift = _norm_shift(str(raw_shift or ""))
            if shift not in ("Mañana", "Tarde"):
                continue

            income = _to_float_money(ws.cell(r, col_income).value)
            var_exp = _to_float_money(ws.cell(r, col_var).value)
            fix_exp = _to_float_money(ws.cell(r, col_fix).value)

            if income == 0 and var_exp == 0 and fix_exp == 0:
                continue

            bday = BusinessDay.query.filter_by(day=d).first()
            if not bday:
                bday = BusinessDay(day=d, note="", status="draft")
                db.session.add(bday)
                db.session.flush()
                ensure_shifts(bday)

            sr = ShiftRecord.query.filter_by(business_day_id=bday.id, shift=shift).first()
            if sr and mode == "skip":
                skipped += 1
                continue

            if not sr:
                sr = ShiftRecord(business_day=bday, shift=shift)
                db.session.add(sr)

            if sr.id and mode == "replace":
                replaced += 1
            else:
                imported += 1

            sr.income = income
            sr.variable_expense_total = var_exp
            sr.fixed_expense_total = fix_exp
            sr.is_closed = True
            recalc_day_status(bday)

    db.session.commit()
    return {"imported": imported, "replaced": replaced, "skipped": skipped}


@app.get("/import/balance")
@login_required
def import_balance_get():
    body = """
    <h1>Importar Balance Diario</h1>
    <div class="card">
      <form method="post" action="/import/balance" enctype="multipart/form-data">
        <label>Archivo Excel (Balance Diario 2026)</label>
        <input type="file" name="file" accept=".xlsx" required />

        <div style="height:12px;"></div>
        <label>Hojas a importar</label><br/>
        <div style="height:6px;"></div>
        <label><input type="checkbox" name="sheets" value="Enero_26" checked /> Enero_26</label><br/>
        <label><input type="checkbox" name="sheets" value="Febrero_26" checked /> Febrero_26</label>

        <div style="height:12px;"></div>
        <label>Modo</label>
        <select name="mode">
          <option value="skip">No tocar existentes (skip)</option>
          <option value="replace">Reemplazar existentes (replace)</option>
        </select>

        <div style="height:12px;"></div>
        <button class="btn primary" type="submit">Importar</button>
      </form>

      <p class="muted" style="margin-top:10px;">
        Nota: este import carga los totales legacy (sin categorías). Si querés categorías: cargalas en “Días”.
      </p>
    </div>
    """
    return render_page(body, show_nav=True)


@app.post("/import/balance")
@login_required
def import_balance_post():
    f = request.files.get("file")
    if not f:
        flash("No se recibió archivo.", "error")
        return redirect(url_for("import_balance_get"))

    sheets = request.form.getlist("sheets")
    mode = (request.form.get("mode") or "skip").strip()

    if not sheets:
        flash("Seleccioná al menos una hoja (Enero_26 / Febrero_26).", "error")
        return redirect(url_for("import_balance_get"))

    uploads_dir = os.path.join(INSTANCE_DIR, "uploads")
    os.makedirs(uploads_dir, exist_ok=True)
    save_path = os.path.join(uploads_dir, f.filename)
    f.save(save_path)

    try:
        result = import_balance_excel(save_path, sheets, mode=mode)
    except Exception as e:
        flash(f"Error importando: {e}", "error")
        return redirect(url_for("import_balance_get"))

    flash(
        f"Import OK — nuevos: {result['imported']}, reemplazados: {result['replaced']}, salteados: {result['skipped']}",
        "ok",
    )
    return redirect(url_for("dashboard_finanzas"))


# ----------------------------
# API simple
# ----------------------------
@app.get("/api/dashboard")
@login_required
def api_dashboard():
    f = request.args.get("from")
    t = request.args.get("to")
    if not f or not t:
        return jsonify({"error": "params from/to required"}), 400
    d1 = parse_ymd(f)
    d2 = parse_ymd(t)
    series = range_series(d1, d2)
    return jsonify({"from": f, "to": t, "series": series})


# ----------------------------
# Init DB (import-time, funciona con Gunicorn)
# ----------------------------
with app.app_context():
    db.create_all()
    ensure_admin()


# ----------------------------
# Main (solo local)
# ----------------------------
if __name__ == "__main__":
    with app.app_context():
        db.create_all()
        ensure_admin()
    app.run(host="127.0.0.1", port=5001, debug=True)