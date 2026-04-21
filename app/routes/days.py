from flask import Blueprint, request, redirect, url_for, flash
from flask_login import login_required
from sqlalchemy import func

from app.extensions import db
from app.models import BusinessDay, ShiftRecord, ExpenseCategory, ExpenseEntry
from app.utils.money import safe_float, ars
from app.utils.dates import is_sunday, parse_ymd, fmt_date_ar


days_bp = Blueprint("days_bp", __name__)


def _owners():
    from app_owners import render_page, ensure_shifts, recalc_day_status, day_totals, margin_bucket
    return render_page, ensure_shifts, recalc_day_status, day_totals, margin_bucket


def _money_input(v):
    return "" if v is None else str(float(v))


@days_bp.get("/days/go")
@login_required
def days_go():
    day = (request.args.get("day") or "").strip()
    if not day:
        return redirect(url_for("dashboard_bp.dashboard_finanzas"))
    return redirect(url_for("days_bp.edit_day", day=day))


@days_bp.get("/days")
@login_required
def list_days():
    render_page, ensure_shifts, recalc_day_status, day_totals, margin_bucket = _owners()

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

        m = (totals["profit"] / totals["income"] * 100.0) if totals["income"] else None
        mlabel, mclass = margin_bucket(m)

        trs += (
            f"<tr>"
            f"<td><a href='/days/{d.day}'>{fmt_date_ar(d.day)}</a></td>"
            f"<td class='num'>{ars(totals['income'])}</td>"
            f"<td class='num'>{ars(totals['expense_total'])}</td>"
            f"<td class='num {profit_cls}'>{ars(totals['profit'])}</td>"
            f"<td>{status_pill}</td>"
            f"<td><span class='{mclass}'>{mlabel}</span></td>"
            f"</tr>"
        )

    if not trs:
        trs = "<tr><td colspan='6' class='muted'>Todavía no cargaste ningún día.</td></tr>"

    body = f"""
    <h1>Días</h1>

    <div class="card">
      <h3>Listado de días cargados</h3>
      <table>
        <thead>
          <tr>
            <th>Fecha</th>
            <th class="num">Ingresos</th>
            <th class="num">Gastos</th>
            <th class="num">Ganancia</th>
            <th>Estado</th>
            <th>Estado del día</th>
          </tr>
        </thead>
        <tbody>{trs}</tbody>
      </table>
    </div>
    """
    db.session.commit()
    return render_page(body, show_nav=True)


@days_bp.get("/days/<day>")
@login_required
def edit_day(day):
    render_page, ensure_shifts, recalc_day_status, day_totals, _ = _owners()

    try:
        d = parse_ymd(day)
    except ValueError:
        flash("Fecha inválida.", "error")
        return redirect(url_for("days_bp.list_days"))

    if is_sunday(d):
        flash("Domingo: no se trabaja. No se crea día.", "error")
        return redirect(url_for("dashboard_bp.dashboard_finanzas"))

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

    real_cash = getattr(bday, "real_cash_profit", None)
    real_digital = getattr(bday, "real_digital_profit", None)
    real_apps = getattr(bday, "real_apps_pending", None)

    if real_cash is None and getattr(bday, "real_profit", None) is not None:
        real_cash = float(bday.real_profit)
    if real_digital is None:
        real_digital = 0.0 if getattr(bday, "real_profit", None) is not None else None

    body = f"""
    <h1>Editar día {fmt_date_ar(bday.day)}</h1>

    <div class="card">
      <h3>Bloque 1 · Datos generales del día</h3>
      <form method="post" action="/days/{bday.day}/save">
        <label>Nota del día</label>
        <textarea name="note">{bday.note or ""}</textarea>

        <div class="grid" style="margin-top:12px;">
          <div class="card">
            <h3>Turno Mañana</h3>
            <label><input type="checkbox" name="Mañana_closed" {c("Mañana")}> Turno cerrado</label>
            <div style="height:10px;"></div>

            <label>Ingreso</label>
            <input name="Mañana_income" value="{v("Mañana","income")}" />
            <div style="height:10px;"></div>

            <label>Nota turno</label>
            <textarea name="Mañana_note">{n("Mañana")}</textarea>
          </div>

          <div class="card">
            <h3>Turno Tarde</h3>
            <label><input type="checkbox" name="Tarde_closed" {c("Tarde")}> Turno cerrado</label>
            <div style="height:10px;"></div>

            <label>Ingreso</label>
            <input name="Tarde_income" value="{v("Tarde","income")}" />
            <div style="height:10px;"></div>

            <label>Nota turno</label>
            <textarea name="Tarde_note">{n("Tarde")}</textarea>
          </div>
        </div>

        <div class="card" style="margin:12px 0 0;">
          <h3>Bloque 2 · Ganancia real y desfase explicado</h3>
          <p class="muted" style="margin-top:0;">
            La ganancia real representa lo efectivamente cobrado. “Apps pendientes” sirve para explicar parte del desfase contra la calculada, sin sumarlo a caja real.
          </p>

          <div class="grid3">
            <div>
              <label>Ganancia en efectivo</label>
              <input name="real_cash_profit" value="{_money_input(real_cash)}" placeholder="Ej: 250000" />
            </div>

            <div>
              <label>Ganancia digital</label>
              <input name="real_digital_profit" value="{_money_input(real_digital)}" placeholder="Ej: 180000" />
            </div>

            <div>
              <label>Apps pendientes (PY + Rappi)</label>
              <input name="real_apps_pending" value="{_money_input(real_apps)}" placeholder="Ej: 95000" />
            </div>
            
            <div>
              <label>Apps cobradas</label>
              <input name="real_apps_collected" value="{_money_input(getattr(bday, 'real_apps_collected', None))}" placeholder="Ej: 120000" />
            </div>
          </div>
        </div>

        <div style="height:12px;"></div>
        <button class="btn primary" type="submit">Guardar</button>
      </form>
    </div>

    <div class="grid">
      <div class="card">
        <h3>Bloque 3 · Gastos variables</h3>
        <div style="display:flex; justify-content:space-between; align-items:center; gap:10px; flex-wrap:wrap;">
          <a class="btn" href="/categories/manage?kind=variable&day={bday.day}">Administrar / editar categorías</a>
        </div>

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
        <h3>Bloque 4 · Gastos fijos</h3>
        <div style="display:flex; justify-content:space-between; align-items:center; gap:10px; flex-wrap:wrap;">
          <a class="btn" href="/categories/manage?kind=fixed&day={bday.day}">Administrar / editar categorías</a>
        </div>

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
      <h3>Bloque 5 · Totales del día</h3>
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
          <div class="label">Ganancia (calculada)</div>
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


@days_bp.post("/days/<day>/save")
@login_required
def save_day(day):
    _, ensure_shifts, recalc_day_status, day_totals, _ = _owners()

    d = parse_ymd(day)
    if is_sunday(d):
        flash("Domingo: no se trabaja. No se guarda día.", "error")
        return redirect(url_for("dashboard_bp.dashboard_finanzas"))

    bday = BusinessDay.query.filter_by(day=d).first()
    if not bday:
        flash("Día no encontrado.", "error")
        return redirect(url_for("days_bp.list_days"))

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

    cash_raw = (request.form.get("real_cash_profit") or "").strip()
    digital_raw = (request.form.get("real_digital_profit") or "").strip()
    apps_raw = (request.form.get("real_apps_pending") or "").strip()
    apps_collected_raw = (request.form.get("real_apps_collected") or "").strip()
    bday.real_apps_collected = None if apps_collected_raw == "" else safe_float(apps_collected_raw)

    bday.real_cash_profit = None if cash_raw == "" else safe_float(cash_raw)
    bday.real_digital_profit = None if digital_raw == "" else safe_float(digital_raw)
    bday.real_apps_pending = None if apps_raw == "" else safe_float(apps_raw)

    if (
        bday.real_cash_profit is not None
        or bday.real_digital_profit is not None
        or bday.real_apps_collected is not None
    ):
        bday.real_profit = (
            float(bday.real_cash_profit or 0.0)
            + float(bday.real_digital_profit or 0.0)
            + float(bday.real_apps_collected or 0.0)
        )
    else:
        t = day_totals(bday)
        bday.real_profit = float(t["profit"])

    recalc_day_status(bday)
    db.session.commit()

    flash("Guardado.", "ok")
    return redirect(url_for("days_bp.edit_day", day=day))


@days_bp.post("/categories/add")
@login_required
def add_category():
    kind = (request.form.get("kind") or "").strip().lower()
    name = (request.form.get("name") or "").strip()
    day = (request.form.get("day") or "").strip()

    if kind not in ("fixed", "variable"):
        flash("Tipo de categoría inválido.", "error")
        return redirect(url_for("dashboard_bp.dashboard_finanzas"))

    if not name:
        flash("Poné un nombre de categoría.", "error")
        return redirect(url_for("days_bp.edit_day", day=day)) if day else redirect(url_for("dashboard_bp.dashboard_finanzas"))

    clean = " ".join(name.split())

    existing = ExpenseCategory.query.filter_by(kind=kind, name=clean).first()
    if existing:
        flash("Esa categoría ya existe.", "error")
    else:
        db.session.add(ExpenseCategory(kind=kind, name=clean))
        db.session.commit()
        flash("Categoría agregada.", "ok")

    if day:
        return redirect(url_for("days_bp.edit_day", day=day))
    return redirect(url_for("dashboard_bp.dashboard_finanzas"))


@days_bp.get("/categories/manage")
@login_required
def manage_categories():
    render_page, _, _, _, _ = _owners()

    kind = (request.args.get("kind") or "").strip().lower()
    day = (request.args.get("day") or "").strip()

    if kind not in ("fixed", "variable"):
        flash("Tipo de categoría inválido.", "error")
        return redirect(url_for("dashboard_bp.dashboard_finanzas"))

    cats = ExpenseCategory.query.filter_by(kind=kind).order_by(ExpenseCategory.name.asc()).all()

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

        merge_options = "".join(
            f"<option value='{other.id}'>{other.name}</option>"
            for other in cats
            if other.id != c.id
        )
        merge_disabled = "disabled" if not merge_options else ""

        rows += f"""
        <tr>
          <td style="vertical-align:top; width:34%;">
            <form method="post" action="/categories/{c.id}/rename" class="inline" style="margin:0;">
              <input type="hidden" name="kind" value="{kind}" />
              <input type="hidden" name="day" value="{day}" />
              <div class="field" style="min-width:260px;">
                <input name="name" value="{c.name}" />
              </div>
              <div style="min-width:140px;">
                <button class="btn" type="submit" style="width:100%;">Guardar nombre</button>
              </div>
            </form>
          </td>

          <td class="num" style="vertical-align:top; width:8%;">{used}</td>

          <td style="vertical-align:top; width:38%;">
            <form method="post" action="/categories/{c.id}/merge" class="inline" style="margin:0;">
              <input type="hidden" name="kind" value="{kind}" />
              <input type="hidden" name="day" value="{day}" />
              <div class="field" style="min-width:220px;">
                <select name="target_id" {merge_disabled}>
                  <option value="" disabled selected>Fusionar en...</option>
                  {merge_options}
                </select>
              </div>
              <div style="min-width:150px;">
                <button class="btn" type="submit" style="width:100%;" {merge_disabled}>Fusionar</button>
              </div>
            </form>
          </td>

          <td class="num" style="vertical-align:top; width:20%;">
            <form method="post" action="/categories/{c.id}/delete" style="margin:0;">
              <input type="hidden" name="kind" value="{kind}" />
              <input type="hidden" name="day" value="{day}" />
              <button class="btn {disabled_class}" type="submit" {disabled}>Borrar</button>
            </form>
          </td>
        </tr>
        """

    if not rows:
        rows = "<tr><td colspan='4' class='muted'>No hay categorías cargadas.</td></tr>"

    back_url = url_for("days_bp.edit_day", day=day) if day else url_for("dashboard_bp.dashboard_finanzas")

    body = f"""
    <h1>Categorías {kind_label}</h1>
    <p class="muted">
      Podés renombrar, fusionar y borrar. Borrar solo si no tiene gastos asociados (Uso = 0).
      Si una categoría está mal escrita y ya tiene movimientos, usá <b>Fusionar</b>.
    </p>

    <div class="card">
      <a class="btn" href="{back_url}">Volver</a>
    </div>

    <div class="card">
      <table>
        <thead>
          <tr>
            <th>Nombre</th>
            <th class="num">Uso</th>
            <th>Fusionar en</th>
            <th class="num">Acción</th>
          </tr>
        </thead>
        <tbody>{rows}</tbody>
      </table>
    </div>
    """
    return render_page(body, show_nav=True)


@days_bp.post("/categories/<int:cid>/rename")
@login_required
def rename_category(cid):
    day = (request.form.get("day") or "").strip()
    name = (request.form.get("name") or "").strip()

    c = db.session.get(ExpenseCategory, cid)
    if not c:
        flash("Categoría no encontrada.", "error")
        return redirect(url_for("days_bp.manage_categories", kind="fixed", day=day))

    if not name:
        flash("El nombre no puede estar vacío.", "error")
        return redirect(url_for("days_bp.manage_categories", kind=c.kind, day=day))

    clean = " ".join(name.split())

    exists = ExpenseCategory.query.filter_by(kind=c.kind, name=clean).first()
    if exists and exists.id != c.id:
        flash("Ya existe una categoría con ese nombre. Usá Fusionar para unificarlas.", "error")
        return redirect(url_for("days_bp.manage_categories", kind=c.kind, day=day))

    c.name = clean
    db.session.commit()
    flash("Categoría actualizada.", "ok")
    return redirect(url_for("days_bp.manage_categories", kind=c.kind, day=day))


@days_bp.post("/categories/<int:cid>/merge")
@login_required
def merge_category(cid):
    kind = (request.form.get("kind") or "").strip().lower()
    day = (request.form.get("day") or "").strip()
    target_id_raw = (request.form.get("target_id") or "").strip()

    src = db.session.get(ExpenseCategory, cid)
    if not src:
        flash("Categoría origen no encontrada.", "error")
        return redirect(url_for("days_bp.manage_categories", kind=kind, day=day))

    if not target_id_raw:
        flash("Elegí la categoría destino para fusionar.", "error")
        return redirect(url_for("days_bp.manage_categories", kind=src.kind, day=day))

    try:
        target_id = int(target_id_raw)
    except ValueError:
        flash("Destino inválido.", "error")
        return redirect(url_for("days_bp.manage_categories", kind=src.kind, day=day))

    if target_id == src.id:
        flash("No podés fusionar una categoría consigo misma.", "error")
        return redirect(url_for("days_bp.manage_categories", kind=src.kind, day=day))

    target = db.session.get(ExpenseCategory, target_id)
    if not target:
        flash("Categoría destino no encontrada.", "error")
        return redirect(url_for("days_bp.manage_categories", kind=src.kind, day=day))

    if src.kind != target.kind:
        flash("Solo se pueden fusionar categorías del mismo tipo.", "error")
        return redirect(url_for("days_bp.manage_categories", kind=src.kind, day=day))

    db.session.query(ExpenseEntry).filter(ExpenseEntry.category_id == src.id).update(
        {ExpenseEntry.category_id: target.id},
        synchronize_session=False,
    )

    db.session.delete(src)
    db.session.commit()

    flash(f"Categoría fusionada en '{target.name}'.", "ok")
    return redirect(url_for("days_bp.manage_categories", kind=target.kind, day=day))


@days_bp.post("/categories/<int:cid>/delete")
@login_required
def delete_category(cid):
    kind = (request.form.get("kind") or "").strip().lower()
    day = (request.form.get("day") or "").strip()

    c = db.session.get(ExpenseCategory, cid)
    if not c:
        flash("Categoría no encontrada.", "error")
        return redirect(url_for("days_bp.manage_categories", kind=kind, day=day))

    used = db.session.query(func.count(ExpenseEntry.id)).filter(ExpenseEntry.category_id == c.id).scalar() or 0
    if used > 0:
        flash("No se puede borrar: la categoría tiene gastos asociados. Usá Fusionar si querés unificarla con otra.", "error")
        return redirect(url_for("days_bp.manage_categories", kind=c.kind, day=day))

    db.session.delete(c)
    db.session.commit()
    flash("Categoría borrada.", "ok")
    return redirect(url_for("days_bp.manage_categories", kind=c.kind, day=day))


@days_bp.post("/days/<day>/expense/add")
@login_required
def add_expense(day):
    _, ensure_shifts, _, _, _ = _owners()

    d = parse_ymd(day)
    if is_sunday(d):
        flash("Domingo: no se trabaja.", "error")
        return redirect(url_for("dashboard_bp.dashboard_finanzas"))

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
        return redirect(url_for("days_bp.edit_day", day=day))

    if not cat_id:
        flash("Elegí una categoría.", "error")
        return redirect(url_for("days_bp.edit_day", day=day))

    amount = safe_float(amt) if amt else 0.0
    if amount <= 0:
        flash("El monto debe ser mayor a 0.", "error")
        return redirect(url_for("days_bp.edit_day", day=day))

    cat = db.session.get(ExpenseCategory, int(cat_id))
    if not cat or cat.kind != kind:
        flash("Categoría inválida.", "error")
        return redirect(url_for("days_bp.edit_day", day=day))

    db.session.add(ExpenseEntry(business_day_id=bday.id, kind=kind, category_id=cat.id, amount=amount, note=note))
    db.session.commit()

    flash("Gasto agregado.", "ok")
    return redirect(url_for("days_bp.edit_day", day=day))


@days_bp.post("/days/<day>/expense/<int:eid>/delete")
@login_required
def delete_expense(day, eid):
    e = db.session.get(ExpenseEntry, eid)
    if e:
        db.session.delete(e)
        db.session.commit()
        flash("Gasto borrado.", "ok")
    return redirect(url_for("days_bp.edit_day", day=day))