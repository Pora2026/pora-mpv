from flask import Blueprint, request, redirect, url_for, flash
from flask_login import login_user, logout_user, login_required
from werkzeug.security import check_password_hash

from app.models import User

auth_bp = Blueprint("auth_bp", __name__)


def _render_page(*args, **kwargs):
    from app_owners import render_page
    return render_page(*args, **kwargs)


@auth_bp.get("/login")
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
    return _render_page(body, show_nav=False)


@auth_bp.post("/login")
def login_post():
    username = (request.form.get("username") or "").strip()
    password = request.form.get("password") or ""

    u = User.query.filter_by(username=username).first()
    if not u or not check_password_hash(u.password_hash, password):
        flash("Usuario o contraseña incorrectos.", "error")
        return redirect(url_for("auth_bp.login_get"))

    login_user(u)
    return redirect(url_for("home_bp.home"))


@auth_bp.get("/logout")
@login_required
def logout():
    logout_user()
    return redirect(url_for("auth_bp.login_get"))
