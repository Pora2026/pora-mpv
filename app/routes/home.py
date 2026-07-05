from flask import Blueprint, redirect, url_for
from flask_login import login_required

home_bp = Blueprint("home_bp", __name__)


def _render_page(*args, **kwargs):
    from app_owners import render_page
    return render_page(*args, **kwargs)


@home_bp.get("/")
@login_required
def root():
    return redirect(url_for("home_bp.home"))


@home_bp.get("/home")
@login_required
def home():
    body = """
    <h1>Panel</h1>
    <p class="muted">Elegí un módulo.</p>

    <div class="grid3">
      <div class="card">
        <h3>Panel Central</h3>
        <p class="muted">Dashboard + gráficos + alertas + ganancia real.</p>
        <a class="btn primary" href="/finanzas">Entrar</a>
      </div>

      <div class="card">
        <h3>Gestión Ingresos y Gastos</h3>
        <p class="muted">Promedios, comparativas, ranking de categorías, trazabilidad.</p>
        <a class="btn primary" href="/io">Entrar</a>
      </div>

      <div class="card">
        <h3>Backup completo</h3>
        <p class="muted">Descargar un respaldo completo y versionado de todos los datos.</p>
        <a class="btn primary" href="/backup">Entrar</a>
      </div>
    </div>
    """
    return _render_page(body, show_nav=True)
