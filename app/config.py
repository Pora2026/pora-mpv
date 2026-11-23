import os

BASE_DIR = os.path.abspath(os.path.dirname(os.path.dirname(__file__)))
INSTANCE_DIR = os.path.join(BASE_DIR, "instance")
os.makedirs(INSTANCE_DIR, exist_ok=True)

DB_PATH = os.path.join(INSTANCE_DIR, "owners.db")
SQLITE_URI = "sqlite:///" + DB_PATH


class Config:
    SECRET_KEY = os.environ.get("SECRET_KEY", "dev-change-me")
    # Lista cerrada para el MVP. Debe coincidir con los usernames reales.
    ARANDU_ALLOWED_USERS = os.environ.get("ARANDU_ALLOWED_USERS", "Bernardo,Ximena")
    # Inicio de la estructura actual de gastos; lo anterior no es confiable
    # para análisis gerenciales comparables.
    ARANDU_ANALYSIS_START_DATE = os.environ.get(
        "ARANDU_ANALYSIS_START_DATE", "2026-03-01"
    )
    ARANDU_OPENAI_MODEL = os.environ.get("ARANDU_OPENAI_MODEL", "gpt-5.6-sol")
    ARANDU_WEB_SEARCH_ENABLED = (
        os.environ.get("ARANDU_WEB_SEARCH_ENABLED", "true").lower() == "true"
    )
    # Umbrales iniciales de screening; generan revisión, nunca una conclusión.
    ARANDU_SALES_DROP_ALERT_PCT = float(
        os.environ.get("ARANDU_SALES_DROP_ALERT_PCT", "15")
    )
    ARANDU_EXPENSE_INCREASE_ALERT_PCT = float(
        os.environ.get("ARANDU_EXPENSE_INCREASE_ALERT_PCT", "30")
    )
    ARANDU_EXPENSE_INCREASE_ALERT_AMOUNT = float(
        os.environ.get("ARANDU_EXPENSE_INCREASE_ALERT_AMOUNT", "50000")
    )
    CAJA_SYNC_TOKEN = os.environ.get("CAJA_SYNC_TOKEN")
    # Protección histórica: CAJA sólo puede sincronizar desde esta fecha.
    CAJA_SYNC_START_DATE = os.environ.get("CAJA_SYNC_START_DATE", "2026-08-13")
    SQLALCHEMY_TRACK_MODIFICATIONS = False

    DATABASE_URL = os.environ.get("DATABASE_URL")

    if DATABASE_URL:
        SQLALCHEMY_DATABASE_URI = DATABASE_URL.replace("postgres://", "postgresql://")
        SQLALCHEMY_ENGINE_OPTIONS = {
            "pool_pre_ping": True,
            "pool_recycle": 280,
            "pool_timeout": 30,
        }
    else:
        SQLALCHEMY_DATABASE_URI = SQLITE_URI
        SQLALCHEMY_ENGINE_OPTIONS = {}
