import os

BASE_DIR = os.path.abspath(os.path.dirname(os.path.dirname(__file__)))
INSTANCE_DIR = os.path.join(BASE_DIR, "instance")
os.makedirs(INSTANCE_DIR, exist_ok=True)

DB_PATH = os.path.join(INSTANCE_DIR, "owners.db")
SQLITE_URI = "sqlite:///" + DB_PATH


class Config:
    SECRET_KEY = os.environ.get("SECRET_KEY", "dev-change-me")
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