import os
from pathlib import Path
from sqlmodel import SQLModel, create_engine

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError(
        "DATABASE_URL is not configured. Please set a MySQL connection string before starting the server."
    )
if DATABASE_URL.startswith("sqlite"):
    raise RuntimeError("SQLite connections are no longer supported. Point DATABASE_URL to a MySQL instance.")

engine = create_engine(
    DATABASE_URL,
    echo=False,
)


def init_db() -> None:
    """Create database tables if they do not exist."""
    SQLModel.metadata.create_all(engine)
    _migrate_legacy_user_roles()


def _migrate_legacy_user_roles() -> None:
    with engine.begin() as connection:
        connection.exec_driver_sql("UPDATE user SET role = 'cs' WHERE role = 'agent'")
