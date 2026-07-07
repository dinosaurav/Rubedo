"""
Database initialization and session management.

Provides utilities for setting up the SQLite database, ensuring the directory
is gitignored, configuring WAL mode for concurrency, and providing sessions.
"""
import os
from .util import _ensure_gitignore
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session
from typing import Optional, Any
from .models import Base

engine: Any = None
SessionLocal: Any = None


def init_db(db_path: Optional[str] = None):
    """
    Initialize the database engine and create tables.

    Args:
        db_path (str, optional): The database URL or file path. If None, uses
            RUBEDO_DB_PATH, or RUBEDO_HOME/rubedo.sqlite, or the default
            '.rubedo/rubedo.sqlite' — in that precedence order.
    """
    global engine, SessionLocal
    if engine is not None:
        try:
            engine.dispose()
        except Exception:
            pass

    if db_path is None:
        db_path = os.environ.get("RUBEDO_DB_PATH") or os.path.join(
            os.environ.get("RUBEDO_HOME", ".rubedo"), "rubedo.sqlite"
        )
    dir_path = (
        db_path.replace("sqlite:///", "")
        if db_path.startswith("sqlite:///")
        else db_path
    )

    db_dir = os.path.dirname(dir_path)
    if db_dir:
        os.makedirs(db_dir, exist_ok=True)
        _ensure_gitignore(db_dir)
    if db_path.startswith("sqlite:///"):
        engine_url = db_path
    else:
        engine_url = f"sqlite:///{db_path}"
    engine = create_engine(engine_url)
    
    if engine_url.startswith("sqlite"):
        from sqlalchemy import event
        @event.listens_for(engine, "connect")
        def set_sqlite_pragma(dbapi_connection, connection_record):
            cursor = dbapi_connection.cursor()
            cursor.execute("PRAGMA journal_mode=WAL")
            cursor.execute("PRAGMA busy_timeout=5000")
            cursor.close()
            
    Base.metadata.create_all(bind=engine)
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


def get_session() -> Session:
    """
    Get a new SQLAlchemy session, initializing the DB if necessary.

    Returns:
        Session: A new database session.
    """
    if SessionLocal is None:
        init_db()
    return SessionLocal()
