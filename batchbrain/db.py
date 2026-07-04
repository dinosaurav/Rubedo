"""
Database initialization and session management.

Provides utilities for setting up the SQLite database, ensuring the directory
is gitignored, configuring WAL mode for concurrency, and providing sessions.
"""
import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session
from .models import Base

DB_PATH = ".batchbrain/batchbrain.sqlite"

engine = None
SessionLocal = None


def _ensure_gitignore(directory: str):
    """
    Ensure the given directory has a .gitignore file that ignores all contents except itself.
    
    Args:
        directory (str): The path to the directory to protect.
    """
    if not directory:
        return
    gitignore_path = os.path.join(directory, ".gitignore")
    if not os.path.exists(gitignore_path):
        try:
            with open(gitignore_path, "w") as f:
                f.write(
                    "# Ignore everything in this directory\n*\n# Except this file\n!.gitignore\n"
                )
        except Exception:
            pass


def init_db(db_path: str = None):
    """
    Initialize the database engine and create tables.

    Args:
        db_path (str, optional): The database URL or file path. If None, uses BATCHBRAIN_DB_PATH
            or the default '.batchbrain/batchbrain.sqlite'.
    """
    global engine, SessionLocal
    if engine is not None:
        try:
            engine.dispose()
        except Exception:
            pass

    if db_path is None:
        db_path = os.environ.get("BATCHBRAIN_DB_PATH", DB_PATH)
        # Strip sqlite:/// prefix if present to get the dir
        dir_path = (
            db_path.replace("sqlite:///", "")
            if db_path.startswith("sqlite:///")
            else db_path
        )
    else:
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
