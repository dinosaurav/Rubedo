import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session
from .models import Base

DB_PATH = ".batchbrain/batchbrain.sqlite"

engine = None
SessionLocal = None

def _ensure_gitignore(directory: str):
    if not directory:
        return
    gitignore_path = os.path.join(directory, ".gitignore")
    if not os.path.exists(gitignore_path):
        try:
            with open(gitignore_path, "w") as f:
                f.write("# Ignore everything in this directory\n*\n# Except this file\n!.gitignore\n")
        except Exception:
            pass

def init_db(db_path: str = DB_PATH):
    global engine, SessionLocal
    db_dir = os.path.dirname(db_path)
    os.makedirs(db_dir, exist_ok=True)
    _ensure_gitignore(db_dir)
    engine = create_engine(f"sqlite:///{db_path}")
    Base.metadata.create_all(bind=engine)
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

def get_session() -> Session:
    if SessionLocal is None:
        init_db()
    return SessionLocal()
