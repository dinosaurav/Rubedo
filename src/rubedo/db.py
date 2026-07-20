"""Database initialization and session management.

Owned by a ``Home``: each home has its own ``Database`` (engine +
session factory). There is no process-global engine.
"""
from __future__ import annotations

import os
from typing import Any, Mapping, Optional, Union

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

from .models import Base
from .util import _ensure_gitignore

PathLike = Union[str, os.PathLike]


class Database:
    """SQLAlchemy engine + session factory for one home."""

    def __init__(
        self,
        *,
        path: Optional[PathLike] = None,
        url: Optional[str] = None,
        engine: Optional[Engine] = None,
        connect_args: Optional[Mapping[str, Any]] = None,
        poolclass: Any = None,
    ):
        if engine is not None and url is not None:
            raise ValueError("Database: pass engine= or url=, not both")
        if engine is not None and path is not None:
            raise ValueError("Database: pass engine= or path=, not both")

        if engine is not None:
            self.engine = engine
        else:
            if url is None:
                if path is None:
                    raise ValueError("Database: need path=, url=, or engine=")
                url = _sqlite_url_for_path(path)
            self.engine = _create_engine(
                url, connect_args=connect_args, poolclass=poolclass
            )

        Base.metadata.create_all(bind=self.engine)
        self._SessionLocal = sessionmaker(
            autocommit=False, autoflush=False, bind=self.engine
        )

    def session(self) -> Session:
        """Return a new SQLAlchemy session bound to this database."""
        return self._SessionLocal()

    def dispose(self) -> None:
        """Dispose the underlying engine (tests / shutdown)."""
        self.engine.dispose()


def _sqlite_url_for_path(path: PathLike) -> str:
    """Build a sqlite:/// URL for a filesystem path, ensuring the dir exists."""
    db_path = os.fspath(path)
    if db_path.startswith("sqlite:///"):
        return db_path
    db_dir = os.path.dirname(db_path)
    if db_dir:
        os.makedirs(db_dir, exist_ok=True)
        _ensure_gitignore(db_dir)
    return f"sqlite:///{db_path}"


def _create_engine(
    engine_url: str,
    *,
    connect_args: Optional[Mapping[str, Any]] = None,
    poolclass: Any = None,
) -> Engine:
    """Create an engine; apply SQLite WAL pragmas when appropriate."""
    # Anything containing :// passes through verbatim (postgres, etc.).
    # Bare paths get sqlite:/// — but callers normally go through
    # _sqlite_url_for_path first.
    if "://" not in engine_url and not engine_url.startswith("sqlite:"):
        engine_url = f"sqlite:///{engine_url}"

    kwargs: dict[str, Any] = {}
    if connect_args is not None:
        kwargs["connect_args"] = dict(connect_args)
    if poolclass is not None:
        kwargs["poolclass"] = poolclass

    engine = create_engine(engine_url, **kwargs)

    if engine_url.startswith("sqlite"):
        from sqlalchemy import event

        @event.listens_for(engine, "connect")
        def set_sqlite_pragma(dbapi_connection, connection_record):
            cursor = dbapi_connection.cursor()
            cursor.execute("PRAGMA journal_mode=WAL")
            cursor.execute("PRAGMA busy_timeout=5000")
            cursor.close()

    return engine
