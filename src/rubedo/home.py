"""A Rubedo home: one root owning the ledger, object store, and lane tables.

``Home`` is the process-local unit of storage identity. Construct one (or
let ``pipeline(home=None)`` use the default) and inject it — there is no
process-global DB/store/lane-table state to repoint. Same absolute path
interns to the same instance so concurrent same-home runs share buffers
and the engine; different paths are independent and safe to run together.
"""
from __future__ import annotations

import os
import threading
from typing import Any, Mapping, Optional, Union

from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from .db import Database
from .lane_store import LaneStore
from .store import LocalStore

PathLike = Union[str, os.PathLike]

_registry_lock = threading.Lock()
_registry: dict[str, "Home"] = {}


def _resolve_path(path: Optional[PathLike]) -> str:
    if path is None:
        path = os.environ.get("RUBEDO_HOME", ".rubedo")
    return os.path.abspath(os.fspath(path))


class Home:
    """Storage root: ``db`` + ``store`` + ``lanes`` for one filesystem path."""

    path: str
    db: Database
    store: LocalStore
    lanes: LaneStore

    def __new__(
        cls,
        path: Optional[PathLike] = None,
        *,
        db_url: Optional[str] = None,
        db_engine: Optional[Engine] = None,
        db_connect_args: Optional[Mapping[str, Any]] = None,
        db_poolclass: Any = None,
        _fresh: bool = False,
    ):
        resolved = _resolve_path(path)
        if _fresh:
            obj = object.__new__(cls)
            obj._init_state(
                resolved,
                db_url=db_url,
                db_engine=db_engine,
                db_connect_args=db_connect_args,
                db_poolclass=db_poolclass,
            )
            return obj
        with _registry_lock:
            existing = _registry.get(resolved)
            if existing is not None:
                if any(
                    x is not None
                    for x in (db_url, db_engine, db_connect_args, db_poolclass)
                ):
                    # Already interned — conflicting hooks are a hard error
                    # so tests don't silently share the wrong engine.
                    raise ValueError(
                        f"Home({resolved!r}) is already constructed; pass "
                        f"_fresh=True for an unshared instance, or reuse the "
                        f"existing Home without db_* overrides"
                    )
                return existing
            obj = object.__new__(cls)
            obj._init_state(
                resolved,
                db_url=db_url,
                db_engine=db_engine,
                db_connect_args=db_connect_args,
                db_poolclass=db_poolclass,
            )
            _registry[resolved] = obj
            return obj

    def __init__(self, *args, **kwargs):
        # Real init is in _init_state via __new__ (interning).
        return

    def _init_state(
        self,
        path: str,
        *,
        db_url: Optional[str] = None,
        db_engine: Optional[Engine] = None,
        db_connect_args: Optional[Mapping[str, Any]] = None,
        db_poolclass: Any = None,
    ) -> None:
        self.path = path
        os.makedirs(self.path, exist_ok=True)
        if db_engine is not None:
            self.db = Database(engine=db_engine)
        elif db_url is not None:
            self.db = Database(
                url=db_url,
                connect_args=db_connect_args,
                poolclass=db_poolclass,
            )
        else:
            # Explicit Home(path) uses path/rubedo.sqlite. The ambient
            # default also honors RUBEDO_DB_PATH (ledger-only override,
            # same precedence the old init_db had) when the caller did
            # not pass db_url=/db_engine=.
            env_db = os.environ.get("RUBEDO_DB_PATH")
            if env_db and path == _resolve_path(None):
                self.db = Database(
                    url=env_db if "://" in env_db else None,
                    path=None if "://" in env_db else env_db,
                    connect_args=db_connect_args,
                    poolclass=db_poolclass,
                )
            else:
                self.db = Database(
                    path=os.path.join(self.path, "rubedo.sqlite"),
                    connect_args=db_connect_args,
                    poolclass=db_poolclass,
                )
        self.store = LocalStore(self.path)
        self.lanes = LaneStore(self.path)

    @classmethod
    def default(cls) -> "Home":
        """The ambient home (``RUBEDO_HOME`` or ``.rubedo``)."""
        return cls(None)

    @classmethod
    def clear_registry_for_tests(cls) -> None:
        """Drop interned homes so tests can rebuild with fresh engines."""
        with _registry_lock:
            for home in list(_registry.values()):
                try:
                    home.db.dispose()
                except Exception:
                    pass
            _registry.clear()

    def session(self) -> Session:
        """Open a new SQLAlchemy session on this home's ledger."""
        return self.db.session()

    def __repr__(self) -> str:
        return f"Home({self.path!r})"
