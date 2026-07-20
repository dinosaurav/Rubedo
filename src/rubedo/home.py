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
from collections.abc import Collection
from typing import TYPE_CHECKING, Any, Mapping, Optional, Union

from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from .db import Database
from .cloud_lane_store import CloudLaneStore
from .lane_store import LaneStore
from .store import LocalStore, ObjectStore, S3Store, open_store

PathLike = Union[str, os.PathLike]

if TYPE_CHECKING:
    from .diff import RunDiff, RunRef
    from .queries import Cell
    from .schemas import RunListItem
    from .selection import Selection

_registry_lock = threading.Lock()
_registry: dict[str, "Home"] = {}


def _resolve_path(path: Optional[PathLike]) -> str:
    if path is None:
        path = os.environ.get("RUBEDO_HOME", ".rubedo")
    return os.path.abspath(os.fspath(path))


def _resolve_store(
    *,
    path: str,
    store: Optional[ObjectStore],
    store_url: Optional[str],
) -> ObjectStore:
    """Explicit ``store=`` wins; else ``store_url=``; else ``RUBEDO_STORE_URL``;
    else a local filesystem store under ``path``."""
    if store is not None and store_url is not None:
        raise ValueError("Home: pass store= or store_url=, not both")
    if store is not None:
        return store
    url = store_url if store_url is not None else os.environ.get("RUBEDO_STORE_URL")
    if url:
        return open_store(url)
    return LocalStore(path)


class Home:
    """Storage root: ``db`` + ``store`` + ``lanes`` for one filesystem path."""

    path: str
    db: Database
    store: ObjectStore
    lanes: LaneStore

    def __new__(
        cls,
        path: Optional[PathLike] = None,
        *,
        db_url: Optional[str] = None,
        db_engine: Optional[Engine] = None,
        db_connect_args: Optional[Mapping[str, Any]] = None,
        db_poolclass: Any = None,
        store: Optional[ObjectStore] = None,
        store_url: Optional[str] = None,
        lanes: Optional[LaneStore] = None,
        fresh: bool = False,
    ):
        resolved = _resolve_path(path)
        if fresh:
            obj = object.__new__(cls)
            obj._init_state(
                resolved,
                db_url=db_url,
                db_engine=db_engine,
                db_connect_args=db_connect_args,
                db_poolclass=db_poolclass,
                store=store,
                store_url=store_url,
                lanes=lanes,
            )
            return obj
        with _registry_lock:
            existing = _registry.get(resolved)
            if existing is not None:
                if any(
                    x is not None
                    for x in (
                        db_url,
                        db_engine,
                        db_connect_args,
                        db_poolclass,
                        store,
                        store_url,
                        lanes,
                    )
                ):
                    # Already interned — conflicting hooks are a hard error
                    # so tests don't silently share the wrong engine/store.
                    raise ValueError(
                        f"Home({resolved!r}) is already constructed; pass "
                        f"fresh=True for an unshared instance, or reuse the "
                        f"existing Home without db_*/store overrides"
                    )
                return existing
            obj = object.__new__(cls)
            obj._init_state(
                resolved,
                db_url=db_url,
                db_engine=db_engine,
                db_connect_args=db_connect_args,
                db_poolclass=db_poolclass,
                store=store,
                store_url=store_url,
                lanes=lanes,
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
        store: Optional[ObjectStore] = None,
        store_url: Optional[str] = None,
        lanes: Optional[LaneStore] = None,
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
        self.store = _resolve_store(path=self.path, store=store, store_url=store_url)
        if lanes is not None:
            self.lanes = lanes
        elif isinstance(self.store, S3Store):
            self.lanes = CloudLaneStore(self.path, self.store)
        else:
            self.lanes = LaneStore(self.path)

    @classmethod
    def default(cls) -> "Home":
        """The ambient home (``RUBEDO_HOME`` or ``.rubedo``).

        Honors ``RUBEDO_STORE_URL`` for the object-store plane when set.
        """
        return cls(None)

    @classmethod
    def ephemeral(
        cls,
        path=None,
        *,
        db_url=None,
        db_engine=None,
        db_connect_args=None,
        db_poolclass=None,
        store=None,
        store_url=None,
        lanes=None,
    ) -> "Home":
        """Unshared Home — not interned. For tests and short-lived roots."""
        return cls(
            path,
            db_url=db_url,
            db_engine=db_engine,
            db_connect_args=db_connect_args,
            db_poolclass=db_poolclass,
            store=store,
            store_url=store_url,
            lanes=lanes,
            fresh=True,
        )

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

    def cells(
        self,
        *,
        run_id: str,
        step: Optional[str] = None,
        status: Optional[str | Collection[str]] = None,
        resolve_output: bool = False,
    ) -> list["Cell"]:
        """Read the cells recorded for one run."""
        from .queries import get_run_cells

        with self.session() as session:
            return get_run_cells(
                session,
                self,
                run_id,
                step=step,
                status=status,
                resolve_output=resolve_output,
            )

    def current(
        self,
        *,
        pipeline: Optional[str] = None,
        step: Optional[str] = None,
        resolve_output: bool = False,
    ) -> list["Cell"]:
        """Read the latest full process run's live cells for each pipeline.

        Partial / declaration / invalidate / gc runs never define current.
        """
        from .queries import get_current_cells

        with self.session() as session:
            return get_current_cells(
                session,
                self,
                pipeline=pipeline,
                step=step,
                resolve_output=resolve_output,
            )

    def select(
        self,
        selection: "Selection | str",
        *,
        run_id: Optional[str] = None,
        resolve_output: bool = False,
    ) -> list["Cell"]:
        """Read cells matching a Selection query."""
        from .queries import select_cells

        with self.session() as session:
            return select_cells(
                session,
                self,
                selection,
                run_id=run_id,
                resolve_output=resolve_output,
            )

    def runs(
        self,
        *,
        pipeline: Optional[str] = None,
        kind: Optional[str] = None,
        status: Optional[str] = None,
        limit: int = 50,
    ) -> list["RunListItem"]:
        """List historical runs on this home, newest first.

        Args:
            pipeline: Restrict to this pipeline name.
            kind: Restrict to a run kind (``process``, ``partial``, …).
            status: Restrict to effective status (``completed``,
                ``running``, ``interrupted``, …).
            limit: Maximum rows to return.

        Returns:
            ``RunListItem`` rows (``.id`` is a valid ``Home.diff`` run ref).
            Partial runs are included unless ``kind`` excludes them.
        """
        from .queries import get_recent_runs

        with self.session() as session:
            return get_recent_runs(
                session,
                limit=limit,
                pipeline=pipeline,
                kind=kind,
                status=status,
            )

    def diff(
        self,
        *,
        step: str,
        before: "RunRef",
        after: "RunRef",
        lanes: Optional[Collection[str]] = None,
    ) -> "RunDiff":
        """Compare one step's outputs across two runs (read-only).

        Args:
            step: Step name within both runs' pipeline.
            before: Earlier run (id str, ``RunSummary``, or ``RunListItem``).
            after: Later run (same ref forms).
            lanes: Optional explicit coordinate universe. When omitted and
                ``after`` is a partial whose scope anchor equals ``step``,
                defaults to that run's ``selection_json.lanes``; otherwise
                the union of coordinates observed at ``step``.

        Returns:
            A ``RunDiff`` with per-coordinate outcomes and value changes.
            Writes nothing to the ledger.
        """
        from .diff import diff_runs

        return diff_runs(
            self,
            step=step,
            before=before,
            after=after,
            lanes=lanes,
        )

    def __repr__(self) -> str:
        return f"Home({self.path!r})"
