"""Tests for InputHashUsage — the ``address -> (last_run_id, fulfilled)`` map.

This table is the single source of truth for "should this lane reuse or
recompute?"  ``fulfilled=True`` means a filled Arrow row exists (reuse);
``fulfilled=False`` means recompute (crash, in-flight claim, or invalidation).
Two data columns, one PK — the minimal liveness gate."""

import os
import uuid

import pytest
from sqlalchemy import create_engine
from sqlalchemy.pool import StaticPool

from rubedo.db import init_db, get_session
from rubedo.models import InputHashUsage, Run
from rubedo.util import utcnow_iso


@pytest.fixture(autouse=True)
def isolated_db():
    os.environ["RUBEDO_DB_PATH"] = (
        f"sqlite:///file:testdb_ihu_{uuid.uuid4().hex}?mode=memory&cache=shared&uri=true"
    )
    init_db()
    import rubedo.db
    rubedo.db._engine = create_engine(
        os.environ["RUBEDO_DB_PATH"],
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    yield


def _make_run(session, run_id: str, kind: str = "pipeline") -> Run:
    run = Run(id=run_id, kind=kind, started_at=utcnow_iso(), last_heartbeat_at=utcnow_iso())
    session.add(run)
    return run


def _make_usage(session, address="addr_001", run_id="run_001", fulfilled=False):
    run = session.query(Run).filter_by(id=run_id).first()
    if not run:
        _make_run(session, run_id)
        session.flush()
    session.add(InputHashUsage(address=address, last_run_id=run_id, fulfilled=fulfilled))


# ---------------------------------------------------------------------------
# fulfilled = the reuse/execute gate
# ---------------------------------------------------------------------------


def test_fulfilled_true_means_reuse():
    with get_session() as s:
        _make_usage(s, fulfilled=True)
        s.commit()
        u = s.query(InputHashUsage).filter_by(address="addr_001").first()
        assert u is not None
        assert u.fulfilled is True


def test_fulfilled_false_means_recompute():
    with get_session() as s:
        _make_usage(s, fulfilled=False)
        s.commit()
        u = s.query(InputHashUsage).filter_by(address="addr_001").first()
        assert u is not None
        assert u.fulfilled is False


def test_no_row_means_cold_cache():
    with get_session() as s:
        _make_run(s, "run_001")
        s.commit()
        u = s.query(InputHashUsage).filter_by(address="nonexistent").first()
        assert u is None


# ---------------------------------------------------------------------------
# Soft lock: claim before execution, fulfill after
# ---------------------------------------------------------------------------


def test_soft_lock_claim_then_fulfill():
    with get_session() as s:
        _make_usage(s, address="addr_lock", fulfilled=False)
        s.commit()
        u = s.query(InputHashUsage).filter_by(address="addr_lock").first()
        assert u.fulfilled is False
        u.fulfilled = True
        s.commit()
        u2 = s.query(InputHashUsage).filter_by(address="addr_lock").first()
        assert u2.fulfilled is True


def test_soft_lock_claim_crash_leaves_unfulfilled():
    with get_session() as s:
        _make_usage(s, address="addr_crash", run_id="run_crash", fulfilled=False)
        s.commit()
        u = s.query(InputHashUsage).filter_by(address="addr_crash").first()
        assert u.fulfilled is False
        assert u.last_run_id == "run_crash"


# ---------------------------------------------------------------------------
# Invalidation tombstone: flip fulfilled=False
# ---------------------------------------------------------------------------


def test_invalidation_flips_fulfilled_false():
    with get_session() as s:
        _make_usage(s, address="addr_inval", run_id="run_orig", fulfilled=True)
        s.commit()
        _make_run(s, "run_inval", kind="invalidate")
        u = s.query(InputHashUsage).filter_by(address="addr_inval").first()
        u.fulfilled = False
        u.last_run_id = "run_inval"
        s.commit()
        u2 = s.query(InputHashUsage).filter_by(address="addr_inval").first()
        assert u2.fulfilled is False
        assert u2.last_run_id == "run_inval"


# ---------------------------------------------------------------------------
# PK uniqueness on address
# ---------------------------------------------------------------------------


def test_address_is_primary_key():
    """Two rows with the same address should fail — address is the PK."""
    from sqlalchemy.exc import IntegrityError

    with get_session() as s:
        _make_usage(s, address="addr_dup", run_id="run_a")
        s.commit()
        _make_run(s, "run_b")
        s.add(InputHashUsage(address="addr_dup", last_run_id="run_b", fulfilled=False))
        with pytest.raises(IntegrityError):
            s.commit()


# ---------------------------------------------------------------------------
# GC handle: last_run_id for retention recency
# ---------------------------------------------------------------------------


def test_last_run_id_tracks_most_recent_run():
    with get_session() as s:
        _make_usage(s, address="addr_gc", run_id="run_old", fulfilled=True)
        s.commit()
        _make_run(s, "run_new")
        u = s.query(InputHashUsage).filter_by(address="addr_gc").first()
        u.last_run_id = "run_new"
        u.fulfilled = True
        s.commit()
        u2 = s.query(InputHashUsage).filter_by(address="addr_gc").first()
        assert u2.last_run_id == "run_new"