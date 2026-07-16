"""Tests for InputHashUsage — the ``address -> liveness`` gate.

This table is the single source of truth for "should this lane reuse or
recompute?"  ``fulfilled=True`` means a filled Arrow row exists (reuse);
``fulfilled=False`` means recompute (crash, in-flight claim, or invalidation).
These tests exercise the three jobs: soft lock, crash detection, and
invalidation tombstone — plus the GC handle (last_run_id recency)."""

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
    run = Run(
        id=run_id,
        kind=kind,
        started_at=utcnow_iso(),
        last_heartbeat_at=utcnow_iso(),
    )
    session.add(run)
    return run


def _make_usage(
    session,
    address="addr_001",
    lane_key="@root#0",
    step_name="extract",
    pipeline_id="test-pipe",
    run_id="run_001",
    fulfilled=False,
) -> InputHashUsage:
    run = session.query(Run).filter_by(id=run_id).first()
    if not run:
        _make_run(session, run_id)
    u = InputHashUsage(
        address=address,
        lane_key=lane_key,
        step_name=step_name,
        pipeline_id=pipeline_id,
        last_run_id=run_id,
        claimed_at=utcnow_iso(),
        fulfilled=fulfilled,
    )
    session.add(u)
    return u


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
    """The scheduler claims an address (fulfilled=False) before executing,
    then flips fulfilled=True after a successful commit."""
    with get_session() as s:
        _make_usage(s, address="addr_lock", fulfilled=False)
        s.commit()

        # Another worker checking sees fulfilled=False → don't reuse, defer
        u = s.query(InputHashUsage).filter_by(address="addr_lock").first()
        assert u.fulfilled is False

        # Worker succeeds → flip fulfilled
        u.fulfilled = True
        s.commit()

        u2 = s.query(InputHashUsage).filter_by(address="addr_lock").first()
        assert u2.fulfilled is True


def test_soft_lock_claim_crash_leaves_unfulfilled():
    """A worker claims (fulfilled=False) and crashes.  The row stays
    fulfilled=False — the next run sees it and recomputes."""
    with get_session() as s:
        _make_usage(s, address="addr_crash", run_id="run_crash", fulfilled=False)
        s.commit()

        # Simulate crash: no flip to fulfilled=True, run goes terminal
        u = s.query(InputHashUsage).filter_by(address="addr_crash").first()
        assert u.fulfilled is False
        assert u.last_run_id == "run_crash"


# ---------------------------------------------------------------------------
# Invalidation tombstone: flip fulfilled=False
# ---------------------------------------------------------------------------


def test_invalidation_flips_fulfilled_false():
    """invalidate() sets fulfilled=False on an existing row.  The Arrow
    row stays as history, but the next run sees fulfilled=False and
    recomputes."""
    with get_session() as s:
        # Lane was computed successfully
        _make_usage(s, address="addr_inval", run_id="run_orig", fulfilled=True)
        s.commit()

        # Invalidation run flips it
        _make_run(s, "run_inval", kind="invalidate")
        u = s.query(InputHashUsage).filter_by(address="addr_inval").first()
        u.fulfilled = False
        u.last_run_id = "run_inval"
        u.claimed_at = utcnow_iso()
        s.commit()

        u2 = s.query(InputHashUsage).filter_by(address="addr_inval").first()
        assert u2.fulfilled is False
        assert u2.last_run_id == "run_inval"


def test_invalidation_when_no_usage_row_exists_creates_one():
    """If the address was never in input_hash_usages (edge case),
    invalidation creates a row with fulfilled=False."""
    with get_session() as s:
        _make_run(s, "run_inval2", kind="invalidate")
        s.add(
            InputHashUsage(
                address="addr_new",
                lane_key="@root#0",
                step_name="extract",
                pipeline_id="test-pipe",
                last_run_id="run_inval2",
                claimed_at=utcnow_iso(),
                fulfilled=False,
            )
        )
        s.commit()
        u = s.query(InputHashUsage).filter_by(address="addr_new").first()
        assert u is not None
        assert u.fulfilled is False


# ---------------------------------------------------------------------------
# Uniqueness: one row per (address, step, pipeline)
# ---------------------------------------------------------------------------


def test_uniqueness_on_address_step_pipeline():
    """Two rows with the same (address, step, pipeline) should fail — the
    unique constraint enforces at most one liveness entry per identity."""
    from sqlalchemy.exc import IntegrityError

    with get_session() as s:
        _make_usage(s, address="addr_dup", step_name="dup_step",
                    pipeline_id="dup_pipe", run_id="run_a")
        s.commit()

        _make_run(s, "run_b")
        s.add(
            InputHashUsage(
                address="addr_dup",
                lane_key="@root#1",
                step_name="dup_step",
                pipeline_id="dup_pipe",
                last_run_id="run_b",
                claimed_at=utcnow_iso(),
                fulfilled=False,
            )
        )
        with pytest.raises(IntegrityError):
            s.commit()


def test_same_address_different_step_is_allowed():
    """Two steps can have the same address hash (different step names)."""
    with get_session() as s:
        _make_usage(s, address="addr_shared", step_name="step_a",
                    pipeline_id="pipe", run_id="run_a", fulfilled=True)
        _make_run(s, "run_b")
        s.add(
            InputHashUsage(
                address="addr_shared",
                lane_key="@root#0",
                step_name="step_b",
                pipeline_id="pipe",
                last_run_id="run_b",
                claimed_at=utcnow_iso(),
                fulfilled=True,
            )
        )
        s.commit()
        rows = s.query(InputHashUsage).filter_by(address="addr_shared").all()
        assert len(rows) == 2


# ---------------------------------------------------------------------------
# GC handle: last_run_id for retention recency
# ---------------------------------------------------------------------------


def test_last_run_id_tracks_most_recent_run():
    """After a recompute, last_run_id updates to the new run.  GC reads
    this to know 'which run last referenced this output?'"""
    with get_session() as s:
        _make_usage(s, address="addr_gc", run_id="run_old", fulfilled=True)
        s.commit()

        _make_run(s, "run_new")
        u = s.query(InputHashUsage).filter_by(address="addr_gc").first()
        u.last_run_id = "run_new"
        u.fulfilled = True
        u.claimed_at = utcnow_iso()
        s.commit()

        u2 = s.query(InputHashUsage).filter_by(address="addr_gc").first()
        assert u2.last_run_id == "run_new"