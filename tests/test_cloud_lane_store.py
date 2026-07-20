"""Moto-backed immutable cloud lane segments, leases, and compaction."""
from __future__ import annotations

import time
from datetime import datetime, timezone

import boto3
import pytest
from moto import mock_aws

from rubedo.cloud_lane_store import CloudLaneStore, PipelineLeaseError
from rubedo.home import Home
from rubedo.pipeline import pipeline
from rubedo.spec import step
from rubedo.store import S3Store


BUCKET = "rubedo-cloud-lanes"


def _store(client, prefix: str = "rubedo/") -> S3Store:
    return S3Store(bucket=BUCKET, prefix=prefix, client=client)


def _append(
    lanes: CloudLaneStore,
    *,
    pipeline_id: str = "pipe",
    step_name: str = "work",
    lane_key: str = "lane",
    address: str = "address",
    ts=None,
) -> None:
    lanes.append_filled(
        pipeline_id,
        step_name,
        lane_key,
        address,
        "input",
        {"value": 1},
        "json",
        "run-1",
        output_identity="identity",
        ts=ts,
    )


@mock_aws
def test_segment_visible_to_second_lane_store(tmp_path):
    client = boto3.client("s3", region_name="us-east-1")
    client.create_bucket(Bucket=BUCKET)
    first = CloudLaneStore(str(tmp_path / "first"), _store(client))
    second = CloudLaneStore(str(tmp_path / "second"), _store(client))

    _append(first)
    assert first.all_filled_rows()[0]["address"] == "address"
    first.flush_step("pipe", "work")

    rows = second.rows_by_address("pipe", "work", {"address"})
    assert rows["address"]["output"] == {"value": 1}
    assert second.all_filled_rows()[0]["pipeline_id"] == "pipe"


@mock_aws
def test_segments_dedupe_by_row_id_and_compact(tmp_path):
    client = boto3.client("s3", region_name="us-east-1")
    client.create_bucket(Bucket=BUCKET)
    lanes = CloudLaneStore(
        str(tmp_path),
        _store(client),
        compaction_threshold=1,
    )
    ts = datetime(2026, 7, 20, tzinfo=timezone.utc)

    _append(lanes, ts=ts)
    lanes.flush_step("pipe", "work")
    _append(lanes, ts=ts)
    lanes.flush_step("pipe", "work")

    assert len(lanes._segment_objects("pipe", "work")) == 2
    table = lanes._read_disk_table("pipe", "work")
    assert table is not None and table.num_rows == 1

    lanes.compact_pipeline("pipe")
    assert len(lanes._segment_objects("pipe", "work")) == 1
    table = lanes._read_disk_table("pipe", "work")
    assert table is not None and table.num_rows == 1


@mock_aws
def test_pipeline_writer_lease_rejects_second_writer(tmp_path):
    client = boto3.client("s3", region_name="us-east-1")
    client.create_bucket(Bucket=BUCKET)
    first = CloudLaneStore(str(tmp_path / "first"), _store(client))
    second = CloudLaneStore(str(tmp_path / "second"), _store(client))

    with first.writer_lease("pipe", "run-one"):
        with pytest.raises(PipelineLeaseError, match="active writer"):
            with second.writer_lease("pipe", "run-two"):
                pass

    with second.writer_lease("pipe", "run-two"):
        pass


@mock_aws
def test_pipeline_writer_lease_renews(tmp_path):
    client = boto3.client("s3", region_name="us-east-1")
    client.create_bucket(Bucket=BUCKET)
    first = CloudLaneStore(
        str(tmp_path / "first"),
        _store(client),
        lease_ttl_seconds=1,
        lease_renew_seconds=0.05,
    )
    second = CloudLaneStore(str(tmp_path / "second"), _store(client))

    with first.writer_lease("pipe", "run-one"):
        time.sleep(0.12)
        with pytest.raises(PipelineLeaseError, match="active writer"):
            with second.writer_lease("pipe", "run-two"):
                pass


@mock_aws
def test_second_home_reuses_cloud_lane_segments(tmp_path):
    client = boto3.client("s3", region_name="us-east-1")
    client.create_bucket(Bucket=BUCKET)
    path = tmp_path / "shared-ledger"

    @step
    def root():
        return b"cloud-lane-value" * 400

    first_home = Home.ephemeral(path, store=_store(client))
    first = pipeline(name="cloud_reuse", steps=[root], home=first_home).run(
        workers=1
    )
    first_home.db.dispose()

    second_home = Home.ephemeral(path, store=_store(client))
    second = pipeline(name="cloud_reuse", steps=[root], home=second_home).run(
        workers=1
    )

    assert first.created_count == 1
    assert second.created_count == 0
    assert second.reused_count == 1
