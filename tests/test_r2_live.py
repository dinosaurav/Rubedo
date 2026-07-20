"""Opt-in integration test against a real Cloudflare R2 bucket.

Skipped unless every required ``RUBEDO_TEST_R2_*`` variable is present.
The test writes only beneath a unique ``rubedo-live-tests/`` prefix and
removes those objects in ``finally``.
"""
from __future__ import annotations

import os
import uuid
from dataclasses import dataclass

import boto3
import pytest
from botocore.config import Config

from rubedo.hashing import hash_bytes
from rubedo.home import Home
from rubedo.pipeline import pipeline
from rubedo.spec import step
from rubedo.store import S3Store, resolve_object_sizes


@dataclass(frozen=True)
class _R2Credentials:
    account_id: str
    access_key_id: str
    secret_access_key: str
    bucket: str
    endpoint_url: str


def _r2_credentials() -> _R2Credentials:
    names = {
        "account_id": "RUBEDO_TEST_R2_ACCOUNT_ID",
        "access_key_id": "RUBEDO_TEST_R2_ACCESS_KEY_ID",
        "secret_access_key": "RUBEDO_TEST_R2_SECRET_ACCESS_KEY",
        "bucket": "RUBEDO_TEST_R2_BUCKET",
    }
    values = {field: os.environ.get(env) for field, env in names.items()}
    missing = [env for field, env in names.items() if not values[field]]
    if missing:
        pytest.skip(
            "live R2 test requires " + ", ".join(missing),
            allow_module_level=True,
        )
    account_id = str(values["account_id"])
    endpoint = os.environ.get(
        "RUBEDO_TEST_R2_ENDPOINT_URL",
        f"https://{account_id}.r2.cloudflarestorage.com",
    )
    return _R2Credentials(
        account_id=account_id,
        access_key_id=str(values["access_key_id"]),
        secret_access_key=str(values["secret_access_key"]),
        bucket=str(values["bucket"]),
        endpoint_url=endpoint,
    )


CREDS = _r2_credentials()


def _client():
    return boto3.client(
        "s3",
        endpoint_url=CREDS.endpoint_url,
        aws_access_key_id=CREDS.access_key_id,
        aws_secret_access_key=CREDS.secret_access_key,
        region_name="auto",
        config=Config(signature_version="s3v4"),
    )


def test_cloudflare_r2_object_store_and_pipeline_reuse(tmp_path):
    """Exercise real R2 conditional puts, reads, LIST, streams, and reuse."""
    prefix = f"rubedo-live-tests/{uuid.uuid4().hex}/"
    store = S3Store(
        bucket=CREDS.bucket,
        prefix=prefix,
        endpoint_url=CREDS.endpoint_url,
        region_name="auto",
        client_factory=_client,
    )
    written: set[str] = set()

    try:
        payload = f"r2-live-{uuid.uuid4().hex}".encode() * 400
        content_hash = hash_bytes(payload)
        missing_hash = hash_bytes(uuid.uuid4().bytes)

        assert store.read(missing_hash) is None
        store.write(content_hash, payload, run_id="live-r2", coordinate="direct")
        written.add(content_hash)
        store.write(content_hash, payload, run_id="live-r2", coordinate="again")

        assert store.exists(content_hash)
        assert store.read(content_hash) == payload
        assert store.size_of(content_hash) == len(payload)
        assert resolve_object_sizes(store, {content_hash, missing_hash}) == {
            content_hash: len(payload)
        }
        chunks = store.stream(content_hash, chunk_size=1024)
        assert chunks is not None
        assert b"".join(chunks) == payload
        assert {obj.key for obj in store.inventory()} == {content_hash}

        home = Home.ephemeral(tmp_path / "home", store=store)

        @step
        def root():
            return payload

        pipe = pipeline(
            name=f"r2_live_{uuid.uuid4().hex}",
            steps=[root],
            home=home,
        )
        first = pipe.run(workers=1)
        second = pipe.run(workers=1)
        assert first.created_count == 1
        assert second.created_count == 0
        assert second.reused_count == 1

        written.update(obj.key for obj in store.inventory())
    finally:
        # Prefix is unique, but delete only hashes observed by this test.
        for content_hash in written:
            store.delete(content_hash)

    assert list(store.inventory()) == []
