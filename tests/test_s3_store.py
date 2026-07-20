"""ObjectStore protocol, LocalStore parity, and moto-backed S3Store."""
from __future__ import annotations

import os
import shutil

import boto3
import pytest
from moto import mock_aws
from sqlalchemy.pool import StaticPool

from rubedo.du import storage_report
from rubedo.gc import gc
from rubedo.hashing import hash_bytes
from rubedo.home import Home
from rubedo.pipeline import pipeline
from rubedo.spec import step
from rubedo.store import (
    LocalStore,
    ObjectStore,
    S3Store,
    open_store,
    parse_store_url,
    resolve_object_sizes,
)


TEST_FOLDER = ".test_s3_store_data"
TEST_ENV = ".test_s3_store_env"
BUCKET = "rubedo-test-bucket"


@pytest.fixture(autouse=True)
def _env(tmp_path_factory):
    Home.clear_registry_for_tests()
    for d in (TEST_FOLDER, TEST_ENV):
        if os.path.isdir(d):
            shutil.rmtree(d)
    os.makedirs(TEST_FOLDER, exist_ok=True)
    yield
    Home.clear_registry_for_tests()
    for d in (TEST_FOLDER, TEST_ENV):
        if os.path.isdir(d):
            shutil.rmtree(d)


def _home_with_store(store) -> Home:
    return Home.ephemeral(
        TEST_ENV,
        db_connect_args={"check_same_thread": False},
        db_poolclass=StaticPool,
        store=store,
    )


def test_parse_store_url_prefix_and_options():
    bucket, prefix, opts = parse_store_url(
        "s3://my-bucket/prod/cache?endpoint_url=https://example.r2.cloudflarestorage.com&region=auto"
    )
    assert bucket == "my-bucket"
    assert prefix == "prod/cache/"
    assert opts["endpoint_url"] == "https://example.r2.cloudflarestorage.com"
    assert opts["region_name"] == "auto"


def test_local_store_satisfies_protocol():
    store = LocalStore(TEST_ENV)
    assert isinstance(store, ObjectStore)
    assert store.capabilities.local_paths
    assert store.capabilities.destructive_gc
    assert store.capabilities.sized_inventory

    data = b"hello-local"
    h = hash_bytes(data)
    store.write(h, data, run_id="r1", coordinate="c")
    assert store.exists(h)
    assert store.read(h) == data
    assert store.size_of(h) == len(data)
    infos = list(store.inventory())
    assert any(i.key == h and i.size == len(data) for i in infos)
    chunks = store.stream(h)
    assert chunks is not None
    assert b"".join(chunks) == data
    store.delete(h)
    assert not store.exists(h)


@mock_aws
def test_s3_store_round_trip_and_conditional_put():
    client = boto3.client("s3", region_name="us-east-1")
    client.create_bucket(Bucket=BUCKET)
    store = S3Store(bucket=BUCKET, prefix="run/", client=client)

    assert isinstance(store, ObjectStore)
    assert not store.capabilities.local_paths
    assert not store.capabilities.destructive_gc
    assert store.capabilities.sized_inventory

    data = b"x" * 100
    h = hash_bytes(data)
    store.write(h, data, run_id="r1", coordinate="lane")
    store.write(h, data, run_id="r2", coordinate="lane")  # idempotent
    assert store.exists(h)
    assert store.read(h) == data
    assert store.size_of(h) == len(data)
    assert store.read("missing" + "0" * 56) is None

    sizes = resolve_object_sizes(store, {h, "deadbeef" + "0" * 56})
    assert sizes == {h: len(data)}

    infos = {i.key: i.size for i in store.inventory()}
    assert infos[h] == len(data)

    chunks = store.stream(h)
    assert chunks is not None
    assert b"".join(chunks) == data

    # spill path via serialize_output
    blob = b"y" * 8000
    ref, ctype = store.serialize_output("r3", "c", blob)
    assert ctype == "bytes"
    assert isinstance(ref, str) and ref.startswith("objects:")
    assert store.read_output(ref, ctype) == blob

    store.cleanup_staged("r3")  # no-op


@mock_aws
def test_open_store_url_and_client_factory():
    client = boto3.client("s3", region_name="us-east-1")
    client.create_bucket(Bucket=BUCKET)
    store = open_store(
        f"s3://{BUCKET}/pfx",
        client_factory=lambda: boto3.client("s3", region_name="us-east-1"),
    )
    data = b"factory"
    h = hash_bytes(data)
    store.write(h, data)
    assert store.read(h) == data
    assert store.store_config.bucket == BUCKET
    assert store.store_config.prefix == "pfx/"


@mock_aws
def test_pipeline_reuse_against_s3_store():
    client = boto3.client("s3", region_name="us-east-1")
    client.create_bucket(Bucket=BUCKET)
    store = S3Store(bucket=BUCKET, prefix="pipes/", client=client)
    home = _home_with_store(store)

    path = os.path.join(TEST_FOLDER, "a.txt")
    with open(path, "w") as f:
        f.write("one\ntwo\n")

    @step
    def scan():
        for name in sorted(os.listdir(TEST_FOLDER)):
            p = os.path.join(TEST_FOLDER, name)
            if os.path.isfile(p):
                yield {"path": name, "text": open(p).read()}

    @step
    def count(scan: dict):
        # Force a spill so the object store is exercised.
        return ("lines=" + scan["text"]).encode("utf-8") * 200

    pipe = pipeline(name="s3_reuse", steps=[scan, count], home=home)
    r1 = pipe.run(workers=1)
    r2 = pipe.run(workers=1)
    assert r1.created_count > 0
    assert r2.reused_count == r1.created_count
    assert r2.created_count == 0


@mock_aws
def test_home_store_url_constructs_s3_store(monkeypatch):
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    client = boto3.client("s3", region_name="us-east-1")
    client.create_bucket(Bucket=BUCKET)
    home = Home.ephemeral(
        TEST_ENV + "_url",
        db_connect_args={"check_same_thread": False},
        db_poolclass=StaticPool,
        store_url=f"s3://{BUCKET}/via-url",
    )
    assert isinstance(home.store, S3Store)
    assert home.store.prefix == "via-url/"
    data = b"from-url"
    h = hash_bytes(data)
    home.store.write(h, data)
    assert home.store.read(h) == data


@mock_aws
def test_home_store_url_and_env(monkeypatch):
    client = boto3.client("s3", region_name="us-east-1")
    client.create_bucket(Bucket=BUCKET)

    def factory():
        return boto3.client("s3", region_name="us-east-1")

    # Explicit store= wins over URL plumbing for the S3 client.
    store = S3Store(bucket=BUCKET, client_factory=factory)
    home = _home_with_store(store)
    assert home.store is store

    monkeypatch.setenv("RUBEDO_STORE_URL", f"s3://{BUCKET}/from-env")
    Home.clear_registry_for_tests()
    env_store = open_store(
        os.environ["RUBEDO_STORE_URL"],
        client_factory=factory,
    )
    home2 = Home.ephemeral(
        TEST_ENV + "_env",
        db_connect_args={"check_same_thread": False},
        db_poolclass=StaticPool,
        store=env_store,
    )
    assert home2.store.prefix == "from-env/"


@mock_aws
def test_du_uses_sized_inventory_on_s3():
    client = boto3.client("s3", region_name="us-east-1")
    client.create_bucket(Bucket=BUCKET)
    store = S3Store(bucket=BUCKET, prefix="du/", client=client)
    home = _home_with_store(store)

    @step
    def root():
        return b"z" * 9000

    pipe = pipeline(name="s3_du", steps=[root], home=home)
    pipe.run(workers=1)
    report = storage_report(home=home)
    assert report.total_bytes >= 9000
    assert report.missing_objects == 0


@mock_aws
def test_gc_refuses_destructive_delete_on_s3():
    client = boto3.client("s3", region_name="us-east-1")
    client.create_bucket(Bucket=BUCKET)
    store = S3Store(bucket=BUCKET, prefix="gc/", client=client)
    home = _home_with_store(store)

    @step
    def root():
        return b"g" * 9000

    pipe = pipeline(name="s3_gc", steps=[root], home=home, retention=1)
    pipe.run(workers=1)
    pipe.run(workers=1)

    dry = gc(delete=False, home=home)
    assert dry.applied is False
    refused = gc(delete=True, home=home)
    assert refused.applied is False
    assert refused.refused and "destructive GC" in refused.refused
