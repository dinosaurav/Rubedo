"""Pins definition() output byte-identical.

The JSON snapshot below is `definition()`'s exact output for a pipeline
exercising a representative slice of step policies (expand root, retries,
rate_limit, stale_after, a reduce with group_key, pipeline-level retention).
History and the dashboard read definition_json, so any change to this
snapshot would be a silent data format change, not just an API change.

TODO 21 added the pipeline-level "secrets"/"env" keys, emitted
unconditionally (even empty, as here) since they're declarations rather
than policy toggles — that's the one legitimate reason this pin moved: the
dashboard only ever reads definitions, so additive JSON here is harmless,
unlike the byte-identity this pin actually guards (the TODO-15 hashing
rotation).
"""
import json

from pydantic import BaseModel

from rubedo import pipeline, step
from rubedo.spec import definition

PINNED_DEFINITION_JSON = """\
{
  "env": [],
  "id": "snap-fixture",
  "name": "snap-fixture",
  "retention": 5,
  "secrets": [],
  "steps": [
    {
      "code": "warn",
      "depends_on": [],
      "name": "scan",
      "shape": "expand",
      "source": "@step(name=\\"scan\\", version=\\"1\\", shape=\\"expand\\")\\n    def scan():\\n        yield {\\"path\\": \\"a.txt\\", \\"text\\": \\"hi\\"}",
      "version": "1",
      "workers": 4
    },
    {
      "code": "warn",
      "depends_on": [
        "scan"
      ],
      "name": "enrich",
      "rate_limit": "10/60s",
      "retries": 2,
      "retry_on": [
        "Exception"
      ],
      "source": "@step(\\n        name=\\"enrich\\", version=\\"2\\", depends_on=[\\"scan\\"], retries=2,\\n        rate_limit=\\"10/min\\", stale_after=\\"24h\\",\\n    )\\n    def enrich(scan: dict):\\n        return scan",
      "stale_after_seconds": 86400.0,
      "version": "2",
      "workers": 4
    },
    {
      "code": "warn",
      "depends_on": [
        "enrich"
      ],
      "group_key": "path",
      "name": "rollup",
      "shape": "reduce",
      "source": "@step(\\n        name=\\"rollup\\", version=\\"1\\", shape=\\"reduce\\", depends_on=[\\"enrich\\"],\\n        group_key=\\"path\\",\\n    )\\n    def rollup(enrich):\\n        return enrich",
      "version": "1",
      "workers": 4
    }
  ]
}"""


class Params(BaseModel):
    threshold: int = 3


def _build_snapshot_spec():
    """The pipeline whose definition() is pinned above."""

    @step(name="scan", version="1", shape="expand")
    def scan():
        yield {"path": "a.txt", "text": "hi"}

    @step(
        name="enrich", version="2", depends_on=["scan"], retries=2,
        rate_limit="10/min", stale_after="24h",
    )
    def enrich(scan: dict):
        return scan

    @step(
        name="rollup", version="1", shape="reduce", depends_on=["enrich"],
        group_key="path",
    )
    def rollup(enrich):
        return enrich

    return scan, enrich, rollup


def test_definition_snapshot_is_byte_identical_across_the_rotation():
    scan, enrich, rollup = _build_snapshot_spec()
    p = pipeline(
        name="snap-fixture",
        steps=[scan, enrich, rollup],
        params_model=Params,
        retention=5,
    )
    snapshot = definition(p.spec)  # Pipeline.spec: validated PipelineSpec, built lazily
    assert json.dumps(snapshot, indent=2, sort_keys=True) == PINNED_DEFINITION_JSON
    # Pipeline.definition() is the same snapshot via the object's own verb.
    assert p.definition() == snapshot
