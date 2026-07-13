"""Pins definition() output byte-identical across the TODO-15 rotation.

Recorded *before* the rotation (one Pipeline object, verbs as methods):
the JSON snapshot below is `definition()`'s exact output for a pipeline
exercising a representative slice of step policies (expand root, retries,
rate_limit, stale_after, a reduce with group_key, pipeline-level retention).
After the rotation, `pipeline(...)` returns a `Pipeline` instead of a bare
`PipelineSpec` — this test's construction lines change to match (`.spec` /
`.definition()`), but the pinned JSON string itself must not, proving the
rotation is a pure API reshuffle with no effect on what the ledger records
(history and the dashboard read definition_json; a fork there would be a
silent data format change, not just an API change).
"""
import json

from pydantic import BaseModel

from rubedo import pipeline, step
from rubedo.spec import definition

PINNED_DEFINITION_JSON = """\
{
  "id": "snap-fixture",
  "name": "snap-fixture",
  "retention": 5,
  "steps": [
    {
      "code": "warn",
      "depends_on": [],
      "name": "scan",
      "shape": "expand",
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
      "version": "1",
      "workers": 4
    }
  ]
}"""


class Params(BaseModel):
    threshold: int = 3


def _build_snapshot_spec():
    """The pipeline whose definition() is pinned above. Kept as a plain
    spec-builder function so both the pre- and post-rotation test bodies
    can share it — only the last line (how a PipelineSpec is obtained from
    `pipeline(...)`) differs across the rotation."""

    @step(name="scan", version="1", shape="expand")
    def scan():
        yield {"path": "a.txt", "text": "hi"}

    @step(
        name="enrich", version="2", depends_on=["scan"], retries=2,
        rate_limit="10/min", stale_after="24h", index=["path"],
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
    spec = pipeline(
        name="snap-fixture",
        steps=[scan, enrich, rollup],
        params_model=Params,
        retention=5,
    )
    snapshot = definition(spec)
    assert json.dumps(snapshot, indent=2, sort_keys=True) == PINNED_DEFINITION_JSON
