# API Reference

Everything here is importable from the top-level `rubedo` package —
`from rubedo import step, pipeline, ...` — except `gc()` and
`storage_report()`, which live in their own submodules (`rubedo.gc`,
`rubedo.du`) and aren't part of `rubedo.__all__`. There is no free
`run()`/`plan()`/`describe()` — they're methods on the `Pipeline` object
`pipeline()` returns; `trace()`/`invalidate()`/`gc()` stay free
functions since they're store-level, not pipeline-level.

These pages are generated from the docstrings in `src/rubedo/`, so the
signatures cannot drift from the source.

- [Steps](step.md) — `@step`, `StepSpec`, and the `Filtered` verdict
- [Pipelines](pipeline.md) — `pipeline()`, `Pipeline`, and the run/plan
  result objects
- [Selection, trace & invalidation](selection.md) — querying the store
  and surgically invalidating it
- [GC & storage](gc.md) — retention garbage collection and disk-usage
  reporting
