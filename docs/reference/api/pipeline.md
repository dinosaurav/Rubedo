# Pipelines

Assembling steps into a runnable DAG: the `pipeline()` factory, the
`Pipeline` object it returns (whose methods — `.run()`, `.plan()`,
`.describe()`, `.definition()` — are the verbs), the `PipelineSpec`
snapshot, and the result objects a run or plan hands back.

::: rubedo.pipeline.pipeline

::: rubedo.pipeline.Pipeline

::: rubedo.spec.PipelineSpec

::: rubedo.runner.RunPlan

::: rubedo.models.RunSummary
