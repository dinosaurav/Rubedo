from pydantic import BaseModel, Field
from batchbrain import ProcessResult, describe, run, step, pipeline


class CountLinesParams(BaseModel):
    min_lines: int = Field(
        default=0,
        ge=0,
        description="Minimum number of lines required for ok=true",
    )
    include_text_preview: bool = Field(
        default=False,
        description="Whether to include a short text preview in metadata",
    )


@step(name="read_lines", version="read-v1", params_model=CountLinesParams)
def read_lines(path: str, params: CountLinesParams):
    text = open(path).read()
    lines = text.splitlines()
    return {"lines": lines, "params": params.model_dump()}


@step(name="count_lines", version="count-v1", depends_on=["read_lines"])
def count_lines(read_lines: dict) -> ProcessResult:
    lines = read_lines["lines"]
    params = CountLinesParams(**read_lines["params"])

    metadata = {
        "line_count": len(lines),
        "empty": len(lines) == 0,
        "ok": len(lines) >= params.min_lines,
        "min_lines": params.min_lines,
    }

    if params.include_text_preview:
        metadata["preview"] = "".join(lines)[:80]

    return ProcessResult(
        value={
            "line_count": len(lines),
            "ok": len(lines) >= params.min_lines,
        },
        metadata=metadata,
    )


count_lines_pipeline = pipeline(
    id="count-lines",
    name="Count Lines DAG",
    folder="examples/input",
    steps=[read_lines, count_lines],
)

if __name__ == "__main__":
    print(describe(count_lines_pipeline))
    print()

    summary = run(
        count_lines_pipeline,
        params={"min_lines": 0, "include_text_preview": False},
    )
    print(f"\nRun ID: {summary.run_id}")
    print(f"Created: {summary.created_count}, Reused: {summary.reused_count}")
