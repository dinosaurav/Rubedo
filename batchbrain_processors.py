from pydantic import BaseModel, Field
from batchbrain import ProcessResult, step, pipeline

class CountLinesInputs(BaseModel):
    min_lines: int = Field(
        default=0,
        ge=0,
        description="Minimum number of lines required for ok=true",
    )
    include_text_preview: bool = Field(
        default=False,
        description="Whether to include a short text preview in metadata",
    )

@step(name="read_lines", version="read-v1", input_model=CountLinesInputs)
def read_lines(path: str, inputs: CountLinesInputs):
    text = open(path).read()
    lines = text.splitlines()
    return {"lines": lines, "inputs": inputs.model_dump()}

@step(name="count_lines", version="count-v1", depends_on=["read_lines"])
def count_lines(read_lines: dict) -> ProcessResult:
    lines = read_lines["lines"]
    inputs = CountLinesInputs(**read_lines["inputs"])
    
    metadata = {
        "line_count": len(lines),
        "empty": len(lines) == 0,
        "ok": len(lines) >= inputs.min_lines,
        "min_lines": inputs.min_lines,
    }

    if inputs.include_text_preview:
        metadata["preview"] = "".join(lines)[:80]

    return ProcessResult(
        value={
            "line_count": len(lines),
            "ok": len(lines) >= inputs.min_lines,
        },
        metadata=metadata,
    )

pipeline(
    id="count-lines",
    name="Count Lines DAG",
    folder="examples/input",
    steps=[read_lines, count_lines]
)

if __name__ == "__main__":
    from batchbrain.processor_runner import run_processor
    summary = run_processor(
        "count-lines",
        inputs={"min_lines": 0, "include_text_preview": False},
    )
    print("\nScript Wrapper Completed.")
    print(f"Run ID: {summary.run_id}")
    print(f"Total processed: {summary.created_count + summary.reused_count + summary.failed_count}")
    print(f"Created: {summary.created_count}, Reused: {summary.reused_count}")
