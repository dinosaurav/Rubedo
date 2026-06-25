from pydantic import BaseModel, Field
from batchbrain import ProcessResult, processor

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

@processor(
    id="count-lines",
    name="Count Lines",
    folder="examples/input",
    code_version="count-lines-v1",
    input_model=CountLinesInputs,
    workers=4,
)
def count_lines(path: str, inputs: CountLinesInputs) -> ProcessResult:
    text = open(path).read()
    lines = text.splitlines()

    metadata = {
        "line_count": len(lines),
        "empty": len(lines) == 0,
        "ok": len(lines) >= inputs.min_lines,
        "min_lines": inputs.min_lines,
    }

    if inputs.include_text_preview:
        metadata["preview"] = text[:80]

    return ProcessResult(
        value={
            "line_count": len(lines),
            "ok": len(lines) >= inputs.min_lines,
        },
        metadata=metadata,
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
