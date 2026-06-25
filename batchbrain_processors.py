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
