import os

from pydantic import BaseModel, Field
from rubedo import pipeline


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


p = pipeline(name="count-lines", params_model=CountLinesParams)


@p.step(check_cache=False)
def input_files():
    folder = os.path.join(os.path.dirname(__file__), "input")
    for name in os.listdir(folder):
        path = os.path.join(folder, name)
        if os.path.isfile(path):
            yield path


@p.step
def read_lines(input_files: str, params: dict):
    # params arrive as the params_model-validated dict (the same form that
    # is hashed into the cache key), not as a model instance.
    text = open(input_files).read()
    lines = text.splitlines()
    return {"lines": lines, "params": params}


@p.step
def count_lines(read_lines: dict):
    lines = read_lines["lines"]
    params = CountLinesParams(**read_lines["params"])

    metadata: dict = {
        "line_count": len(lines),
        "empty": len(lines) == 0,
        "ok": len(lines) >= params.min_lines,
        "min_lines": params.min_lines,
    }

    if params.include_text_preview:
        metadata["preview"] = "".join(lines)[:80]

    return {
        "line_count": len(lines),
        "ok": len(lines) >= params.min_lines,
    }


@p.step(shape="reduce")
def total_lines(count_lines: dict):
    return sum(v["line_count"] for v in count_lines.values())


if __name__ == "__main__":
    print(p.describe())
    print()

    summary = p.run(params={"min_lines": 0, "include_text_preview": False})
    print(f"\nRun ID: {summary.run_id}")
    print(f"Created: {summary.created_count}, Reused: {summary.reused_count}")

    print("\n--- Final Output (total_lines) ---")
    import json
    print(json.dumps(summary.output_for("total_lines"), indent=2, default=str))
