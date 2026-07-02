from batchbrain import ProcessResult, step, pipeline, run_pipeline


@step(name="count-lines", version="1.0.0")
def count_lines(path: str) -> ProcessResult:
    text = open(path, "r", encoding="utf-8").read()
    lines = text.splitlines()

    # Intentionally fail for a specific file to test failures
    if "fail.txt" in path:
        raise ValueError("Intentional failure")

    return ProcessResult(
        value={
            "path": path,
            "line_count": len(lines),
        },
        metadata={
            "path": path,
            "line_count": len(lines),
            "empty": len(lines) == 0,
        },
    )


p = pipeline(
    id="line-counter", name="Line Counter", folder="examples/input", steps=[count_lines]
)

if __name__ == "__main__":
    summary = run_pipeline(
        pipeline=p,
        source="examples/input",
        workers=1,
    )

    print(f"Run {summary.run_id} finished")
    print(f"Created: {summary.created_count}")
    print(f"Reused: {summary.reused_count}")
    print(f"Failed: {summary.failed_count}")
