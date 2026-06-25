import os
import sys
import time

from batchbrain import ProcessResult, process

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

if __name__ == "__main__":
    from batchbrain.processor_runner import run_processor
    summary = run_processor(
        "count-lines",
        inputs={"min_lines": 0, "include_text_preview": False},
    )
    print(f"Run {summary.run_id} finished")
    print(f"Created: {summary.created_count}")
    print(f"Reused: {summary.reused_count}")
    print(f"Failed: {summary.failed_count}")
