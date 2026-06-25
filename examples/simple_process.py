import os
import sys

# Add engine to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "engine")))

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
    input_dir = os.path.join(os.path.dirname(__file__), "input")
    
    summary = process(
        folder=input_dir,
        fn=count_lines,
        code_version="count-lines-v2",
        workers=4,
    )
    print(f"Run {summary.run_id} finished with status: {summary.status}")
    print(f"Created: {summary.created_count}")
    print(f"Reused: {summary.reused_count}")
    print(f"Failed: {summary.failed_count}")
