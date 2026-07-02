import os
from batchbrain import step, pipeline, run_pipeline
from batchbrain.invalidation import invalidate
from batchbrain.selection import select


@step(name="count-lines", version="1")
def count_lines(path: str) -> dict:
    text = open(path, "r", encoding="utf-8").read()
    return {"line_count": len(text.splitlines())}


p = pipeline(id="p-count", name="Counter", folder="examples/input", steps=[count_lines])

if __name__ == "__main__":
    input_dir = os.path.join(os.path.dirname(__file__), "input")

    # 1. First run (should create everything or reuse if already run)
    summary = run_pipeline(pipeline=p, folder=input_dir, workers=4)
    print(f"Run 1: Created {summary.created_count}, Reused {summary.reused_count}")

    # 2. Select coordinate glob
    sel = select(source_folder=input_dir, coordinate_glob="*b.txt*")

    # 3. Invalidate
    res = invalidate(sel, reason="testing invalidation")
    print(f"Invalidated: {res['invalidated_count']}")

    # 4. Process again (should create 1, reuse the rest)
    summary2 = run_pipeline(pipeline=p, folder=input_dir, workers=4)
    print(f"Run 2: Created {summary2.created_count}, Reused {summary2.reused_count}")
