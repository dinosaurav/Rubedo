import os
import shutil
from batchbrain import process
from batchbrain.invalidation import invalidate, recompute
from batchbrain.db import get_session

def count_lines(path: str) -> dict:
    text = open(path, "r", encoding="utf-8").read()
    return {"line_count": len(text.splitlines())}

if __name__ == "__main__":
    input_dir = os.path.join(os.path.dirname(__file__), "input")
    
    # 1. First run (should create everything or reuse if already run)
    summary = process(folder=input_dir, fn=count_lines, code_version="count-lines-v1", workers=4)
    print(f"Run 1: Created {summary.created_count}, Reused {summary.reused_count}")

    # 2. Select coordinate glob
    sel = select(source_folder=input_dir, coordinate_glob="*b.txt*")
    
    # 3. Invalidate
    res = invalidate(sel, reason="testing invalidation")
    print(f"Invalidated: {res['invalidated_count']}")

    # 4. Process again (should create 1, reuse the rest)
    summary2 = process(folder=input_dir, fn=count_lines, code_version="count-lines-v1", workers=4)
    print(f"Run 2: Created {summary2.created_count}, Reused {summary2.reused_count}")
