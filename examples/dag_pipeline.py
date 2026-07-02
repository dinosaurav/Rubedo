import os
from batchbrain import step, pipeline, run_pipeline


# 1. First Step: Read file and split into lines
@step(name="read_lines", version="1")
def read_lines(path: str) -> list[str]:
    with open(path, "r", encoding="utf-8") as f:
        return [line.strip() for line in f.readlines() if line.strip()]


# 2. Second Step: Convert lines to uppercase (depends on read_lines)
@step(name="uppercase_lines", version="1", depends_on=["read_lines"])
def uppercase_lines(read_lines: list[str]) -> list[str]:
    return [line.upper() for line in read_lines]


# 3. Third Step: Count vowels in all uppercase lines (depends on uppercase_lines)
@step(name="count_vowels", version="1", depends_on=["uppercase_lines"])
def count_vowels(uppercase_lines: list[str]) -> dict:
    vowels = "AEIOU"
    count = sum(1 for line in uppercase_lines for char in line if char in vowels)
    return {"total_vowels": count, "total_lines": len(uppercase_lines)}


# Combine all steps into a DAG pipeline
dag_pipeline_spec = pipeline(
    id="text-analyzer-dag",
    name="Text Analyzer DAG",
    folder="examples/input",
    steps=[read_lines, uppercase_lines, count_vowels],
)

if __name__ == "__main__":
    # Ensure some inputs exist
    input_dir = os.path.join(os.path.dirname(__file__), "input")
    os.makedirs(input_dir, exist_ok=True)
    with open(os.path.join(input_dir, "sample1.txt"), "w") as f:
        f.write("Hello World\nThis is a DAG pipeline\nBatchbrain is awesome!")
    with open(os.path.join(input_dir, "sample2.txt"), "w") as f:
        f.write("Another test file\nTo test caching and DAG logic.")

    print(f"Running DAG Pipeline '{dag_pipeline_spec.name}'...")
    summary = run_pipeline(
        pipeline=dag_pipeline_spec,
        folder=input_dir,
        workers=2,
    )

    print("\n--- Execution Summary ---")
    print(f"Run ID: {summary.run_id}")
    print(f"Outputs created: {summary.created_count}")
    print(f"Outputs reused: {summary.reused_count}")
    print(f"Outputs failed: {summary.failed_count}")
    print(f"Outputs blocked: {summary.blocked_count}")

    print("\nRun it again to see cached hits (reused)!")
