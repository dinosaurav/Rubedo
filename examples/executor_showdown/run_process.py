"""executor="process" — see common.py for the full pipeline.

    uv run python examples/executor_showdown/run_process.py [--force]

Each of the 8 chunks runs in its own OS process, so this should scale with
however many CPU cores are available, unlike run_thread.py's GIL-bound
version. Compare the two elapsed times.
"""
from common import main

if __name__ == "__main__":
    main("process")
