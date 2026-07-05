"""executor="thread" (the default) — see common.py for the full pipeline.

    uv run python examples/executor_showdown/run_thread.py [--force]

CPU-bound work under threads doesn't parallelize (the GIL lets only one
thread execute Python bytecode at a time), so this should take roughly as
long as doing all 8 chunks serially. Compare against run_process.py.
"""
from common import main

if __name__ == "__main__":
    main("thread")
