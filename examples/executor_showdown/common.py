"""Shared pipeline for the executor showdown — thread vs process on real CPU work.

    dictionary (8 chunks) ─▶ analyze ─▶ combine (reduce)
                             (CPU-bound)  (merge + report)

Downloads a real English word list (~370k words, dwyl/english-words on
GitHub — public, no key) and splits it into 8 chunks, one per lane. Each
chunk gets a genuinely CPU-bound feature-extraction pass over every word:
an anagram signature, a 26-letter frequency vector, and a rotation-invariant
canonical form (the lexicographically smallest of all 26 Caesar shifts of
the signature). The reduce step merges every chunk's partial groups and
reports the largest anagram group, the largest rotation-invariant group,
and the dictionary's most common letter.

`run_thread.py` and `run_process.py` both call `build_pipeline()` here,
differing only in `executor=`. Their step names differ too
(`analyze_thread` vs `analyze_process`) — deliberately: an output address is
`hash(step_name, version, input_hash, ...)` (rubedo/hashing.py), which does
NOT include the pipeline id, so two pipelines with a same-named step over
identical input would silently share one cached materialization. Distinct
names keep each script's timing honest regardless of run order.

Note on `analyze_chunk` below: it's built into a StepSpec via the call form
`step(...)(analyze_chunk)`, not the `@step` decorator, on purpose — decorating
in place would rebind the module-level name `analyze_chunk` to the StepSpec,
and a process-executor step is pickled by looking up its `module.qualname`.
If that name no longer pointed at the function, pickling `step.fn` would
fail. Keeping the function and its StepSpec under different names (like
`examples/gutenberg_stats/gutenberg_stats.py` does with `analyze_book`/
`analyze`) sidesteps that entirely — and here it's required twice over,
since two different StepSpecs wrap the same function.

The word list download happens once and is cached to a local file next to
this script (gitignored) — that's different from every other example's
"download inside a step" pattern, because Rubedo's own materialization
cache only covers step outputs, not Source.scan() itself, and chunking has
to happen before any step exists to cache anything.

Run either variant:

    uv run python examples/executor_showdown/run_thread.py
    uv run python examples/executor_showdown/run_process.py

Pass --force to either to bypass Rubedo's cache and re-pay the full CPU
cost (otherwise a second run of the *same* script just reuses its cached
result almost instantly — which is Rubedo's whole point, but it means the
thread-vs-process comparison only shows up on a first/forced run).
"""
import hashlib
import os
import sys
import time
import urllib.request

from rubedo import Source, SourceItem, describe, pipeline, run, step
from rubedo.db import get_session
from rubedo.models import Materialization, RunCoordinateStatus
from rubedo.store import read_materialization_output

WORDLIST_URL = "https://raw.githubusercontent.com/dwyl/english-words/master/words_alpha.txt"
CACHE_PATH = os.path.join(os.path.dirname(__file__), ".wordlist_cache.txt")
NUM_CHUNKS = 8


def _ensure_wordlist() -> list:
    """Download the word list once; reuse the local cache on later runs."""
    if not os.path.exists(CACHE_PATH):
        req = urllib.request.Request(WORDLIST_URL, headers={"User-Agent": "rubedo-example"})
        with urllib.request.urlopen(req, timeout=30) as r:
            data = r.read().decode("utf-8")
        with open(CACHE_PATH, "w") as f:
            f.write(data)
    with open(CACHE_PATH) as f:
        return [w.strip() for w in f if len(w.strip()) >= 2]


class ChunkedWordlist(Source):
    """The word list split into NUM_CHUNKS lanes, one coordinate per chunk."""

    def __init__(self, num_chunks: int = NUM_CHUNKS):
        self.num_chunks = num_chunks

    @property
    def id(self) -> str:
        return f"wordlist-chunks:{self.num_chunks}"

    def scan(self):
        words = _ensure_wordlist()
        chunk_size = -(-len(words) // self.num_chunks)  # ceil div
        items = []
        for i in range(self.num_chunks):
            chunk = words[i * chunk_size : (i + 1) * chunk_size]
            if not chunk:
                continue
            content_hash = hashlib.sha256("\n".join(chunk).encode()).hexdigest()
            items.append(
                SourceItem(coordinate=f"chunk_{i:02d}", content_hash=content_hash, ref=chunk)
            )
        return items

    def load(self, item: SourceItem) -> list:
        return item.ref


def _rotation_invariant(sig: str) -> str:
    """The lexicographically smallest of all 26 Caesar shifts of sig."""
    best = sig
    for shift in range(1, 26):
        shifted = "".join(chr((ord(c) - 97 + shift) % 26 + 97) for c in sig)
        if shifted < best:
            best = shifted
    return best


def analyze_chunk(words: list) -> dict:
    """CPU-bound feature extraction over one chunk of the dictionary.

    Real, if somewhat contrived, per-word work — an anagram signature, a
    letter-frequency histogram, and a rotation-invariant canonical form —
    chosen to be intensive enough that the thread-vs-process difference is
    visible on ordinary hardware, not lost in noise.
    """
    anagram_groups = {}
    rotation_groups = {}
    letter_freq = [0] * 26
    for w in words:
        sig = "".join(sorted(w))
        for ch in w:
            letter_freq[ord(ch) - 97] += 1
        rot = _rotation_invariant(sig)
        anagram_groups.setdefault(sig, []).append(w)
        rotation_groups.setdefault(rot, []).append(w)
    return {
        "anagram_groups": anagram_groups,
        "rotation_groups": rotation_groups,
        "letter_freq": letter_freq,
        "word_count": len(words),
    }


def combine_chunks(**parent_outputs) -> dict:
    """Merge every chunk's partial groups into one global answer.

    Takes **kwargs rather than a fixed `analyze: dict` parameter because the
    upstream step is named `analyze_thread` or `analyze_process` (see
    build_pipeline) — a step's parameter name must match its declared
    `depends_on` entry exactly, and this function is shared by both variants.
    There's exactly one dependency, so we just take whichever key shows up.
    """
    analyze = next(iter(parent_outputs.values()))
    anagram_groups = {}
    rotation_groups = {}
    letter_totals = [0] * 26
    total_words = 0
    for chunk_result in analyze.values():
        for sig, ws in chunk_result["anagram_groups"].items():
            anagram_groups.setdefault(sig, []).extend(ws)
        for rot, ws in chunk_result["rotation_groups"].items():
            rotation_groups.setdefault(rot, []).extend(ws)
        for i, c in enumerate(chunk_result["letter_freq"]):
            letter_totals[i] += c
        total_words += chunk_result["word_count"]

    biggest_anagram = max(anagram_groups.values(), key=len)
    biggest_rotation = max(rotation_groups.values(), key=len)
    top_letter = "abcdefghijklmnopqrstuvwxyz"[letter_totals.index(max(letter_totals))]

    return {
        "total_words": total_words,
        "largest_anagram_group": sorted(biggest_anagram),
        "largest_rotation_invariant_group_size": len(biggest_rotation),
        "most_common_letter": top_letter,
    }


def build_pipeline(executor: str):
    """executor is 'thread' or 'process' — also used to suffix step names
    so the two variants never share a cached materialization (see module
    docstring)."""
    analyze = step(
        name=f"analyze_{executor}",
        version="1",
        executor=executor,
        workers=NUM_CHUNKS,
    )(analyze_chunk)

    combine = step(
        name=f"combine_{executor}",
        version="1",
        depends_on=[f"analyze_{executor}"],
        shape="reduce",
    )(combine_chunks)

    return pipeline(
        id=f"executor-showdown-{executor}",
        name=f"Executor Showdown ({executor})",
        source=ChunkedWordlist(),
        steps=[analyze, combine],
    )


def _fetch_result(run_id: str, combine_step_name: str):
    """Read back what the reduce step actually produced, for this run."""
    with get_session() as session:
        rc = (
            session.query(RunCoordinateStatus)
            .filter_by(run_id=run_id, step_name=combine_step_name, coordinate="@all")
            .first()
        )
        if not rc or not rc.materialization_id:
            return None
        mat = session.query(Materialization).filter_by(id=rc.materialization_id).first()
        return read_materialization_output(mat)


def main(executor: str):
    force = "--force" in sys.argv
    pipe = build_pipeline(executor)
    print(describe(pipe))
    print()

    t0 = time.perf_counter()
    summary = run(pipe, force=force)
    elapsed = time.perf_counter() - t0

    print(
        f"executor={executor!r} elapsed={elapsed:.2f}s "
        f"created={summary.created_count} reused={summary.reused_count}"
    )

    if summary.reused_count and not summary.created_count:
        print(
            "\nEverything was reused from a previous run (Rubedo caches by content) — "
            "elapsed time above reflects lookup/ledger overhead, not the real compute "
            "cost. Pass --force to pay it again and see the executor difference."
        )

    result = _fetch_result(summary.run_id, f"combine_{executor}")
    if result:
        print(f"\ntotal words analyzed:        {result['total_words']}")
        print(
            f"largest anagram group ({len(result['largest_anagram_group'])} words): "
            f"{result['largest_anagram_group']}"
        )
        print(
            "largest rotation-invariant group size: "
            f"{result['largest_rotation_invariant_group_size']}"
        )
        print(f"most common letter:          {result['most_common_letter']!r}")
