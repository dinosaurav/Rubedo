"""executor="thread" vs executor="process" on real CPU-bound work, timed.

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

This runs the *same* pipeline twice — once with executor="thread" (the
default), once with executor="process" — and times each. CPU-bound work
under threads doesn't parallelize (the GIL lets only one thread execute
Python bytecode at a time); under processes it does, one core per chunk.

Run it:

    uv run python examples/executor_showdown/executor_showdown.py [--force]

Pass --force to bypass Rubedo's cache and re-pay the full CPU cost —
otherwise the *second* time you run this script, both variants just reuse
their cached results almost instantly (Rubedo's whole point), and the
thread-vs-process difference only shows up on a first/forced run.

Two things worth knowing about how this is built:

- The "thread" and "process" pipelines use differently-named steps
  (`analyze_thread`/`analyze_process`) rather than one shared step name.
  An output address is `hash(step_name, version, input_hash, ...)`
  (rubedo/hashing.py) — it does NOT include the pipeline id, so two
  pipelines with a same-named step over identical input would silently
  share one cached materialization, which would quietly break the timing
  comparison after the first run.
- `analyze_chunk` is wired into a StepSpec via the call form
  `step(...)(analyze_chunk)`, not the `@step` decorator — decorating in
  place would rebind the module-level name `analyze_chunk` to the
  StepSpec, and a process-executor step is pickled by looking up its
  `module.qualname`. If that name no longer pointed at the function,
  pickling `step.fn` would fail. Keeping the function and its StepSpec(s)
  under different names (like `examples/gutenberg_stats/gutenberg_stats.py`
  does with `analyze_book`/`analyze`) sidesteps that — needed twice over
  here, since the same function is wrapped into two different StepSpecs.

The word list download happens once and is cached to a local file next to
this script (gitignored) — different from every other example's "download
inside a step" pattern, because Rubedo's own materialization cache only
covers step outputs, not Source.scan() itself, and chunking has to happen
before any step exists to cache anything.
"""
import os
import sys
import time
import urllib.request

from rubedo import pipeline


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
    anagram_groups: dict[str, list[str]] = {}
    rotation_groups: dict[str, list[str]] = {}
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
    anagram_groups: dict[str, list[str]] = {}
    rotation_groups: dict[str, list[str]] = {}
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
    p = pipeline(name=f"executor-showdown-{executor}")

    @p.step(name="wordlist_chunks", version="1")
    def wordlist_chunks():
        words = _ensure_wordlist()
        chunk_size = -(-len(words) // NUM_CHUNKS)
        for i in range(NUM_CHUNKS):
            chunk = words[i * chunk_size : (i + 1) * chunk_size]
            if chunk:
                yield chunk

    p.step(
        name=f"analyze_{executor}",
        version="1",
        depends_on=["wordlist_chunks"],
        executor=executor,
        workers=NUM_CHUNKS,
    )(analyze_chunk)

    p.step(
        name=f"combine_{executor}",
        version="1",
        depends_on=[f"analyze_{executor}"],
        shape="reduce",
    )(combine_chunks)

    return p


def run_variant(executor: str, force: bool) -> float:
    pipe = build_pipeline(executor)
    print(pipe.describe())
    print()

    t0 = time.perf_counter()
    summary = pipe.run(force=force)
    elapsed = time.perf_counter() - t0

    print(
        f"executor={executor!r} elapsed={elapsed:.2f}s "
        f"created={summary.created_count} reused={summary.reused_count}"
    )
    if summary.reused_count and not summary.created_count:
        if force:
            # force=True re-executes every step regardless of cache — elapsed
            # above is real compute time. reused_count is still high because
            # the recomputed bytes are identical to what's already stored, so
            # the generations protocol dedupes rather than writing a new one
            # (rubedo/ledger.py's _commit_materialization) — reused here means
            # "no new generation," not "skipped execution."
            print("(recomputed via --force; bytes matched the prior run, so no new generation)")
        else:
            print(
                "(reused from a previous run — pass --force to pay the real compute "
                "cost and see the executor difference)"
            )

    result_dict = summary.output_for(f"combine_{executor}")
    result = result_dict.get("@all") if result_dict else None
    
    if result:
        print(f"total words analyzed: {result['total_words']}")
        print(
            f"largest anagram group ({len(result['largest_anagram_group'])} words): "
            f"{result['largest_anagram_group']}"
        )
        print(
            "largest rotation-invariant group size: "
            f"{result['largest_rotation_invariant_group_size']}"
        )
        print(f"most common letter: {result['most_common_letter']!r}")
    print()
    return elapsed


def main():
    force = "--force" in sys.argv
    thread_elapsed = run_variant("thread", force)
    process_elapsed = run_variant("process", force)

    print(f"thread:  {thread_elapsed:.2f}s")
    print(f"process: {process_elapsed:.2f}s")
    if process_elapsed > 0:
        print(f"({thread_elapsed / process_elapsed:.1f}x)")


if __name__ == "__main__":
    main()
