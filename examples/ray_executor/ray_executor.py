"""Public-domain books + simulated Shor's algorithm on a local Ray cluster.

Book pipeline
-------------

    books.csv ─▶ fetch ─▶ clean ─▶ chapters ─▶ lexicon ─▶ style ─▶ phrases
                 (HTTP)   (skip_   (expand)    (Ray)      (Ray)    (Ray)
                          cache)
                                    └─▶ digest (aggregate, group_key=book)
                                          └─▶ report (aggregate)

Shor pipeline
-------------

    targets ─▶ factor (Ray) ─▶ shor_report

``factor`` runs a **classical simulation** of Shor's period-finding
(modular exponentiation over a ``Q ≈ N²`` register + IQFT via NumPy FFT +
continued fractions). That cost is ``O(N²)``, so only small semiprimes are
in reach — roughly **12-bit in a few seconds**, **13-bit in ~15s**, and
**14-bit is memory-bound** (~1 GiB+ for the IQFT) on a laptop. A real
quantum device would scale as ``poly(log N)``.

Ray is in the repo's ``dev`` dependency group (Rubedo never imports it):

    uv run python examples/ray_executor/ray_executor.py

    RUBEDO_RAY_ONLY=books|shor|all   # default all
    RUBEDO_RAY_CPUS=4
    RUBEDO_RAY_TTR_WINDOW=250
    RUBEDO_RAY_TTR_STRIDE=10
"""
from __future__ import annotations

import csv
import math
import os
import re
import sys
import tempfile
import time
from collections import Counter

from rubedo import Home, pipeline, step

# Runnable as ``python examples/ray_executor/ray_executor.py`` (cwd on
# sys.path) *or* imported as ``examples.ray_executor.ray_executor``.
try:
    from examples.ray_executor.shor import shor_factor
except ModuleNotFoundError:  # script execution from repo root
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    from shor import shor_factor  # type: ignore




_ray_started = False

GUTENBERG = "https://www.gutenberg.org/cache/epub/{id}/pg{id}.txt"
BOOKS_CSV = os.path.join(os.path.dirname(__file__), "books.csv")
RAY_CPUS = int(os.environ.get("RUBEDO_RAY_CPUS", str(os.cpu_count() or 4)))
TTR_WINDOW = int(os.environ.get("RUBEDO_RAY_TTR_WINDOW", "250"))
TTR_STRIDE = int(os.environ.get("RUBEDO_RAY_TTR_STRIDE", "10"))

FUNCTION_WORDS = (
    "the", "and", "of", "to", "a", "in", "that", "it", "is", "was",
    "for", "on", "with", "as", "he", "she", "at", "by", "be", "this",
    "have", "from", "or", "one", "had", "but", "not", "what", "all",
    "were", "when", "we", "there", "can", "an", "your", "which", "their",
    "said", "if", "do", "will", "each", "about", "how", "up", "out",
    "them", "then", "so", "some", "her", "would", "make", "like", "him",
    "into", "time", "has", "look", "two", "more", "write", "go", "see",
    "number", "no", "way", "could", "people", "my", "than", "first",
    "water", "been", "call", "who", "oil", "its", "now", "find", "long",
    "down", "day", "did", "get", "come", "made", "may", "part",
)

_WORD_RE = re.compile(r"[a-zA-Z']+")
_SENT_RE = re.compile(r"[^.!?]+[.!?]+|[^.!?]+$")
_CHAPTER_RE = re.compile(
    r"(?m)^(CHAPTER|Chapter|LETTER|Letter|ACT|Act)"
    r"[\t ]+[IVXLCDM\d][\w.\-',;: ]{0,100}\r?$"
)


class RayPool:
    """Thin ``submit``/``shutdown`` adapter around Ray remote tasks."""

    def submit(self, fn, *args, **kwargs):
        import ray

        return ray.remote(fn).remote(*args, **kwargs).future()

    def shutdown(self, wait: bool = True) -> None:
        del wait


def make_ray_pool():
    """Zero-argument factory returning a Future-shaped Ray pool."""
    global _ray_started
    import ray

    if not _ray_started:
        ray.init(num_cpus=RAY_CPUS, ignore_reinit_error=True)
        _ray_started = True
    return RayPool()


def _tokenize(text: str) -> list[str]:
    return [w.lower() for w in _WORD_RE.findall(text)]


def _syllable_count(word: str) -> int:
    word = re.sub(r"[^a-z]", "", word.lower())
    if not word:
        return 0
    groups = re.findall(r"[aeiouy]+", word)
    count = len(groups)
    if word.endswith("e") and count > 1:
        count -= 1
    return max(count, 1)


def _windowed_ttr(words: list[str], window: int, stride: int) -> list[float]:
    if not words:
        return []
    if len(words) <= window:
        return [len(set(words)) / len(words)]
    ratios: list[float] = []
    last = len(words) - window
    for start in range(0, last + 1, max(stride, 1)):
        chunk = words[start : start + window]
        ratios.append(len(set(chunk)) / window)
    return ratios


def _split_chapters(text: str) -> list[tuple[str, str]]:
    """Split on common Gutenberg chapter/letter/act headings."""
    matches = list(_CHAPTER_RE.finditer(text))
    if len(matches) < 2:
        return [("full text", text)]

    chapters: list[tuple[str, str]] = []
    for index, match in enumerate(matches):
        heading = match.group(0).strip()
        start = match.end()
        end = matches[index + 1].start() if index + 1 < len(matches) else len(text)
        body = text[start:end].strip()
        if len(_tokenize(body)) < 80:
            continue
        chapters.append((heading, body))
    return chapters or [("full text", text)]


@step
def books():
    with open(BOOKS_CSV, newline="") as handle:
        for row in csv.DictReader(handle):
            yield {"id": row["id"], "title": row["title"]}


@step(retries=3, retry_delay=2)
def fetch(books: dict) -> dict:
    """Download one Project Gutenberg text."""
    import urllib.request

    url = GUTENBERG.format(id=books["id"])
    req = urllib.request.Request(url, headers={"User-Agent": "rubedo-ray-example"})
    with urllib.request.urlopen(req, timeout=60) as response:
        text = response.read().decode("utf-8", errors="replace")
    return {"id": books["id"], "title": books["title"], "text": text}


@step(skip_cache=True)
def clean(fetch: dict) -> dict:
    """Strip Gutenberg boilerplate; fused into downstream cache keys."""
    text = fetch["text"]
    start = re.search(r"\*\*\* START OF.*?\*\*\*", text, re.S)
    end = re.search(r"\*\*\* END OF.*?\*\*\*", text, re.S)
    body = text[start.end() : end.start()] if start and end else text
    return {"id": fetch["id"], "title": fetch["title"], "text": body}


@step
def chapters(clean: dict):
    """Fan out one lane per chapter — this is where Ray gets real width."""
    for index, (heading, body) in enumerate(_split_chapters(clean["text"])):
        yield {
            "book_id": clean["id"],
            "title": clean["title"],
            "chapter_index": index,
            "chapter": heading,
            "text": body,
        }


@step(executor=make_ray_pool)
def lexicon(chapters: dict) -> dict:
    """Ray hop 1: tokenization and bag-of-words stats for one chapter."""
    words = _tokenize(chapters["text"])
    total = len(words)
    counts = Counter(words)
    unique = len(counts)
    return {
        "book_id": chapters["book_id"],
        "title": chapters["title"],
        "chapter_index": chapters["chapter_index"],
        "chapter": chapters["chapter"],
        "text": chapters["text"],
        "words": total,
        "unique": unique,
        "hapax": sum(1 for count in counts.values() if count == 1),
        "lexical_diversity": round(unique / total, 4) if total else 0.0,
        "avg_word_len": round(sum(len(w) for w in words) / total, 3) if total else 0.0,
        "top_words": counts.most_common(20),
    }


@step(executor=make_ray_pool)
def style(lexicon: dict) -> dict:
    """Ray hop 2: chapter stylometry (Flesch + windowed TTR + function words)."""
    text = lexicon["text"]
    words = _tokenize(text)
    counts = Counter(words)
    sentences = [s.strip() for s in _SENT_RE.findall(text) if s.strip()]
    n_sent = max(len(sentences), 1)
    n_words = max(len(words), 1)
    syllables = sum(_syllable_count(w) for w in words)
    flesch = 206.835 - 1.015 * (n_words / n_sent) - 84.6 * (syllables / n_words)
    window = max(TTR_WINDOW, 50)
    stride = max(TTR_STRIDE, 1)
    ttr_windows = _windowed_ttr(words, window, stride)
    return {
        "book_id": lexicon["book_id"],
        "title": lexicon["title"],
        "chapter_index": lexicon["chapter_index"],
        "chapter": lexicon["chapter"],
        "text": text,
        "words": lexicon["words"],
        "unique": lexicon["unique"],
        "lexical_diversity": lexicon["lexical_diversity"],
        "top_words": lexicon["top_words"],
        "sentences": len(sentences),
        "avg_sentence_words": round(n_words / n_sent, 2),
        "flesch_reading_ease": round(flesch, 2),
        "ttr_windows": len(ttr_windows),
        "ttr_mean": round(sum(ttr_windows) / len(ttr_windows), 4) if ttr_windows else 0.0,
        "function_words_per_k": {
            word: round(counts[word] * 1000 / n_words, 3)
            for word in FUNCTION_WORDS
        },
    }


@step(executor=make_ray_pool)
def phrases(style: dict) -> dict:
    """Ray hop 3: PMI-ranked bigrams/trigrams for one chapter."""
    words = _tokenize(style["text"])
    total = len(words)
    unigrams = Counter(words)
    bigrams = Counter(zip(words, words[1:]))
    trigrams = Counter(zip(words, words[1:], words[2:]))

    def pmi_rows(counter: Counter, order: int, limit: int = 10):
        rows = []
        for gram, count in counter.items():
            if count < 3:
                continue
            if order == 2:
                a, b = gram
                expected = (unigrams[a] * unigrams[b]) / max(total, 1)
                label = f"{a} {b}"
            else:
                a, b, c = gram
                expected = (unigrams[a] * unigrams[b] * unigrams[c]) / (total * total)
                label = f"{a} {b} {c}"
            if expected <= 0:
                continue
            rows.append((math.log2(count / expected), count, label))
        rows.sort(key=lambda row: (row[0], row[1]), reverse=True)
        return [
            {"phrase": label, "count": count, "pmi": round(score, 3)}
            for score, count, label in rows[:limit]
        ]

    return {
        "book_id": style["book_id"],
        "title": style["title"],
        "chapter_index": style["chapter_index"],
        "chapter": style["chapter"],
        "words": style["words"],
        "unique": style["unique"],
        "lexical_diversity": style["lexical_diversity"],
        "flesch_reading_ease": style["flesch_reading_ease"],
        "ttr_mean": style["ttr_mean"],
        "top_bigrams": pmi_rows(bigrams, 2),
        "top_trigrams": pmi_rows(trigrams, 3),
    }


@step(group_key="book_id", depends_on=["phrases"])
def digest(phrases: dict) -> dict:
    """Roll chapter lanes back into one profile per book."""
    chapters = sorted(phrases.values(), key=lambda row: row["chapter_index"])
    total_words = sum(row["words"] for row in chapters)
    # Word-weighted means so short chapters don't dominate.
    def weighted(field: str) -> float:
        if not total_words:
            return 0.0
        return sum(row[field] * row["words"] for row in chapters) / total_words

    # Merge chapter bigram tables by phrase and re-rank by total count.
    bigram_counts: Counter[str] = Counter()
    bigram_pmi: dict[str, float] = {}
    for row in chapters:
        for item in row["top_bigrams"]:
            bigram_counts[item["phrase"]] += item["count"]
            bigram_pmi[item["phrase"]] = max(
                bigram_pmi.get(item["phrase"], item["pmi"]),
                item["pmi"],
            )
    top_bigrams = [
        {"phrase": phrase, "count": count, "pmi": bigram_pmi[phrase]}
        for phrase, count in bigram_counts.most_common(8)
    ]
    title = chapters[0]["title"]
    return {
        "book_id": chapters[0]["book_id"],
        "title": title,
        "chapters": len(chapters),
        "words": total_words,
        "lexical_diversity": round(weighted("lexical_diversity"), 4),
        "flesch_reading_ease": round(weighted("flesch_reading_ease"), 2),
        "ttr_mean": round(weighted("ttr_mean"), 4),
        "top_bigrams": top_bigrams,
    }


@step(in_shape="aggregate", depends_on=["digest"])
def report(digest: dict) -> str:
    rows = sorted(
        digest.values(),
        key=lambda row: row["lexical_diversity"],
        reverse=True,
    )
    lines = [
        "Project Gutenberg — chapter-level Ray stylometry",
        "",
        f"{'diversity':>9}  {'flesch':>7}  {'ttr':>6}  {'ch':>4}  "
        f"{'words':>8}  title",
    ]
    for row in rows:
        lines.append(
            f"{row['lexical_diversity']:.4f}  "
            f"{row['flesch_reading_ease']:>7.1f}  "
            f"{row['ttr_mean']:.4f}  "
            f"{row['chapters']:>4}  "
            f"{row['words']:>8}  "
            f"{row['title']}"
        )

    richest = rows[0]
    lines.extend(["", f"Richest lexicon: {richest['title']}", "Top bigrams:"])
    for item in richest["top_bigrams"]:
        lines.append(
            f"  {item['pmi']:>6.2f} pmi  n={item['count']:<4}  {item['phrase']}"
        )
    return "\n".join(lines)


# Fixed semiprimes so the Shor ladder is reproducible across runs.
SHOR_LADDER = [
    {"label": "classic-15", "n": 15},          # 3×5, 4-bit
    {"label": "classic-35", "n": 35},          # 5×7, 6-bit
    {"label": "classic-91", "n": 91},          # 7×13, 7-bit
    {"label": "bits-8", "n": 143},             # 11×13
    {"label": "bits-9", "n": 299},             # 13×23
    {"label": "bits-10", "n": 667},            # 23×29
    {"label": "bits-11", "n": 1591},           # 37×43
    {"label": "bits-12", "n": 3127},           # 53×59
    {"label": "bits-13", "n": 8051},           # 83×97  (~15s classically)
]


@step
def targets():
    """One lane per semiprime challenge."""
    for row in SHOR_LADDER:
        yield dict(row)


@step(executor=make_ray_pool)
def factor(targets: dict) -> dict:
    """Ray: classical simulation of Shor's algorithm on one N."""
    result = shor_factor(targets["n"], max_attempts=8, seed=targets["n"])
    return {
        "label": targets["label"],
        "n": result.n,
        "bits": result.n.bit_length(),
        "ok": result.ok,
        "factors": list(result.factors) if result.factors else None,
        "attempts": result.attempts,
        "period": result.period,
        "base": result.base,
        "register_size": result.register_size,
        "elapsed_s": round(result.elapsed_s, 3),
        "detail": result.detail,
    }


@step(in_shape="aggregate", depends_on=["factor"])
def shor_report(factor: dict) -> str:
    rows = sorted(factor.values(), key=lambda row: (row["bits"], row["n"]))
    lines = [
        "Simulated Shor's algorithm (modexp + IQFT + continued fractions)",
        "Cost is O(N²) classically — a quantum device would be poly(log N).",
        "",
        f"{'label':<12} {'N':>8} {'bits':>4} {'Q':>12} {'time':>7}  result",
    ]
    minute_bits = None
    for row in rows:
        factors = (
            f"{row['factors'][0]} × {row['factors'][1]}"
            if row["factors"]
            else row["detail"]
        )
        lines.append(
            f"{row['label']:<12} {row['n']:>8} {row['bits']:>4} "
            f"{row['register_size']:>12,} {row['elapsed_s']:>6.2f}s  "
            f"{'ok' if row['ok'] else 'fail'} {factors}"
        )
        if row["ok"] and row["elapsed_s"] <= 60:
            minute_bits = row["bits"]
    lines.extend(
        [
            "",
            f"Largest size that finished in ≤60s on this box: "
            f"{minute_bits}-bit"
            if minute_bits
            else "No successful factorization within 60s.",
            "Rule of thumb: 12-bit ≈ seconds, 13-bit ≈ 15s, "
            "14-bit ≈ memory-bound for the IQFT state vector.",
        ]
    )
    return "\n".join(lines)


def _run_books(home) -> None:
    with open(BOOKS_CSV, newline="") as handle:
        book_count = sum(1 for _ in csv.DictReader(handle))
    print(
        f"Ray Gutenberg workload: {book_count} books → chapters → "
        f"3 Ray hops, {RAY_CPUS} CPUs, "
        f"TTR window={max(TTR_WINDOW, 50)}/stride={max(TTR_STRIDE, 1)}"
    )
    pipe = pipeline(
        name="ray_executor_books",
        steps=[
            books,
            fetch,
            clean,
            chapters,
            lexicon,
            style,
            phrases,
            digest,
            report,
        ],
        home=home,
    )
    print(pipe.describe())
    print()
    t0 = time.perf_counter()
    first = pipe.run(workers=RAY_CPUS)
    t1 = time.perf_counter()
    second = pipe.run(workers=RAY_CPUS)
    t2 = time.perf_counter()
    print(
        f"First:  Created {first.created_count}, "
        f"Reused {first.reused_count}  ({t1 - t0:.1f}s)"
    )
    print(
        f"Second: Created {second.created_count}, "
        f"Reused {second.reused_count}  ({t2 - t1:.1f}s)"
    )
    print()
    print(first.output_for("report").get("@all", ""))
    assert first.created_count > book_count * 5
    assert second.created_count == 0
    assert second.reused_count == first.created_count


def _run_shor(home) -> None:
    print(
        f"Ray Shor workload: {len(SHOR_LADDER)} semiprimes, {RAY_CPUS} CPUs"
    )
    pipe = pipeline(
        name="ray_executor_shor",
        steps=[targets, factor, shor_report],
        home=home,
    )
    print(pipe.describe())
    print()
    t0 = time.perf_counter()
    first = pipe.run(workers=RAY_CPUS)
    t1 = time.perf_counter()
    second = pipe.run(workers=RAY_CPUS)
    t2 = time.perf_counter()
    print(
        f"First:  Created {first.created_count}, "
        f"Reused {first.reused_count}  ({t1 - t0:.1f}s)"
    )
    print(
        f"Second: Created {second.created_count}, "
        f"Reused {second.reused_count}  ({t2 - t1:.1f}s)"
    )
    print()
    print(first.output_for("shor_report").get("@all", ""))
    assert first.created_count == 2 * len(SHOR_LADDER) + 1
    assert second.created_count == 0
    assert second.reused_count == first.created_count


def main() -> None:
    only = os.environ.get("RUBEDO_RAY_ONLY", "all").lower()
    try:
        with tempfile.TemporaryDirectory(prefix="rubedo-ray-") as root:
            home = Home.ephemeral(root)
            if only in {"all", "books"}:
                _run_books(home)
                if only == "all":
                    print()
                    print("=" * 60)
                    print()
            if only in {"all", "shor"}:
                _run_shor(home)
    finally:
        if _ray_started:
            import ray

            ray.shutdown()


if __name__ == "__main__":
    main()
