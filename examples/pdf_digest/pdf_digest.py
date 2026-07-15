"""PDF digest — a source-less pipeline whose head is a plain map step.

    load_pdf ─▶ split_chunks ─▶ caption ─▶ rejoin ─▶ summary_visual
    (map root)   (expand)       (vision    (reduce,   (text LLM)
     params=pdf                  LLM on     ordered) ─▶ summary_textonly
                                 images)               (text LLM)

The head, `load_pdf`, takes **no source and no expand** — it is a plain map
root. It reads the PDF path from `params` and mints a single '@root' lane (the
chunk manifest). That is the feature this example exists to show: a pipeline
can begin with an ordinary step whose input is its params. Same params reuse
the cached manifest; a different `pdf=` recomputes.

From there it is a normal DAG:
  - `split_chunks` (expand) fans the one document lane into one content-
    addressed lane per ordered chunk (a page of text, or a figure image).
  - `caption` (map) sends *only the image chunks* to a cheap vision LLM; text
    chunks pass straight through. Each caption is cached by the chunk's
    content, so a second run captions nothing and makes zero vision calls.
  - `rejoin` (reduce) sorts the chunks back into reading order and rebuilds
    two documents: one where figures are replaced by their captions
    (picture-aware), and one that drops the figures entirely (text-only).
  - `summary_visual` / `summary_textonly` (map, text LLM) summarize each —
    a side-by-side of what the pictures were worth.

Run it (PyMuPDF is in the dev dependency group, so this just works):

    uv run python examples/pdf_digest/pdf_digest.py
    uv run python examples/pdf_digest/pdf_digest.py --pdf mydoc.pdf

With no --pdf it generates a small mixed text/figure sample PDF next to this
script and processes that. Put your key in a .env at the repo root:

    OPENROUTER_API_KEY=sk-or-...
"""

import argparse
import base64
import json
import os
import urllib.request

from pydantic import BaseModel, Field
from dotenv import load_dotenv

from rubedo import pipeline

load_dotenv()

OPENROUTER = "https://openrouter.ai/api/v1/chat/completions"
# Best value on OpenRouter for document page images: cheap, modern, reliable.
# Override either with an env var to try another model.
VISION_MODEL = os.environ.get("OPENROUTER_VISION_MODEL", "google/gemini-2.5-flash-lite")
TEXT_MODEL = os.environ.get("OPENROUTER_TEXT_MODEL", "google/gemini-2.5-flash-lite")

# A page with less than this many characters of extractable text is treated as
# a figure — rasterized and sent to the vision model.
TEXT_CHUNK_MIN_CHARS = 20


def _chat(prompt: str, image_png: bytes | None = None, max_tokens: int = 400) -> str:
    """One-shot chat via OpenRouter. Pass image_png to use the vision model."""
    key = os.environ.get("OPENROUTER_API_KEY")
    if not key:
        raise RuntimeError(
            "OPENROUTER_API_KEY is not set — put it in a .env file at the repo root."
        )
    if image_png is not None:
        data_url = "data:image/png;base64," + base64.b64encode(image_png).decode()
        content = [
            {"type": "text", "text": prompt},
            {"type": "image_url", "image_url": {"url": data_url}},
        ]
        model = VISION_MODEL
    else:
        content = prompt  # type: ignore[assignment]
        model = TEXT_MODEL
    body = json.dumps(
        {
            "model": model,
            "messages": [{"role": "user", "content": content}],
            "max_tokens": max_tokens,
        }
    ).encode()
    req = urllib.request.Request(
        OPENROUTER,
        data=body,
        headers={"Authorization": f"Bearer {key}", "Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=90) as r:
        return json.load(r)["choices"][0]["message"]["content"] or ""


def _render_page_png(pdf_path: str, index: int) -> bytes:
    """Rasterize one page of a PDF to PNG bytes."""
    import fitz  # PyMuPDF

    with fitz.open(pdf_path) as doc:
        return doc[index].get_pixmap(dpi=110).tobytes("png")


class PdfParams(BaseModel):
    pdf: str = Field(description="Path to the PDF to digest")


p = pipeline(name="pdf-digest", params_model=PdfParams)


@p.step
def load_pdf(params: dict) -> list[dict]:
    """HEADLESS MAP ROOT — no source, no expand. Reads the PDF path from
    params and mints one '@root' lane: an ordered manifest of chunks.

    A chunk is 'text' (the page's extractable text) or 'image' (a figure page
    with little/no text, to be rasterized and captioned downstream). Cached by
    params, so re-running the same pdf= reuses this manifest untouched.
    """
    import fitz  # PyMuPDF

    path = params["pdf"]
    chunks = []
    with fitz.open(path) as doc:
        for i, page in enumerate(doc):
            text = page.get_text().strip()
            if len(text) >= TEXT_CHUNK_MIN_CHARS:
                chunks.append({"index": i, "kind": "text", "text": text})
            else:
                chunks.append({"index": i, "kind": "image", "text": ""})
    return chunks


@p.step
def split_chunks(load_pdf: list[dict]):
    """Fan the manifest into one content-addressed lane per ordered chunk.

    `index` rides in each payload so two pages with identical text still stay
    distinct lanes (and so `rejoin` can restore reading order)."""
    for chunk in load_pdf:
        yield chunk


@p.step(
    params_model=PdfParams,
    retries=3,
    retry_delay=2,
    rate_limit="20/min",
    index=["kind"],  # Selection.parse("kind:image") to find captioned figures
)
def caption(split_chunks: dict, params: PdfParams) -> dict:
    """Image chunks -> cheap vision LLM caption; text chunks pass through.

    Expensive + non-idempotent for figures, so it is cached per chunk content:
    a second run makes zero vision calls."""
    chunk = split_chunks
    if chunk["kind"] == "text":
        return {"index": chunk["index"], "kind": "text", "content": chunk["text"]}

    png = _render_page_png(params.pdf, chunk["index"])
    caption_text = _chat(
        "This is a figure from a document. Describe what it shows in 1-2 plain "
        "sentences, including any numbers or labels you can read.",
        image_png=png,
        max_tokens=200,
    ).strip()
    return {"index": chunk["index"], "kind": "image", "content": caption_text}


@p.step(depends_on=["caption"], shape="reduce")
def rejoin(caption: dict) -> dict:
    """Fan the captioned chunks back into reading order and build two docs:
    picture-aware (figures -> captions) and text-only (figures dropped)."""
    ordered = sorted(caption.values(), key=lambda c: c["index"])
    visual_parts = []
    textonly_parts = []
    for c in ordered:
        if c["kind"] == "image":
            visual_parts.append(f"[FIGURE] {c['content']}")
        else:
            visual_parts.append(c["content"])
            textonly_parts.append(c["content"])
    return {
        "visual": "\n\n".join(visual_parts),
        "textonly": "\n\n".join(textonly_parts),
        "n_figures": sum(1 for c in ordered if c["kind"] == "image"),
    }


def _summarize(doc: str) -> str:
    return _chat(
        "Summarize this document in 3-4 sentences for someone who has not read "
        "it:\n\n" + doc,
        max_tokens=300,
    ).strip()


@p.step
def summary_visual(rejoin: dict) -> str:
    """Summary that CAN see the figures (their captions are in the text)."""
    return _summarize(rejoin["visual"])


@p.step
def summary_textonly(rejoin: dict) -> str:
    """Summary of the same document with the figures removed — the control."""
    return _summarize(rejoin["textonly"])


def _make_sample_pdf(path: str) -> None:
    """A small PDF mixing text pages and drawn 'figure' pages (no real image
    dependency needed — the figure pages are vector shapes we rasterize)."""
    import fitz  # PyMuPDF

    doc = fitz.open()

    page = doc.new_page()
    page.insert_text(
        (72, 90),
        "Rubedo Quarterly Report\n\n"
        "Adoption grew across every region this quarter. The engine's caching\n"
        "meant teams reprocessed only what changed, and pipeline runtimes fell\n"
        "sharply once the incremental path was in place. The following figure\n"
        "breaks down where the time went.",
        fontsize=13,
    )

    page = doc.new_page()  # figure page: a little bar chart, no text
    shape = page.new_shape()
    bars = [(120, 0.9, (0.20, 0.45, 0.85)), (200, 0.6, (0.30, 0.70, 0.45)),
            (280, 0.35, (0.90, 0.60, 0.20)), (360, 0.75, (0.75, 0.30, 0.55))]
    base_y = 400
    for x, h, color in bars:
        shape.draw_rect(fitz.Rect(x, base_y - int(220 * h), x + 50, base_y))
        shape.finish(fill=color)
    shape.draw_line(fitz.Point(90, base_y), fitz.Point(430, base_y))
    shape.finish()
    shape.commit()

    page = doc.new_page()
    page.insert_text(
        (72, 90),
        "Outlook\n\n"
        "We expect the next quarter to build on this momentum. Investment in\n"
        "the surgical invalidation path should further cut redundant work, and\n"
        "the roadmap below sketches the sequencing.",
        fontsize=13,
    )

    page = doc.new_page()  # second figure page: a simple pie-ish set of wedges
    shape = page.new_shape()
    center = fitz.Point(260, 320)
    shape.draw_circle(center, 130)
    shape.finish(fill=(0.85, 0.88, 0.95))
    shape.draw_sector(center, fitz.Point(390, 320), 130, fullSector=True)
    shape.finish(fill=(0.20, 0.45, 0.85))
    shape.commit()

    doc.save(path)
    doc.close()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--pdf", help="Path to a PDF (default: a generated sample)")
    args = ap.parse_args()

    pdf_path = args.pdf
    if not pdf_path:
        pdf_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "sample.pdf")
        if not os.path.exists(pdf_path):
            print(f"No --pdf given; generating a sample at {pdf_path}")
            _make_sample_pdf(pdf_path)

    print(p.describe())
    print()

    summary = p.run(params={"pdf": pdf_path})
    print(
        f"created={summary.created_count} reused={summary.reused_count} "
        f"failed={summary.failed_count}"
    )

    visual = next(iter(summary.output_for("summary_visual").values()), "(none)")
    textonly = next(iter(summary.output_for("summary_textonly").values()), "(none)")
    print("\n--- Picture-aware summary ---\n" + visual)
    print("\n--- Text-only summary (figures removed) ---\n" + textonly)
    print(
        "\nRun it again: the manifest and every caption are cached, so a second "
        "run makes zero vision calls (only the two summaries may re-run if you "
        "bump their version)."
    )


if __name__ == "__main__":
    main()
