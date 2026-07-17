"""Roll up orders straight from a SQL table with a bare @p.step recipe.

    orders table ─▶ classify ─▶ rollup (reduce)
                    (index)

This one is self-contained: it creates a small SQLite "orders" database in your
temp dir, then runs a Rubedo pipeline over it. Each row is a coordinate.

The `orders` source is a plain SELECT loop: a table recipe is just a
parentless generator `@p.step` yielding one dict per row, buffered like any
other expand (no batch_size streaming mode yet; see docs/concepts/sources.md
for the pattern on a table too big to pull in one query).

Run it:

    uv run python examples/orders_rollup/orders_rollup.py
"""

import os
import tempfile

from sqlalchemy import create_engine, text

from rubedo import pipeline


DB_PATH = os.path.join(tempfile.gettempdir(), "rubedo_demo_orders.db")
DB_URL = f"sqlite:///{DB_PATH}"

SEED = [
    (1, "Acme", 40.0), (2, "Globex", 250.0), (3, "Initech", 1200.0),
    (4, "Umbrella", 90.0), (5, "Soylent", 610.0), (6, "Hooli", 30.0),
    (7, "Stark", 4800.0), (8, "Wayne", 150.0),
]


def seed_db():
    """Idempotently (re)create the demo orders table. Real work points at a real DB."""
    engine = create_engine(DB_URL)
    with engine.begin() as conn:
        conn.execute(text("CREATE TABLE IF NOT EXISTS orders "
                          "(id INTEGER PRIMARY KEY, customer TEXT, amount REAL)"))
        for oid, customer, amount in SEED:
            conn.execute(
                text("INSERT OR REPLACE INTO orders (id, customer, amount) "
                     "VALUES (:id, :c, :a)"),
                {"id": oid, "c": customer, "a": amount},
            )
    engine.dispose()


p = pipeline(name="orders-rollup")


@p.step
def orders():
    engine = create_engine(DB_URL)
    with engine.connect() as conn:
        res = conn.execute(text("SELECT * FROM orders"))
        for row in res.mappings():
            yield dict(row)


@p.step(index=["tier"])
def classify(orders: dict):
    """Bucket each order by size."""
    row = orders
    amount = row["amount"]
    tier = "whale" if amount >= 1000 else "mid" if amount >= 200 else "small"
    return {"customer": row["customer"], "amount": amount, "tier": tier}


@p.step(shape="reduce")
def rollup(classify: dict) -> str:
    """Total revenue and order count per tier."""
    totals: dict[str, tuple[int, float]] = {}
    for o in classify.values():
        count, revenue = totals.get(o["tier"], (0, 0.0))
        totals[o["tier"]] = (count + 1, revenue + o["amount"])
    order = {"whale": 0, "mid": 1, "small": 2}
    lines = [
        f"{tier:<6} {count} orders  ${revenue:,.2f}"
        for tier, (count, revenue) in sorted(totals.items(), key=lambda kv: order[kv[0]])
    ]
    return "Revenue by tier:\n" + "\n".join(lines)


def main():
    seed_db()
    print(p.describe())
    print()
    summary = p.run()
    print(f"created={summary.created_count} reused={summary.reused_count}")
    print("\n--- Final Output (rollup) ---")
    import json
    print(json.dumps(summary.output_for("rollup"), indent=2, default=str))


if __name__ == "__main__":
    main()
