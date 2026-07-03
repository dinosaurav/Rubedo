import os
import time
from batchbrain import step, pipeline, run
from batchbrain.sources import CsvSource

# 1. Fetch step: mock fetching HTML from a URL with a TTL
@step(
    name="fetch",
    version="1",
    stale_after="24h", # Results older than 24 hours will be recomputed
    retries=2,
    rate_limit="1/s"   # Max 1 request per second
)
def fetch(row: dict) -> dict:
    """Mock fetcher. In reality, this would use requests or httpx."""
    url = row["url"]
    print(f"Fetching {url}...")
    time.sleep(0.5) # Mock network delay
    
    return {
        "url": url,
        "html": f"<html><body><h1>Hello from {url}</h1></body></html>",
        "fetched_at": time.time()
    }

def main():
    source = CsvSource(
        os.path.join(os.path.dirname(__file__), "urls.csv"),
        key="url"
    )
    
    p = pipeline(
        id="scraper",
        name="Daily Scraper",
        source=source,
        steps=[fetch]
    )
    
    print("Running Scraper Pipeline...")
    res = run(p)
    print(f"Run {res.run_id} completed: {res.created_count} created, {res.reused_count} reused.")
    
    print("\nRun it again immediately — notice how everything is reused!")
    print("If you wait 24 hours (or change stale_after), it will re-fetch.")

if __name__ == "__main__":
    main()
