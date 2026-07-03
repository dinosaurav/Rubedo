import os
from batchbrain import step, pipeline, run, Filtered
from batchbrain.sources import CsvSource

# 1. Screen step: filter out companies we don't want to process
@step(name="screen", version="1")
def screen(row: dict) -> dict:
    """Filter out Umbrella Corp because they are evil."""
    if row.get("company") == "Umbrella Corp":
        return Filtered("Evil corporation")
    return row

# 2. Enrich step: Mock LLM call to get company industry
@step(
    name="enrich",
    version="1",
    depends_on=["screen"],
    retries=3,
    retry_on=Exception,
    rate_limit="30/min",
    index=["company"]
)
def enrich(screen: dict) -> dict:
    """Mock LLM call. In reality, this would be an OpenAI API call."""
    # Simulated LLM logic
    company = screen["company"]
    industries = {
        "Acme Corp": "Manufacturing",
        "Globex": "Technology",
        "Soylent": "Food and Beverage",
        "Initech": "Software",
    }
    
    # Real LLM call would go here
    industry = industries.get(company, "Unknown")
    
    return {
        "company": company,
        "website": screen["website"],
        "industry": industry,
        "enriched_by": "mock-llm-1.0"
    }

# 3. Summarize step: Reduce all enriched companies into one report
@step(name="summarize", version="1", depends_on=["enrich"], shape="reduce")
def summarize(enrich: dict) -> dict:
    """Combines all enriched outputs into a single summary."""
    industries = {}
    for lane_key, data in enrich.items():
        ind = data["industry"]
        industries[ind] = industries.get(ind, 0) + 1
        
    return {
        "total_processed": len(enrich),
        "industry_breakdown": industries
    }

def main():
    # Define the pipeline using a CsvSource
    source = CsvSource(
        os.path.join(os.path.dirname(__file__), "companies.csv"),
        key="company"
    )
    
    p = pipeline(
        id="llm-enrichment",
        name="Company LLM Enrichment",
        source=source,
        steps=[screen, enrich, summarize]
    )
    
    print("Running LLM Enrichment Pipeline...")
    res = run(p)
    print(f"Run {res.run_id} completed: {res.created_count} created, {res.reused_count} reused, {res.filtered_count} filtered.")
    
    print("\nCheck the dashboard (npm run dev in /web) to see the DAG and outputs!")

if __name__ == "__main__":
    main()
