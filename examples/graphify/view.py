# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "networkx>=3.0",
# ]
# ///

import json
import os

def main():
    path = os.path.join(os.path.dirname(__file__), "graph.json")
    if not os.path.exists(path):
        print("Graph not found. Run graphify.py first.")
        return

    with open(path, "r") as f:
        data = json.load(f)

    nodes = data.get("nodes", [])
    # networkx node_link_data exports edges as 'links'
    edges = data.get("links", [])

    print("=== Graphify Stats ===")
    print(f"  Nodes: {len(nodes)}")
    print(f"  Edges: {len(edges)}")
    
    files = [n for n in nodes if n.get("type") == "file"]
    print(f"  Files parsed: {len(files)}")
    
    communities = set(n.get("community_id") for n in nodes if "community_id" in n)
    print(f"  Communities detected: {len(communities)}")
    
    print("\n=== Top 5 God Nodes (by PageRank) ===")
    nodes_with_pr = [n for n in nodes if "pagerank" in n]
    nodes_with_pr.sort(key=lambda x: x["pagerank"], reverse=True)
    for i, n in enumerate(nodes_with_pr[:5]):
        print(f"  {i+1}. {n['id']} ({n.get('type', 'unknown')}) - PR: {n['pagerank']:.4f}")
        
    print("\n=== Sample Semantic Summaries ===")
    files_with_summary = [n for n in nodes if "summary" in n and n.get("type") == "file"]
    for i, n in enumerate(files_with_summary[:3]):
        print(f"  {n['id']}:\n    {n['summary']}")

if __name__ == "__main__":
    main()
