#!/usr/bin/env python3
# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "mcp>=1.0.0",
#     "networkx>=3.0",
# ]
# ///

import json
import os
import networkx as nx
from mcp.server.fastmcp import FastMCP

# Initialize FastMCP server
mcp = FastMCP("Graphify Server")

def load_graph():
    path = os.path.join(os.path.dirname(__file__), "graph.json")
    if not os.path.exists(path):
        return None
    with open(path, "r") as f:
        data = json.load(f)
    return nx.node_link_graph(data)

@mcp.tool()
def get_file_context(filepath: str) -> str:
    """Get the semantic summary, classes, functions, and imports for a specific file.
    
    Args:
        filepath: The path to the file (e.g. 'models.py')
    """
    G = load_graph()
    if not G:
        return "Graph not built yet."
        
    if not G.has_node(filepath):
        # Try finding it by suffix
        matches = [n for n in G.nodes if n.endswith(filepath)]
        if not matches:
            return f"File {filepath} not found in the graph."
        filepath = matches[0]
        
    node = G.nodes[filepath]
    summary = node.get("summary", "No summary available.")
    
    # Get children (classes, functions)
    children = [t for s, t, attr in G.out_edges(filepath, data=True) if attr.get("type") == "contains"]
    
    # Get imports
    imports = [t for s, t, attr in G.out_edges(filepath, data=True) if attr.get("type") in ("imports", "imports_from")]
    
    result = [f"File: {filepath}", f"Summary: {summary}", ""]
    if children:
        result.append("Contains:")
        for child in children:
            result.append(f"  - {child}")
    if imports:
        result.append("Imports:")
        for imp in imports:
            result.append(f"  - {imp}")
            
    return "\n".join(result)

@mcp.tool()
def get_god_nodes(top_n: int = 10) -> str:
    """Get the most highly connected nodes (god nodes) in the codebase.
    
    Args:
        top_n: Number of nodes to return
    """
    G = load_graph()
    if not G:
        return "Graph not built yet."
        
    nodes_with_pr = [(n, data.get("pagerank", 0)) for n, data in G.nodes(data=True) if "pagerank" in data]
    nodes_with_pr.sort(key=lambda x: x[1], reverse=True)
    
    result = [f"Top {top_n} God Nodes (by PageRank):"]
    for i, (node_id, pr) in enumerate(nodes_with_pr[:top_n]):
        node_type = G.nodes[node_id].get("type", "unknown")
        result.append(f"{i+1}. {node_id} ({node_type}) - PR: {pr:.4f}")
        
    return "\n".join(result)

@mcp.tool()
def find_path(source: str, target: str) -> str:
    """Find the shortest dependency path between two components in the codebase.
    
    Args:
        source: The starting component/file
        target: The target component/file
    """
    G = load_graph()
    if not G:
        return "Graph not built yet."
        
    # Naive search if exact match fails
    if not G.has_node(source):
        matches = [n for n in G.nodes if n.endswith(source)]
        if matches: source = matches[0]
    if not G.has_node(target):
        matches = [n for n in G.nodes if n.endswith(target)]
        if matches: target = matches[0]
        
    if not G.has_node(source) or not G.has_node(target):
        return "Source or target not found."
        
    try:
        path = nx.shortest_path(G, source, target)
        return " -> ".join(path)
    except nx.NetworkXNoPath:
        return f"No path found between {source} and {target}."

if __name__ == "__main__":
    mcp.run()
