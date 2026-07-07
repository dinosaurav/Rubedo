# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "litellm>=1.40.0",
#     "networkx>=3.0",
#     "scipy>=1.10.0",
#     "python-dotenv>=1.0.0",
# ]
# ///

"""Graphify-like implementation in Rubedo.

This builds a rich knowledge graph by combining deterministic AST extraction with
semantic extraction via LLM, then reducing into NetworkX for Louvain community
clustering and PageRank.
"""

import json
import os
import sys

# Inject the local rubedo package into the isolated script environment
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../../src")))

import litellm
import networkx as nx
from networkx.algorithms import community

from rubedo import ProcessResult, describe, run, PipelineBuilder
from rubedo.sources import FolderSource

p = PipelineBuilder(
    id="graphify",
    name="Graphify DAG",
    # Target our own src folder
    source=FolderSource(os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "src")),
)

import tree_sitter_python as tspython
from tree_sitter import Language, Parser

PY_LANGUAGE = Language(tspython.language())

@p.step(name="extract_code_nodes", version="v3")
def extract_code_nodes(path: str) -> ProcessResult:
    """Extract classes, functions, and import edges using Tree-sitter."""
    if not path.endswith(".py"):
        return ProcessResult(value={"nodes": [], "edges": []})

    try:
        with open(path, "r", encoding="utf-8") as f:
            code = f.read()
        parser = Parser(PY_LANGUAGE)
        tree = parser.parse(bytes(code, "utf8"))
    except Exception:
        return ProcessResult(value={"nodes": [], "edges": []})

    file_id = os.path.relpath(path, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
    nodes = [{"id": file_id, "type": "file", "name": os.path.basename(path)}]
    edges = []

    def walk(node):
        if node.type == "class_definition":
            name_node = node.child_by_field_name("name")
            if name_node:
                name = name_node.text.decode("utf8")
                class_id = f"{file_id}::{name}"
                nodes.append({"id": class_id, "type": "class", "name": name, "file": file_id})
                edges.append({"source": file_id, "target": class_id, "type": "contains"})
        elif node.type == "function_definition":
            name_node = node.child_by_field_name("name")
            if name_node:
                name = name_node.text.decode("utf8")
                func_id = f"{file_id}::{name}"
                nodes.append({"id": func_id, "type": "function", "name": name, "file": file_id})
                edges.append({"source": file_id, "target": func_id, "type": "contains"})
        elif node.type == "import_statement":
            for child in node.children:
                if child.type == "dotted_name":
                    edges.append({"source": file_id, "target": child.text.decode("utf8"), "type": "imports", "is_import": True})
                elif child.type == "aliased_import":
                    for c in child.children:
                        if c.type == "dotted_name":
                            edges.append({"source": file_id, "target": c.text.decode("utf8"), "type": "imports", "is_import": True})
        elif node.type == "import_from_statement":
            mod_node = node.child_by_field_name("module_name")
            if mod_node:
                edges.append({"source": file_id, "target": mod_node.text.decode("utf8"), "type": "imports", "is_import": True})
        
        for child in node.children:
            walk(child)

    walk(tree.root_node)

    return ProcessResult(value={"nodes": nodes, "edges": edges, "file_id": file_id})

@p.step(name="extract_semantic_nodes", version="v3", retries=2, rate_limit="20/min")
def extract_semantic_nodes(path: str) -> ProcessResult:
    """Use an LLM to generate a semantic summary of the file."""
    if not path.endswith(".py") and not path.endswith(".md"):
        return ProcessResult(value={"summary": ""})

    file_id = os.path.relpath(path, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

    try:
        with open(path, "r", encoding="utf-8") as f:
            content = f.read()[:3000]
    except Exception:
        return ProcessResult(value={"summary": ""})

    prompt = (
        f"Summarize the following file ({os.path.basename(path)}) in 1-2 sentences. "
        f"Focus on what it does and its role in the system architecture.\n\n{content}"
    )

    try:
        from dotenv import load_dotenv
        # Load .env from the root of the repo
        load_dotenv(os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), ".env"))
        
        response = litellm.completion(
            model="openrouter/anthropic/claude-sonnet-5",
            messages=[{"role": "user", "content": prompt}]
        )
        summary = response.choices[0].message.content
    except Exception as e:
        summary = f"Error: {e}"

    return ProcessResult(value={"file_id": file_id, "summary": summary})

@p.step(name="build_networkx_graph", version="v4", depends_on=["extract_code_nodes", "extract_semantic_nodes"], shape="reduce")
def build_networkx_graph(extract_code_nodes: dict, extract_semantic_nodes: dict) -> ProcessResult:
    """Fan-in all the nodes and edges, merging semantic summaries into the Graph."""
    G = nx.DiGraph()

    # Index valid internal files to resolve imports
    valid_files = set()
    module_to_file = {}
    for res in extract_code_nodes.values():
        val = res.value if isinstance(res, ProcessResult) else res
        for node in val.get("nodes", []):
            if node.get("type") == "file":
                fid = node["id"]
                valid_files.add(fid)
                # Map 'src/rubedo/ledger.py' -> 'rubedo.ledger' and 'ledger'
                parts = fid.replace(".py", "").split("/")
                if parts and parts[0] == "src":
                    parts = parts[1:]
                mod_name = ".".join(parts)
                module_to_file[mod_name] = fid
                module_to_file[parts[-1]] = fid

    for res in extract_code_nodes.values():
        val = res.value if isinstance(res, ProcessResult) else res
        for node in val.get("nodes", []):
            G.add_node(node["id"], **node)
        for edge in val.get("edges", []):
            target = edge["target"]
            if edge.get("is_import"):
                # Attempt to resolve the imported module to a concrete internal file_id
                clean_target = target.lstrip(".")
                if target in module_to_file:
                    target = module_to_file[target]
                elif clean_target in module_to_file:
                    target = module_to_file[clean_target]
                
            G.add_edge(edge["source"], target, type=edge["type"])

    for res in extract_semantic_nodes.values():
        val = res.value if isinstance(res, ProcessResult) else res
        file_id = val.get("file_id")
        summary = val.get("summary")
        if file_id and summary and G.has_node(file_id):
            G.nodes[file_id]["summary"] = summary

    # Implicit nodes created by edges are external imports (e.g. typing, sqlalchemy)
    for n, attr in G.nodes(data=True):
        if "type" not in attr:
            attr["type"] = "external"
            attr["name"] = n

    # Rubedo serializes outputs to JSON, so we must return node_link_data, not the DiGraph object.
    return ProcessResult(value=nx.node_link_data(G))

@p.step(name="detect_communities", version="v4", depends_on=["build_networkx_graph"])
def detect_communities(build_networkx_graph) -> ProcessResult:
    """Run Louvain clustering to find architectural boundaries."""
    data = build_networkx_graph.value if isinstance(build_networkx_graph, ProcessResult) else build_networkx_graph
    G = nx.node_link_graph(data)
    
    # Undirected graph is required for Louvain
    UG = G.to_undirected()
    try:
        communities = community.louvain_communities(UG)
        for i, comm in enumerate(communities):
            for node_id in comm:
                G.nodes[node_id]["community_id"] = i
    except Exception:
        pass

    return ProcessResult(value=nx.node_link_data(G))

@p.step(name="find_god_nodes", version="v7", depends_on=["detect_communities"])
def find_god_nodes(detect_communities) -> ProcessResult:
    """Find central 'God nodes' using PageRank, excluding external libraries."""
    data = detect_communities.value if isinstance(detect_communities, ProcessResult) else detect_communities
    G = nx.node_link_graph(data)
    
    # Exclude external libraries (e.g. typing) from sucking up all the centrality
    internal_nodes = [n for n, attr in G.nodes(data=True) if attr.get("type") != "external"]
    H = G.subgraph(internal_nodes)
    
    # Use undirected graph so PageRank doesn't pool in leaf nodes (functions with no outgoing edges)
    UG = H.to_undirected()
    
    try:
        pagerank = nx.pagerank(UG)
        for node_id, score in pagerank.items():
            G.nodes[node_id]["pagerank"] = score
    except Exception as e:
        print(f"PAGERANK ERROR: {e}")

    return ProcessResult(value=nx.node_link_data(G))

@p.step(name="export_graph", version="v7", depends_on=["find_god_nodes"])
def export_graph(find_god_nodes) -> ProcessResult:
    """Export the fully enriched graph to JSON."""
    data = find_god_nodes.value if isinstance(find_god_nodes, ProcessResult) else find_god_nodes
    G = nx.node_link_graph(data)
    
    out_path = os.path.join(os.path.dirname(__file__), "graph.json")
    with open(out_path, "w") as f:
        json.dump(data, f, indent=2)

    return ProcessResult(
        value={"nodes_count": G.number_of_nodes(), "edges_count": G.number_of_edges(), "path": out_path},
        metadata={"nodes": G.number_of_nodes()}
    )

def main():
    pipe = p.build()
    print(describe(pipe))
    print()
    summary = run(pipe)
    print(f"Run ID: {summary.run_id}")
    print(f"Created: {summary.created_count}, Reused: {summary.reused_count}")

if __name__ == "__main__":
    main()
