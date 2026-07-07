#!/usr/bin/env python3
# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "litellm>=1.40.0",
#     "python-dotenv>=1.0.0"
# ]
# ///

import http.server
import socketserver
import os
import json
import sys
import litellm
from dotenv import load_dotenv

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
WEB_DIR = os.path.join(BASE_DIR, "web")
GRAPH_JSON_PATH = os.path.join(BASE_DIR, "graph.json")

# Load environment variables (e.g. OPENROUTER_API_KEY)
load_dotenv(os.path.join(os.path.dirname(os.path.dirname(BASE_DIR)), ".env"))

# Load graph data into memory for the LLM
graph_context = ""
if os.path.exists(GRAPH_JSON_PATH):
    with open(GRAPH_JSON_PATH, "r") as f:
        data = json.load(f)
        nodes = data.get("nodes", [])
        # Create a compressed context of the codebase
        summaries = []
        for n in nodes:
            if n.get("type") == "file":
                summaries.append(f"File: {n.get('id')}\nSummary: {n.get('summary', 'None')}\nPageRank: {n.get('pagerank', 0)}\n")
        
        # Take the top 50 files by pagerank to avoid blowing up context window
        summaries.sort(key=lambda x: "PageRank: " in x and float(x.split("PageRank: ")[1]) or 0, reverse=True)
        graph_context = "\n".join(summaries[:50])

class GraphifyHandler(http.server.SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=WEB_DIR, **kwargs)

    def do_GET(self):
        if self.path == '/api/graph':
            if not os.path.exists(GRAPH_JSON_PATH):
                self.send_response(404)
                self.end_headers()
                self.wfile.write(b'{"error": "graph.json not found"}')
                return
                
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.end_headers()
            with open(GRAPH_JSON_PATH, 'rb') as f:
                self.wfile.write(f.read())
            return
            
        return super().do_GET()

    def do_POST(self):
        if self.path == '/api/chat':
            content_length = int(self.headers['Content-Length'])
            post_data = self.rfile.read(content_length)
            req = json.loads(post_data)
            user_msg = req.get("message", "")
            
            system_prompt = f"""You are the Graphify AI Expert. You have full context of this codebase's architecture graph.
Here is a summary of the most central files in the codebase (by PageRank):
{graph_context}

Answer the user's question about the codebase. Be concise.
IMPORTANT: You must return your response in EXACTLY this JSON format:
{{
  "reply": "Your textual answer here",
  "highlight_nodes": ["src/rubedo/store.py", "src/rubedo/db.py"]
}}
Provide node IDs in highlight_nodes if your answer mentions them, so the UI can highlight them. If none, pass an empty array.
Do not output markdown code blocks around the JSON. ONLY output the raw JSON object.
"""

            try:
                response = litellm.completion(
                    model="openrouter/anthropic/claude-sonnet-5",
                    messages=[
                        {"role": "system", "content": system_prompt},
                        {"role": "user", "content": user_msg}
                    ],
                    # Force JSON output if possible, but the prompt should handle it
                )
                raw_content = response.choices[0].message.content
                
                # Strip markdown code blocks if the model included them
                if raw_content.startswith("```json"):
                    raw_content = raw_content[7:-3].strip()
                elif raw_content.startswith("```"):
                    raw_content = raw_content[3:-3].strip()
                    
                parsed_res = json.loads(raw_content)
                
                self.send_response(200)
                self.send_header('Content-Type', 'application/json')
                self.end_headers()
                self.wfile.write(json.dumps(parsed_res).encode('utf-8'))
                
            except Exception as e:
                self.send_response(500)
                self.send_header('Content-Type', 'application/json')
                self.end_headers()
                self.wfile.write(json.dumps({"error": str(e)}).encode('utf-8'))
            return

        self.send_response(404)
        self.end_headers()

    def log_message(self, format, *args):
        pass

if __name__ == "__main__":
    if not os.path.exists(GRAPH_JSON_PATH):
        print("Warning: graph.json not found. Did you run graphify.py first?", file=sys.stderr)
        
    port = 8080
    while True:
        try:
            httpd = socketserver.TCPServer(("", port), GraphifyHandler)
            break
        except OSError as e:
            if e.errno == 48:
                port += 1
            else:
                raise
                
    print(f"Starting Graphify Visualizer on http://localhost:{port}")
    print("Press Ctrl+C to stop.")
    
    with httpd:
        try:
            httpd.serve_forever()
        except KeyboardInterrupt:
            print("\nShutting down.")
