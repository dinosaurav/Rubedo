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
IMPORTANT: You must structure your response EXACTLY as follows:
First, use <thinking>...</thinking> tags to reason about the user's question step-by-step.
Then, provide your final answer to the user.
Finally, if your answer mentions specific files or nodes from the architecture graph, list them inside <nodes>...</nodes> separated by commas (e.g. <nodes>src/rubedo/store.py, src/rubedo/db.py</nodes>). If none are mentioned, omit the <nodes> block.
Do not use JSON or markdown code blocks for the output format.
"""

            headers_sent = False
            try:
                response = litellm.completion(
                    model="openrouter/anthropic/claude-sonnet-5",
                    messages=[
                        {"role": "system", "content": system_prompt},
                        {"role": "user", "content": user_msg}
                    ],
                    stream=True
                )
                
                self.send_response(200)
                self.send_header('Content-Type', 'text/event-stream')
                self.send_header('Cache-Control', 'no-cache')
                self.send_header('Connection', 'keep-alive')
                self.end_headers()
                headers_sent = True
                
                for chunk in response:
                    content = chunk.choices[0].delta.content or ""
                    if content:
                        msg = json.dumps({"text": content})
                        self.wfile.write(f"data: {msg}\n\n".encode('utf-8'))
                        self.wfile.flush()
                        
            except Exception as e:
                if not headers_sent:
                    self.send_response(500)
                    self.send_header('Content-Type', 'application/json')
                    self.end_headers()
                    self.wfile.write(json.dumps({"error": str(e)}).encode('utf-8'))
                else:
                    # Stream already open: try to report the error as an SSE
                    # frame, but the client may have disconnected (which is
                    # what raised in the first place), so writing can fail too.
                    try:
                        msg = json.dumps({"error": str(e)})
                        self.wfile.write(f"data: {msg}\n\n".encode('utf-8'))
                        self.wfile.flush()
                    except (BrokenPipeError, ConnectionResetError):
                        pass
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
