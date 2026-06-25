import argparse
import sys
import uvicorn
from .server import app

def main():
    parser = argparse.ArgumentParser(description="BatchBrain CLI")
    subparsers = parser.add_subparsers(dest="command")

    # web command
    web_parser = subparsers.add_parser("web", help="Start the API server")
    web_parser.add_argument("--port", type=int, default=8000)

    args = parser.parse_args()

    if args.command == "web":
        uvicorn.run(app, host="0.0.0.0", port=args.port)
    else:
        parser.print_help()
        sys.exit(1)

if __name__ == "__main__":
    main()
