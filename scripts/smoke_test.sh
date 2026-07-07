#!/bin/bash
set -e

echo "Building wheel..."
rm -rf dist/
uv build

echo "Creating clean virtual environment..."
rm -rf .smoke-venv
uv venv .smoke-venv --python=3.11
source .smoke-venv/bin/activate

echo "Installing wheel..."
uv pip install dist/*.whl

echo "Running count_lines example..."
python examples/count_lines/count_lines.py

echo "Smoke test passed!"
deactivate
rm -rf .smoke-venv
