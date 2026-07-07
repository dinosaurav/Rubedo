#!/bin/bash
OUTPUT_FILE="/Users/sauravdas/.gemini/antigravity/brain/02c90317-4a05-4702-86e4-fae6271282e4/scratch/run_results.txt"
mkdir -p "$(dirname "$OUTPUT_FILE")"
echo "" > "$OUTPUT_FILE"

for d in examples/*/; do
    name=$(basename "$d")
    script="${d}${name}.py"
    if [ -f "$script" ]; then
        echo "=== $name (Run 1) ==="
        echo "=== $name (Run 1) ===" >> "$OUTPUT_FILE"
        uv run python "$script" >> "$OUTPUT_FILE" 2>&1
        
        echo "=== $name (Run 2) ==="
        echo "=== $name (Run 2) ===" >> "$OUTPUT_FILE"
        uv run python "$script" >> "$OUTPUT_FILE" 2>&1
    fi
done
