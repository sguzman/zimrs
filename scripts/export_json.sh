#!/usr/bin/env bash
set -euo pipefail

CONFIG_PATH="${1:-config/wiktionary.toml}"
OUT_PATH="${2:-out/wiktionary.jsonl}"

cargo run --release -- --config "$CONFIG_PATH" export-json --output "$OUT_PATH"

echo "Export written to $OUT_PATH"
