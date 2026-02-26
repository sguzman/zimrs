#!/usr/bin/env bash
set -euo pipefail

CONFIG_PATH="${1:-config/wiktionary.toml}"

cargo run --release -- --config "$CONFIG_PATH" build-artifacts --build-release

echo "Release artifacts created under dist/"
