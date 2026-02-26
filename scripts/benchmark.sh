#!/usr/bin/env bash
set -euo pipefail

cargo bench --bench extraction_bench -- --sample-size 20
