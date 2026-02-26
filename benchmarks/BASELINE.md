# Benchmark Baseline

Date: 2026-02-26
Command:

```bash
cargo bench --bench extraction_bench -- --sample-size 10
```

Environment:

- Host: local development container
- Profile: `bench` (optimized)
- Backend: `plotters` (gnuplot unavailable)

## extract_from_html

| Input scale | Mean time (approx) | Throughput |
| --- | ---: | ---: |
| `1x` | `37.61 us` | `19.32 MiB/s` |
| `5x` | `185.23 us` | `19.62 MiB/s` |
| `20x` | `737.52 us` | `19.71 MiB/s` |

Notes:

- These numbers are intended as regression baselines, not absolute throughput guarantees.
- Re-run after parser/schema changes and update this table when changes are intentional.
