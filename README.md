# Corva CLI Prototype

CLI scaffold for Corva-style pluggable tools using Python 3.14, Typer, and optional plotext previews.

## Current Tooling

| Command | Description |
| --- | --- |
| `timelog` | Resolves `auto_*` windows (or uses a record limit) and proxies an aggregate pipeline to the Corva Data API. |

Example:

```bash
uv run corva timelog \
  --api-key YOUR_TOKEN \
  --asset-ids 101,202 \
  --start-time auto_2h30m \
  --end-time auto_0d \
  --output markdown
```

The `auto_*` syntax subtracts durations from "now", so `auto_0d` equals the current UTC timestamp, `auto_2h30m` subtracts 2.5 hours, etc. Multiple units can be chained in any order. Omit both `--start-time` and `--end-time` to fall back to a simple limit (default `1000` documents) using `--limit`. By default the CLI prints only the raw API response; add `--verbose` to include query/debug metadata.

### Settings & Overrides

Configuration lives in `src/corva_cli/settings.py` with sensible defaults:

- `CORVA_TIMELOG_STEP_MINUTES` (default `60`)
- `CORVA_TIMELOG_STATUSES` (default `online,maintenance,offline`)
- `CORVA_DATA_API_ROOT_URL` (default `https://data.example.com`)
- `CORVA_DATA_API_TIMEOUT_SECONDS` (default `30`)
- `CORVA_TIMELOG_PROVIDER` / `CORVA_TIMELOG_DATASET` (default `corva` / `drilling.timelog.data`)

Drop a `.env` file (see `.env.example`) or set environment variables to override these values globally. Any command can still override them ad-hoc:

```bash
uv run corva timelog \
  --api-key YOUR_TOKEN \
  --asset-ids 303 \
  --start-time auto_6h \
  --end-time auto_0d \
  --step-minutes 30 \
  --statuses online,idle \
  --limit 500 \
  --skip 100
```

## Notes on Retired Samples

Earlier iterations shipped extra demo commands (`fetch-production`, `summarize-metric`) to prove out completions and aggregations. They were removed from the runtime to keep the focus on timelog tooling, but the concepts remain useful when defining future plugins:

- **fetch-production**: accepted `well_id`, `start`, `end` and produced hourly synthetic volumes. Had well id completions.
- **summarize-metric**: accepted a `metric_id` and returned min/max/avg aggregates with optional limits and completions.

Reintroduce them by creating new modules under `src/corva_cli/tools/` and decorating with `@registry.tool(...)`. Use this README as a reference for the parameter patterns if you need to recreate MQL equivalents.
