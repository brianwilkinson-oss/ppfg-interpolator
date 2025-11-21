# Corva CLI Prototype

CLI scaffold for Corva-style pluggable tools using Python 3.14, Typer, and optional plotext previews.

## Current Tooling

| Command | Description |
| --- | --- |
| `timelog` | Resolves `auto_*` windows (or uses a record limit) and proxies an aggregate pipeline to the Corva Data API for timelog entries. |
| `assets` | Shares the same interface but queries the `assets` dataset for asset metadata. |
| `dvd` | Auto-generated group command (see `groups/generated_groups.json`) that runs the curated dataset list below and returns a combined JSON payload. |
| `dataset-<name>` | Auto-generated per Corva company 3 dataset (e.g., `dataset-activities`, `dataset-directional-tool-face`). These commands accept `--asset-ids`, `--company-id`, `--start-time/--end-time`, `--depth-start/--depth-end`, or `--limit`/`--skip` in any combination. (Some datasets may still require specific filters in the future.) |

Example:

```bash
uv run corva timelog \
  --api-key YOUR_TOKEN \
  --asset-ids 101,202 \
  --company-id 42 \
  --start-time auto_2h30m \
  --end-time auto_0d \
  --output markdown
```

### DVD dataset coverage

The `dvd` command returns a payload shaped like `{"datasets": {...}}`. The `datasets` map contains outputs for each curated dataset command (also available individually via `dataset-<slug>`):

- `data.costs`
- `data.afe`
- `data.custom_curves`
- `data.casing`
- `data.drillstring`
- `data.formations`
- `data.well_sections`
- `activity-groups`
- `well.design_optimization`
- `well.design_optimization.timelog`
- `wits.summary-30m`
- `composite.curves`
- `assets`
- `data.metrics` (the default `dvd` group invokes this with `metric_type=bha` and `metric_keys=[on_bottom_percentage, drilled_feet_rotary_percentage, drilled_feet_slide_percentage, rop]`)

#### Dataset filter requirements

Each dataset command inspects the index metadata stored in `docs/dataset.json` and enforces whichever filter combinations are supported by indexes on `asset_id`, `company_id`, `timestamp`/`start_time`, or `depth`. When you invoke a dataset (or the `dvd` group), the CLI lists the acceptable combinations (for example, `--asset-ids + --start-time/--end-time` or simply `--company-id`) and raises an error until you provide enough parameters to satisfy one of them. Depth-oriented datasets therefore require both `--depth-start` and `--depth-end`, while time-series datasets typically expect a time window together with asset identifiers.

`dataset-data-metrics` also accepts `--metric-type` (maps to `data.type`) and `--metric-keys` (comma-separated list mapping to `data.key`). The `dvd` group uses `metric_type=bha` and the key list shown above, but you can override them when running the dataset command directly.

### Auto-generate groups

Corva apps already know which datasets they depend on. Generate a reusable group that calls every matching dataset command in one shot:

```bash
uv run corva group create \
  --token YOUR_API_TOKEN \
  --app-id 1234 \
  --group-name my-app-datasets \
  --groups-file groups/generated_groups.json \
  --asset-ids 101,202 \
  --company-id 3 \
  --start-time auto_1h \
  --end-time auto_0d
```

The command fetches datasets assigned to the app, maps them to available `dataset-<slug>` commands, and appends a group entry to the target JSON file. Run it later with the standard runner:

```bash
uv run corva group run groups/generated_groups.json \
  --name my-app-datasets \
  --token YOUR_API_TOKEN \
  --asset-ids 101,202 \
  --start-time auto_1h \
  --end-time auto_0d
```

Every generated group also becomes a first-class CLI command that matches the group name (e.g., `uv run corva my-app-datasets --token ...`). Any `--asset-ids`, `--company-id`, `--start-time`, `--end-time`, `--depth-start`, `--depth-end`, `--limit`, or `--skip` values you pass to `group create` are copied into each dataset tool entry, and you can still override them at invocation time for both `group run` and the direct command. The CLI aggregates all dataset responses into a single payload shaped like `{"group": "my-app-datasets", "results": {"dataset-foo": {...}}}` so it mirrors the structured output of `dvd`.

If an app references datasets that are missing from `docs/dataset.json`, the generator lists them so you can refresh the local catalog.

The `auto_*` syntax subtracts durations from "now", so `auto_0d` equals the current UTC timestamp, `auto_2h30m` subtracts 2.5 hours, etc. Multiple units can be chained in any order. Omit both `--start-time` and `--end-time` to fall back to a simple limit (default `1000` documents) using `--limit`. Scope requests to a single company via `--company-id`. By default the CLI prints only the raw API response; add `--verbose` to include query/debug metadata.

Need only asset metadata? Use the `assets` command (same flags apply, and `--asset-ids` is optional as long as you supply `--company-id`):

```bash
uv run corva assets \
  --api-key YOUR_TOKEN \
  --company-id 42 \
  --limit 25
```

To query any other Corva dataset owned by company 3, find its friendly name in `docs/dataset.json` and use the matching `dataset-<slug>` command. Time-indexed datasets require `--start-time/--end-time`; depth-indexed datasets require `--depth-start/--depth-end`.

### Build a standalone binary

Ship the CLI as a single executable (per platform) by running the helper script from the repo root:

```bash
./build_binary.sh
```

The script runs `uv sync` and then invokes `pyinstaller` via `uv run`, bundling `docs/dataset.json` and `groups/generated_groups.json` so dataset commands and generated group commands work offline. The resulting binary lands in `dist/corva` (or `corva.exe` on Windows). Copy it to your target machine, `chmod +x dist/corva`, and run it just like the Python version.

### Settings & Overrides

Configuration lives in `src/corva_cli/settings.py` with a small set of overrides:

- `CORVA_DATA_API_ROOT_URL` (default `https://data.corva.ai`)
- `CORVA_DATA_API_TIMEOUT_SECONDS` (default `30`)

Drop a `.env` file (see `.env.example`) or set environment variables to override these values globally. Any command can still override them ad-hoc:

```bash
uv run corva timelog \
  --api-key YOUR_TOKEN \
  --asset-ids 303 \
  --company-id 99 \
  --start-time auto_6h \
  --end-time auto_0d \
  --limit 500 \
  --skip 100
```

## Notes on Retired Samples

Earlier iterations shipped extra demo commands (`fetch-production`, `summarize-metric`) to prove out completions and aggregations. They were removed from the runtime to keep the focus on timelog tooling, but the concepts remain useful when defining future plugins:

- **fetch-production**: accepted `well_id`, `start`, `end` and produced hourly synthetic volumes. Had well id completions.
- **summarize-metric**: accepted a `metric_id` and returned min/max/avg aggregates with optional limits and completions.

Reintroduce them by creating new modules under `src/corva_cli/tools/` and decorating with `@registry.tool(...)`. Use this README as a reference for the parameter patterns if you need to recreate MQL equivalents.
