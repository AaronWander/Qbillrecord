# Qbillrecord

Personal finance pipeline: ingest bank SMS/notifications → parse → classify (rules + optional AI) → export/push to Firefly III.

The default scenario in this repo is: **macOS iMessage sender=95588 (ICBC) → Firefly III**.

## Features

- **Pipeline-driven**: use `pipelines/*.yml` to select/compose steps (`state/source/transform/sink`).
- **Incremental watermark**: ROWID watermark state to avoid re-processing.
- **Auditable artifacts**: each run writes `exports/runs/<timestamp>/` for replay/debug.
- **Optional AI assist**: use AI to suggest categories for unresolved items (configurable, can fail-hard).
- **Idempotent push**: push to Firefly III and record idempotency state in `push_state.jsonl`.

## Quickstart (CLI)

This project is a CLI app (no packaging in v1). Run from repo root:

```bash
python -m qbillrecord list
python -m qbillrecord doctor --pipeline pipelines/qbillrecord_icbc95588_inc.yml
python -m qbillrecord run --pipeline pipelines/qbillrecord_icbc95588_inc.yml
```

Tip: run `doctor` first to validate config and required environment variables without executing the pipeline.

## Configuration

### 1) Pipeline YAML (no secrets)

Pipelines live in `pipelines/*.yml`. They select step implementations by `type`.

Environment variables may be referenced only via `*_env/*_ENV` keys (no `${VAR}` interpolation), e.g.:

- `token_env: FIREFLY_TOKEN`
- `base_url_env: FIREFLY_BASE_URL`

Example pipeline: `pipelines/qbillrecord_icbc95588_inc.yml`.

### 2) Environment variables (secrets / environment-specific)

Copy `.env.example` to `.env` and fill values (do **not** commit `.env`).

Required for pushing to Firefly:
- `FIREFLY_BASE_URL`
- `FIREFLY_TOKEN`

Optional for AI classification (when enabled by pipeline):
- `DEEPSEEK_BASE_URL`
- `DEEPSEEK_API_KEY`
- `DEEPSEEK_MODEL`

## Artifacts (audit / replay)

Each run writes a timestamped folder under `exports/runs/<timestamp>/`:

- `raw.jsonl`: raw exported messages (incremental)
- `firefly.jsonl`: Firefly III transaction payloads (JSONL)
- `push_state.jsonl`: push status + idempotency record
- `ai_audit/`: AI request/response logs (when enabled)
- `run_manifest.json`: run manifest (timings, rc, config hash)

These artifacts may include sensitive personal financial data and are ignored by git by default.

## Repository layout

- `qbillrecord/`: core code (CLI, pipeline runner, steps, parsers/exporters)
- `pipelines/`: pipeline YAMLs
- `rules/`: parsing + classification rules
- `docs/`: design docs
- `exports/`: local artifacts (ignored by git)

## Troubleshooting

- Config/env validation: `python -m qbillrecord doctor --pipeline ...`
- Push failures: inspect `exports/runs/<ts>/push_state.jsonl` and stderr logs
- Parse/classify issues: inspect `exports/runs/<ts>/raw.jsonl` (inputs) and `exports/runs/<ts>/firefly.jsonl` (outputs)

## Project status

The repo is a `python -m qbillrecord` CLI driven by YAML pipelines. Background/design:
- `docs/ARCHITECTURE.md`
