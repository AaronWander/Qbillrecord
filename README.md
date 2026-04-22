# jizhang

Personal finance pipeline: ingest bank SMS → parse → classify (rules + optional AI) → export/push to Firefly III.

## Quickstart (CLI)

This project is a CLI app (no packaging in v1). Run via:

```bash
python -m jizhang list
python -m jizhang doctor --pipeline pipelines/icbc95588_inc.yml
python -m jizhang run --pipeline pipelines/icbc95588_inc.yml
```

## Pipelines

Pipelines are YAML files under `pipelines/`. They select built-in step implementations by `type`.

Environment variables may be referenced only via `*_env/*_ENV` keys (no `${VAR}` interpolation).

## Configuration

### 1) Pipeline YAML (no secrets)

Pipelines live in `pipelines/*.yml`. They select step implementations by `type` and may reference environment variables via `*_env` keys.

Example: `pipelines/icbc95588_inc.yml`.

### 2) Environment variables (secrets / environment-specific)

Copy `.env.example` to `.env` and fill values (do **not** commit `.env`).

Required for pushing to Firefly:
- `FIREFLY_BASE_URL`
- `FIREFLY_TOKEN`

Optional for AI classification (when enabled by pipeline):
- `DEEPSEEK_API` (e.g. `openai-chat-completions` or `openai-responses`)
- `DEEPSEEK_BASE_URL`
- `DEEPSEEK_API_KEY`
- `DEEPSEEK_MODEL`

## Artifacts (audit / replay)

Each run writes a timestamped folder under `exports/runs/<timestamp>/` with raw inputs, intermediate JSONL files, AI audit logs, and push state.

These artifacts may include sensitive personal financial data and are ignored by git by default.

## Project status

The repo is currently being rebuilt from a set of scripts into a mature `python -m jizhang` CLI driven by YAML pipelines. See:
- `docs/plans/2026-04-22-cli-pipeline-rebuild-design.md`
