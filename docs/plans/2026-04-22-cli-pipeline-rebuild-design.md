# CLI + Pipeline Rebuild (Design)

**Date:** 2026-04-22  
**Status:** Approved (user confirmed: CLI-only, YAML pipelines, env references only via `*_env` / `*_ENV`)

## Goal

Rebuild this repo from “many Python scripts” into a mature, GitHub-friendly CLI project with:

- A single entrypoint: `python -m jizhang ...`
- A pipeline-driven architecture (source → parser → classifier → exporter → sink) where each step is replaceable without impacting other steps.
- Strong artifact/audit outputs for replay/debugging.
- Open-source hygiene (README, LICENSE, CONTRIBUTING, sane `.gitignore`).

Non-goals (v1):
- Packaging/distribution (`pip install`, `pipx`, etc.)
- Multi-bank “universal” coverage out of the box (design supports it; we will ship a minimal set of built-ins).

## User-facing CLI

Commands (v1):

- `python -m jizhang run --pipeline pipelines/<name>.yml`
- `python -m jizhang doctor --pipeline pipelines/<name>.yml`
- `python -m jizhang list`

Exit codes (v1):

- `0` success
- `2` configuration / input validation failure
- `3` AI call failure or AI validation failure (hard-fail requirement)
- `4` sink/push failure

## Pipeline configuration format

### Format

- YAML file (in-repo), e.g. `pipelines/icbc95588_inc.yml`.
- No `${ENV_VAR}` string interpolation.
- Environment variables are referenced **only** by special config keys ending with `_env` (or `_ENV`) that contain the *name* of an env var.

Example:

```yaml
sink:
  type: firefly_api
  base_url_env: FIREFLY_BASE_URL
  token_env: FIREFLY_TOKEN
```

### Required top-level keys (v1)

- `name` (string)
- `artifacts_dir` (string path)
- `state` (object)
- `source` (object)
- `parser` (object)
- `classifier` (object)
- `exporter` (object)
- `sink` (object)

### Reference pipeline (v1)

```yaml
name: icbc95588_inc
artifacts_dir: exports/runs

state:
  type: rowid_watermark
  path: exports/95588_state.json

source:
  type: imessage_sqlite
  sender: "95588"
  db_path: "~/Library/Messages/chat.db"

parser:
  type: icbc95588_sms

classifier:
  type: rules_ai
  rules_path: rules/icbc_95588_rules.json
  ai:
    enabled: true
    provider: openai_compatible
    api: openai-responses
    base_url_env: DEEPSEEK_BASE_URL
    api_key_env: DEEPSEEK_API_KEY
    model_env: DEEPSEEK_MODEL
    fail_hard: true

exporter:
  type: firefly_jsonl
  tz: "+08:00"
  asset_prefix: "工商银行"

sink:
  type: firefly_api
  base_url_env: FIREFLY_BASE_URL
  token_env: FIREFLY_TOKEN
  bootstrap_assets: true
  idempotency:
    state_path: push_state.jsonl
    server_duplicate_hash: true
```

## Architecture

### Core principle: replaceability by contracts

Each step is selected by `type` (string id). Implementations are registered in a small in-code registry.

Steps exchange data only via shared typed models:

- `RawMessage` — normalized raw message (source-agnostic)
- `Transaction` — parsed transaction (parser-specific but output contract stable)
- `ClassifiedTransaction` — transaction + category/tags
- `FireflyRecord` — one JSONL import/push record for Firefly

### Step contracts (concept)

- `Source.read(state) -> Iterable[RawMessage]`
- `Parser.parse(messages) -> Iterable[Transaction]`
- `Classifier.classify(txns) -> Iterable[ClassifiedTransaction]`
- `Exporter.export(classified) -> Iterable[FireflyRecord]` (+ writes JSONL artifact)
- `Sink.push(records) -> PushResult` (+ writes `push_state.jsonl`)
- `StateStore.load()/save()` (e.g. rowid watermark)

### Hard-fail AI requirement

If `classifier.ai.enabled=true` and any AI batch call fails, the run must abort with rc `3` and must not update state.

### Artifacts / audit (pipeline thinking)

Every `run` creates `exports/runs/<timestamp>/` and writes:

- `raw.jsonl` (RawMessage)
- `txns.jsonl` (Transaction)
- `classified.jsonl` (ClassifiedTransaction)
- `firefly.jsonl` (FireflyRecord JSONL)
- `ai_audit/*` (requests/responses/candidates; optional but recommended when AI enabled)
- `push_state.jsonl` (sink result log)
- `run_manifest.json` (pipeline name, config digest, start/end time, rc, versions)

This enables replay and debugging without re-reading `chat.db`.

## Open-source repository hygiene (v1)

Add/standardize:

- `README.md` (quickstart, pipeline example, troubleshooting)
- `LICENSE` (MIT or Apache-2.0; choose explicitly)
- `CONTRIBUTING.md`
- `.gitignore` (must ignore `.env`, `exports/`, `reports/`, `__pycache__/`, `.DS_Store`, and any raw bank data)

## Migration strategy

Phased, to keep functionality:

1. Introduce `jizhang/` package + `python -m jizhang` CLI that can run the existing pipeline end-to-end.
2. Move implementations from `scripts/*.py` into `jizhang/` modules step-by-step.
3. Keep `scripts/` as thin wrappers temporarily (optional); mark deprecated in README.
4. Once stable, remove old entrypoints or keep a minimal compatibility layer.

