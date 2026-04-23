# Qbillrecord: 95588 → Firefly pipeline architecture

This repo is a **`python -m qbillrecord`** CLI project organized as a pipeline of replaceable steps:
`state → source → transform → sink`.

It provides an end-to-end path and uses YAML pipeline configs to swap inputs, rules, and sinks.

## 1) One-liner (Why / What)

Export raw messages (built-in: macOS iMessage with `sender=95588`) → parse and classify using rules → optionally use AI to fill unresolved items → export Firefly III TransactionStore JSONL → push to Firefly III via API, with an incremental watermark and replayable artifacts.

## 2) Scope and non-goals

**In scope**
- Export: read sender-matched messages from `chat.db` into JSONL
- Parse: extract transactions from message content (direction, amount, card last4, merchant, balance, etc.)
- Classify: apply `rules/*.json` and optionally use AI for unresolved items
- Export: generate Firefly III `POST /api/v1/transactions` JSONL payloads
- Push: push JSONL to Firefly III with idempotency/dedup and optional asset-account bootstrap
- Archive: write `exports/runs/<timestamp>/` artifacts for audit/replay

**Explicitly not in scope (current)**
- Hosting/operating Firefly III itself (this project integrates with an external Firefly instance)
- A generic “all banks / all templates” solution (current focus is ICBC 95588)
- A fully rigorous reconciliation system (artifacts help manual audit/debug)

## 3) High-level architecture (static)

### 3.1 Layers

- **Control / orchestration**
  - `qbillrecord/cli.py`: unified CLI entrypoint (`python -m qbillrecord ...`)
  - `qbillrecord/pipeline/runner.py`: pipeline orchestration + run artifacts

- **Execution / processing (replaceable steps)**
  - state: `qbillrecord/steps/state_rowid.py`
  - source: `qbillrecord/steps/source_imessage.py`
  - transform: `qbillrecord/steps/transform_icbc95588.py` (calls `qbillrecord/transform/icbc95588_pipeline.py`)
  - sink: `qbillrecord/steps/sink_firefly.py`

- **Configuration**
  - pipeline configs: `pipelines/*.yml`
  - rules and taxonomy: `rules/*.json`
  - environment: `.env` / `.env.example`
  - artifacts: `exports/` (ignored by git by default)

### 3.2 Responsibilities table

| Module/Step | Responsibility | Main input | Main output |
|---|---|---|---|
| `qbillrecord/steps/source_imessage.py` | Export sender-matched messages from Messages `chat.db` into JSONL; normalize `content` | `chat.db` | `exports/runs/<ts>/raw.jsonl` |
| `qbillrecord/steps/transform_icbc95588.py` | Parse + rule classify + optional AI fill + export Firefly JSONL | raw JSONL + rules | `exports/runs/<ts>/firefly.jsonl` + `ai_audit/` |
| `qbillrecord/steps/sink_firefly.py` | Push to Firefly III API; optional asset bootstrap; idempotency/state | firefly JSONL | `exports/runs/<ts>/push_state.jsonl` |
| `qbillrecord/steps/state_rowid.py` | Read/write ROWID watermark state | state json | state json |
| `qbillrecord/pipeline/runner.py` | Orchestrate state → source → transform → sink → update state | pipeline + env | run dir + updated state |

## 4) Core data objects (implicit model)

Even though scripts communicate via JSONL, the implicit data model is stable:

- **RawMsg (exported JSONL row)**
  - key fields: `rowid`, `date_local`, `sender`, `content`

- **ParsedTxn (parsed transaction)**
  - key fields: direction, `amount`, `card_last4`, `merchant`, `counterparty`, `raw_bracket`, `short_info`

- **Firefly TransactionStore payload (per-line push payload)**
  - key fields: `type` (deposit/withdrawal), `amount`, `date`, `source_name/destination_name`, `category_name`, `external_id`, `notes`

## 5) Primary flows (dynamic)

### 5.1 Incremental flow (recommended for daily use)

Entry:

`python -m qbillrecord run --pipeline pipelines/qbillrecord_icbc95588_inc.yml`

1) Load watermark: `exports/95588_state.json:last_rowid`
2) Export delta to `exports/runs/<ts>/raw.jsonl`
3) Validate export anomalies (abort if present)
4) Transform: parse/classify/export to `exports/runs/<ts>/firefly.jsonl` and optional `ai_audit/`
5) Sink: push to Firefly, write `exports/runs/<ts>/push_state.jsonl`
6) Update watermark: write back `exports/95588_state.json:last_rowid=<new_max>`

**Idempotency / dedup semantics**
- Local: push state tracks succeeded `external_id` entries to skip on re-run
- Server: optional Firefly duplicate-hash protection (if enabled by sink config)
- Stable IDs: export uses sha256-based `external_id` generation

### 5.2 Full import flow (first run / rebuild)

In a full run you may choose not to update the incremental watermark to avoid moving the delta cursor accidentally. The project still writes per-run artifacts for audit/replay.

## 6) Ops loop: config, artifacts, debugging

### 6.1 Required config (.env)

Repo-root `.env` (see `.env.example`):

- DeepSeek (optional; only used when AI is enabled by the pipeline)
  - `DEEPSEEK_API_KEY`
  - `DEEPSEEK_BASE_URL` (default `https://api.deepseek.com`)
  - `DEEPSEEK_MODEL` (default `deepseek-chat`)
  - `DEEPSEEK_API` (optional: `openai-chat-completions` or `openai-responses`)
- Firefly (required for pushing)
  - `FIREFLY_BASE_URL`
  - `FIREFLY_TOKEN`

### 6.2 Artifact directories

- `exports/95588_state.json`: incremental watermark (ROWID)
- `exports/runs/<timestamp>/`: per-run archive (raw/firefly/push_state/ai_audit)
- `reports/`: human-readable reports (markdown)

### 6.3 Common debugging entry points

- Export anomalies: inspect `exports/runs/<ts>/export_alerts.jsonl` (if present)
- Parse/classify issues: inspect `exports/runs/<ts>/raw.jsonl` and `exports/runs/<ts>/firefly.jsonl`
- Firefly push failures: inspect `exports/runs/<ts>/push_state.jsonl` and stderr output

## 7) Diagrams

Diagram file: `docs/diagrams/pipeline.mmd` (preview with a Mermaid-capable editor).

