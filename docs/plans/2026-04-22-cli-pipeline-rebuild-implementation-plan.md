# CLI + Pipeline Rebuild Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Replace the current “many scripts” repo with a single `python -m jizhang` CLI driven by YAML pipeline config (env references only via `*_env` / `*_ENV`), while preserving current behavior and hard-fail AI semantics.

**Architecture:** Implement a small step registry (`type` → implementation) for `source/parser/classifier/exporter/sink/state`. The CLI loads a YAML pipeline file, resolves env references, runs the pipeline, and writes standardized artifacts per run.

**Tech Stack:** Python 3.12+, standard library `argparse`, `dataclasses`, `json`; add dependency `PyYAML` for YAML parsing.

---

### Task 1: Add baseline open-source docs and ignores

**Files:**
- Create: `README.md`
- Create: `LICENSE` (choose MIT or Apache-2.0)
- Create: `CONTRIBUTING.md`
- Modify: `.gitignore`

**Step 1: Write README skeleton**
- Include: quickstart, pipeline example, artifacts layout, common errors (AI hard-fail, missing env).

**Step 2: Add `.gitignore`**
- Must ignore: `.env`, `exports/`, `reports/`, `__pycache__/`, `.DS_Store`, `*.jsonl` (optional policy), `backup.zip`.
- Keep `rules/` and `docs/` tracked.

**Step 3: Add license + contribution guide**
- Keep minimal, no extra tooling promises.

**Step 4: Commit**
Run:
- `git add README.md LICENSE CONTRIBUTING.md .gitignore`
- `git commit -m "docs: add OSS project basics"`

---

### Task 2: Introduce `jizhang/` package and `python -m jizhang` entry

**Files:**
- Create: `jizhang/__init__.py`
- Create: `jizhang/__main__.py`
- Create: `jizhang/cli.py`

**Step 1: Implement CLI skeleton**
- Subcommands: `run`, `doctor`, `list`
- `--pipeline` required for `run`/`doctor`

**Step 2: Add `python -m jizhang` hook**
- `__main__.py` calls `jizhang.cli.main()`

**Step 3: Commit**
Run:
- `git add jizhang/__init__.py jizhang/__main__.py jizhang/cli.py`
- `git commit -m "feat: add jizhang CLI skeleton"`

---

### Task 3: Add pipeline YAML loader with `_env` resolution

**Files:**
- Create: `jizhang/pipeline/config.py`
- Create: `jizhang/pipeline/errors.py`

**Step 1: Add PyYAML dependency**
- Decide whether to vendor or require `pip install pyyaml` in README (v1 ok).

**Step 2: Implement loader**
- Load YAML dict.
- Resolve keys that end with `_env` or `_ENV` into actual values from `os.environ`.
- Validate required keys exist and types are correct; raise `ConfigError` (rc=2).

**Step 3: Add `doctor` checks**
- Ensure all referenced env vars are present/non-empty.
- Ensure file paths exist where expected (`rules_path`, pipeline file).

**Step 4: Commit**
Run:
- `git add jizhang/pipeline/config.py jizhang/pipeline/errors.py`
- `git commit -m "feat: load pipeline YAML with env resolution"`

---

### Task 4: Define core data models and JSONL helpers

**Files:**
- Create: `jizhang/types.py`
- Create: `jizhang/io/jsonl.py`

**Step 1: Define dataclasses**
- `RawMessage`, `Transaction`, `ClassifiedTransaction`, `FireflyRecord`

**Step 2: Add JSONL reader/writer helpers**
- Stable ordering where possible; safe UTF-8 handling.

**Step 3: Commit**

---

### Task 5: Implement registry + step interfaces

**Files:**
- Create: `jizhang/registry.py`
- Create: `jizhang/steps/base.py`

**Step 1: Define step protocols / base classes**
- Keep minimal; don’t over-abstract.

**Step 2: Implement registry**
- `register(kind, type_id, factory)`
- `create(kind, type_id, config)`
- Used by `list` command.

**Step 3: Commit**

---

### Task 6: Port existing functionality into built-in steps (thin wrappers first)

**Files:**
- Create: `jizhang/steps/state_rowid.py`
- Create: `jizhang/steps/source_imessage_sqlite.py`
- Create: `jizhang/steps/parser_icbc95588.py`
- Create: `jizhang/steps/classifier_rules_ai.py`
- Create: `jizhang/steps/exporter_firefly_jsonl.py`
- Create: `jizhang/steps/sink_firefly_api.py`

**Step 1: Wrap existing scripts initially**
- Call existing functions from `scripts/*` to preserve behavior.
- Keep the hard-fail AI validation semantics.

**Step 2: Register built-ins**
- Ensure `list` shows them.

**Step 3: Commit**

---

### Task 7: Implement pipeline runner + artifacts layout

**Files:**
- Create: `jizhang/pipeline/runner.py`
- Modify: `jizhang/cli.py`

**Step 1: Runner writes standardized artifacts**
- Create run dir `artifacts_dir/<timestamp>/`
- Write `run_manifest.json` with config digest + rc.

**Step 2: Wire `run` command**
- Execute steps in order with clear stderr logging.
- Enforce: on AI failure, abort rc=3 and do not update state.

**Step 3: Commit**

---

### Task 8: Provide example pipeline file(s)

**Files:**
- Create: `pipelines/icbc95588_inc.yml`

**Step 1: Add pipeline example without secrets**
- Use `_env` keys for all secrets.

**Step 2: Document usage in README**

**Step 3: Commit**

---

### Task 9: Deprecate or slim down `scripts/` entrypoints

**Files:**
- Modify: `scripts/jizhang.py` (optional wrapper) or mark deprecated in README

**Step 1: Decide policy**
- Either keep as wrapper calling `python -m jizhang`, or leave as legacy but documented.

**Step 2: Commit**

---

### Task 10: Smoke tests (manual + minimal automated)

**Files:**
- Create: `tests/test_config_env_resolution.py` (only if we add a test framework)

**Step 1: Manual smoke**
- `python -m jizhang list`
- `python -m jizhang doctor --pipeline pipelines/icbc95588_inc.yml`

**Step 2: End-to-end smoke (local)**
- Run the pipeline in a dry mode if we add one, or run against existing artifacts.

**Step 3: Commit**

---

## Execution Handoff

Plan complete and saved to `docs/plans/2026-04-22-cli-pipeline-rebuild-implementation-plan.md`. Two execution options:

1. Subagent-Driven (this session) — dispatch fresh subagent per task, review between tasks
2. Parallel Session (separate) — run with superpowers:executing-plans

Which approach?

