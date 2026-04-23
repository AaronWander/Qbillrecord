from __future__ import annotations

import hashlib
import json
import os
import sys
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from qbillrecord.pipeline.config import PipelineConfig
from qbillrecord.pipeline.errors import ConfigError, JizhangError
from qbillrecord.registry import REGISTRY
from qbillrecord.steps.base import RunContext, Sink, StateStore, Source, Transform
from qbillrecord.steps import builtins as _builtins  # noqa: F401


def _ts() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def _sha256_json(obj: Any) -> str:
    data = json.dumps(obj, ensure_ascii=False, sort_keys=True).encode("utf-8")
    return hashlib.sha256(data).hexdigest()


def _ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def _max_rowid_in_jsonl(path: Path) -> int:
    if not path.exists():
        return 0
    m = 0
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
        except Exception:
            continue
        rid = obj.get("rowid")
        if isinstance(rid, int) and rid > m:
            m = rid
    return m


def _load_rowid_state(path: Path) -> int:
    if not path.exists():
        return 0
    try:
        st = json.loads(path.read_text(encoding="utf-8"))
    except Exception as e:
        raise ConfigError(f"Failed to read state file: {path}: {e}") from e
    try:
        return int(st.get("last_rowid") or 0)
    except Exception:
        return 0


def _save_rowid_state(path: Path, last_rowid: int) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({"last_rowid": int(last_rowid)}, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


@dataclass(frozen=True)
class RunResult:
    rc: int
    run_dir: Path


def run_pipeline(cfg: PipelineConfig) -> RunResult:
    """
    v1 runner: supports the built-in pipeline types declared in `pipelines/icbc95588_inc.yml`.

    Behavior notes:
    - Hard-fail AI: if scripts/pipeline_95588_classify_with_ai.py returns rc!=0, abort (rc propagated).
    - Do not update rowid watermark state on failure.
    - Writes a timestamped run folder with artifacts + run_manifest.json.
    """
    started = time.time()
    raw = cfg.raw

    artifacts_dir = Path(str(raw.get("artifacts_dir") or "exports/runs"))
    run_dir = artifacts_dir / _ts()
    _ensure_dir(run_dir)
    audit_dir = run_dir / "ai_audit"

    manifest: dict[str, Any] = {
        "pipeline": {"name": cfg.name, "path": str(cfg.path)},
        "started_at": int(started),
        "config_sha256": _sha256_json(raw),
        "run_dir": str(run_dir),
        "rc": None,
    }

    def _write_manifest(rc: int) -> None:
        manifest["rc"] = int(rc)
        manifest["ended_at"] = int(time.time())
        manifest["elapsed_ms"] = int((time.time() - started) * 1000)
        (run_dir / "run_manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    state_cfg = raw.get("state") or {}
    source_cfg = raw.get("source") or {}
    classifier_cfg = raw.get("classifier") or {}
    exporter_cfg = raw.get("exporter") or {}
    sink_cfg = raw.get("sink") or {}

    if not isinstance(state_cfg, dict) or not isinstance(source_cfg, dict) or not isinstance(classifier_cfg, dict) or not isinstance(exporter_cfg, dict) or not isinstance(sink_cfg, dict):
        _write_manifest(2)
        raise ConfigError("Pipeline sections must be objects: state/source/classifier/exporter/sink")

    state_type = str(state_cfg.get("type") or "").strip()
    source_type = str(source_cfg.get("type") or "").strip()
    transform_type = str(classifier_cfg.get("type") or "").strip()
    sink_type = str(sink_cfg.get("type") or "").strip()
    if not state_type or not source_type or not transform_type or not sink_type:
        _write_manifest(2)
        raise ConfigError("Pipeline must set type for: state/source/classifier/sink")

    try:
        state_store = REGISTRY.create(kind="state", type_id=state_type, config=state_cfg)
        source_step = REGISTRY.create(kind="source", type_id=source_type, config=source_cfg)
        transform_step = REGISTRY.create(
            kind="transform",
            type_id=transform_type,
            config={"rules_path": classifier_cfg.get("rules_path"), "ai": classifier_cfg.get("ai"), "exporter": exporter_cfg},
        )
        sink_step = REGISTRY.create(kind="sink", type_id=sink_type, config=sink_cfg)
    except KeyError as e:
        _write_manifest(2)
        raise ConfigError(str(e)) from e

    if not isinstance(state_store, StateStore) or not isinstance(source_step, Source) or not isinstance(transform_step, Transform) or not isinstance(sink_step, Sink):
        _write_manifest(2)
        raise ConfigError("Step registration returned wrong types")

    state_obj = state_store.load()
    last_rowid = int(state_obj.get("last_rowid") or 0) if isinstance(state_obj, dict) else 0
    manifest["state"] = {"type": state_type, "last_rowid": last_rowid}
    print(f"[run] last_rowid={last_rowid}", file=sys.stderr, flush=True)

    ctx = RunContext(
        run_dir=run_dir,
        audit_dir=audit_dir,
        raw_path=run_dir / "raw.jsonl",
        firefly_path=run_dir / "firefly.jsonl",
        push_state_path=run_dir / "push_state.jsonl",
        manifest=manifest,
    )

    src_meta = source_step.export(ctx=ctx, state=state_obj)
    manifest["source"] = {"type": source_type, **(src_meta or {})}
    new_max = int((src_meta or {}).get("rowid_max") or 0)
    if int((src_meta or {}).get("alerts") or 0) > 0:
        _write_manifest(2)
        print("[run] export anomalies found; abort before transform/push/state update.", file=sys.stderr, flush=True)
        return RunResult(rc=2, run_dir=run_dir)
    if new_max <= last_rowid:
        _write_manifest(0)
        print("[run] no new messages; nothing to do.", file=sys.stderr, flush=True)
        return RunResult(rc=0, run_dir=run_dir)

    # 3) Parse + classify + export
    from qbillrecord.transform.icbc95588_pipeline import run_pipeline as transform_run

    _ensure_dir(audit_dir)
    pipe_rc = int(transform_step.run(ctx=ctx) or 0)
    manifest["transform"] = {"type": transform_type, "rc": pipe_rc, "firefly_out": str(ctx.firefly_path), "audit_dir": str(audit_dir)}
    if pipe_rc != 0:
        _write_manifest(pipe_rc)
        print(f"[run] pipeline failed rc={pipe_rc}; abort before push/state update.", file=sys.stderr, flush=True)
        return RunResult(rc=pipe_rc, run_dir=run_dir)

    # 4) Sink: push to Firefly
    summary = sink_step.push(ctx=ctx)
    manifest["sink"] = {"type": sink_type, "push_state": str(ctx.push_state_path), "summary": summary}

    # 5) Update watermark state
    if isinstance(state_obj, dict):
        state_obj["last_rowid"] = new_max
    state_store.save(state_obj)
    manifest["state"]["new_last_rowid"] = int(new_max)

    _write_manifest(0)
    print(f"[run] state updated last_rowid={new_max}", file=sys.stderr, flush=True)
    return RunResult(rc=0, run_dir=run_dir)


def safe_run_pipeline(cfg: PipelineConfig) -> RunResult:
    try:
        return run_pipeline(cfg)
    except ConfigError:
        raise
    except JizhangError:
        raise
    except Exception as e:
        # Unexpected errors -> rc=2 for now.
        raise JizhangError(str(e)) from e
