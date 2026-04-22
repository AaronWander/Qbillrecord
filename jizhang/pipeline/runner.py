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

from jizhang.pipeline.config import PipelineConfig
from jizhang.pipeline.errors import ConfigError, JizhangError


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

    # Resolve required types (we keep it explicit in v1)
    state = raw.get("state") or {}
    source = raw.get("source") or {}
    parser = raw.get("parser") or {}
    classifier = raw.get("classifier") or {}
    exporter = raw.get("exporter") or {}
    sink = raw.get("sink") or {}

    state_type = str(state.get("type") or "")
    source_type = str(source.get("type") or "")
    parser_type = str(parser.get("type") or "")
    classifier_type = str(classifier.get("type") or "")
    exporter_type = str(exporter.get("type") or "")
    sink_type = str(sink.get("type") or "")

    supported = {
        ("state", "rowid_watermark"),
        ("source", "imessage_sqlite"),
        ("parser", "icbc95588_sms"),
        ("classifier", "rules_ai"),
        ("exporter", "firefly_jsonl"),
        ("sink", "firefly_api"),
    }
    requested = {
        ("state", state_type),
        ("source", source_type),
        ("parser", parser_type),
        ("classifier", classifier_type),
        ("exporter", exporter_type),
        ("sink", sink_type),
    }
    unsupported = sorted([f"{k}:{t}" for (k, t) in requested if (k, t) not in supported])
    if unsupported:
        _write_manifest(2)
        raise ConfigError(f"Unsupported step types (v1 runner): {', '.join(unsupported)}")

    # 1) State: load watermark
    state_path = Path(str(state.get("path") or "exports/95588_state.json"))
    last_rowid = _load_rowid_state(state_path)
    manifest["state"] = {"path": str(state_path), "last_rowid": last_rowid}
    print(f"[run] last_rowid={last_rowid}", file=sys.stderr, flush=True)

    # 2) Source: export delta to run_dir/raw.jsonl
    from jizhang.ingest.imessage import iter_sender_messages, write_jsonl
    from jizhang.ingest.validate import validate as validate_export

    sender = str(source.get("sender") or "95588")
    db_path = os.path.expanduser(str(source.get("db_path") or ""))
    raw_out = run_dir / "raw.jsonl"

    count = write_jsonl(
        iter_sender_messages(
            db_path=db_path,
            sender_like=f"%{sender}%",
            since_rowid=int(last_rowid),
        ),
        raw_out,
    )
    print(f"[run] exported messages={count} to {raw_out}", file=sys.stderr, flush=True)

    new_max = _max_rowid_in_jsonl(raw_out)
    manifest["source"] = {"sender": sender, "db_path": db_path, "raw_out": str(raw_out), "rowid_max": new_max}
    if new_max <= last_rowid:
        # no new messages
        _write_manifest(0)
        print("[run] no new messages; nothing to do.", file=sys.stderr, flush=True)
        return RunResult(rc=0, run_dir=run_dir)

    alerts = validate_export(str(raw_out))
    if alerts:
        alerts_path = run_dir / "export_alerts.jsonl"
        alerts_path.write_text("\n".join(json.dumps(a, ensure_ascii=False) for a in alerts) + "\n", encoding="utf-8")
        _write_manifest(2)
        print(f"[run] export anomalies found; saved {len(alerts)} alerts to {alerts_path}.", file=sys.stderr, flush=True)
        return RunResult(rc=2, run_dir=run_dir)

    # 3) Parse + classify + export
    from jizhang.transform.icbc95588_pipeline import run_pipeline as transform_run

    rules_path = str(classifier.get("rules_path") or "")
    ai_cfg = classifier.get("ai") or {}
    ai_enabled = bool(ai_cfg.get("enabled", False))

    firefly_out = run_dir / "firefly.jsonl"
    _ensure_dir(audit_dir)

    pipe_rc = int(
        transform_run(
            in_path=str(raw_out),
            rules_path=rules_path,
            firefly_out=str(firefly_out),
            audit_dir=str(audit_dir),
            tz=str(exporter.get("tz") or "+08:00"),
            asset_prefix=str(exporter.get("asset_prefix") or "工商银行"),
            no_ai=(not ai_enabled),
            apply_rules=bool(exporter.get("apply_rules", False)),
        )
        or 0
    )
    manifest["transform"] = {
        "rules_path": rules_path,
        "ai_enabled": ai_enabled,
        "firefly_out": str(firefly_out),
        "audit_dir": str(audit_dir),
        "rc": pipe_rc,
    }
    if pipe_rc != 0:
        _write_manifest(pipe_rc)
        print(f"[run] pipeline failed rc={pipe_rc}; abort before push/state update.", file=sys.stderr, flush=True)
        return RunResult(rc=pipe_rc, run_dir=run_dir)

    # 4) Sink: push to Firefly
    from jizhang.sink.firefly import push_firefly_jsonl

    push_state = run_dir / "push_state.jsonl"
    summary = push_firefly_jsonl(
        in_path=firefly_out,
        state_path=push_state,
        base_url=str(sink.get("base_url") or ""),
        token=str(sink.get("token") or ""),
        timeout_s=int(sink.get("timeout_s") or 30),
        retries=int(sink.get("retries") or 3),
        retry_sleep_s=float(sink.get("retry_sleep_s") or 1.5),
        bootstrap_assets=bool(sink.get("bootstrap_assets", False)),
        skip_using_state=True,
        no_error_if_duplicate=bool(sink.get("no_error_if_duplicate", False)),
        dry_run=bool(sink.get("dry_run", False)),
        limit=int(sink.get("limit") or 0),
    )
    manifest["sink"] = {"type": sink_type, "push_state": str(push_state), "summary": summary.__dict__}

    # 5) Update watermark state
    _save_rowid_state(state_path, new_max)
    manifest["state"]["new_last_rowid"] = new_max

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
