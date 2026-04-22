#!/usr/bin/env python3
"""
Interactive menu entrypoint for the 95588 -> Firefly pipeline.

Menu:
1) Incremental update (ROWID watermark, archives run, updates watermark)
2) Full export + classify + push (does NOT update watermark)
3) Replay an incremental run folder (does NOT update watermark)

This script is meant for human use (interactive). For automation, call the underlying scripts directly.
"""

from __future__ import annotations

import os
import sys
from datetime import datetime
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


def _load_env() -> None:
    # Keep it simple: rely on shell `set -a; source .env; set +a` when possible.
    # But also support direct running.
    env_path = REPO_ROOT / ".env"
    if not env_path.exists():
        return
    for line in env_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        k = k.strip()
        v = v.strip().strip('"').strip("'")
        if k and (k not in os.environ or os.environ.get(k, "").strip() == ""):
            os.environ[k] = v


def _ts() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def _safe_input(prompt: str) -> str:
    try:
        return input(prompt)
    except EOFError:
        return ""


def _list_runs(runs_dir: Path) -> list[Path]:
    if not runs_dir.exists():
        return []
    dirs = [p for p in runs_dir.iterdir() if p.is_dir()]
    # Sort newest first by folder name (timestamp format).
    dirs.sort(key=lambda p: p.name, reverse=True)
    return dirs


def _print_header() -> None:
    print("=== jizhang (95588 -> Firefly) ===")
    print(f"repo: {REPO_ROOT}")
    state_path = REPO_ROOT / "exports" / "95588_state.json"
    if state_path.exists():
        try:
            import json

            st = json.loads(state_path.read_text(encoding="utf-8"))
            print(f"watermark exports/95588_state.json last_rowid={st.get('last_rowid')}")
        except Exception:
            print("watermark exports/95588_state.json (unreadable)")
    else:
        print("watermark exports/95588_state.json (missing)")
    print("")


def run_incremental() -> int:
    from scripts.run_incremental_95588_to_firefly import main as inc_main  # type: ignore

    sys.argv = ["run_incremental_95588_to_firefly.py"]
    return int(inc_main() or 0)


def run_full() -> int:
    """
    Full: export all 95588 messages -> classify+AI -> export Firefly JSONL -> push.
    Does NOT update ROWID watermark.
    """
    from scripts.export_imessage_sender import main as export_main  # type: ignore
    from scripts.pipeline_95588_classify_with_ai import main as pipe_main  # type: ignore
    from scripts.push_firefly_jsonl import main as push_main  # type: ignore
    from scripts.validate_imessage_export import validate as validate_export  # type: ignore

    ts = _ts()
    run_dir = REPO_ROOT / "exports" / "runs" / f"full_{ts}"
    run_dir.mkdir(parents=True, exist_ok=True)

    raw_all = run_dir / "raw_all.jsonl"
    firefly_all = run_dir / "firefly_all.jsonl"
    audit_dir = run_dir / "ai_audit"
    push_state = run_dir / "push_state.jsonl"

    print(f"[full] run_dir={run_dir}")

    # 1) Export all 95588
    sys.argv = [
        "export_imessage_sender.py",
        "--sender",
        "95588",
        "--out",
        str(raw_all),
    ]
    export_main()

    alerts = validate_export(str(raw_all))
    if alerts:
        alerts_path = run_dir / "export_alerts.jsonl"
        alerts_path.write_text(
            "\n".join(__import__("json").dumps(a, ensure_ascii=False) for a in alerts) + "\n",
            encoding="utf-8",
        )
        print(f"[full] export anomalies found; saved {len(alerts)} alerts to {alerts_path}. Aborting.", file=sys.stderr)
        return 2

    # 2) Classify + AI -> Firefly JSONL
    sys.argv = [
        "pipeline_95588_classify_with_ai.py",
        "--in",
        str(raw_all),
        "--rules",
        "rules/icbc_95588_rules.json",
        "--firefly-out",
        str(firefly_all),
        "--audit-dir",
        str(audit_dir),
    ]
    pipe_rc = int(pipe_main() or 0)
    if pipe_rc != 0:
        print(f"[full] pipeline failed rc={pipe_rc}; abort before push.", file=sys.stderr)
        return pipe_rc

    # 3) Push (Strategy C): local state skip (per-run) + server duplicate hash protection (default)
    sys.argv = [
        "push_firefly_jsonl.py",
        "--in",
        str(firefly_all),
        "--state",
        str(push_state),
        "--bootstrap-assets",
        "--skip-using-state",
    ]
    push_main()
    return 0


def replay_run() -> int:
    from scripts.push_firefly_jsonl import main as push_main  # type: ignore

    runs_dir = REPO_ROOT / "exports" / "runs"
    runs = _list_runs(runs_dir)
    if not runs:
        print("No runs found under exports/runs/")
        return 1

    print("Available run folders (newest first):")
    for i, p in enumerate(runs, start=1):
        print(f"{i}) {p.name}")
    print("0) 返回上级")
    print("")
    choice = _safe_input("Select a run number to replay: ").strip()
    if not choice or choice == "0":
        print("Back.")
        return 0
    try:
        idx = int(choice)
    except ValueError:
        print("Invalid number.")
        return 1
    if idx < 1 or idx > len(runs):
        print("Out of range.")
        return 1

    run_dir = runs[idx - 1]
    # Prefer delta file if present, else full.
    firefly_file = None
    for cand in ("firefly_delta.jsonl", "firefly_all.jsonl"):
        p = run_dir / cand
        if p.exists():
            firefly_file = p
            break
    if firefly_file is None:
        print(f"No firefly jsonl found in {run_dir}")
        return 1

    push_state = run_dir / "push_state.jsonl"
    print(f"[replay] run_dir={run_dir}")
    print(f"[replay] in={firefly_file}")
    sys.argv = [
        "push_firefly_jsonl.py",
        "--in",
        str(firefly_file),
        "--state",
        str(push_state),
        "--bootstrap-assets",
        "--skip-using-state",
    ]
    push_main()
    return 0


def main() -> int:
    _load_env()
    _print_header()
    print("1) 增量更新（更新水位）")
    print("2) 全量导出+解析+推送（不更新水位）")
    print("3) 选择某次增量/全量目录重新推送（不更新水位）")
    print("0) 退出")
    print("")
    choice = _safe_input("请选择功能编号: ").strip()

    if choice == "1":
        return run_incremental()
    if choice == "2":
        return run_full()
    if choice == "3":
        return replay_run()
    if choice == "0" or choice == "":
        return 0

    print("未知选项。")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
