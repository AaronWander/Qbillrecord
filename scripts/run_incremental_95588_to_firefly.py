#!/usr/bin/env python3
"""
Incremental end-to-end run:

1) Export new iMessage rows for sender 95588 from chat.db (ROWID > last_rowid)
2) Classify + (optional) DeepSeek refine for unknowns
3) Generate Firefly TransactionStore JSONL for ONLY the new rows
4) Push the delta JSONL into Firefly III (local state skip + server-side duplicate hash protection)
5) Archive run artifacts for replay/debugging
6) Update state with new last_rowid

State file:
  exports/95588_state.json

Run archive folder (created per run when there is delta):
  exports/runs/<timestamp>/
    raw_delta.jsonl
    firefly_delta.jsonl
    push_state.jsonl
    (optional) deepseek_requests.jsonl / deepseek_responses.jsonl / 95588_ai_candidates.jsonl

Env (.env supported, repo root):
  DEEPSEEK_API_KEY / DEEPSEEK_BASE_URL / DEEPSEEK_MODEL
  FIREFLY_BASE_URL / FIREFLY_TOKEN
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.export_imessage_sender import main as export_main  # type: ignore
from scripts.pipeline_95588_classify_with_ai import main as pipeline_main  # type: ignore
from scripts.push_firefly_jsonl import main as push_main  # type: ignore
from scripts.validate_imessage_export import validate as validate_export  # type: ignore


def load_state(path: Path) -> dict:
    if not path.exists():
        return {"last_rowid": 0}
    return json.loads(path.read_text(encoding="utf-8"))


def save_state(path: Path, state: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(state, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def max_rowid_in_jsonl(path: Path) -> int:
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


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--sender", default="95588")
    ap.add_argument("--db", default=os.path.expanduser("~/Library/Messages/chat.db"))
    ap.add_argument("--rules", default="rules/icbc_95588_rules.json")
    ap.add_argument("--state", default="exports/95588_state.json")
    ap.add_argument("--runs-dir", default="exports/runs", help="Where to archive per-run artifacts")
    ap.add_argument("--no-ai", action="store_true")
    args, unknown = ap.parse_known_args()

    state_path = Path(args.state)
    state = load_state(state_path)
    last_rowid = int(state.get("last_rowid") or 0)

    # 1) Export delta
    print(f"[inc] last_rowid={last_rowid}", file=sys.stderr, flush=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    run_dir = Path(args.runs_dir) / ts
    run_dir.mkdir(parents=True, exist_ok=True)
    export_out = str(run_dir / "raw_delta.jsonl")
    firefly_out = str(run_dir / "firefly_delta.jsonl")
    audit_dir = str(run_dir / "ai_audit")
    push_state = str(run_dir / "push_state.jsonl")

    sys.argv = [
        "export_imessage_sender.py",
        "--db",
        args.db,
        "--sender",
        args.sender,
        "--out",
        export_out,
        "--since-rowid",
        str(last_rowid),
    ]
    export_main()

    delta_path = Path(export_out)
    new_max = max_rowid_in_jsonl(delta_path)
    if new_max <= last_rowid:
        print("[inc] no new messages; nothing to do.", file=sys.stderr, flush=True)
        # Cleanup empty run dir
        try:
            delta_path.unlink(missing_ok=True)  # type: ignore[arg-type]
            run_dir.rmdir()
        except Exception:
            pass
        return 0
    print(f"[inc] exported delta rowid_max={new_max}", file=sys.stderr, flush=True)

    # Validate export before doing any parsing/pushing.
    alerts = validate_export(export_out)
    if alerts:
        alerts_path = run_dir / "export_alerts.jsonl"
        alerts_path.write_text(
            "\n".join(json.dumps(a, ensure_ascii=False) for a in alerts) + "\n",
            encoding="utf-8",
        )
        print(
            f"[inc] export anomalies found; saved {len(alerts)} alerts to {alerts_path}. Aborting before parse/push.",
            file=sys.stderr,
            flush=True,
        )
        return 2

    # 2) Build firefly JSONL for delta only
    sys.argv = [
        "pipeline_95588_classify_with_ai.py",
        "--in",
        export_out,
        "--rules",
        args.rules,
        "--firefly-out",
        firefly_out,
        "--audit-dir",
        audit_dir,
    ]
    if args.no_ai:
        sys.argv.append("--no-ai")
    pipeline_rc = int(pipeline_main() or 0)
    if pipeline_rc != 0:
        print(
            f"[inc] pipeline failed rc={pipeline_rc}; abort before push/state update.",
            file=sys.stderr,
            flush=True,
        )
        return pipeline_rc

    # 3) Push delta into Firefly
    sys.argv = [
        "push_firefly_jsonl.py",
        "--in",
        firefly_out,
        "--state",
        push_state,
        "--bootstrap-assets",
        "--skip-using-state",
    ]
    push_main()

    # 4) Update state
    state["last_rowid"] = new_max
    save_state(state_path, state)
    print(f"[inc] state updated last_rowid={new_max}", file=sys.stderr, flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
