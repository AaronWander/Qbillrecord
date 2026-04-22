#!/usr/bin/env python3
"""
Validate exported iMessage JSONL for anomalies that can break downstream parsing/pushing.

Anomalies (conservative):
- content is missing/empty AND no attributedBody blob AND no attachments
- attachments-only messages (cache_has_attachments==1) are only flagged if they also have no content
  and no attributedBody blob.

Outputs:
- prints a short list of anomalies to stderr
- optionally writes alerts JSONL for archiving/debugging

Exit code:
- 0: no anomalies
- 2: anomalies found
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any


def iter_jsonl(path: str):
    with open(path, "r", encoding="utf-8") as f:
        for line_no, line in enumerate(f, start=1):
            line = line.strip()
            if not line:
                continue
            yield line_no, json.loads(line)


def is_empty_content(obj: dict[str, Any]) -> bool:
    content = obj.get("content")
    if content is None:
        return True
    s = str(content).strip()
    return s == ""


def has_attachments(obj: dict[str, Any]) -> bool:
    try:
        return int(obj.get("cache_has_attachments") or 0) == 1
    except Exception:
        return False


def has_attributed_blob(obj: dict[str, Any]) -> bool:
    v = obj.get("attributedBody_len")
    try:
        return v is not None and int(v) > 0
    except Exception:
        return False


def summarize(obj: dict[str, Any], reason: str) -> dict[str, Any]:
    content = obj.get("content") or ""
    content_s = str(content).replace("\r", " ").replace("\n", " ").strip()
    return {
        "reason": reason,
        "rowid": obj.get("rowid"),
        "date_local": obj.get("date_local"),
        "sender": obj.get("sender"),
        "cache_has_attachments": obj.get("cache_has_attachments"),
        "text_len": obj.get("text_len"),
        "attributedBody_len": obj.get("attributedBody_len"),
        "content_preview": content_s[:160],
    }


def validate(path: str) -> list[dict[str, Any]]:
    alerts: list[dict[str, Any]] = []
    for _line_no, obj in iter_jsonl(path):
        if not isinstance(obj, dict):
            continue
        empty = is_empty_content(obj)
        attributed = has_attributed_blob(obj)
        attach = has_attachments(obj)

        # Fail only when we truly have nothing usable:
        # - no content text
        # - no attributedBody blob to decode
        # - no attachments hint
        if empty and (not attributed) and (not attach):
            alerts.append(summarize(obj, "no_content_no_attributed_no_attachments"))
            continue

        # If it's an attachments-only message with no content and no attributed blob,
        # it is not parseable in our current pipeline.
        if attach and empty and (not attributed):
            alerts.append(summarize(obj, "attachments_only_no_text"))
            continue
    return alerts


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="in_path", required=True, help="Export JSONL path")
    ap.add_argument("--out", default="", help="Optional alerts JSONL path")
    ap.add_argument("--max-print", type=int, default=30, help="Max alerts to print to stderr")
    args = ap.parse_args()

    alerts = validate(args.in_path)
    if args.out:
        Path(args.out).parent.mkdir(parents=True, exist_ok=True)
        with open(args.out, "w", encoding="utf-8") as f:
            for a in alerts:
                f.write(json.dumps(a, ensure_ascii=False) + "\n")

    if not alerts:
        return 0

    print(f"[validate] anomalies={len(alerts)} in {args.in_path}", file=sys.stderr)
    for a in alerts[: max(args.max_print, 0)]:
        print(
            f"- reason={a.get('reason')} rowid={a.get('rowid')} date={a.get('date_local')} preview={a.get('content_preview')}",
            file=sys.stderr,
        )
    if len(alerts) > args.max_print:
        print(f"- ... {len(alerts) - args.max_print} more", file=sys.stderr)
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
