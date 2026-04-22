from __future__ import annotations

import json
from typing import Any, Iterator


def iter_jsonl(path: str) -> Iterator[tuple[int, dict[str, Any]]]:
    with open(path, "r", encoding="utf-8") as f:
        for line_no, line in enumerate(f, start=1):
            line = line.strip()
            if not line:
                continue
            obj = json.loads(line)
            if isinstance(obj, dict):
                yield line_no, obj


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
        empty = is_empty_content(obj)
        attributed = has_attributed_blob(obj)
        attach = has_attachments(obj)

        if empty and (not attributed) and (not attach):
            alerts.append(summarize(obj, "no_content_no_attributed_no_attachments"))
            continue

        if attach and empty and (not attributed):
            alerts.append(summarize(obj, "attachments_only_no_text"))
            continue
    return alerts

