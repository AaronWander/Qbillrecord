#!/usr/bin/env python3
"""
Export iMessage messages for a sender (handle.id) to JSONL.

Features:
- Includes message.text (may be NULL)
- Decodes message.attributedBody (common when text is NULL)
- Emits a unified `content` field: text if present, else decoded attributedBody
- Designed for downstream analysis / rule building

Example:
  python3 scripts/export_imessage_sender.py --sender 95588 --out exports/95588_all.jsonl
"""

from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys
from datetime import datetime

try:
    import Foundation  # type: ignore
except Exception as exc:  # pragma: no cover
    raise SystemExit(
        "Missing PyObjC Foundation bindings. Run with macOS system Python or install PyObjC."
    ) from exc


APPLE_EPOCH_UNIX = 978307200  # 2001-01-01T00:00:00Z


def apple_ns_to_local_str(apple_ns: int | None) -> str | None:
    if apple_ns is None:
        return None
    unix_ts = apple_ns / 1_000_000_000 + APPLE_EPOCH_UNIX
    return datetime.fromtimestamp(unix_ts).astimezone().strftime("%Y-%m-%d %H:%M:%S")


def decode_attributed_body(blob: bytes | None) -> str | None:
    if not blob:
        return None

    data = Foundation.NSData.dataWithBytes_length_(blob, len(blob))

    # Modern API.
    try:
        obj, err = Foundation.NSKeyedUnarchiver.unarchivedObjectOfClass_fromData_error_(
            Foundation.NSAttributedString, data, None
        )
        if err is None and obj is not None:
            return str(obj.string())
    except Exception:
        pass

    # Generic keyed archive.
    try:
        obj, err = Foundation.NSKeyedUnarchiver.unarchiveTopLevelObjectWithData_error_(data, None)
        if err is None and obj is not None:
            if hasattr(obj, "string"):
                return str(obj.string())
            return str(obj)
    except Exception:
        pass

    # Typedstream fallback (common in Messages DB).
    try:
        if hasattr(Foundation, "NSUnarchiver"):
            obj = Foundation.NSUnarchiver.unarchiveObjectWithData_(data)
            if obj is not None:
                if hasattr(obj, "string"):
                    return str(obj.string())
                return str(obj)
    except Exception:
        pass

    return None


def open_db(path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    return conn


def normalize_text(text: str | None, replace_newlines: bool) -> str | None:
    if text is None:
        return None
    if replace_newlines:
        return text.replace("\r", " ").replace("\n", " ")
    return text


def ensure_parent_dir(path: str) -> None:
    parent = os.path.dirname(os.path.abspath(path))
    if parent:
        os.makedirs(parent, exist_ok=True)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--db",
        default=os.path.expanduser("~/Library/Messages/chat.db"),
        help="Path to chat.db (default: ~/Library/Messages/chat.db)",
    )
    parser.add_argument("--sender", required=True, help="Filter by handle.id LIKE %sender%")
    parser.add_argument("--out", required=True, help="Output JSONL file path")
    parser.add_argument("--limit", type=int, default=0, help="Limit rows (0 = no limit)")
    parser.add_argument(
        "--since-rowid",
        type=int,
        default=0,
        help="Only export messages with message.ROWID > since_rowid (for incremental exports).",
    )
    parser.add_argument(
        "--keep-newlines",
        action="store_true",
        help="Keep newlines in text fields (default replaces \\n/\\r with spaces)",
    )
    args = parser.parse_args()

    limit_sql = "" if args.limit <= 0 else "LIMIT ?"
    params: list[object] = [f"%{args.sender}%"]
    where_extra = ""
    order_by = "m.date DESC"
    if args.since_rowid and args.since_rowid > 0:
        where_extra = "AND m.ROWID > ?"
        params.append(int(args.since_rowid))
        # For incremental exports, keep stable increasing order for easy appends.
        order_by = "m.ROWID ASC"
    if args.limit > 0:
        params.append(args.limit)

    sql = f"""
      SELECT
        m.ROWID AS rowid,
        m.date AS apple_date,
        h.id AS sender,
        m.text AS text,
        m.attributedBody AS attributed_body,
        m.cache_has_attachments AS cache_has_attachments
      FROM message m
      JOIN handle h ON m.handle_id = h.ROWID
      WHERE h.id LIKE ?
      {where_extra}
      ORDER BY {order_by}
      {limit_sql}
    """

    ensure_parent_dir(args.out)
    conn = open_db(args.db)
    replace_newlines = not args.keep_newlines

    count = 0
    try:
        cur = conn.execute(sql, params)
        with open(args.out, "w", encoding="utf-8") as f:
            for r in cur:
                text = r["text"]
                text_norm = normalize_text(str(text), replace_newlines) if text is not None else None

                blob = r["attributed_body"]
                blob_bytes = bytes(blob) if blob is not None else None
                decoded = decode_attributed_body(blob_bytes)
                decoded_norm = normalize_text(decoded, replace_newlines)

                content = text_norm if text_norm else decoded_norm

                obj = {
                    "rowid": int(r["rowid"]),
                    "date_local": apple_ns_to_local_str(int(r["apple_date"]) if r["apple_date"] is not None else None),
                    "sender": str(r["sender"]) if r["sender"] is not None else None,
                    "text": text_norm,
                    "decoded_attributedBody": decoded_norm,
                    "content": content,
                    "cache_has_attachments": int(r["cache_has_attachments"]) if r["cache_has_attachments"] is not None else 0,
                    "text_len": len(text_norm) if text_norm is not None else None,
                    "attributedBody_len": len(blob_bytes) if blob_bytes is not None else None,
                }
                f.write(json.dumps(obj, ensure_ascii=False) + "\n")
                count += 1
    finally:
        conn.close()

    print(f"Wrote {count} messages to {args.out}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
