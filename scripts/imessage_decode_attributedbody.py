#!/usr/bin/env python3
"""
Decode iMessage `message.attributedBody` blobs from ~/Library/Messages/chat.db.

Why:
Some messages have `message.text` = NULL but still contain visible text stored
inside the archived rich-text `attributedBody`.

Usage examples:
  python3 scripts/imessage_decode_attributedbody.py --rowid 6354
  python3 scripts/imessage_decode_attributedbody.py --rowid 6354 6353 6348
  python3 scripts/imessage_decode_attributedbody.py --sender 95588 --only-null-text --limit 20
"""

from __future__ import annotations

import argparse
import os
import sqlite3
import sys
from dataclasses import dataclass
from datetime import datetime, timezone

try:
    import Foundation  # type: ignore
except Exception as exc:  # pragma: no cover
    raise SystemExit(
        "Missing PyObjC Foundation bindings. Run this script with the system Python "
        "or ensure PyObjC is installed."
    ) from exc


APPLE_EPOCH_UNIX = 978307200  # 2001-01-01T00:00:00Z


@dataclass(frozen=True)
class MessageRow:
    rowid: int
    apple_date: int | None
    sender: str | None
    text: str | None
    attributed_body: bytes | None


def apple_nsdate_to_local_str(apple_ns: int | None) -> str:
    if apple_ns is None:
        return "unknown_date"
    unix_ts = apple_ns / 1_000_000_000 + APPLE_EPOCH_UNIX
    dt = datetime.fromtimestamp(unix_ts).astimezone()
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def decode_attributed_body(blob: bytes | None) -> str | None:
    if not blob:
        return None

    data = Foundation.NSData.dataWithBytes_length_(blob, len(blob))

    # Try the modern API first.
    try:
        obj, err = Foundation.NSKeyedUnarchiver.unarchivedObjectOfClass_fromData_error_(
            Foundation.NSAttributedString, data, None
        )
        if err is None and obj is not None:
            return str(obj.string())
    except Exception:
        pass

    # Fallback: generic unarchive.
    try:
        obj, err = Foundation.NSKeyedUnarchiver.unarchiveTopLevelObjectWithData_error_(data, None)
        if err is None and obj is not None:
            # Often it's an NSAttributedString
            if hasattr(obj, "string"):
                return str(obj.string())
            return str(obj)
    except Exception:
        pass

    # Fallback for older "typedstream" archives (common in Messages DB).
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


def fetch_by_rowids(conn: sqlite3.Connection, rowids: list[int]) -> list[MessageRow]:
    placeholders = ",".join("?" for _ in rowids)
    sql = f"""
      SELECT
        m.ROWID AS rowid,
        m.date AS apple_date,
        h.id AS sender,
        m.text AS text,
        m.attributedBody AS attributed_body
      FROM message m
      LEFT JOIN handle h ON m.handle_id = h.ROWID
      WHERE m.ROWID IN ({placeholders})
      ORDER BY m.date DESC
    """
    rows = conn.execute(sql, rowids).fetchall()
    return [
        MessageRow(
            rowid=int(r["rowid"]),
            apple_date=int(r["apple_date"]) if r["apple_date"] is not None else None,
            sender=str(r["sender"]) if r["sender"] is not None else None,
            text=str(r["text"]) if r["text"] is not None else None,
            attributed_body=bytes(r["attributed_body"]) if r["attributed_body"] is not None else None,
        )
        for r in rows
    ]


def fetch_by_sender(
    conn: sqlite3.Connection, sender_like: str, only_null_text: bool, limit: int
) -> list[MessageRow]:
    extra = "AND m.text IS NULL" if only_null_text else ""
    sql = f"""
      SELECT
        m.ROWID AS rowid,
        m.date AS apple_date,
        h.id AS sender,
        m.text AS text,
        m.attributedBody AS attributed_body
      FROM message m
      JOIN handle h ON m.handle_id = h.ROWID
      WHERE h.id LIKE ?
        {extra}
      ORDER BY m.date DESC
      LIMIT ?
    """
    rows = conn.execute(sql, (f"%{sender_like}%", limit)).fetchall()
    return [
        MessageRow(
            rowid=int(r["rowid"]),
            apple_date=int(r["apple_date"]) if r["apple_date"] is not None else None,
            sender=str(r["sender"]) if r["sender"] is not None else None,
            text=str(r["text"]) if r["text"] is not None else None,
            attributed_body=bytes(r["attributed_body"]) if r["attributed_body"] is not None else None,
        )
        for r in rows
    ]


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--db",
        default=os.path.expanduser("~/Library/Messages/chat.db"),
        help="Path to chat.db (default: ~/Library/Messages/chat.db)",
    )
    parser.add_argument("--rowid", nargs="*", type=int, help="Message ROWID(s) to decode")
    parser.add_argument("--sender", help="Filter by sender (handle.id LIKE %sender%)")
    parser.add_argument("--only-null-text", action="store_true", help="Only rows where message.text IS NULL")
    parser.add_argument("--limit", type=int, default=20, help="Limit for --sender queries")
    args = parser.parse_args()

    if not args.rowid and not args.sender:
        parser.error("Provide --rowid or --sender")

    conn = open_db(args.db)
    try:
        if args.rowid:
            messages = fetch_by_rowids(conn, args.rowid)
        else:
            messages = fetch_by_sender(conn, args.sender, args.only_null_text, args.limit)
    finally:
        conn.close()

    for m in messages:
        decoded = decode_attributed_body(m.attributed_body)
        local_time = apple_nsdate_to_local_str(m.apple_date)
        print(f"ROWID={m.rowid} | {local_time} | sender={m.sender or ''}")
        if m.text is not None:
            print(f"text: {m.text}")
        else:
            print("text: <NULL>")
        print(f"decoded_attributedBody: {decoded if decoded is not None else '<decode_failed>'}")
        print("-" * 80)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
