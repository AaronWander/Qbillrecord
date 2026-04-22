from __future__ import annotations

import json
import os
import sqlite3
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Iterator

try:
    import Foundation  # type: ignore
except Exception as exc:  # pragma: no cover
    Foundation = None  # type: ignore[assignment]
    _FOUNDATION_IMPORT_ERROR = exc
else:  # pragma: no cover
    _FOUNDATION_IMPORT_ERROR = None


APPLE_EPOCH_UNIX = 978307200  # 2001-01-01T00:00:00Z


@dataclass(frozen=True)
class RawMessage:
    rowid: int
    date_local: str | None
    sender: str | None
    content: str | None
    cache_has_attachments: int
    text_len: int | None
    attributedBody_len: int | None

    def to_json(self) -> dict:
        return {
            "rowid": self.rowid,
            "date_local": self.date_local,
            "sender": self.sender,
            "content": self.content,
            "cache_has_attachments": self.cache_has_attachments,
            "text_len": self.text_len,
            "attributedBody_len": self.attributedBody_len,
        }


def apple_ns_to_local_str(apple_ns: int | None) -> str | None:
    if apple_ns is None:
        return None
    unix_ts = apple_ns / 1_000_000_000 + APPLE_EPOCH_UNIX
    return datetime.fromtimestamp(unix_ts).astimezone().strftime("%Y-%m-%d %H:%M:%S")


def _ensure_foundation() -> None:
    if Foundation is None:  # pragma: no cover
        raise RuntimeError(
            "Missing PyObjC Foundation bindings. Run with macOS system Python or install PyObjC."
        ) from _FOUNDATION_IMPORT_ERROR


def decode_attributed_body(blob: bytes | None) -> str | None:
    if not blob:
        return None
    _ensure_foundation()

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


def iter_sender_messages(
    *,
    db_path: str,
    sender_like: str,
    since_rowid: int = 0,
    limit: int = 0,
    keep_newlines: bool = False,
) -> Iterator[RawMessage]:
    """
    Export iMessage rows for a sender to an iterator of normalized RawMessage.
    """
    db_path = os.path.expanduser(db_path)

    limit_sql = "" if limit <= 0 else "LIMIT ?"
    params: list[object] = [sender_like]
    where_extra = ""
    order_by = "m.date DESC"
    if since_rowid and since_rowid > 0:
        where_extra = "AND m.ROWID > ?"
        params.append(int(since_rowid))
        order_by = "m.ROWID ASC"
    if limit > 0:
        params.append(limit)

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

    replace_newlines = not keep_newlines
    conn = open_db(db_path)
    try:
        cur = conn.execute(sql, params)
        for r in cur:
            text = r["text"]
            text_norm = normalize_text(str(text), replace_newlines) if text is not None else None

            blob = r["attributed_body"]
            blob_bytes = bytes(blob) if blob is not None else None
            decoded = decode_attributed_body(blob_bytes)
            decoded_norm = normalize_text(decoded, replace_newlines)

            content = text_norm if text_norm else decoded_norm

            yield RawMessage(
                rowid=int(r["rowid"]),
                date_local=apple_ns_to_local_str(int(r["apple_date"]) if r["apple_date"] is not None else None),
                sender=str(r["sender"]) if r["sender"] is not None else None,
                content=content,
                cache_has_attachments=int(r["cache_has_attachments"]) if r["cache_has_attachments"] is not None else 0,
                text_len=len(text_norm) if text_norm is not None else None,
                attributedBody_len=len(blob_bytes) if blob_bytes is not None else None,
            )
    finally:
        conn.close()


def write_jsonl(messages: Iterator[RawMessage], out_path: str | Path) -> int:
    out = Path(out_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    count = 0
    with out.open("w", encoding="utf-8") as f:
        for msg in messages:
            f.write(json.dumps(msg.to_json(), ensure_ascii=False) + "\n")
            count += 1
    return count

