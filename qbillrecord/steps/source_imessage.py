from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

from qbillrecord.ingest.imessage import iter_sender_messages, write_jsonl
from qbillrecord.ingest.validate import validate
from qbillrecord.steps.base import RunContext, Source


class IMessageSqliteSource(Source):
    type_id = "imessage_sqlite"

    def export(self, *, ctx: RunContext, state: Any) -> dict[str, Any]:
        sender = str(self.cfg.get("sender") or "95588")
        db_path = os.path.expanduser(str(self.cfg.get("db_path") or "~/Library/Messages/chat.db"))
        since_rowid = 0
        if isinstance(state, dict):
            try:
                since_rowid = int(state.get("last_rowid") or 0)
            except Exception:
                since_rowid = 0

        count = write_jsonl(
            iter_sender_messages(
                db_path=db_path,
                sender_like=f"%{sender}%",
                since_rowid=since_rowid,
            ),
            ctx.raw_path,
        )

        # Compute max rowid
        rowid_max = 0
        if ctx.raw_path.exists():
            for line in ctx.raw_path.read_text(encoding="utf-8").splitlines():
                if not line.strip():
                    continue
                try:
                    obj = json.loads(line)
                except Exception:
                    continue
                rid = obj.get("rowid")
                if isinstance(rid, int) and rid > rowid_max:
                    rowid_max = rid

        alerts = validate(str(ctx.raw_path))
        if alerts:
            alerts_path = ctx.run_dir / "export_alerts.jsonl"
            alerts_path.write_text("\n".join(json.dumps(a, ensure_ascii=False) for a in alerts) + "\n", encoding="utf-8")
            return {"count": count, "rowid_max": rowid_max, "alerts": len(alerts), "alerts_path": str(alerts_path)}

        return {"count": count, "rowid_max": rowid_max, "alerts": 0}
