from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from qbillrecord.pipeline.errors import ConfigError
from qbillrecord.steps.base import StateStore


class RowidWatermarkState(StateStore):
    type_id = "rowid_watermark"

    def __init__(self, cfg: dict[str, Any]) -> None:
        super().__init__(cfg)
        self.path = Path(str(cfg.get("path") or "exports/95588_state.json"))

    def load(self) -> dict[str, Any]:
        if not self.path.exists():
            return {"last_rowid": 0}
        try:
            obj = json.loads(self.path.read_text(encoding="utf-8"))
        except Exception as e:
            raise ConfigError(f"Failed to read state file: {self.path}: {e}") from e
        if not isinstance(obj, dict):
            return {"last_rowid": 0}
        try:
            obj["last_rowid"] = int(obj.get("last_rowid") or 0)
        except Exception:
            obj["last_rowid"] = 0
        return obj

    def save(self, state: Any) -> None:
        last_rowid = 0
        if isinstance(state, dict):
            try:
                last_rowid = int(state.get("last_rowid") or 0)
            except Exception:
                last_rowid = 0
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text(json.dumps({"last_rowid": last_rowid}, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
