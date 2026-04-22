from __future__ import annotations

from typing import Any

from jizhang.sink.firefly import push_firefly_jsonl
from jizhang.steps.base import RunContext, Sink


class FireflyApiSink(Sink):
    type_id = "firefly_api"

    def push(self, *, ctx: RunContext) -> dict[str, Any]:
        summary = push_firefly_jsonl(
            in_path=ctx.firefly_path,
            state_path=ctx.push_state_path,
            base_url=str(self.cfg.get("base_url") or ""),
            token=str(self.cfg.get("token") or ""),
            timeout_s=int(self.cfg.get("timeout_s") or 30),
            retries=int(self.cfg.get("retries") or 3),
            retry_sleep_s=float(self.cfg.get("retry_sleep_s") or 1.5),
            bootstrap_assets=bool(self.cfg.get("bootstrap_assets", False)),
            skip_using_state=True,
            no_error_if_duplicate=bool(self.cfg.get("no_error_if_duplicate", False)),
            dry_run=bool(self.cfg.get("dry_run", False)),
            limit=int(self.cfg.get("limit") or 0),
        )
        return summary.__dict__

