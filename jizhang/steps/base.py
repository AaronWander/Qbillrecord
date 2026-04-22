from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass
class RunContext:
    run_dir: Path
    audit_dir: Path
    raw_path: Path
    firefly_path: Path
    push_state_path: Path
    manifest: dict[str, Any]


class Step:
    kind: str
    type_id: str

    def __init__(self, cfg: dict[str, Any]) -> None:
        self.cfg = cfg


class StateStore(Step):
    kind = "state"

    def load(self) -> Any:  # pragma: no cover
        raise NotImplementedError

    def save(self, state: Any) -> None:  # pragma: no cover
        raise NotImplementedError


class Source(Step):
    kind = "source"

    def export(self, *, ctx: RunContext, state: Any) -> dict[str, Any]:  # pragma: no cover
        """
        Export raw messages to ctx.raw_path. Returns metadata (e.g. rowid_max, count).
        """
        raise NotImplementedError


class Transform(Step):
    kind = "transform"

    def run(self, *, ctx: RunContext) -> int:  # pragma: no cover
        """
        Transform ctx.raw_path -> ctx.firefly_path. Return rc (0 ok; 3 for AI failure).
        """
        raise NotImplementedError


class Sink(Step):
    kind = "sink"

    def push(self, *, ctx: RunContext) -> dict[str, Any]:  # pragma: no cover
        """
        Push ctx.firefly_path and write state to ctx.push_state_path. Return summary dict.
        """
        raise NotImplementedError

