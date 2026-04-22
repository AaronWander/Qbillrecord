from __future__ import annotations

from typing import Any

from jizhang.registry import REGISTRY
from jizhang.steps.state_rowid import RowidWatermarkState
from jizhang.steps.source_imessage import IMessageSqliteSource
from jizhang.steps.transform_icbc95588 import Icbc95588RulesAiTransform
from jizhang.steps.sink_firefly import FireflyApiSink


def _register() -> None:
    REGISTRY.register(
        kind="state",
        type_id="rowid_watermark",
        factory=lambda cfg: RowidWatermarkState(cfg),
        description="ROWID watermark state (exports/95588_state.json)",
    )
    REGISTRY.register(
        kind="source",
        type_id="imessage_sqlite",
        factory=lambda cfg: IMessageSqliteSource(cfg),
        description="macOS Messages chat.db source (sender filter)",
    )
    REGISTRY.register(
        kind="transform",
        type_id="icbc95588_rules_ai",
        factory=lambda cfg: Icbc95588RulesAiTransform(cfg),
        description="ICBC 95588 parse+rules+optional AI+Firefly export",
    )
    REGISTRY.register(
        kind="sink",
        type_id="firefly_api",
        factory=lambda cfg: FireflyApiSink(cfg),
        description="Push Firefly JSONL to Firefly III API",
    )


_register()
