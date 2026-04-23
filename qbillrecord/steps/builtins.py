from __future__ import annotations

from typing import Any

from qbillrecord.registry import REGISTRY
from qbillrecord.steps.state_rowid import RowidWatermarkState
from qbillrecord.steps.source_imessage import IMessageSqliteSource
from qbillrecord.steps.transform_icbc95588 import Icbc95588RulesAiTransform
from qbillrecord.steps.sink_firefly import FireflyApiSink


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
