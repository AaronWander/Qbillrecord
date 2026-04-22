from __future__ import annotations

from jizhang.registry import REGISTRY


def _register_placeholders() -> None:
    # Placeholder registrations for visibility while we migrate scripts into steps.
    REGISTRY.register(kind="state", type_id="rowid_watermark", factory=lambda cfg: cfg, description="ROWID watermark state")
    REGISTRY.register(kind="source", type_id="imessage_sqlite", factory=lambda cfg: cfg, description="macOS Messages chat.db source")
    REGISTRY.register(kind="parser", type_id="icbc95588_sms", factory=lambda cfg: cfg, description="ICBC 95588 SMS parser")
    REGISTRY.register(kind="classifier", type_id="rules_ai", factory=lambda cfg: cfg, description="Rule-based + optional AI classifier")
    REGISTRY.register(kind="exporter", type_id="firefly_jsonl", factory=lambda cfg: cfg, description="Export Firefly JSONL")
    REGISTRY.register(kind="sink", type_id="firefly_api", factory=lambda cfg: cfg, description="Push to Firefly III API")


_register_placeholders()

