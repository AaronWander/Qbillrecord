from __future__ import annotations

from typing import Any

from qbillrecord.steps.base import RunContext, Transform
from qbillrecord.transform.icbc95588_pipeline import run_pipeline


class Icbc95588RulesAiTransform(Transform):
    type_id = "icbc95588_rules_ai"

    def run(self, *, ctx: RunContext) -> int:
        rules_path = str(self.cfg.get("rules_path") or "rules/icbc_95588_rules.json")
        exporter = self.cfg.get("exporter") or {}
        ai_cfg = self.cfg.get("ai") or {}
        ai_enabled = bool(ai_cfg.get("enabled", False))

        tz = "+08:00"
        asset_prefix = "工商银行"
        apply_rules = False
        if isinstance(exporter, dict):
            tz = str(exporter.get("tz") or tz)
            asset_prefix = str(exporter.get("asset_prefix") or asset_prefix)
            apply_rules = bool(exporter.get("apply_rules", False))

        return int(
            run_pipeline(
                in_path=str(ctx.raw_path),
                rules_path=rules_path,
                firefly_out=str(ctx.firefly_path),
                audit_dir=str(ctx.audit_dir),
                tz=tz,
                asset_prefix=asset_prefix,
                no_ai=(not ai_enabled),
                apply_rules=apply_rules,
            )
            or 0
        )
