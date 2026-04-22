#!/usr/bin/env python3
"""
Export ICBC 95588 messages (JSONL) to Firefly III TransactionStore payloads.

User requirements:
- Merchant / counterparty uses "口径B": create destination/source accounts by name.
- Category uses unified categories (no "收入/支出" split). Direction is derived from transaction type.

Input:
  exports/95588_all.jsonl (from scripts/export_imessage_sender.py)
Rules:
  rules/icbc_95588_rules.json

Output:
  JSONL where each line is a Firefly III `POST /v1/transactions` payload.

Example:
  python3 scripts/export_firefly_transactions_from_jsonl.py \\
    --in exports/95588_all.jsonl \\
    --out exports/firefly_transactions.jsonl
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

# Allow importing sibling scripts/ modules when executed from repo root.
REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import scripts.classify_95588_jsonl_to_md as classifier  # type: ignore


def ensure_parent(path: str) -> None:
    Path(path).parent.mkdir(parents=True, exist_ok=True)


def iso8601_with_tz(dt_local: str, tz_offset: str) -> str:
    # dt_local is like "2026-03-30 19:47:37"
    dt = datetime.strptime(dt_local, "%Y-%m-%d %H:%M:%S")
    # We keep seconds; Firefly accepts full ISO8601.
    return dt.strftime("%Y-%m-%dT%H:%M:%S") + tz_offset


def stable_external_id(parts: list[str]) -> str:
    raw = "|".join(parts).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def parse_bracket_for_display(txn: classifier.ParsedTxn) -> str:
    # Prefer merchant if present, otherwise fall back to full raw_bracket.
    if txn.merchant:
        return txn.merchant
    return txn.raw_bracket or "unknown"


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--rules", default="rules/icbc_95588_rules.json")
    ap.add_argument("--in", dest="in_path", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--tz", default="+08:00", help="Timezone offset for Firefly date, default +08:00")
    ap.add_argument(
        "--asset-prefix",
        default="工商银行",
        help="Asset account name prefix. Final name becomes '<prefix>(<last4>)'.",
    )
    ap.add_argument(
        "--apply-rules",
        action="store_true",
        help="Set apply_rules=true in Firefly payload (optional).",
    )
    args = ap.parse_args()

    rules = classifier.load_rules(args.rules)
    ignore_keywords = rules.get("ignore_if_text_matches_any", [])
    force_regexes = rules.get("force_parse_if_text_matches_any_regex", [])
    compiled_patterns = classifier.compile_patterns(rules.get("transaction_patterns", []))

    ensure_parent(args.out)
    count = 0
    skipped = {"ignored": 0, "unparsed": 0, "no_content": 0}

    with open(args.in_path, "r", encoding="utf-8") as f_in, open(args.out, "w", encoding="utf-8") as f_out:
        for line in f_in:
            line = line.strip()
            if not line:
                continue
            obj = json.loads(line)

            dt_local = obj.get("date_local") or ""
            content = obj.get("content") or ""
            if not isinstance(content, str):
                content = str(content)
            content = content.replace("\r", " ").replace("\n", " ").strip()
            if not content:
                skipped["no_content"] += 1
                continue

            if classifier.should_ignore(content, ignore_keywords) and not classifier.should_force_parse(content, force_regexes):
                skipped["ignored"] += 1
                continue

            txn = classifier.parse_txn("unknown卡", dt_local or "unknown_date", content, compiled_patterns)
            if txn is None:
                skipped["unparsed"] += 1
                continue

            category_name = classifier.classify(txn, rules)

            # Tags: store channel/biz metadata (category is stored in category_name).
            tags: list[str] = []

            if txn.biz_type:
                tags.append(txn.biz_type)
            if txn.channel:
                tags.append(txn.channel)
            # Deduplicate while preserving order
            seen = set()
            tags = [t for t in tags if t and not (t in seen or seen.add(t))]

            # Firefly mapping (口径B):
            # - Expense (withdrawal): source = asset (card), destination = merchant (expense account)
            # - Income (deposit): destination = asset (card), source = counterparty/merchant (revenue account)
            last4 = txn.card_last4 or "unknown"
            asset_account = f"{args.asset_prefix}({last4})"

            firefly_type = "deposit" if txn.direction_cn == "收入" else "withdrawal"
            amount_str = txn.amount  # already normalized (no commas)
            date_iso = iso8601_with_tz(dt_local, args.tz) if dt_local else None

            merchant_or_counterparty = parse_bracket_for_display(txn)
            if txn.direction_cn == "收入":
                source_name = txn.counterparty or merchant_or_counterparty
                destination_name = asset_account
            else:
                source_name = asset_account
                destination_name = merchant_or_counterparty

            description = merchant_or_counterparty
            notes = content

            ext_id = stable_external_id(
                [
                    "icbc95588",
                    str(last4),
                    str(dt_local),
                    str(firefly_type),
                    str(amount_str),
                    str(destination_name),
                    str(source_name),
                ]
            )

            payload: dict[str, Any] = {
                "transactions": [
                    {
                        "type": firefly_type,
                        "date": date_iso,
                        "amount": amount_str,
                        "description": description,
                        "source_name": source_name,
                        "destination_name": destination_name,
                        "category_name": category_name,
                        "tags": tags,
                        "notes": notes,
                        "external_id": ext_id,
                    }
                ]
            }
            if args.apply_rules:
                payload["apply_rules"] = True

            f_out.write(json.dumps(payload, ensure_ascii=False) + "\n")
            count += 1

    print(
        json.dumps(
            {"written": count, "skipped": skipped, "out": args.out},
            ensure_ascii=False,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
