#!/usr/bin/env python3
"""
Audit: any message that *should* be parsed (contains 余额+支出/收入/转账) but still fails parsing.

This is meant to enforce: "all balance-change messages must be parsed".

Usage:
  python3 scripts/audit_force_parse_unparsed.py --in exports/95588_all.jsonl --rules rules/icbc_95588_rules.json
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import scripts.classify_95588_jsonl_to_md as cls  # type: ignore


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="in_path", required=True)
    ap.add_argument("--rules", default="rules/icbc_95588_rules.json")
    args = ap.parse_args()

    rules = cls.load_rules(args.rules)
    ignore_keywords = rules.get("ignore_if_text_matches_any", [])
    force_regexes = rules.get("force_parse_if_text_matches_any_regex", [])
    patterns = cls.compile_patterns(rules.get("transaction_patterns", []))

    forced_total = 0
    forced_unparsed = []

    with open(args.in_path, "r", encoding="utf-8") as f:
        for line in f:
            obj = json.loads(line)
            dt = obj.get("date_local") or "unknown_date"
            content = (obj.get("content") or "").replace("\r", " ").replace("\n", " ").strip()
            if not content:
                continue

            forced = cls.should_force_parse(content, force_regexes)
            if not forced:
                continue

            forced_total += 1
            txn = cls.parse_txn("unknown卡", dt, content, patterns)
            if txn is None:
                forced_unparsed.append((obj.get("rowid"), dt, content))

    print(json.dumps({"forced_total": forced_total, "forced_unparsed": len(forced_unparsed)}, ensure_ascii=False))
    for rowid, dt, content in forced_unparsed[:200]:
        print("---")
        print(f"rowid={rowid} date_local={dt}")
        print(content)

    return 0 if not forced_unparsed else 2


if __name__ == "__main__":
    raise SystemExit(main())
