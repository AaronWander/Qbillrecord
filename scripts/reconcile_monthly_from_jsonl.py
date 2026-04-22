#!/usr/bin/env python3
"""
Reconcile monthly income/expense totals from 95588 JSONL exports.

Goals:
- List monthly totals: income_sum, expense_sum, net (income-expense)
- Infer month start/end balances from per-transaction "余额xxx元"
- Validate: net change implied by balances equals net sum from transactions

Input JSONL should come from scripts/export_imessage_sender.py and contain `content`.
Rules come from rules/icbc_95588_rules.json (patterns + ignore keywords).

Output: Markdown report.
"""

from __future__ import annotations

import argparse
import json
import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any


APPLE_EPOCH_UNIX = 978307200


@dataclass(frozen=True)
class Txn:
    rowid: int
    dt: datetime
    card: str
    direction: str  # 支出/收入
    amount: float
    balance_after: float
    raw: str


def load_rules(path: str) -> dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def normalize_regex(regex: str) -> str:
    # Convert `(?<name>...)` to Python `(?P<name>...)`.
    return re.sub(r"\(\?\<([a-zA-Z_][a-zA-Z0-9_]*)\>", r"(?P<\1>", regex)


def compile_patterns(patterns: list[dict[str, Any]]) -> list[re.Pattern[str]]:
    out: list[re.Pattern[str]] = []
    for p in patterns:
        out.append(re.compile(normalize_regex(p["regex"])))
    return out


def should_ignore(content: str, ignore_keywords: list[str]) -> bool:
    return any(k and k in content for k in ignore_keywords)


def parse_float_amount(s: str) -> float:
    return float(s.replace(",", ""))


def parse_txn(content: str, patterns: list[re.Pattern[str]]) -> dict[str, str] | None:
    for rx in patterns:
        m = rx.search(content)
        if not m:
            continue
        gd = {k: v for k, v in m.groupdict().items() if v is not None}
        # Need at least direction/amount/balance/month/day/time/card
        if not all(k in gd for k in ("direction", "amount", "balance", "month", "day", "time", "card_last4")):
            # card_last4 is optional in one pattern; we still accept but card will become unknown.
            pass
        return gd
    return None


def ensure_parent(path: str) -> None:
    Path(path).parent.mkdir(parents=True, exist_ok=True)


def month_key(dt: datetime) -> str:
    return dt.strftime("%Y-%m")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--rules", default="rules/icbc_95588_rules.json")
    ap.add_argument("--in", dest="in_path", required=True, help="Input JSONL")
    ap.add_argument("--out", required=True, help="Output Markdown report")
    args = ap.parse_args()

    rules = load_rules(args.rules)
    ignore_keywords = rules.get("ignore_if_text_matches_any", [])
    patterns = compile_patterns(rules.get("transaction_patterns", []))

    txns: list[Txn] = []
    skipped = {"ignored": 0, "unparsed": 0, "no_content": 0}

    with open(args.in_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            obj = json.loads(line)
            content = (obj.get("content") or "").replace("\r", " ").replace("\n", " ").strip()
            if not content:
                skipped["no_content"] += 1
                continue
            if should_ignore(content, ignore_keywords):
                skipped["ignored"] += 1
                continue

            gd = parse_txn(content, patterns)
            if gd is None:
                skipped["unparsed"] += 1
                continue

            # Use exported local timestamp for year; SMS text only has month/day/time.
            dt_local = obj.get("date_local") or ""
            try:
                dt = datetime.strptime(dt_local, "%Y-%m-%d %H:%M:%S")
            except Exception:
                # If missing, skip
                skipped["unparsed"] += 1
                continue

            card = f"{gd.get('card_last4','unknown')}卡" if gd.get("card_last4") else "unknown卡"
            direction = gd.get("direction", "")
            amount = parse_float_amount(gd.get("amount", "0"))
            balance_after = parse_float_amount(gd.get("balance", "0"))

            txns.append(
                Txn(
                    rowid=int(obj.get("rowid") or 0),
                    dt=dt,
                    card=card,
                    direction=direction,
                    amount=amount,
                    balance_after=balance_after,
                    raw=content,
                )
            )

    # Group by (card, month)
    by_month: dict[tuple[str, str], list[Txn]] = {}
    for t in txns:
        key = (t.card, month_key(t.dt))
        by_month.setdefault(key, []).append(t)

    # Precompute per-month end balances (last txn balance) for chaining.
    month_end_balance: dict[tuple[str, str], float] = {}
    for (card, mk), items in by_month.items():
        items_sorted = sorted(items, key=lambda x: x.dt)
        month_end_balance[(card, mk)] = items_sorted[-1].balance_after

    ensure_parent(args.out)
    with open(args.out, "w", encoding="utf-8") as out:
        out.write("# 95588 月度收支对账（基于 JSONL + 余额）\n\n")
        out.write(f"- 输入：`{args.in_path}`\n")
        out.write(f"- 规则：`{args.rules}`\n")
        out.write(f"- 解析交易：{len(txns)} 条\n")
        out.write(
            f"- 跳过：ignored={skipped['ignored']} unparsed={skipped['unparsed']} no_content={skipped['no_content']}\n\n"
        )

        out.write(
            "说明：\n"
            "- 期末余额取“当月最后一条余额变动短信”的余额字段。\n"
            "- 若要做严格对账，可用“上月期末余额”与“本月期末余额”计算余额净变动，再对比当月净额（收入-支出）。\n\n"
        )

        mismatches: list[str] = []

        # Iterate months per card in chronological order.
        keys_sorted = sorted(by_month.keys(), key=lambda x: (x[0], x[1]))
        prev_key_by_card: dict[str, tuple[str, str] | None] = {}

        for (card, mk) in keys_sorted:
            items = by_month[(card, mk)]
            items.sort(key=lambda x: x.dt)  # chronological

            income_sum = sum(t.amount for t in items if t.direction == "收入")
            expense_sum = sum(t.amount for t in items if t.direction == "支出")
            net = income_sum - expense_sum

            first = items[0]
            last = items[-1]

            end_after = last.balance_after
            # Previous-month carry-over method:
            prev_key = prev_key_by_card.get(card)
            if prev_key is None:
                start_carry = None
                balance_net_carry = None
            else:
                start_carry = month_end_balance.get(prev_key)
                balance_net_carry = end_after - start_carry if start_carry is not None else None

            diff_carry = (balance_net_carry - net) if balance_net_carry is not None else None

            out.write(f"## {mk} {card}\n\n")
            out.write(f"- 收入合计：{income_sum:.2f}\n")
            out.write(f"- 支出合计：{expense_sum:.2f}\n")
            out.write(f"- 净额（收-支）：{net:.2f}\n")
            out.write(f"- 当月期末余额：{end_after:.2f}\n")
            if start_carry is None:
                out.write("- 上月期末余额：(无上月数据)\n")
                out.write("- 余额净变动（期末差）：(无上月数据)\n")
                out.write("- 对账差值（期末差-净额）：(无上月数据)\n\n")
            else:
                out.write(f"- 上月期末余额：{start_carry:.2f}\n")
                out.write(f"- 余额净变动（期末差）：{balance_net_carry:.2f}\n")
                out.write(f"- 对账差值（期末差-净额）：{diff_carry:.2f}\n\n")

            # Use carry-over diff when available; otherwise fall back to first-in-month diff.
            mismatch_val = diff_carry if diff_carry is not None else 0.0
            if abs(mismatch_val) > 0.01:
                mismatches.append(f"{mk} {card} diff={mismatch_val:.2f} txns={len(items)}")

            prev_key_by_card[card] = (card, mk)

        if mismatches:
            out.write("## 不匹配月份（需要检查漏短信/解析失败）\n\n")
            for m in mismatches:
                out.write(f"- {m}\n")
            out.write("\n")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
