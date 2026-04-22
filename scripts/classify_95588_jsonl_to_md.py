#!/usr/bin/env python3
"""
Classify ICBC 95588 iMessage exports (JSONL) into categories using rules/*.json,
and render a compact Markdown report grouped by category.

Input JSONL should be produced by:
  scripts/export_imessage_sender.py

Output keeps only: source, date, short info, amount.
"""

from __future__ import annotations

import argparse
import json
import os
import re
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class ParsedTxn:
    source: str  # card last4, e.g. "5108卡"
    date_local: str
    direction_cn: str  # 支出/收入
    amount: str
    card_last4: str | None
    month: str
    day: str
    time_hm: str
    biz_type: str
    channel: str
    merchant: str | None
    counterparty: str | None
    raw_bracket: str
    short_info: str
    full_content: str

@dataclass(frozen=True)
class RawMsg:
    source: str
    date_local: str
    short_line: str
    full_content: str

def extract_card_label(content: str) -> str:
    m = re.search(r"尾号(\\d{4})卡", content)
    if m:
        return f"{m.group(1)}卡"
    return "unknown卡"


def load_rules(path: str) -> dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def should_ignore(content: str, ignore_keywords: list[str]) -> bool:
    return any(k and k in content for k in ignore_keywords)

def should_force_parse(content: str, force_regexes: list[str] | None) -> bool:
    """
    Force-parse if message looks like a balance-change SMS.
    We use regexes to avoid false positives from help/marketing messages that mention “余额/收入/支出”.
    """
    if not force_regexes:
        return False
    for rx in force_regexes:
        try:
            if re.search(rx, content):
                return True
        except re.error:
            continue
    return False


def compile_patterns(patterns: list[dict[str, Any]]) -> list[tuple[str, re.Pattern[str], dict[str, str]]]:
    compiled: list[tuple[str, re.Pattern[str], dict[str, str]]] = []
    for p in patterns:
        name = p.get("name", "pattern")
        regex = p["regex"]
        # Our rules use the Firefly/PCRE-style `(?<name>...)` named groups.
        # Python uses `(?P<name>...)`.
        regex = re.sub(r"\(\?\<([a-zA-Z_][a-zA-Z0-9_]*)\>", r"(?P<\1>", regex)
        direction_map = p.get("direction_map", {})
        compiled.append((name, re.compile(regex), direction_map))
    return compiled


def parse_txn(source: str, date_local: str, content: str, compiled_patterns) -> ParsedTxn | None:
    for _name, rx, direction_map in compiled_patterns:
        m = rx.search(content)
        if not m:
            continue
        gd = m.groupdict()
        direction = gd.get("direction") or ""
        direction_cn = direction
        direction_en = direction_map.get(direction, "")

        # Required bits from our patterns.
        amount = (gd.get("amount") or "").replace(",", "")
        month = gd.get("month") or ""
        day = gd.get("day") or ""
        time_hm = gd.get("time") or ""
        card_last4 = gd.get("card_last4") or None

        biz_type = (gd.get("biz_type") or "").strip()
        channel = (gd.get("channel") or "").strip()
        merchant = (gd.get("merchant") or None)

        # Catchall pattern provides `bracket` instead of split groups.
        bracket = (gd.get("bracket") or "").strip()
        if bracket and (not biz_type and not channel and merchant is None):
            # Heuristic split:
            # Examples:
            #   "消费财付通-山东智慧行"
            #   "跨行汇款"
            if "-" in bracket:
                left, right = bracket.split("-", 1)
                merchant = right.strip() or None
            else:
                left = bracket
            # Try to split left into biz_type + channel by known channel tokens.
            known_channels = ["财付通", "支付宝", "拼多多支付", "抖音支付", "网银在线", "京东", "闲鱼"]
            biz_type = left
            channel = ""
            for kc in known_channels:
                if kc in left:
                    idx = left.find(kc)
                    biz_type = left[:idx].strip() or left
                    channel = left[idx:].strip()
                    break
        if merchant is not None:
            merchant = merchant.strip()
            if merchant == "":
                merchant = None

        counterparty = None
        # Common in income/transfer messages:
        # "... 对方户名：xxx，对方账户尾号：0163。"
        m_cp = re.search(r"对方户名[:：]([^，,。]+)", content)
        if m_cp:
            counterparty = m_cp.group(1).strip()
            if counterparty == "":
                counterparty = None

        inside = f"{biz_type}{channel}"
        if merchant:
            inside += f"-{merchant}"
        raw_bracket = inside

        # Example requested:
        # "5108卡 3月30日19:47 支出 (消费网银在线-xxx) 80.80元"
        card_label = f"{card_last4}卡" if card_last4 else "unknown卡"
        # For reporting, keep `info` minimal (no repeated card/date/amount),
        # but include counterparty for salary / inbound transfers.
        if direction_cn == "收入" and counterparty:
            short_info = f"({biz_type}) 对方户名:{counterparty}"
        else:
            short_info = f"({raw_bracket})"

        return ParsedTxn(
            source=card_label,
            date_local=date_local,
            direction_cn=direction_cn,
            amount=amount,
            card_last4=card_last4,
            month=month,
            day=day,
            time_hm=time_hm,
            biz_type=biz_type,
            channel=channel,
            merchant=merchant,
            counterparty=counterparty,
            raw_bracket=raw_bracket,
            short_info=short_info,
            full_content=content,
        )
    return None


def rule_matches(txn: ParsedTxn, rule: dict[str, Any]) -> bool:
    # All specified constraints must pass.
    merchant = txn.merchant or ""
    channel = txn.channel or ""
    biz_type = txn.biz_type or ""
    full_content = txn.full_content or ""

    if "if_biz_type_in" in rule:
        allowed = rule["if_biz_type_in"] or []
        if biz_type not in allowed:
            return False

    if "if_channel_matches_any" in rule:
        needles = rule["if_channel_matches_any"] or []
        if not any(n and n in channel for n in needles):
            return False

    if "if_merchant_matches_any" in rule:
        needles = rule["if_merchant_matches_any"] or []
        # Match against merchant first; if merchant missing, match against raw bracket too.
        haystack = merchant if merchant else txn.raw_bracket
        if not any(n and n in haystack for n in needles):
            return False

    if "if_text_matches_any" in rule:
        needles = rule["if_text_matches_any"] or []
        if not any(n and n in full_content for n in needles):
            return False

    return True


def classify(txn: ParsedTxn, rules: dict[str, Any]) -> str:
    for rule in rules.get("category_rules", []):
        if rule_matches(txn, rule):
            return rule["set_category"]

    defaults = rules.get("defaults", {})
    return defaults.get("unknown_category") or "其他（待分类）"


def read_jsonl(path: str):
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            yield json.loads(line)


def ensure_parent(path: str) -> None:
    Path(path).parent.mkdir(parents=True, exist_ok=True)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--rules", default="rules/icbc_95588_rules.json", help="Rules JSON path")
    ap.add_argument("--in", dest="in_path", required=True, help="Input JSONL (exported messages)")
    ap.add_argument("--out", required=True, help="Output Markdown path")
    ap.add_argument("--max-per-category", type=int, default=0, help="0 = no limit")
    args = ap.parse_args()

    rules = load_rules(args.rules)
    ignore_keywords = rules.get("ignore_if_text_matches_any", [])
    force_regexes = rules.get("force_parse_if_text_matches_any_regex", [])
    compiled_patterns = compile_patterns(rules.get("transaction_patterns", []))

    grouped: dict[str, list[ParsedTxn]] = defaultdict(list)
    ignored_msgs: list[RawMsg] = []
    unparsed_msgs: list[RawMsg] = []
    stats = {"total_with_content": 0}

    for obj in read_jsonl(args.in_path):
        # "source" in report means the bank card (尾号xxxx卡), not the SMS sender 95588.
        date_local = obj.get("date_local") or "unknown_date"
        content = obj.get("content") or ""
        if not isinstance(content, str):
            content = str(content)
        content = content.replace("\r", " ").replace("\n", " ").strip()
        if not content:
            continue
        stats["total_with_content"] += 1

        if should_ignore(content, ignore_keywords) and not should_force_parse(content, force_regexes):
            ignored_msgs.append(
                RawMsg(
                    source=extract_card_label(content),
                    date_local=date_local,
                    short_line=content[:120],
                    full_content=content,
                )
            )
            continue

        txn = parse_txn("unknown卡", date_local, content, compiled_patterns)
        if txn is None:
            unparsed_msgs.append(
                RawMsg(
                    source=extract_card_label(content),
                    date_local=date_local,
                    short_line=content[:120],
                    full_content=content,
                )
            )
            continue

        category = classify(txn, rules)
        grouped[category].append(txn)

    # Sort categories: show taxonomy order first, then any extra.
    taxonomy: list[str] = []
    tax = rules.get("category_taxonomy", {})
    if isinstance(tax, dict):
        taxonomy.extend(tax.get("all") or [])
        # Back-compat: older rules used income/expense split
        if not taxonomy:
            taxonomy.extend(tax.get("income") or [])
            taxonomy.extend(tax.get("expense") or [])

    seen = set()
    ordered_categories: list[str] = []
    # Always include taxonomy categories even if empty.
    for c in taxonomy:
        if c not in seen:
            ordered_categories.append(c)
            seen.add(c)
    for c in sorted(grouped.keys()):
        if c not in seen:
            ordered_categories.append(c)
            seen.add(c)

    ensure_parent(args.out)
    with open(args.out, "w", encoding="utf-8") as f:
        f.write("# 95588 流水分类汇总（按 rules）\n\n")
        f.write(f"- 输入：`{args.in_path}`\n")
        f.write(f"- 规则：`{args.rules}`\n")
        f.write(f"- 总消息（有内容）：{stats['total_with_content']} 条\n")
        f.write(f"- 忽略：{len(ignored_msgs)} 条（验证码/安全提醒等，仍会在文末列出）\n")
        f.write(f"- 未解析：{len(unparsed_msgs)} 条（不符合余额变动模板，仍会在文末列出）\n\n")

        for cat in ordered_categories:
            items = grouped.get(cat, [])
            # Newest first (already in desc order mostly), but keep stable sort by date_local string.
            items.sort(key=lambda x: x.date_local, reverse=True)

            f.write(f"## {cat}\n\n")
            if not items:
                f.write("- （本期无记录）\n\n")
                continue

            out_items = items if args.max_per_category <= 0 else items[: args.max_per_category]
            for t in out_items:
                f.write(
                    f"- 来源:{t.source} 日期:{t.date_local} 类型:{t.direction_cn} 信息:{t.short_info} 费用:{t.amount}元\n"
                )
            if args.max_per_category > 0 and len(items) > len(out_items):
                f.write(f"- ... 省略 {len(items) - len(out_items)} 条\n")
            f.write("\n")

        if unparsed_msgs:
            f.write("## 未解析（待完善规则）\n\n")
            for m in sorted(unparsed_msgs, key=lambda x: x.date_local, reverse=True):
                f.write(f"- 来源:{m.source} 日期:{m.date_local} 信息:{m.short_line}\n")
            f.write("\n")

        if ignored_msgs:
            f.write("## 忽略（非记账短信）\n\n")
            for m in sorted(ignored_msgs, key=lambda x: x.date_local, reverse=True):
                f.write(f"- 来源:{m.source} 日期:{m.date_local} 信息:{m.short_line}\n")
            f.write("\n")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
