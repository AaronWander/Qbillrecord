#!/usr/bin/env python3
"""
Compute monthly net change expressions from `reports/95588_classified.md` ONLY.

Requirement (from user):
- Data source must be `95588_classified.md` (no JSONL, no DB).
- For each month, show an expression like: "1+3+4-4=4"
- Treat income as +amount, expense as -amount.
- Output final net (positive if income>expense, negative otherwise).

Usage:
  python3 scripts/monthly_net_from_classified_md.py --in reports/95588_classified.md --out reports/95588_monthly_net_from_classified.md
"""

from __future__ import annotations

import argparse
import re
from collections import defaultdict
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path


LINE_RX = re.compile(
    r"^\- 来源:(?P<src>\S+)\s+日期:(?P<dt>\d{4}-\d{2}-\d{2})\s+\d{2}:\d{2}:\d{2}\s+类型:(?P<typ>收入|支出)\s+信息:.*?费用:(?P<amt>[0-9,]+(?:\.[0-9]+)?)元"
)


def ensure_parent(path: str) -> None:
    Path(path).parent.mkdir(parents=True, exist_ok=True)


def fmt_amt_for_expr(x: Decimal) -> str:
    """
    For the expression display:
    - drop trailing zeros
    - keep up to 2 decimals as in source
    """
    x = x.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    s = format(x, "f")
    if "." in s:
        s = s.rstrip("0").rstrip(".")
    return s or "0"


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="in_path", required=True, help="Input classified md")
    ap.add_argument("--out", required=True, help="Output markdown")
    args = ap.parse_args()

    # month -> list of signed amounts (+ income, - expense) in appearance order
    month_terms: dict[str, list[Decimal]] = defaultdict(list)

    total_lines = 0
    parsed_lines = 0

    with open(args.in_path, "r", encoding="utf-8") as f:
        for line in f:
            total_lines += 1
            line = line.strip()
            m = LINE_RX.match(line)
            if not m:
                continue
            parsed_lines += 1
            ym = m.group("dt")[:7]
            typ = m.group("typ")
            amt = Decimal(m.group("amt").replace(",", ""))
            month_terms[ym].append(amt if typ == "收入" else -amt)

    months = sorted(month_terms.keys())

    ensure_parent(args.out)
    with open(args.out, "w", encoding="utf-8") as out:
        out.write("# 95588 月度净变动（仅来自 95588_classified.md）\n\n")
        out.write(f"- 输入：`{args.in_path}`\n")
        out.write(f"- 解析到的变动条目：{parsed_lines}\n\n")
        out.write("说明：收入记为 `+金额`，支出记为 `-金额`。每月给出一条计算表达式与最终净变动。\n\n")

        out.write("| 月份 | 计算过程 | 净变动 |\n")
        out.write("| --- | --- | ---: |\n")

        for ym in months:
            terms = month_terms[ym]
            if not terms:
                continue
            expr_parts: list[str] = []
            net = Decimal("0")
            for t in terms:
                net += t
                # expression requested example is like "1+3+4-4", i.e. first term no leading '+'
                if not expr_parts:
                    expr_parts.append(fmt_amt_for_expr(abs(t)) if t >= 0 else f"-{fmt_amt_for_expr(abs(t))}")
                else:
                    sign = "+" if t >= 0 else "-"
                    expr_parts.append(f"{sign}{fmt_amt_for_expr(abs(t))}")
            expr = "".join(expr_parts) + "=" + fmt_amt_for_expr(net)
            out.write(f"| {ym} | `{expr}` | {fmt_amt_for_expr(net)} |\n")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

