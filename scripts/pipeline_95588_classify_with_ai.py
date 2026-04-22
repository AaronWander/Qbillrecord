#!/usr/bin/env python3
"""
End-to-end pipeline:
1) Read exports/95588_all.jsonl
2) Parse + classify using rules/icbc_95588_rules.json
3) Collect "待分类" items (income + expense) and save to a file
4) Call DeepSeek to suggest categories for those items (batched, with progress)
5) Apply AI results to re-classify; unresolved remain in "待分类"
6) Export a single Firefly III JSONL file for import/posting
7) (Optional) Render final Markdown report for review

Audit outputs (optional, only if --audit-dir is set):
- <audit-dir>/deepseek_requests.jsonl
- <audit-dir>/deepseek_responses.jsonl
- <audit-dir>/95588_ai_candidates.jsonl

Usage:
  python3 scripts/pipeline_95588_classify_with_ai.py \
    --in exports/95588_all.jsonl \
    --rules rules/icbc_95588_rules.json \
    --firefly-out exports/firefly_transactions_ai.jsonl
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
import threading
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any

# Allow running as `python3 scripts/...py` by ensuring repo root is on sys.path.
REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.ai_classify_from_classified import (
    deepseek_chat_completion,
    extract_assistant_text,
    load_dotenv,
    load_rules_taxonomy,
    normalize_info_remove_parens,
    sha256_id,
    try_parse_json,
)
from scripts.classify_95588_jsonl_to_md import (
    RawMsg,
    classify,
    compile_patterns,
    ensure_parent,
    extract_card_label,
    load_rules,
    parse_txn,
    read_jsonl,
    should_force_parse,
    should_ignore,
)

import scripts.export_firefly_transactions_from_jsonl as firefly_export  # type: ignore


@dataclass(frozen=True)
class ClassifiedRow:
    txn: Any  # ParsedTxn from classify_95588_jsonl_to_md
    category: str


def env_flag(name: str, default: bool = False) -> bool:
    raw = str(os.environ.get(name, "")).strip().lower()
    if not raw:
        return default
    return raw in {"1", "true", "yes", "on"}


def _looks_like_placeholder_api_key(api_key: str) -> bool:
    k = (api_key or "").strip().lower()
    if not k:
        return True
    if k in {"your_api_key_here", "xxx", "changeme", "todo"}:
        return True
    return "your_" in k and "key" in k


def txn_to_md_line(txn) -> str:
    return f"- 来源:{txn.source} 日期:{txn.date_local} 类型:{txn.direction_cn} 信息:{txn.short_info} 费用:{txn.amount}元"


def get_candidate_key_and_info(txn) -> tuple[str, str]:
    """
    Candidate key should be stable for dedupe and should match the DeepSeek request:
    - Use normalized info without any parentheses.
    - Include direction in request_id, not in merchant_key.
    """
    info = normalize_info_remove_parens(str(txn.short_info or "").strip())
    if not info:
        info = normalize_info_remove_parens(str(txn.raw_bracket or "").strip())
    if not info:
        info = "unknown"
    return info, info


def render_report(
    out_path: str,
    in_path: str,
    rules_path: str,
    rules: dict[str, Any],
    grouped: dict[str, list[Any]],
    ignored_msgs: list[RawMsg],
    unparsed_msgs: list[RawMsg],
    stats: dict[str, Any],
    max_per_category: int = 0,
) -> None:
    if not out_path:
        return
    taxonomy: list[str] = []
    tax = rules.get("category_taxonomy", {})
    if isinstance(tax, dict):
        taxonomy.extend(tax.get("income") or [])
        taxonomy.extend(tax.get("expense") or [])

    seen = set()
    ordered_categories: list[str] = []
    for c in taxonomy:
        if c not in seen:
            ordered_categories.append(c)
            seen.add(c)
    for c in sorted(grouped.keys()):
        if c not in seen:
            ordered_categories.append(c)
            seen.add(c)

    ensure_parent(out_path)
    with open(out_path, "w", encoding="utf-8") as f:
        f.write("# 95588 流水分类汇总（rules + AI）\n\n")
        f.write(f"- 输入：`{in_path}`\n")
        f.write(f"- 规则：`{rules_path}`\n")
        f.write(f"- 总消息（有内容）：{stats.get('total_with_content', 0)} 条\n")
        f.write(f"- 解析成功：{stats.get('parsed', 0)} 条\n")
        f.write(f"- 规则待分类：{stats.get('unknown_before_ai', 0)} 条\n")
        f.write(f"- AI 参与：{stats.get('ai_candidates', 0)} 条（去重后 merchant_key 数）\n")
        f.write(f"- AI 应用：{stats.get('ai_applied', 0)} 条（按 merchant_key 命中）\n")
        f.write(f"- 最终待分类：{stats.get('unknown_after_ai', 0)} 条\n")
        f.write(f"- 忽略：{len(ignored_msgs)} 条（验证码/安全提醒等，仍会在文末列出）\n")
        f.write(f"- 未解析：{len(unparsed_msgs)} 条（不符合余额变动模板，仍会在文末列出）\n\n")

        for cat in ordered_categories:
            items = grouped.get(cat, [])
            items.sort(key=lambda x: x.date_local, reverse=True)
            f.write(f"## {cat}\n\n")
            if not items:
                f.write("- （本期无记录）\n\n")
                continue

            out_items = items if max_per_category <= 0 else items[: max_per_category]
            for t in out_items:
                f.write(txn_to_md_line(t) + "\n")
            if max_per_category > 0 and len(items) > len(out_items):
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


def load_ai_mapping(
    responses_path: str, category_enum: list[str]
) -> dict[tuple[str, str], str]:
    """
    Return mapping: (direction, merchant_key) -> category
    Only accept categories within enum.
    """
    allowed = set(category_enum)
    mapping: dict[tuple[str, str], str] = {}
    p = Path(responses_path)
    if not p.exists():
        return mapping
    for line in p.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        try:
            obj = json.loads(line)
        except Exception:
            continue
        parsed = obj.get("parsed")
        if not isinstance(parsed, dict):
            continue
        results = parsed.get("results") or []
        if not isinstance(results, list):
            continue
        for r in results:
            if not isinstance(r, dict):
                continue
            merchant_key = str(r.get("merchant_key") or "").strip()
            category = str(r.get("category") or "").strip()
            request_id = str(r.get("request_id") or "").strip()
            # direction isn't guaranteed in result; reconstruct from request_id if needed is hard.
            # We instead store direction in merchant_key uniqueness by requiring request_id match later,
            # but for simplicity in this pipeline we will only use merchant_key mapping (direction from request_id prefix).
            # However our request_id is sha256(direction|merchant_key), so we cannot decode direction.
            # => require direction in result; if missing, skip.
            direction = str(r.get("direction") or "").strip()
            if not direction:
                # Back-compat: some models might omit; skip to be safe.
                continue
            if not merchant_key or not category:
                continue
            if category not in allowed:
                continue
            mapping[(direction, merchant_key)] = category
    return mapping


def load_ai_mapping_from_records(
    response_records: list[dict[str, Any]], category_enum: list[str]
) -> dict[tuple[str, str], str]:
    allowed = set(category_enum)
    mapping: dict[tuple[str, str], str] = {}
    for obj in response_records:
        parsed = obj.get("parsed")
        if not isinstance(parsed, dict):
            continue
        results = parsed.get("results") or []
        if not isinstance(results, list):
            continue
        for r in results:
            if not isinstance(r, dict):
                continue
            direction = str(r.get("direction") or "").strip()
            merchant_key = str(r.get("merchant_key") or "").strip()
            category = str(r.get("category") or "").strip()
            if not direction or not merchant_key or not category:
                continue
            if category not in allowed:
                continue
            mapping[(direction, merchant_key)] = category
    return mapping


def validate_ai_response_records(
    response_records: list[dict[str, Any]],
    expected_items: list[dict[str, Any]],
    category_enum: list[str],
) -> tuple[bool, str]:
    allowed = set(category_enum)
    expected_by_request_id = {
        str(item["request_id"]): (str(item["direction"]), str(item["merchant_key"]))
        for item in expected_items
    }
    seen_request_ids: set[str] = set()

    for record in response_records:
        if record.get("error"):
            return False, f"ai_request_failed:{record.get('error')}"
        parsed = record.get("parsed")
        if not isinstance(parsed, dict):
            return False, "ai_response_missing_parsed_json"
        results = parsed.get("results")
        if not isinstance(results, list) or not results:
            return False, "ai_response_missing_results"
        for result in results:
            if not isinstance(result, dict):
                return False, "ai_result_item_invalid"
            request_id = str(result.get("request_id") or "").strip()
            direction = str(result.get("direction") or "").strip()
            merchant_key = str(result.get("merchant_key") or "").strip()
            category = str(result.get("category") or "").strip()
            if not request_id or not direction or not merchant_key or not category:
                return False, "ai_result_missing_required_fields"
            if request_id not in expected_by_request_id:
                continue
            expected_direction, expected_merchant_key = expected_by_request_id[request_id]
            if direction != expected_direction or merchant_key != expected_merchant_key:
                return False, "ai_result_identity_mismatch"
            if category not in allowed:
                return False, "ai_result_invalid_category"
            seen_request_ids.add(request_id)

    missing = sorted(set(expected_by_request_id) - seen_request_ids)
    if missing:
        return False, f"ai_result_incomplete:{len(missing)}"
    return True, ""


def _start_heartbeat(label: str, heartbeat_s: float) -> tuple[threading.Event, threading.Thread]:
    stop = threading.Event()

    def _run() -> None:
        started = time.time()
        i = 0
        while not stop.wait(timeout=heartbeat_s):
            i += 1
            elapsed = int(time.time() - started)
            print(f"{label} waiting... elapsed={elapsed}s tick={i}", file=sys.stderr, flush=True)

    t = threading.Thread(target=_run, daemon=True)
    t.start()
    return stop, t


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="in_path", default="exports/95588_all.jsonl")
    ap.add_argument("--rules", default="rules/icbc_95588_rules.json")
    ap.add_argument(
        "--firefly-out",
        default="exports/firefly_transactions_ai.jsonl",
        help="Final output file (JSONL). Each line is a Firefly III POST /v1/transactions payload.",
    )
    ap.add_argument(
        "--out-md",
        default="",
        help="Optional: also write a grouped Markdown report for review (empty disables).",
    )
    ap.add_argument("--max-per-category", type=int, default=0, help="Only applies to --out-md")
    ap.add_argument(
        "--audit-dir",
        default="",
        help="Optional: write audit files (candidates/requests/responses) into this directory (empty disables).",
    )
    ap.add_argument("--batch-size", type=int, default=20)
    ap.add_argument("--timeout-s", type=int, default=180)
    ap.add_argument("--heartbeat-s", type=float, default=5.0, help="Print a heartbeat while waiting for API response")
    ap.add_argument("--sleep-ms", type=int, default=300)
    ap.add_argument("--limit-ai", type=int, default=0, help="Limit number of unique merchant candidates sent to AI (0=no limit)")
    ap.add_argument("--no-ai", action="store_true", help="Do not call AI; only rule-based classification")
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
    load_dotenv(REPO_ROOT / ".env")
    ai_classify_enabled = env_flag("AI_CLASSIFY_ENABLED", default=False)

    rules = load_rules(args.rules)
    ignore_keywords = rules.get("ignore_if_text_matches_any", [])
    force_regexes = rules.get("force_parse_if_text_matches_any_regex", [])
    compiled_patterns = compile_patterns(rules.get("transaction_patterns", []))

    grouped: dict[str, list[Any]] = defaultdict(list)
    ignored_msgs: list[RawMsg] = []
    unparsed_msgs: list[RawMsg] = []
    rows: list[ClassifiedRow] = []

    stats: dict[str, Any] = {
        "total_with_content": 0,
        "parsed": 0,
        "unknown_before_ai": 0,
        "ai_candidates": 0,
        "ai_applied": 0,
        "unknown_after_ai": 0,
    }

    # 1) Parse + rule-classify
    print("[1/4] parse+classify (rules)...", file=sys.stderr, flush=True)
    for obj in read_jsonl(args.in_path):
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

        stats["parsed"] += 1
        cat = classify(txn, rules)
        if cat.endswith("其他（待分类）"):
            stats["unknown_before_ai"] += 1
        rows.append(ClassifiedRow(txn=txn, category=cat))

    # 2) Collect candidates
    print("[2/4] collect 待分类 candidates...", file=sys.stderr, flush=True)
    candidates: dict[tuple[str, str], dict[str, Any]] = {}
    for row in rows:
        txn = row.txn
        cat = row.category
        if cat != "其他（待分类）":
            continue
        merchant_key, info = get_candidate_key_and_info(txn)
        direction = str(txn.direction_cn)
        rid = sha256_id(f"{direction}|{merchant_key}")
        key = (direction, merchant_key)
        if key in candidates:
            continue
        candidates[key] = {
            "request_id": rid,
            "direction": direction,
            "merchant_key": merchant_key,
            "info": info,
        }

    cand_items = list(candidates.values())
    cand_items.sort(key=lambda x: (x["direction"], x["merchant_key"]))
    if args.limit_ai and args.limit_ai > 0:
        cand_items = cand_items[: args.limit_ai]
    stats["ai_candidates"] = len(cand_items)

    audit_dir = (args.audit_dir or "").strip()
    candidates_out = ""
    requests_out = ""
    responses_out = ""
    if audit_dir:
        Path(audit_dir).mkdir(parents=True, exist_ok=True)
        candidates_out = str(Path(audit_dir) / "95588_ai_candidates.jsonl")
        requests_out = str(Path(audit_dir) / "deepseek_requests.jsonl")
        responses_out = str(Path(audit_dir) / "deepseek_responses.jsonl")

        ensure_parent(candidates_out)
        with open(candidates_out, "w", encoding="utf-8") as f:
            for it in cand_items:
                f.write(json.dumps(it, ensure_ascii=False) + "\n")

    # 3) AI classify (optional)
    mapping: dict[tuple[str, str], str] = {}
    category_enum = load_rules_taxonomy(args.rules)
    should_call_ai = ai_classify_enabled and not args.no_ai and bool(cand_items)
    stats["ai_enabled"] = ai_classify_enabled
    stats["ai_called"] = should_call_ai

    if should_call_ai:
        print("[3/4] call DeepSeek for 待分类...", file=sys.stderr, flush=True)
        api_key = os.environ.get("DEEPSEEK_API_KEY", "").strip()
        if _looks_like_placeholder_api_key(api_key):
            raise SystemExit(
                "DEEPSEEK_API_KEY looks missing/placeholder. Set it in .env or export it before running.\n"
                "Example: DEEPSEEK_API_KEY=sk-... (DeepSeek key)\n"
                "Tip: if you just copied `.env.example`, replace `your_api_key_here` with a real key."
            )
        base_url = os.environ.get("DEEPSEEK_BASE_URL", "https://api.deepseek.com").strip()
        model = os.environ.get("DEEPSEEK_MODEL", "deepseek-chat").strip()

        system_prompt = (
            "你是记账分类助手。你必须只输出 JSON，不能输出任何额外文本。"
            "你将收到一个 items 数组，每个 item 都需要输出一个对应的 result。"
            "每个 result 必须包含：request_id、merchant_key、direction、category、confidence、reason、needs_review。"
            "category 必须从以下枚举中选择："
            + json.dumps(category_enum, ensure_ascii=False)
            + "。"
            "无法确定时：category 返回 '其他（待分类）'，"
            "confidence 设为 <=0.5 且 needs_review=true。"
            "最终输出 JSON 结构必须是：{ \"results\": [ ... ] }。"
        )

        batch_size = args.batch_size if args.batch_size and args.batch_size > 0 else len(cand_items)
        batches = [cand_items[i : i + batch_size] for i in range(0, len(cand_items), batch_size)]
        total_batches = len(batches)

        if requests_out:
            ensure_parent(requests_out)
            with open(requests_out, "w", encoding="utf-8") as f_req:
                for idx, batch in enumerate(batches):
                    f_req.write(
                        json.dumps(
                            {
                                "batch_id": idx + 1,
                                "batch_size": len(batch),
                                "items": batch,
                            },
                            ensure_ascii=False,
                        )
                        + "\n"
                    )

        # Call API
        response_records: list[dict[str, Any]] = []
        for idx, batch in enumerate(batches):
            batch_no = idx + 1
            label = f"[{batch_no}/{total_batches}]"
            print(
                f"{label} start: batch_size={len(batch)} timeout_s={args.timeout_s} model={model}",
                file=sys.stderr,
                flush=True,
            )
            started = time.time()
            user_json = {"task": "classify_batch", "language": "zh-CN", "items": batch}
            payload_bytes = len(json.dumps(user_json, ensure_ascii=False).encode("utf-8"))
            print(f"{label} request_bytes={payload_bytes}", file=sys.stderr, flush=True)
            record: dict[str, Any] = {
                "batch_id": batch_no,
                "batch_size": len(batch),
                "request_ids": [x["request_id"] for x in batch],
                "model": model,
                "base_url": base_url,
                "ts": int(time.time()),
            }
            hb_stop: threading.Event | None = None
            hb_thread: threading.Thread | None = None
            try:
                if args.heartbeat_s and args.heartbeat_s > 0:
                    hb_stop, hb_thread = _start_heartbeat(label, float(args.heartbeat_s))
                resp = deepseek_chat_completion(
                    base_url=base_url,
                    api_key=api_key,
                    model=model,
                    system_prompt=system_prompt,
                    user_json=user_json,
                    timeout_s=max(int(args.timeout_s), 1),
                )
                assistant_text = extract_assistant_text(resp)
                parsed = try_parse_json(assistant_text)
                record["raw_response"] = assistant_text
                record["parsed"] = parsed
                record["latency_ms"] = int((time.time() - started) * 1000)
                if parsed is None:
                    record["error"] = "invalid_json_response"
                    print(
                        f"{label} done: ERROR invalid_json_response latency_ms={record['latency_ms']}",
                        file=sys.stderr,
                        flush=True,
                    )
                else:
                    results_n = len((parsed or {}).get("results") or []) if isinstance(parsed, dict) else 0
                    print(
                        f"{label} done: ok latency_ms={record['latency_ms']} results={results_n}",
                        file=sys.stderr,
                        flush=True,
                    )
            except Exception as e:
                record["error"] = f"exception:{type(e).__name__}"
                record["message"] = str(e)
                record["latency_ms"] = int((time.time() - started) * 1000)
                print(
                    f"{label} done: ERROR {record['error']} latency_ms={record['latency_ms']} message={record.get('message','')}",
                    file=sys.stderr,
                    flush=True,
                )
                msg = str(e)
                if "HTTP 401" in msg or " 401" in msg:
                    print(
                        f"{label} hint: DeepSeek returned 401. Re-check `DEEPSEEK_API_KEY` (expired/wrong key) and `DEEPSEEK_BASE_URL`.",
                        file=sys.stderr,
                        flush=True,
                    )
            finally:
                if hb_stop is not None:
                    hb_stop.set()
                if hb_thread is not None:
                    hb_thread.join(timeout=1.0)
            response_records.append(record)
            time.sleep(max(args.sleep_ms, 0) / 1000.0)

        if responses_out:
            ensure_parent(responses_out)
            with open(responses_out, "w", encoding="utf-8") as f_resp:
                for rec in response_records:
                    f_resp.write(json.dumps(rec, ensure_ascii=False) + "\n")
                f_resp.flush()

        mapping = load_ai_mapping_from_records(response_records, category_enum)
        ok, reason = validate_ai_response_records(response_records, cand_items, category_enum)
        if not ok:
            print(f"[3/4] AI classification failed validation: {reason}", file=sys.stderr, flush=True)
            return 3
    elif ai_classify_enabled and cand_items and args.no_ai:
        print("[3/4] AI classification disabled by --no-ai", file=sys.stderr, flush=True)
    elif ai_classify_enabled and not cand_items:
        print("[3/4] no 待分类 candidates; skip AI", file=sys.stderr, flush=True)
    else:
        print("[3/4] AI classification disabled by AI_CLASSIFY_ENABLED", file=sys.stderr, flush=True)

    # 4) Apply AI + export
    print("[4/4] apply AI + export firefly...", file=sys.stderr, flush=True)
    for row in rows:
        txn = row.txn
        cat = row.category
        if cat != "其他（待分类）":
            grouped[cat].append(txn)
            continue
        merchant_key, _info = get_candidate_key_and_info(txn)
        key = (str(txn.direction_cn), merchant_key)
        new_cat = mapping.get(key)
        if new_cat and new_cat != cat:
            stats["ai_applied"] += 1
            cat = new_cat
        if cat == "其他（待分类）":
            stats["unknown_after_ai"] += 1
        grouped[cat].append(txn)

    render_report(
        out_path=(args.out_md or "").strip(),
        in_path=args.in_path,
        rules_path=args.rules,
        rules=rules,
        grouped=grouped,
        ignored_msgs=ignored_msgs,
        unparsed_msgs=unparsed_msgs,
        stats=stats,
        max_per_category=args.max_per_category,
    )

    # Export a single file for Firefly import/posting (JSONL).
    ensure_parent(args.firefly_out)
    written = 0
    with open(args.firefly_out, "w", encoding="utf-8") as f_out:
        for category_full, txns in grouped.items():
            for txn in txns:
                tags: list[str] = []
                # Category is stored as category_name; tags keep biz/channel metadata.
                if getattr(txn, "biz_type", ""):
                    tags.append(txn.biz_type)
                if getattr(txn, "channel", ""):
                    tags.append(txn.channel)
                seen = set()
                tags = [t for t in tags if t and not (t in seen or seen.add(t))]

                last4 = txn.card_last4 or "unknown"
                asset_account = f"{args.asset_prefix}({last4})"

                firefly_type = "deposit" if txn.direction_cn == "收入" else "withdrawal"
                amount_str = txn.amount
                date_iso = firefly_export.iso8601_with_tz(txn.date_local, args.tz) if txn.date_local else None

                merchant_or_counterparty = firefly_export.parse_bracket_for_display(txn)
                if txn.direction_cn == "收入":
                    source_name = txn.counterparty or merchant_or_counterparty
                    destination_name = asset_account
                else:
                    source_name = asset_account
                    destination_name = merchant_or_counterparty

                description = merchant_or_counterparty
                notes = txn.full_content

                ext_id = firefly_export.stable_external_id(
                    [
                        "icbc95588",
                        str(last4),
                        str(txn.date_local),
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
                            "category_name": category_full,
                            "tags": tags,
                            "notes": notes,
                            "external_id": ext_id,
                        }
                    ]
                }
                if args.apply_rules:
                    payload["apply_rules"] = True

                f_out.write(json.dumps(payload, ensure_ascii=False) + "\n")
                written += 1

    stats["firefly_written"] = written
    stats["firefly_out"] = args.firefly_out

    print(json.dumps(stats, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
