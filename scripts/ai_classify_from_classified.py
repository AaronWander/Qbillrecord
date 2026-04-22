#!/usr/bin/env python3
"""
Batch classify "待分类" items from reports/95588_classified.md using DeepSeek API.

Per user requirements:
- Source data MUST be the classified markdown file (reports/95588_classified.md).
- Send/return payloads must be written to separate JSONL files.
- Run on all "待分类" items (income + expense), deduplicated by merchant key.

Outputs:
- exports/deepseek_requests.jsonl  (one batch request JSON per line; batched)
- exports/deepseek_responses.jsonl (one batch response record JSON per line; includes raw + parsed)

Env:
- Read .env (repo root) and process env for:
  DEEPSEEK_API_KEY
  DEEPSEEK_BASE_URL (default https://api.deepseek.com)
  DEEPSEEK_MODEL (default deepseek-chat)
  AI_CLASSIFY_CONFIDENCE_THRESHOLD (default 0.75)

Usage:
  python3 scripts/ai_classify_from_classified.py \
    --in reports/95588_classified.md \
    --rules rules/icbc_95588_rules.json \
    --requests exports/deepseek_requests.jsonl \
    --responses exports/deepseek_responses.jsonl
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sys
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[1]


def load_dotenv(path: Path, *, override: bool = True) -> None:
    if not path.exists():
        return
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            continue
        k, v = line.split("=", 1)
        k = k.strip()
        v = v.strip().strip('"').strip("'")
        if not k:
            continue
        if not override:
            # Only fill when missing/empty.
            if k not in os.environ or os.environ.get(k, "").strip() == "":
                os.environ[k] = v
            continue

        # Default: .env should win for this repo. Avoid clobbering a non-empty env var with an empty .env value.
        if v == "" and os.environ.get(k, "").strip() != "":
            continue
        os.environ[k] = v


def looks_like_placeholder_api_key(api_key: str) -> bool:
    k = (api_key or "").strip().lower()
    if not k:
        return True
    if k in {"your_api_key_here", "xxx", "changeme", "todo"}:
        return True
    return "your_" in k and "key" in k


def ensure_parent(path: str) -> None:
    Path(path).parent.mkdir(parents=True, exist_ok=True)


def sha256_id(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


def load_rules_taxonomy(path: str) -> list[str]:
    with open(path, "r", encoding="utf-8") as f:
        rules = json.load(f)
    tax = rules.get("category_taxonomy", {})
    out: list[str] = []
    if isinstance(tax, dict):
        out.extend(tax.get("all") or [])
        if not out:
            # Back-compat: older rules used income/expense split
            out.extend(tax.get("expense") or [])
            out.extend(tax.get("income") or [])
    # Remove duplicates while preserving order
    seen = set()
    out2: list[str] = []
    for c in out:
        if c and c not in seen:
            seen.add(c)
            out2.append(c)
    return out2


LINE_RX = re.compile(
    r"^\- 来源:(?P<src>\S+)\s+日期:(?P<dt>\d{4}-\d{2}-\d{2})\s+\d{2}:\d{2}:\d{2}\s+类型:(?P<typ>收入|支出)\s+信息:(?P<info>.*?)\s+费用:(?P<amt>[0-9,]+(?:\.[0-9]+)?)元\s*$"
)


def normalize_info_remove_parens(info: str) -> str:
    """
    Per user requirement: send only "信息" and do not keep parentheses.
    Keep the text inside parentheses, but remove bracket characters.
    """
    s = info.strip()
    if (s.startswith("(") and s.endswith(")")) or (s.startswith("（") and s.endswith("）")):
        s = s[1:-1].strip()
    s = s.replace("(", "").replace(")", "").replace("（", "").replace("）", "")
    return re.sub(r"\s+", " ", s).strip()


@dataclass(frozen=True)
class Candidate:
    request_id: str
    direction: str  # 收入/支出
    merchant_key: str
    info_raw: str
    sample_lines: list[str]


def extract_candidates_from_classified(md_path: str) -> list[Candidate]:
    """
    Only read from classified MD.
    Candidates are rows under:
      - ## 支出/其他（待分类）
      - ## 收入/其他（待分类）
    Deduplicate by merchant_key.
    """
    wanted_sections = {"其他（待分类）", "支出/其他（待分类）", "收入/其他（待分类）"}
    current_section: str | None = None

    # key -> Candidate (accumulate sample lines)
    by_key: dict[str, Candidate] = {}

    with open(md_path, "r", encoding="utf-8") as f:
        for raw in f:
            line = raw.rstrip("\n")
            if line.startswith("## "):
                current_section = line[3:].strip()
                continue
            if current_section not in wanted_sections:
                continue
            m = LINE_RX.match(line.strip())
            if not m:
                continue
            direction = m.group("typ")
            info = normalize_info_remove_parens(m.group("info"))

            # Per user requirement: do not rely on parentheses for dedupe.
            merchant_key = info.strip()
            if not merchant_key:
                merchant_key = "unknown"

            request_id = sha256_id(f"{direction}|{merchant_key}")

            if merchant_key in by_key:
                # Append sample line (cap to avoid huge memory)
                existing = by_key[merchant_key]
                samples = existing.sample_lines
                if len(samples) < 5:
                    samples = [*samples, line.strip()]
                by_key[merchant_key] = Candidate(
                    request_id=existing.request_id,
                    direction=existing.direction,
                    merchant_key=existing.merchant_key,
                    info_raw=existing.info_raw,
                    sample_lines=samples,
                )
                continue

            by_key[merchant_key] = Candidate(
                request_id=request_id,
                direction=direction,
                merchant_key=merchant_key,
                info_raw=info,
                sample_lines=[line.strip()],
            )

    return list(by_key.values())


def deepseek_chat_completion(
    base_url: str,
    api_key: str,
    model: str,
    system_prompt: str,
    user_json: dict[str, Any],
    timeout_s: int = 60,
    api: str | None = None,
) -> dict[str, Any]:
    base = base_url.rstrip("/")
    api_mode = (api or os.environ.get("DEEPSEEK_API", "") or "").strip().lower()
    # Historical default: DeepSeek OpenAI-compatible chat/completions.
    if not api_mode:
        api_mode = "openai-chat-completions"

    def _candidate_urls_for_chat_completions() -> list[str]:
        # Support both ".../v1" and base roots.
        if base.endswith("/v1"):
            return [f"{base}/chat/completions"]
        return [f"{base}/v1/chat/completions"]

    def _candidate_urls_for_responses() -> list[str]:
        # Common gateway patterns:
        # - https://host/openai  -> /openai/v1/responses
        # - https://host/openai/v1 -> /openai/v1/responses
        # - https://host/v1 -> /v1/responses
        # - https://host -> /v1/responses
        if base.endswith("/v1"):
            return [f"{base}/responses"]
        if base.endswith("/openai"):
            return [f"{base}/v1/responses", f"{base}/responses"]
        return [f"{base}/v1/responses", f"{base}/responses"]

    if api_mode in {"openai-responses", "responses"}:
        urls = _candidate_urls_for_responses()
        payload = {
            "model": model,
            # Many gateways accept the simple message form:
            # input = [{"role":"user","content":"..."}, ...]
            # and require stream=true.
            "input": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": json.dumps(user_json, ensure_ascii=False)},
            ],
            "temperature": 0,
            "stream": True,
        }
    else:
        urls = _candidate_urls_for_chat_completions()
        payload = {
            "model": model,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": json.dumps(user_json, ensure_ascii=False)},
            ],
            "temperature": 0,
            "stream": False,
            "max_tokens": 1024,
        }

    def _maybe_parse_sse_as_openai_json(body_text: str) -> dict[str, Any] | None:
        """
        Some OpenAI-compatible gateways return Server-Sent Events (SSE) even when stream=false.
        Convert SSE chat.completion.chunk events into a single OpenAI-style response dict.
        """
        text = (body_text or "").strip()
        if not text:
            return None
        if not (text.startswith("data:") or "\n\ndata:" in text or "\ndata:" in text):
            return None

        content_parts: list[str] = []
        chunks: list[dict[str, Any]] = []
        saw_delta = False
        for raw_line in body_text.splitlines():
            line = raw_line.strip()
            if not line.startswith("data:"):
                continue
            data = line[len("data:") :].strip()
            if not data:
                continue
            if data == "[DONE]":
                break
            try:
                obj = json.loads(data)
            except Exception:
                continue
            if isinstance(obj, dict):
                chunks.append(obj)
                # ChatCompletions stream: choices[0].delta.content
                try:
                    delta = obj.get("choices", [{}])[0].get("delta", {})  # type: ignore[union-attr]
                    if isinstance(delta, dict):
                        piece = delta.get("content")
                        if isinstance(piece, str) and piece:
                            saw_delta = True
                            content_parts.append(piece)
                except Exception:
                    pass
                # Responses stream: {"type":"response.output_text.delta","delta":"..."}
                try:
                    piece = obj.get("delta")
                    if isinstance(piece, str) and piece:
                        saw_delta = True
                        content_parts.append(piece)
                except Exception:
                    pass
                # Some gateways: {"text":"..."} or {"type":"response.output_text","text":"..."}
                try:
                    if saw_delta:
                        raise KeyError("skip text when deltas were seen")
                    piece = obj.get("text")
                    if isinstance(piece, str) and piece:
                        content_parts.append(piece)
                except Exception:
                    pass

        content = "".join(content_parts)
        # Always return something JSON-like so caller can continue; if content is empty,
        # include raw chunks for debugging.
        return {
            "choices": [{"message": {"content": content}}],
            "_sse_chunks": chunks,
        }

    last_err: Exception | None = None
    for url in urls:
        data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        req = urllib.request.Request(url, data=data, method="POST")
        req.add_header("Content-Type", "application/json")
        req.add_header("Authorization", f"Bearer {api_key}")

        try:
            with urllib.request.urlopen(req, timeout=timeout_s) as resp:
                body = resp.read().decode("utf-8", errors="replace")
                try:
                    return json.loads(body)
                except json.JSONDecodeError as e:
                    content_type = ""
                    try:
                        content_type = str(resp.headers.get("Content-Type") or "")
                    except Exception:
                        content_type = ""
                    ct = (content_type or "").lower()
                    if "text/event-stream" in ct or (body.lstrip().startswith("event:") or "\nevent:" in body):
                        converted = _maybe_parse_sse_as_openai_json(body)
                        if converted is not None:
                            return converted
                    snippet = (body or "").strip().replace("\r", " ").replace("\n", " ")
                    if len(snippet) > 500:
                        snippet = snippet[:500] + "...(truncated)"
                    status = getattr(resp, "status", "?")
                    raise RuntimeError(
                        f"Non-JSON response from {url} status={status} content_type={content_type!r} body={snippet!r}"
                    ) from e
        except urllib.error.HTTPError as e:
            # Improve diagnostics (401 is very common when the key is missing/expired).
            # Do not leak secrets; include only status + response body snippet.
            try:
                raw = e.read().decode("utf-8", errors="replace")
            except Exception:
                raw = ""
            snippet = (raw or "").strip().replace("\r", " ").replace("\n", " ")
            if len(snippet) > 500:
                snippet = snippet[:500] + "...(truncated)"
            msg = f"HTTP {getattr(e, 'code', '?')}: {getattr(e, 'reason', '')}".strip()
            if snippet:
                msg += f" | body={snippet}"
            last_err = RuntimeError(msg)
            # Try next candidate URL on 404-like errors; otherwise raise immediately.
            if str(getattr(e, "code", "")) in {"404"} or "Not Found" in str(getattr(e, "reason", "")):
                continue
            raise last_err from e
        except Exception as e:
            last_err = e
            break

    if last_err is not None:
        raise last_err
    raise RuntimeError("LLM request failed with unknown error")


def extract_assistant_text(resp: dict[str, Any]) -> str:
    # Chat Completions: choices[0].message.content
    try:
        content = resp.get("choices", [{}])[0].get("message", {}).get("content")  # type: ignore[union-attr]
        if isinstance(content, str) and content.strip():
            return content
    except Exception:
        pass

    # Responses API: output[].content[].text
    try:
        out = resp.get("output") or []
        if not isinstance(out, list):
            return ""
        parts: list[str] = []
        for item in out:
            if not isinstance(item, dict):
                continue
            if item.get("type") not in {"message", "output_text"}:
                # Gateways vary; keep it permissive.
                pass
            if item.get("role") not in {None, "", "assistant"}:
                continue
            content_list = item.get("content") or []
            if isinstance(content_list, list):
                for c in content_list:
                    if isinstance(c, dict) and c.get("type") == "output_text" and isinstance(c.get("text"), str):
                        parts.append(c["text"])
                    if isinstance(c, dict) and c.get("type") == "text" and isinstance(c.get("text"), str):
                        parts.append(c["text"])
        text = "".join(parts).strip()
        if text:
            return text
    except Exception:
        pass

    return ""


def try_parse_json(text: str) -> dict[str, Any] | None:
    text = text.strip()
    if not text:
        return None
    # If model wraps in ```json ...```, strip fences.
    if text.startswith("```"):
        text = re.sub(r"^```[a-zA-Z]*\s*", "", text)
        text = re.sub(r"\s*```$", "", text)
        text = text.strip()
    try:
        obj = json.loads(text)
        if isinstance(obj, dict):
            return obj
    except Exception:
        return None
    return None


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="in_path", default="reports/95588_classified.md")
    ap.add_argument("--rules", default="rules/icbc_95588_rules.json")
    ap.add_argument("--requests", default="exports/deepseek_requests.jsonl")
    ap.add_argument("--responses", default="exports/deepseek_responses.jsonl")
    ap.add_argument("--resume", action="store_true", help="Skip request_ids already in responses file")
    ap.add_argument(
        "--retry-errors",
        action="store_true",
        help="When resuming, re-run request_ids whose latest recorded response has an error or invalid JSON.",
    )
    ap.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Limit number of candidates to process (0 means no limit). Applied after extraction + sorting.",
    )
    ap.add_argument(
        "--batch-size",
        type=int,
        default=50,
        help="Number of merchant candidates per API call (default 50). Use 0 to send all in one call (may exceed model limits).",
    )
    ap.add_argument("--sleep-ms", type=int, default=200, help="Sleep between calls (ms)")
    ap.add_argument("--timeout-s", type=int, default=180, help="HTTP timeout seconds for each API call (default 180)")
    args = ap.parse_args()

    load_dotenv(REPO_ROOT / ".env")

    api_key = os.environ.get("DEEPSEEK_API_KEY", "").strip()
    if looks_like_placeholder_api_key(api_key):
        raise SystemExit(
            "DEEPSEEK_API_KEY looks missing/placeholder. Set it in .env or export it before running.\n"
            "Example: DEEPSEEK_API_KEY=sk-... (DeepSeek key)\n"
            "Tip: if you just copied `.env.example`, replace `your_api_key_here` with a real key."
        )
    base_url = os.environ.get("DEEPSEEK_BASE_URL", "https://api.deepseek.com").strip()
    model = os.environ.get("DEEPSEEK_MODEL", "deepseek-chat").strip()

    category_enum = load_rules_taxonomy(args.rules)
    if not category_enum:
        raise SystemExit("No category taxonomy found in rules file.")

    candidates = extract_candidates_from_classified(args.in_path)
    candidates.sort(key=lambda c: (c.direction, c.merchant_key))
    if args.limit and args.limit > 0:
        candidates = candidates[: args.limit]

    # Batch candidates to reduce redundancy (send category_enum once in system prompt).
    batch_size = args.batch_size if args.batch_size and args.batch_size > 0 else len(candidates)
    batches: list[list[Candidate]] = [candidates[i : i + batch_size] for i in range(0, len(candidates), batch_size)]
    total_batches = len(batches)

    ensure_parent(args.requests)
    ensure_parent(args.responses)

    done: set[str] = set()
    retryable: set[str] = set()
    if args.resume and Path(args.responses).exists():
        with open(args.responses, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    obj = json.loads(line)
                    request_ids = obj.get("request_ids") or []
                    if isinstance(request_ids, list):
                        for rid in request_ids:
                            if not rid:
                                continue
                            rid_s = str(rid)
                            done.add(rid_s)
                            if obj.get("error") or obj.get("parsed") is None:
                                retryable.add(rid_s)
                except Exception:
                    continue

    system_prompt = (
        "你是记账分类助手。你必须只输出 JSON，不能输出任何额外文本。"
        "你将收到一个 items 数组，每个 item 都需要输出一个对应的 result。"
        "每个 result 必须包含：request_id、merchant_key、category、confidence、reason、needs_review。"
        "category 必须从以下枚举中选择："
        + json.dumps(category_enum, ensure_ascii=False)
        + "。"
        "无法确定时：category 返回 '其他（待分类）'，"
        "confidence 设为 <=0.5 且 needs_review=true。"
        "最终输出 JSON 结构必须是：{ \"results\": [ ... ] }。"
    )

    # Write all request payloads (even if resuming) for auditability.
    with open(args.requests, "w", encoding="utf-8") as f_req:
        for idx, batch in enumerate(batches):
            items = []
            for c in batch:
                items.append(
                    {
                        "request_id": c.request_id,
                        "merchant_key": c.merchant_key,
                        "direction": c.direction,
                        "info": c.info_raw,
                    }
                )
            f_req.write(
                json.dumps(
                    {
                        "batch_id": idx + 1,
                        "batch_size": len(batch),
                        "items": items,
                    },
                    ensure_ascii=False,
                )
                + "\n"
            )

    # Call API and store responses.
    written = 0
    skipped = 0
    errors = 0
    try:
        with open(args.responses, "a", encoding="utf-8") as f_resp:
            for idx, batch in enumerate(batches):
                batch_no = idx + 1
                batch_ids = [c.request_id for c in batch]
                batch_label = f"[{batch_no}/{total_batches}]"

                # Resume logic: if batch already fully done (all request_ids seen), skip unless retry_errors.
                if args.resume and all(rid in done for rid in batch_ids):
                    if args.retry_errors and any(rid in retryable for rid in batch_ids):
                        pass
                    else:
                        print(
                            f"{batch_label} skip (already done): batch_size={len(batch)}",
                            file=sys.stderr,
                            flush=True,
                        )
                        skipped += 1
                        continue

                # Progress: show start
                print(
                    f"{batch_label} start: batch_size={len(batch)} timeout_s={args.timeout_s} model={model}",
                    file=sys.stderr,
                    flush=True,
                )

                items = []
                for c in batch:
                    items.append(
                        {
                            "request_id": c.request_id,
                            "merchant_key": c.merchant_key,
                            "direction": c.direction,
                            "info": c.info_raw,
                        }
                    )

                user_json = {"task": "classify_batch", "language": "zh-CN", "items": items}

                started = time.time()
                record: dict[str, Any] = {
                    "batch_id": idx + 1,
                    "batch_size": len(batch),
                    "request_ids": batch_ids,
                    "model": model,
                    "base_url": base_url,
                    "ts": int(time.time()),
                }
                try:
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
                        errors += 1
                        print(
                            f"{batch_label} done: ERROR invalid_json_response latency_ms={record['latency_ms']}",
                            file=sys.stderr,
                            flush=True,
                        )
                    else:
                        results_n = len((parsed or {}).get("results") or []) if isinstance(parsed, dict) else 0
                        print(
                            f"{batch_label} done: ok latency_ms={record['latency_ms']} results={results_n}",
                            file=sys.stderr,
                            flush=True,
                        )
                    written += 1
                except Exception as e:
                    record["error"] = f"exception:{type(e).__name__}"
                    record["message"] = str(e)
                    errors += 1
                    record["latency_ms"] = int((time.time() - started) * 1000)
                    print(
                        f"{batch_label} done: ERROR {record['error']} latency_ms={record['latency_ms']} message={record.get('message','')}",
                        file=sys.stderr,
                        flush=True,
                    )
                    msg = str(e)
                    if "HTTP 401" in msg or " 401" in msg:
                        print(
                            f"{batch_label} hint: DeepSeek returned 401. Re-check `DEEPSEEK_API_KEY` and `DEEPSEEK_BASE_URL`.",
                            file=sys.stderr,
                            flush=True,
                        )

                f_resp.write(json.dumps(record, ensure_ascii=False) + "\n")
                f_resp.flush()
                time.sleep(max(args.sleep_ms, 0) / 1000.0)
    except KeyboardInterrupt:
        print(
            json.dumps(
                {
                    "interrupted": True,
                    "candidates": len(candidates),
                    "requests_file": args.requests,
                    "responses_file": args.responses,
                    "api_calls_written": written,
                    "api_calls_skipped": skipped,
                    "errors": errors,
                },
                ensure_ascii=False,
            )
        )
        return 130

    print(
        json.dumps(
            {
                "candidates": len(candidates),
                "requests_file": args.requests,
                "responses_file": args.responses,
                "api_calls_written": written,
                "api_calls_skipped": skipped,
                "errors": errors,
            },
            ensure_ascii=False,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
