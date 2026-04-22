#!/usr/bin/env python3
"""
Push Firefly III TransactionStore JSONL into Firefly III via API.

Input: JSONL where each line is a payload for POST /api/v1/transactions:
  {"transactions":[{...}]}

Features:
- Uses Personal Access Token (Bearer token).
- Progress output + simple retry on transient errors.
- Strategy C support:
  - Optional local state skip for speed (transaction-level external_id).
  - Always uses Firefly server-side duplicate protection by default (error_if_duplicate_hash=true).
- Bootstraps missing asset accounts by creating them before pushing transactions.

Env (.env supported, repo root):
  FIREFLY_BASE_URL=http://localhost:8080
  FIREFLY_TOKEN=...

Usage:
  python3 scripts/push_firefly_jsonl.py \
    --in exports/firefly_transactions_ai.jsonl \
    --state exports/firefly_push_state.jsonl
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[1]


def load_dotenv(path: Path, *, override: bool = True) -> None:
    if not path.exists():
        return
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        k = k.strip()
        v = v.strip().strip('"').strip("'")
        if not k:
            continue
        if not override:
            if k not in os.environ or os.environ.get(k, "").strip() == "":
                os.environ[k] = v
            continue
        if v == "" and os.environ.get(k, "").strip() != "":
            continue
        os.environ[k] = v


def ensure_parent(path: str) -> None:
    Path(path).parent.mkdir(parents=True, exist_ok=True)


def http_json(
    method: str,
    url: str,
    token: str,
    payload: dict[str, Any] | None = None,
    timeout_s: int = 30,
) -> tuple[int, dict[str, Any] | None, str]:
    data = None
    if payload is not None:
        data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    req = urllib.request.Request(url, data=data, method=method)
    req.add_header("Authorization", f"Bearer {token}")
    req.add_header("Accept", "application/json")
    if payload is not None:
        req.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(req, timeout=timeout_s) as resp:
            body = resp.read().decode("utf-8", errors="replace")
            ctype = resp.headers.get("Content-Type", "")
            if "json" in (ctype or ""):
                try:
                    return resp.status, json.loads(body), body
                except Exception:
                    return resp.status, None, body
            return resp.status, None, body
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", errors="replace") if hasattr(e, "read") else ""
        try:
            obj = json.loads(body)
        except Exception:
            obj = None
        return int(getattr(e, "code", 0) or 0), obj, body


def iter_jsonl(path: str):
    with open(path, "r", encoding="utf-8") as f:
        for idx, line in enumerate(f, start=1):
            line = line.strip()
            if not line:
                continue
            yield idx, json.loads(line)


def load_state(path: str) -> set[str]:
    p = Path(path)
    if not p.exists():
        return set()
    done: set[str] = set()
    for line in p.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
        except Exception:
            continue
        if obj.get("status") != "ok":
            continue
        ext = str(obj.get("external_id") or "").strip()
        if ext:
            done.add(ext)
    return done


def append_state(path: str, record: dict[str, Any]) -> None:
    ensure_parent(path)
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")


def extract_asset_accounts(payload: dict[str, Any]) -> list[str]:
    """
    Firefly requires asset accounts to exist. Our export uses asset as either source_name (withdrawal)
    or destination_name (deposit).
    Heuristic: any account name that looks like '<prefix>(<4 digits>)' is an asset account.
    """
    out: list[str] = []
    txns = payload.get("transactions") or []
    if not isinstance(txns, list):
        return out
    for t in txns:
        if not isinstance(t, dict):
            continue
        for k in ("source_name", "destination_name"):
            name = str(t.get(k) or "").strip()
            if not name:
                continue
            if len(name) >= 7 and name.endswith(")") and "(" in name:
                inside = name.rsplit("(", 1)[-1].rstrip(")")
                if inside.isdigit() and len(inside) == 4:
                    out.append(name)
    # dedupe preserve order
    seen = set()
    return [n for n in out if not (n in seen or seen.add(n))]


def create_asset_account_if_missing(base_url: str, token: str, name: str, timeout_s: int) -> None:
    url = base_url.rstrip("/") + "/api/v1/accounts"
    payload = {"name": name, "type": "asset", "account_role": "defaultAsset", "currency_code": "CNY"}
    status, obj, body = http_json("POST", url, token, payload=payload, timeout_s=timeout_s)
    if status in (200, 201):
        return
    # If already exists, Firefly returns 422 with a validation error; accept it as "exists".
    if status == 422:
        return
    raise RuntimeError(f"create asset account failed: status={status} name={name} body={body[:300]}")


def post_transaction(base_url: str, token: str, payload: dict[str, Any], timeout_s: int) -> tuple[int, dict[str, Any] | None, str]:
    url = base_url.rstrip("/") + "/api/v1/transactions"
    return http_json("POST", url, token, payload=payload, timeout_s=timeout_s)


def external_id_exists_remote(base_url: str, token: str, external_id: str, timeout_s: int) -> tuple[bool, str]:
    """
    Check if a transaction with the given external_id exists in Firefly.
    Returns (exists, status_string).
    """
    q = urllib.parse.quote(external_id, safe="")
    # Firefly search endpoint uses `search` query parameter (not `query`).
    url = base_url.rstrip("/") + f"/api/v1/search/transactions?search={q}"
    status, obj, body = http_json("GET", url, token, payload=None, timeout_s=timeout_s)
    if status != 200:
        return False, f"http_{status}"
    if not isinstance(obj, dict):
        return False, "invalid_json"
    data = obj.get("data")
    if isinstance(data, list) and len(data) > 0:
        return True, "found"
    return False, "not_found"


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="in_path", required=True, help="Firefly TransactionStore JSONL")
    ap.add_argument("--state", default="exports/firefly_push_state.jsonl", help="Local state JSONL for audit/debugging")
    ap.add_argument("--timeout-s", type=int, default=30)
    ap.add_argument("--retries", type=int, default=3)
    ap.add_argument("--retry-sleep-s", type=float, default=1.5)
    ap.add_argument("--bootstrap-assets", action="store_true", help="Create asset accounts if missing")
    ap.add_argument(
        "--skip-using-state",
        action="store_true",
        help="Skip lines whose external_id is already recorded as ok in --state (legacy behavior).",
    )
    ap.add_argument(
        "--no-error-if-duplicate",
        action="store_true",
        help="Disable Firefly duplicate protection (NOT recommended). By default this script sets error_if_duplicate_hash=true.",
    )
    # NOTE: Remote verification via Firefly search is intentionally not supported here.
    # Firefly search does not reliably find transactions by external_id in all versions/configs,
    # which can lead to accidental full re-imports.
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--limit", type=int, default=0, help="Limit number of lines to push (0=no limit)")
    args = ap.parse_args()

    load_dotenv(REPO_ROOT / ".env")
    base_url = os.environ.get("FIREFLY_BASE_URL", "").strip()
    token = os.environ.get("FIREFLY_TOKEN", "").strip()
    if not base_url or not token:
        raise SystemExit("Missing FIREFLY_BASE_URL or FIREFLY_TOKEN in environment/.env")

    done = load_state(args.state) if args.skip_using_state else set()
    pushed = 0
    skipped = 0  # only used when --skip-using-state is enabled
    failed = 0
    bootstrapped = 0
    duplicate_skipped = 0

    # Optional bootstrap: pre-scan input for asset accounts.
    if args.bootstrap_assets and not args.dry_run:
        assets: list[str] = []
        for _idx, payload in iter_jsonl(args.in_path):
            assets.extend(extract_asset_accounts(payload))
        seen = set()
        assets = [a for a in assets if not (a in seen or seen.add(a))]
        print(f"[bootstrap] asset_accounts={len(assets)}", file=sys.stderr, flush=True)
        for i, name in enumerate(assets, start=1):
            create_asset_account_if_missing(base_url, token, name, timeout_s=max(args.timeout_s, 1))
            bootstrapped += 1
            if i % 5 == 0 or i == len(assets):
                print(f"[bootstrap] {i}/{len(assets)} ok", file=sys.stderr, flush=True)

    # Push transactions
    total_lines = sum(1 for _ in open(args.in_path, "r", encoding="utf-8") if _.strip())
    for line_no, payload in iter_jsonl(args.in_path):
        if args.limit and pushed + skipped + failed >= args.limit:
            break
        txns = payload.get("transactions") or []
        ext = ""
        if isinstance(txns, list) and txns and isinstance(txns[0], dict):
            ext = str(txns[0].get("external_id") or "").strip()
        # Optional legacy behavior: skip using local state only (no remote verification).
        if ext and ext in done:
            skipped += 1
            if skipped % 100 == 0:
                print(
                    f"[push] {line_no}/{total_lines} skipped={skipped} pushed={pushed} failed={failed}",
                    file=sys.stderr,
                    flush=True,
                )
            continue

        if args.dry_run:
            pushed += 1
            continue

        attempt = 0
        # Default to duplicate protection (Firefly uses import hash).
        if (not args.no_error_if_duplicate) and "error_if_duplicate_hash" not in payload:
            payload = {**payload, "error_if_duplicate_hash": True}
        while True:
            attempt += 1
            status, obj, body = post_transaction(base_url, token, payload, timeout_s=max(args.timeout_s, 1))
            if status in (200, 201):
                pushed += 1
                if ext:
                    done.add(ext)
                    append_state(
                        args.state,
                        {"ts": int(time.time()), "line_no": line_no, "external_id": ext, "status": "ok"},
                    )
                if pushed % 25 == 0 or pushed == 1:
                    print(f"[push] {line_no}/{total_lines} pushed={pushed} skipped={skipped} failed={failed}", file=sys.stderr, flush=True)
                break

            # Treat duplicate-hash errors as "already imported" (skip).
            if status == 422 and isinstance(body, str):
                body_l = body.lower()
                is_dup = (
                    ("duplicate" in body_l)
                    or ("import_hash" in body_l)
                    or ("重复" in body)
                    or ("已存在" in body)
                )
                if is_dup:
                    skipped += 1
                    duplicate_skipped += 1
                if ext:
                    done.add(ext)
                    append_state(
                        args.state,
                        {"ts": int(time.time()), "line_no": line_no, "external_id": ext, "status": "dup"},
                    )
                if skipped % 25 == 0:
                    print(
                        f"[push] {line_no}/{total_lines} duplicate skipped={skipped} pushed={pushed} failed={failed}",
                        file=sys.stderr,
                        flush=True,
                    )
                    break

            # Common recoverable cases:
            # - 429 / 5xx
            if status in (429, 500, 502, 503, 504) and attempt <= max(args.retries, 0):
                time.sleep(args.retry_sleep_s * attempt)
                continue

            failed += 1
            append_state(
                args.state,
                {
                    "ts": int(time.time()),
                    "line_no": line_no,
                    "external_id": ext,
                    "status": "error",
                    "http_status": status,
                    "body": (body or "")[:1000],
                },
            )
            print(f"[push] ERROR line={line_no} status={status} ext={ext} body={body[:200]}", file=sys.stderr, flush=True)
            break

    print(
        json.dumps(
            {
                "in": args.in_path,
                "base_url": base_url,
                "pushed": pushed,
                "skipped": skipped,
                "duplicate_skipped": duplicate_skipped,
                "failed": failed,
                "state": args.state,
                "bootstrapped_assets": bootstrapped,
                "dry_run": args.dry_run,
            },
            ensure_ascii=False,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
