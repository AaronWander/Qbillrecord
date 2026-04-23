from __future__ import annotations

import json
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable


def ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


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


def iter_jsonl(path: Path):
    with path.open("r", encoding="utf-8") as f:
        for idx, line in enumerate(f, start=1):
            line = line.strip()
            if not line:
                continue
            yield idx, json.loads(line)


def load_state(path: Path) -> set[str]:
    if not path.exists():
        return set()
    done: set[str] = set()
    for line in path.read_text(encoding="utf-8").splitlines():
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


def append_state(path: Path, record: dict[str, Any]) -> None:
    ensure_parent(path)
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")


def extract_asset_accounts(payload: dict[str, Any]) -> list[str]:
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
    seen = set()
    return [n for n in out if not (n in seen or seen.add(n))]


def create_asset_account_if_missing(base_url: str, token: str, name: str, timeout_s: int) -> None:
    url = base_url.rstrip("/") + "/api/v1/accounts"
    payload = {"name": name, "type": "asset", "account_role": "defaultAsset", "currency_code": "CNY"}
    status, _obj, body = http_json("POST", url, token, payload=payload, timeout_s=timeout_s)
    if status in (200, 201):
        return
    if status == 422:
        return
    raise RuntimeError(f"create asset account failed: status={status} name={name} body={body[:300]}")


def post_transaction(base_url: str, token: str, payload: dict[str, Any], timeout_s: int) -> tuple[int, dict[str, Any] | None, str]:
    url = base_url.rstrip("/") + "/api/v1/transactions"
    return http_json("POST", url, token, payload=payload, timeout_s=timeout_s)


@dataclass(frozen=True)
class PushSummary:
    pushed: int
    skipped: int
    duplicate_skipped: int
    failed: int
    bootstrapped_assets: int


def push_firefly_jsonl(
    *,
    in_path: Path,
    state_path: Path,
    base_url: str,
    token: str,
    timeout_s: int = 30,
    retries: int = 3,
    retry_sleep_s: float = 1.5,
    bootstrap_assets: bool = False,
    skip_using_state: bool = True,
    no_error_if_duplicate: bool = False,
    dry_run: bool = False,
    limit: int = 0,
) -> PushSummary:
    done = load_state(state_path) if skip_using_state else set()
    pushed = 0
    skipped = 0
    failed = 0
    bootstrapped = 0
    duplicate_skipped = 0

    if bootstrap_assets and not dry_run:
        assets: list[str] = []
        for _idx, payload in iter_jsonl(in_path):
            if isinstance(payload, dict):
                assets.extend(extract_asset_accounts(payload))
        seen = set()
        assets = [a for a in assets if not (a in seen or seen.add(a))]
        for name in assets:
            create_asset_account_if_missing(base_url, token, name, timeout_s=max(timeout_s, 1))
            bootstrapped += 1

    total_lines = sum(1 for line in in_path.read_text(encoding="utf-8").splitlines() if line.strip())
    for line_no, payload in iter_jsonl(in_path):
        if limit and pushed + skipped + failed >= limit:
            break
        if not isinstance(payload, dict):
            continue
        txns = payload.get("transactions") or []
        ext = ""
        if isinstance(txns, list) and txns and isinstance(txns[0], dict):
            ext = str(txns[0].get("external_id") or "").strip()
        if ext and ext in done:
            skipped += 1
            continue

        if dry_run:
            pushed += 1
            continue

        attempt = 0
        if (not no_error_if_duplicate) and "error_if_duplicate_hash" not in payload:
            payload = {**payload, "error_if_duplicate_hash": True}

        while True:
            attempt += 1
            status, _obj, body = post_transaction(base_url, token, payload, timeout_s=max(timeout_s, 1))
            if status in (200, 201):
                pushed += 1
                if ext:
                    done.add(ext)
                    append_state(state_path, {"ts": int(time.time()), "line_no": line_no, "external_id": ext, "status": "ok"})
                break

            if status == 422 and isinstance(body, str):
                body_l = body.lower()
                is_dup = ("duplicate" in body_l) or ("import_hash" in body_l) or ("重复" in body) or ("已存在" in body)
                if is_dup:
                    skipped += 1
                    duplicate_skipped += 1
                    if ext:
                        done.add(ext)
                        append_state(state_path, {"ts": int(time.time()), "line_no": line_no, "external_id": ext, "status": "dup"})
                    break

            if status in (429, 500, 502, 503, 504) and attempt <= max(retries, 0):
                time.sleep(retry_sleep_s * attempt)
                continue

            failed += 1
            append_state(
                state_path,
                {"ts": int(time.time()), "line_no": line_no, "external_id": ext, "status": "error", "http_status": status, "body": (body or "")[:1000]},
            )
            raise RuntimeError(f"push failed: line={line_no}/{total_lines} status={status} ext={ext} body={body[:200]}")

    return PushSummary(
        pushed=pushed,
        skipped=skipped,
        duplicate_skipped=duplicate_skipped,
        failed=failed,
        bootstrapped_assets=bootstrapped,
    )

