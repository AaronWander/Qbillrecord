from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

from qbillrecord import __version__
from qbillrecord.env import load_dotenv
from qbillrecord.pipeline.config import collect_env_refs, load_pipeline, load_pipeline_raw
from qbillrecord.pipeline.errors import ConfigError
from qbillrecord.steps import builtins as _builtins  # noqa: F401
from qbillrecord.registry import REGISTRY
from qbillrecord.pipeline.runner import safe_run_pipeline


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="python -m qbillrecord", description="Qbillrecord CLI (pipeline-driven)")
    p.add_argument("--version", action="version", version=f"Qbillrecord {__version__}")

    sub = p.add_subparsers(dest="cmd", required=True)

    sp_list = sub.add_parser("list", help="List built-in step types")
    sp_list.set_defaults(func=_cmd_list)

    sp_doctor = sub.add_parser("doctor", help="Validate pipeline config and env (no execution)")
    sp_doctor.add_argument("--pipeline", required=True, help="Path to pipeline YAML")
    sp_doctor.set_defaults(func=_cmd_doctor)

    sp_run = sub.add_parser("run", help="Run a pipeline")
    sp_run.add_argument("--pipeline", required=True, help="Path to pipeline YAML")
    sp_run.set_defaults(func=_cmd_run)

    return p


def _cmd_list(_args: argparse.Namespace) -> int:
    for kind in REGISTRY.kinds():
        print(f"{kind}:")
        for reg in REGISTRY.types_for(kind):
            desc = f" - {reg.description}" if reg.description else ""
            print(f"  - {reg.type_id}{desc}")
    return 0


def _cmd_doctor(args: argparse.Namespace) -> int:
    # Phase 1: load raw YAML and report missing env vars and obvious path issues
    try:
        raw = load_pipeline_raw(args.pipeline)
    except ConfigError as e:
        print(f"doctor: invalid pipeline config: {e}", file=sys.stderr)
        return 2

    issues: list[str] = []

    # Collect missing env vars
    refs = collect_env_refs(raw)
    missing_env: list[str] = []
    invalid_env_ref: list[str] = []
    for key, env_name in refs:
        if not env_name:
            invalid_env_ref.append(key)
            continue
        if (os.environ.get(env_name) or "").strip() == "":
            missing_env.append(env_name)
    missing_env = sorted(set(missing_env))
    invalid_env_ref = sorted(set(invalid_env_ref))
    if invalid_env_ref:
        issues.append(f"invalid *_env values for keys: {', '.join(invalid_env_ref)}")
    if missing_env:
        issues.append(f"missing env vars: {', '.join(missing_env)}")

    # Path checks (best-effort; some steps may not need these)
    try:
        src = raw.get("source") or {}
        if isinstance(src, dict):
            db_path = src.get("db_path")
            if isinstance(db_path, str) and db_path.strip():
                p = Path(os.path.expanduser(db_path.strip()))
                if not p.exists():
                    issues.append(f"source.db_path not found: {p}")
    except Exception:
        pass

    try:
        cls = raw.get("classifier") or {}
        if isinstance(cls, dict):
            rules_path = cls.get("rules_path")
            if isinstance(rules_path, str) and rules_path.strip():
                p = Path(rules_path.strip())
                if not p.exists():
                    issues.append(f"classifier.rules_path not found: {p}")
    except Exception:
        pass

    if issues:
        print(f"doctor: issues found for pipeline={args.pipeline}", file=sys.stderr)
        for it in issues:
            print(f"- {it}", file=sys.stderr)
        return 2

    # Phase 2: fully resolve env refs and validate required keys
    try:
        cfg = load_pipeline(args.pipeline)
    except ConfigError as e:
        print(f"doctor: invalid pipeline config: {e}", file=sys.stderr)
        return 2

    print(f"doctor: ok pipeline={cfg.path}")
    print(f"doctor: name={cfg.name}")
    return 0


def _cmd_run(args: argparse.Namespace) -> int:
    try:
        cfg = load_pipeline(args.pipeline)
    except ConfigError as e:
        print(f"run: invalid pipeline config: {e}", file=sys.stderr)
        return 2

    try:
        res = safe_run_pipeline(cfg)
    except ConfigError as e:
        print(f"run: invalid pipeline config: {e}", file=sys.stderr)
        return 2
    except Exception as e:
        print(f"run: failed: {e}", file=sys.stderr)
        return 2

    print(f"run: ok rc={res.rc} run_dir={res.run_dir}")
    return int(res.rc)


def main(argv: list[str] | None = None) -> int:
    # Load repo-root .env if present (users can still override via real env vars).
    load_dotenv(Path(".env"), override=False)

    parser = _build_parser()
    args = parser.parse_args(argv)
    func = getattr(args, "func", None)
    if func is None:
        parser.print_help(sys.stderr)
        return 2
    return int(func(args) or 0)
