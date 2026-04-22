from __future__ import annotations

import argparse
import sys
from pathlib import Path

from jizhang import __version__
from jizhang.env import load_dotenv
from jizhang.pipeline.config import load_pipeline
from jizhang.pipeline.errors import ConfigError


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="python -m jizhang", description="jizhang CLI (pipeline-driven)")
    p.add_argument("--version", action="version", version=f"jizhang {__version__}")

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
    print("Built-ins are not wired yet (Task 5/6).")
    print("Next: implement registry + built-in steps.")
    return 0


def _cmd_doctor(args: argparse.Namespace) -> int:
    try:
        cfg = load_pipeline(args.pipeline)
    except ConfigError as e:
        print(f"doctor: invalid pipeline config: {e}", file=sys.stderr)
        return 2

    print(f"doctor: ok pipeline={cfg.path}")
    print(f"doctor: name={cfg.name}")
    return 0


def _cmd_run(args: argparse.Namespace) -> int:
    print(f"run: pipeline={args.pipeline}")
    print("Not implemented yet (Task 7).")
    return 2


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
