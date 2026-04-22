from __future__ import annotations

import argparse
import sys

from jizhang import __version__


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
    print(f"doctor: pipeline={args.pipeline}")
    print("Not implemented yet (Task 3).")
    return 2


def _cmd_run(args: argparse.Namespace) -> int:
    print(f"run: pipeline={args.pipeline}")
    print("Not implemented yet (Task 7).")
    return 2


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    func = getattr(args, "func", None)
    if func is None:
        parser.print_help(sys.stderr)
        return 2
    return int(func(args) or 0)

