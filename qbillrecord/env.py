from __future__ import annotations

import os
from pathlib import Path


def load_dotenv(path: str | Path, *, override: bool = True) -> None:
    """
    Minimal .env loader.
    - Supports KEY=VALUE lines (no export, no shell evaluation)
    - Strips simple quotes
    - If override=False, only fills missing/empty vars
    """
    p = Path(path)
    if not p.exists():
        return
    for line in p.read_text(encoding="utf-8").splitlines():
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
            if k not in os.environ or os.environ.get(k, "").strip() == "":
                os.environ[k] = v
            continue
        if v == "" and os.environ.get(k, "").strip() != "":
            continue
        os.environ[k] = v

