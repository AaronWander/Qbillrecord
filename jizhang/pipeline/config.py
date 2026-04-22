from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from jizhang.pipeline.errors import ConfigError


def _is_env_ref_key(key: str) -> bool:
    return isinstance(key, str) and (key.endswith("_env") or key.endswith("_ENV"))


def collect_env_refs(obj: Any) -> list[tuple[str, str]]:
    """
    Return a list of (key, env_var_name) referenced via *_env/*_ENV keys.
    Intended for `doctor` to show missing env vars without aborting early.
    """
    out: list[tuple[str, str]] = []
    if isinstance(obj, dict):
        for k, v in obj.items():
            if _is_env_ref_key(k):
                if isinstance(v, str) and v.strip():
                    out.append((str(k), v.strip()))
                else:
                    out.append((str(k), ""))
                continue
            out.extend(collect_env_refs(v))
        return out
    if isinstance(obj, list):
        for x in obj:
            out.extend(collect_env_refs(x))
        return out
    return out


def _resolve_env_refs(obj: Any, *, env: dict[str, str]) -> Any:
    """
    Resolve keys ending with _env/_ENV:
      api_key_env: FIREFLY_TOKEN  -> api_key: <value>

    The original *_env key is removed.
    """
    if isinstance(obj, dict):
        out: dict[str, Any] = {}
        for k, v in obj.items():
            if _is_env_ref_key(k):
                if not isinstance(v, str) or not v.strip():
                    raise ConfigError(f"Invalid env var name for key {k!r}")
                env_name = v.strip()
                resolved = (env.get(env_name) or "").strip()
                target_key = str(k)[: -len("_env")] if k.endswith("_env") else str(k)[: -len("_ENV")]
                if resolved == "":
                    raise ConfigError(f"Missing required environment variable {env_name!r} (from {k!r})")
                out[target_key] = resolved
                continue
            out[k] = _resolve_env_refs(v, env=env)
        return out
    if isinstance(obj, list):
        return [_resolve_env_refs(x, env=env) for x in obj]
    return obj


def _validate_required_top_keys(cfg: dict[str, Any]) -> None:
    required = ["name", "artifacts_dir", "state", "source", "parser", "classifier", "exporter", "sink"]
    missing = [k for k in required if k not in cfg]
    if missing:
        raise ConfigError(f"Pipeline config missing required keys: {', '.join(missing)}")


@dataclass(frozen=True)
class PipelineConfig:
    raw: dict[str, Any]
    path: Path

    @property
    def name(self) -> str:
        return str(self.raw.get("name") or "").strip()


def load_pipeline(path: str | Path, *, env: dict[str, str] | None = None) -> PipelineConfig:
    p = Path(path)
    if not p.exists():
        raise ConfigError(f"Pipeline file not found: {p}")
    if not p.is_file():
        raise ConfigError(f"Pipeline path is not a file: {p}")

    try:
        data = yaml.safe_load(p.read_text(encoding="utf-8"))
    except Exception as e:
        raise ConfigError(f"Failed to parse YAML: {p}: {e}") from e

    if not isinstance(data, dict):
        raise ConfigError("Pipeline YAML must be a mapping/object at top-level")

    resolved = _resolve_env_refs(data, env=dict(env or os.environ))
    if not isinstance(resolved, dict):
        raise ConfigError("Pipeline YAML invalid after env resolution")

    _validate_required_top_keys(resolved)
    if not str(resolved.get("name") or "").strip():
        raise ConfigError("Pipeline config 'name' must be non-empty")

    return PipelineConfig(raw=resolved, path=p)


def load_pipeline_raw(path: str | Path) -> dict[str, Any]:
    """
    Load pipeline YAML without resolving env references.
    Used by `doctor` for more detailed diagnostics.
    """
    p = Path(path)
    if not p.exists():
        raise ConfigError(f"Pipeline file not found: {p}")
    if not p.is_file():
        raise ConfigError(f"Pipeline path is not a file: {p}")
    try:
        data = yaml.safe_load(p.read_text(encoding="utf-8"))
    except Exception as e:
        raise ConfigError(f"Failed to parse YAML: {p}: {e}") from e
    if not isinstance(data, dict):
        raise ConfigError("Pipeline YAML must be a mapping/object at top-level")
    return data
