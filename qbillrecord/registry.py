from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable


Factory = Callable[[dict[str, Any]], Any]


@dataclass(frozen=True)
class Registered:
    kind: str
    type_id: str
    factory: Factory
    description: str


class Registry:
    def __init__(self) -> None:
        self._items: dict[str, dict[str, Registered]] = {}

    def register(self, *, kind: str, type_id: str, factory: Factory, description: str = "") -> None:
        kind = (kind or "").strip()
        type_id = (type_id or "").strip()
        if not kind:
            raise ValueError("kind must be non-empty")
        if not type_id:
            raise ValueError("type_id must be non-empty")
        self._items.setdefault(kind, {})
        if type_id in self._items[kind]:
            raise ValueError(f"Duplicate registration: {kind}:{type_id}")
        self._items[kind][type_id] = Registered(kind=kind, type_id=type_id, factory=factory, description=description)

    def kinds(self) -> list[str]:
        return sorted(self._items.keys())

    def types_for(self, kind: str) -> list[Registered]:
        return sorted(self._items.get(kind, {}).values(), key=lambda r: r.type_id)

    def create(self, *, kind: str, type_id: str, config: dict[str, Any]) -> Any:
        try:
            reg = self._items[kind][type_id]
        except Exception as e:
            raise KeyError(f"Unknown step type: {kind}:{type_id}") from e
        return reg.factory(config)


REGISTRY = Registry()

