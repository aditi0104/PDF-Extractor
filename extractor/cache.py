from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
from pathlib import Path
from threading import Lock
from typing import Any


@dataclass
class CacheEntry:
    key: str
    payload: dict[str, Any]


class ExtractionCache:
    def __init__(self, cache_file: Path) -> None:
        self.cache_file = cache_file
        self._lock = Lock()
        self._entries: dict[str, dict[str, Any]] = {}
        self._load()

    def get(self, key: str) -> dict[str, Any] | None:
        with self._lock:
            value = self._entries.get(key)
            return dict(value) if value is not None else None

    def set(self, key: str, payload: dict[str, Any]) -> None:
        with self._lock:
            self._entries[key] = dict(payload)
            self._persist()

    def stats(self) -> dict[str, int]:
        with self._lock:
            return {"entries": len(self._entries)}

    def _load(self) -> None:
        if not self.cache_file.exists():
            return
        try:
            self._entries = json.loads(self.cache_file.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            self._entries = {}

    def _persist(self) -> None:
        self.cache_file.parent.mkdir(parents=True, exist_ok=True)
        self.cache_file.write_text(json.dumps(self._entries, ensure_ascii=True), encoding="utf-8")


def build_cache_key(path: Path, version: str = "v1") -> str:
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    return f"{version}:{digest}"
