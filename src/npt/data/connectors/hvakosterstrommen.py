from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any

import requests

VALID_ZONES = frozenset({"NO1", "NO2", "NO3", "NO4", "NO5"})


@dataclass(frozen=True)
class HvakosterstrommenClient:
    base_url: str = "https://www.hvakosterstrommen.no/api/v1/prices"

    def build_url(self, d: date, zone: str) -> str:
        zone = zone.upper()
        if zone not in VALID_ZONES:
            raise ValueError(f"Invalid zone: {zone!r}. Expected one of {sorted(VALID_ZONES)}.")
        return f"{self.base_url}/{d.year}/{d:%m-%d}_{zone}.json"

    def fetch_day(self, d: date, zone: str, timeout_s: int = 20) -> list[dict[str, Any]]:
        url = self.build_url(d, zone)
        r = requests.get(url, timeout=timeout_s)
        r.raise_for_status()
        data = r.json()
        if not isinstance(data, list):
            raise ValueError(f"Unexpected payload from {url}: expected list, got {type(data)}")
        return data

    def save_bronze(self, d: date, zone: str, bronze_dir: Path, data: list[dict[str, Any]]) -> Path:
        """Persist already-fetched data to the bronze layer as JSON."""
        bronze_dir.mkdir(parents=True, exist_ok=True)
        out = bronze_dir / f"hks_{d.year}_{d:%m-%d}_{zone.upper()}.json"
        out.write_text(json.dumps(data, indent=2), encoding="utf-8")
        return out
