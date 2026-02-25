from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any

import requests


@dataclass(frozen=True)
class HvakosterstrommenClient:
    base_url: str = "https://www.hvakosterstrommen.no/api/v1/prices"

    def build_url(self, d: date, zone: str) -> str:
        zone = zone.upper()
        if zone not in {"NO1", "NO2", "NO3", "NO4", "NO5"}:
            raise ValueError(f"Invalid zone: {zone}. Expected one of NO1..NO5.")
        return f"{self.base_url}/{d.year}/{d:%m-%d}_{zone}.json"

    def fetch_day(self, d: date, zone: str, timeout_s: int = 20) -> list[dict[str, Any]]:
        url = self.build_url(d, zone)
        r = requests.get(url, timeout=timeout_s)
        r.raise_for_status()
        data = r.json()
        if not isinstance(data, list):
            raise ValueError(f"Unexpected payload from {url}: expected list, got {type(data)}")
        return data

    def save_bronze(self, d: date, zone: str, bronze_dir: Path) -> Path:
        bronze_dir.mkdir(parents=True, exist_ok=True)
        url = self.build_url(d, zone)
        r = requests.get(url, timeout=20)
        r.raise_for_status()

        out = bronze_dir / f"hks_{d.year}_{d:%m-%d}_{zone.upper()}.json"
        out.write_bytes(r.content)
        return out