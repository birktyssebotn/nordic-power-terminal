from __future__ import annotations

from datetime import timezone

import pandas as pd
import pytest


def _make_prices(
    n_hours: int = 24 * 10,
    zone: str = "NO1",
    base_nok: float = 0.80,
    base_eur: float = 0.07,
) -> pd.DataFrame:
    """Return a synthetic spot-price DataFrame with n_hours of hourly data."""
    idx = pd.date_range("2025-01-01", periods=n_hours, freq="h", tz=timezone.utc)
    df = pd.DataFrame(
        {
            "zone": zone,
            "time_start": idx,
            "time_end": idx + pd.Timedelta(hours=1),
            "NOK_per_kWh": base_nok + 0.1 * (pd.Series(range(n_hours)) % 24 / 24),
            "EUR_per_kWh": base_eur + 0.01 * (pd.Series(range(n_hours)) % 24 / 24),
            "EXR": 11.5,
        }
    )
    return df.reset_index(drop=True)


@pytest.fixture
def sample_prices() -> pd.DataFrame:
    return _make_prices()


@pytest.fixture
def long_prices() -> pd.DataFrame:
    """10 days of hourly data — enough for the walk-forward backtester."""
    return _make_prices(n_hours=24 * 10)
