from __future__ import annotations

import pandas as pd

REQUIRED_COLS = {"NOK_per_kWh", "EUR_per_kWh", "EXR", "time_start", "time_end"}
PRICE_COLS = ["NOK_per_kWh", "EUR_per_kWh"]


def clean_spot_prices(df: pd.DataFrame) -> pd.DataFrame:
    """
    Silver-layer cleaning for raw spot-price records from the HvakosterStrommen API.

    Rules applied:
    - Drop rows missing price data.
    - Parse time columns to UTC-aware timestamps.
    - Drop rows where time_start >= time_end (malformed interval).
    - Remove exact duplicates on (time_start, zone) if zone column present.
    - Sort by time_start and reset index.
    """
    if df.empty:
        return df.copy()

    missing = REQUIRED_COLS - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns: {sorted(missing)}")

    out = df.copy()

    out["time_start"] = pd.to_datetime(out["time_start"], utc=True)
    out["time_end"] = pd.to_datetime(out["time_end"], utc=True)

    out = out.dropna(subset=PRICE_COLS)

    valid_interval = out["time_start"] < out["time_end"]
    out = out[valid_interval]

    dedup_cols = ["time_start", "zone"] if "zone" in out.columns else ["time_start"]
    out = out.drop_duplicates(subset=dedup_cols)

    return out.sort_values("time_start").reset_index(drop=True)


def flag_anomalies(df: pd.DataFrame, price_col: str = "NOK_per_kWh") -> pd.DataFrame:
    """
    Add a boolean 'is_anomaly' column flagging hours more than 3 std-devs from
    the rolling 7-day mean. Useful for data-quality monitoring.
    """
    if df.empty or price_col not in df.columns:
        return df.assign(is_anomaly=False)

    out = df.copy()
    rolling_mean = out[price_col].rolling(window=168, min_periods=24, center=False).mean()
    rolling_std = out[price_col].rolling(window=168, min_periods=24, center=False).std()
    z = (out[price_col] - rolling_mean).abs() / rolling_std.replace(0, float("nan"))
    out["is_anomaly"] = z > 3.0
    return out
