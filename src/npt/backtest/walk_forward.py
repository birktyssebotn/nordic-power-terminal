from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd


@dataclass(frozen=True)
class WalkForwardResult:
    preds: pd.DataFrame  # columns: y, yhat


def walk_forward_daily_seasonal_naive(df: pd.DataFrame, season_lag: int = 168) -> WalkForwardResult:
    """
    Walk-forward evaluation forecasting the next 24 hours each step.
    Forecast uses the same hours from `season_lag` hours earlier (default 7 days).
    df must be hourly UTC-indexed and contain column 'nok_per_kwh'.
    """
    if df.empty:
        return WalkForwardResult(preds=pd.DataFrame(columns=["y", "yhat"]))

    s = df["nok_per_kwh"].copy().asfreq("h")

    preds = []

    # Need at least 7 days history + 1 day forecast
    if len(s) < season_lag + 24:
        return WalkForwardResult(preds=pd.DataFrame(columns=["y", "yhat"]))

    # Step forward one day at a time
    for i in range(season_lag, len(s) - 24 + 1, 24):
        train = s.iloc[:i]
        test = s.iloc[i : i + 24]

        # Use the same 24 hours from 7 days earlier
        yhat = train.iloc[-season_lag : -season_lag + 24].to_numpy(dtype=float)
        y = test.to_numpy(dtype=float)

        out = pd.DataFrame({"y": y, "yhat": yhat}, index=test.index)
        preds.append(out)

    pred_df = pd.concat(preds).sort_index() if preds else pd.DataFrame(columns=["y", "yhat"])
    return WalkForwardResult(preds=pred_df)


def mae_rmse(preds: pd.DataFrame) -> tuple[float, float]:
    err = preds["y"] - preds["yhat"]
    mae = float(np.mean(np.abs(err)))
    rmse = float(np.sqrt(np.mean(err**2)))
    return mae, rmse