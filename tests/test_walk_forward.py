from __future__ import annotations

import pandas as pd
import pytest

from npt.backtest.walk_forward import WalkForwardResult, mae_rmse, walk_forward_daily_seasonal_naive


def _make_ts(n_hours: int, base: float = 1.0) -> pd.DataFrame:
    idx = pd.date_range("2025-01-01", periods=n_hours, freq="h", tz="UTC")
    return pd.DataFrame({"nok_per_kwh": base}, index=idx)


class TestWalkForwardSeasonalNaive:
    def test_returns_result_type(self):
        df = _make_ts(24 * 10)
        result = walk_forward_daily_seasonal_naive(df)
        assert isinstance(result, WalkForwardResult)

    def test_preds_have_y_and_yhat_columns(self):
        df = _make_ts(24 * 10)
        result = walk_forward_daily_seasonal_naive(df)
        assert "y" in result.preds.columns
        assert "yhat" in result.preds.columns

    def test_perfect_forecast_on_constant_series(self):
        df = _make_ts(24 * 10, base=2.5)
        result = walk_forward_daily_seasonal_naive(df)
        assert not result.preds.empty
        assert (result.preds["y"] == result.preds["yhat"]).all()

    def test_empty_input_returns_empty_preds(self):
        result = walk_forward_daily_seasonal_naive(pd.DataFrame(columns=["nok_per_kwh"]))
        assert result.preds.empty

    def test_too_short_returns_empty_preds(self):
        df = _make_ts(24 * 7)  # exactly 168 hours — not enough for 7-day lag + 1 day ahead
        result = walk_forward_daily_seasonal_naive(df)
        assert result.preds.empty

    def test_minimum_viable_length(self):
        df = _make_ts(24 * 8 + 1)  # just enough
        result = walk_forward_daily_seasonal_naive(df)
        assert not result.preds.empty

    def test_custom_season_lag(self):
        df = _make_ts(24 * 10)
        result = walk_forward_daily_seasonal_naive(df, season_lag=48)
        assert not result.preds.empty

    def test_preds_index_is_datetime(self):
        df = _make_ts(24 * 10)
        result = walk_forward_daily_seasonal_naive(df)
        assert isinstance(result.preds.index, pd.DatetimeIndex)


class TestMaeRmse:
    def test_zero_error_on_perfect_forecast(self):
        preds = pd.DataFrame({"y": [1.0, 2.0, 3.0], "yhat": [1.0, 2.0, 3.0]})
        mae, rmse = mae_rmse(preds)
        assert mae == pytest.approx(0.0)
        assert rmse == pytest.approx(0.0)

    def test_known_values(self):
        preds = pd.DataFrame({"y": [1.0, 3.0], "yhat": [0.0, 0.0]})
        mae, rmse = mae_rmse(preds)
        assert mae == pytest.approx(2.0)
        assert rmse == pytest.approx((1.0**2 + 3.0**2) ** 0.5 / 2.0**0.5, rel=1e-6)

    def test_rmse_ge_mae(self):
        preds = pd.DataFrame({"y": [1.0, 2.0, 3.0, 10.0], "yhat": [1.0, 2.0, 3.0, 0.0]})
        mae, rmse = mae_rmse(preds)
        assert rmse >= mae
