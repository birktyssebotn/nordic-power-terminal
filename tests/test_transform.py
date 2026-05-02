from __future__ import annotations

from datetime import timezone

import pandas as pd
import pytest

from npt.data.transform import clean_spot_prices, flag_anomalies


class TestCleanSpotPrices:
    def test_passthrough_clean_data(self, sample_prices):
        out = clean_spot_prices(sample_prices)
        assert len(out) == len(sample_prices)

    def test_drops_null_prices(self, sample_prices):
        dirty = sample_prices.copy()
        dirty.loc[0, "NOK_per_kWh"] = None
        out = clean_spot_prices(dirty)
        assert len(out) == len(sample_prices) - 1

    def test_drops_invalid_time_interval(self, sample_prices):
        dirty = sample_prices.copy()
        dirty.loc[0, "time_end"] = dirty.loc[0, "time_start"]  # start == end
        out = clean_spot_prices(dirty)
        assert len(out) == len(sample_prices) - 1

    def test_deduplicates_by_time_start_and_zone(self, sample_prices):
        duped = pd.concat([sample_prices, sample_prices]).reset_index(drop=True)
        out = clean_spot_prices(duped)
        assert len(out) == len(sample_prices)

    def test_output_is_sorted_by_time_start(self, sample_prices):
        shuffled = sample_prices.sample(frac=1, random_state=42).reset_index(drop=True)
        out = clean_spot_prices(shuffled)
        assert out["time_start"].is_monotonic_increasing

    def test_empty_input_returns_empty(self):
        empty = pd.DataFrame(
            columns=["NOK_per_kWh", "EUR_per_kWh", "EXR", "time_start", "time_end"]
        )
        out = clean_spot_prices(empty)
        assert out.empty

    def test_missing_required_column_raises(self, sample_prices):
        bad = sample_prices.drop(columns=["NOK_per_kWh"])
        with pytest.raises(ValueError, match="Missing required columns"):
            clean_spot_prices(bad)


class TestFlagAnomalies:
    def test_no_anomaly_in_flat_series(self, sample_prices):
        flat = sample_prices.copy()
        flat["NOK_per_kWh"] = 1.0
        out = flag_anomalies(flat)
        # Flat series — std=0 → NaN → treated as False
        assert "is_anomaly" in out.columns

    def test_spike_flagged_as_anomaly(self, long_prices):
        spiked = long_prices.copy()
        spiked.loc[200, "NOK_per_kWh"] = 1000.0  # extreme spike
        out = flag_anomalies(spiked)
        assert out["is_anomaly"].any()

    def test_empty_input(self):
        out = flag_anomalies(pd.DataFrame())
        assert out.empty
