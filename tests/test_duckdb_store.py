from __future__ import annotations

from datetime import date, timezone

import pandas as pd
import pytest

from npt.data.storage.duckdb_store import DuckDBStore


@pytest.fixture
def store(tmp_path) -> DuckDBStore:
    return DuckDBStore(tmp_path / "test.duckdb")


def _write(store: DuckDBStore, df: pd.DataFrame) -> int:
    store.ensure_tables()
    return store.upsert_spot_prices(df)


class TestEnsureTables:
    def test_creates_spot_prices_table(self, store):
        store.ensure_tables()
        with store.connect() as con:
            tables = con.execute("SHOW TABLES").fetchall()
        assert any("spot_prices" in t for t in tables)

    def test_idempotent(self, store):
        store.ensure_tables()
        store.ensure_tables()  # should not raise


class TestUpsertSpotPrices:
    def test_basic_insert(self, store, sample_prices):
        rows = _write(store, sample_prices)
        assert rows == len(sample_prices)

    def test_upsert_is_idempotent(self, store, sample_prices):
        _write(store, sample_prices)
        _write(store, sample_prices)
        df = store.query_prices()
        assert len(df) == len(sample_prices)

    def test_missing_columns_raises(self, store):
        bad = pd.DataFrame({"zone": ["NO1"], "time_start": [pd.Timestamp("2025-01-01", tz="UTC")]})
        with pytest.raises(ValueError, match="Missing columns"):
            store.upsert_spot_prices(bad)

    def test_overwrites_existing_rows(self, store, sample_prices):
        _write(store, sample_prices)
        updated = sample_prices.copy()
        updated["NOK_per_kWh"] = 9.99
        _write(store, updated)
        df = store.query_prices()
        assert (df["nok_per_kwh"] == 9.99).all()


class TestQueryPrices:
    def test_returns_all_rows_by_default(self, store, sample_prices):
        _write(store, sample_prices)
        df = store.query_prices()
        assert len(df) == len(sample_prices)

    def test_filter_by_zone(self, store, sample_prices):
        # add a second zone
        other = sample_prices.copy()
        other["zone"] = "NO2"
        _write(store, sample_prices)
        _write(store, other)

        df = store.query_prices(zones=["NO1"])
        assert set(df["zone"].unique()) == {"NO1"}

    def test_filter_by_date_range(self, store, sample_prices):
        _write(store, sample_prices)
        df = store.query_prices(start=date(2025, 1, 2), end=date(2025, 1, 2))
        assert not df.empty
        utc_dates = df["time_start"].dt.tz_convert("UTC").dt.date
        assert utc_dates.min() >= date(2025, 1, 2)
        assert utc_dates.max() <= date(2025, 1, 2)

    def test_empty_result_for_missing_zone(self, store, sample_prices):
        _write(store, sample_prices)
        df = store.query_prices(zones=["NO5"])
        assert df.empty

    def test_returns_empty_df_on_fresh_db(self, store):
        df = store.query_prices()
        assert df.empty


class TestSummary:
    def test_summary_has_one_row_per_zone(self, store, sample_prices):
        _write(store, sample_prices)
        s = store.summary()
        assert len(s) == 1
        assert s["zone"].iloc[0] == "NO1"

    def test_summary_row_count(self, store, sample_prices):
        _write(store, sample_prices)
        s = store.summary()
        assert int(s["rows"].iloc[0]) == len(sample_prices)

    def test_summary_empty_on_fresh_db(self, store):
        assert store.summary().empty
