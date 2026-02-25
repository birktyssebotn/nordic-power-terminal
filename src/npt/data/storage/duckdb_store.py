from __future__ import annotations

from pathlib import Path

import duckdb
import pandas as pd


class DuckDBStore:
    def __init__(self, path: Path):
        self.path = Path(path)

    def connect(self) -> duckdb.DuckDBPyConnection:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        return duckdb.connect(str(self.path))

    def ensure_tables(self) -> None:
        with self.connect() as con:
            con.execute(
                """
                CREATE TABLE IF NOT EXISTS spot_prices (
                    zone TEXT,
                    time_start TIMESTAMPTZ,
                    time_end TIMESTAMPTZ,
                    nok_per_kwh DOUBLE,
                    eur_per_kwh DOUBLE,
                    exr DOUBLE,
                    source TEXT,
                    ingested_at TIMESTAMPTZ DEFAULT now()
                );
                """
            )

    def upsert_spot_prices(self, df: pd.DataFrame) -> None:
        required = {"zone", "time_start", "time_end", "NOK_per_kWh", "EUR_per_kWh", "EXR"}
        missing = required - set(df.columns)
        if missing:
            raise ValueError(f"Missing columns in dataframe: {sorted(missing)}")

        out = df.copy()
        out["source"] = "hvakosterstrommen"
        out = out.rename(
            columns={
                "NOK_per_kWh": "nok_per_kwh",
                "EUR_per_kWh": "eur_per_kwh",
                "EXR": "exr",
            }
        )

        with self.connect() as con:
            self.ensure_tables()

            # Remove duplicates for same (zone, time_start) then insert fresh
            con.register("tmp_prices", out)
            con.execute(
                """
                DELETE FROM spot_prices
                WHERE (zone, time_start) IN (SELECT zone, time_start FROM tmp_prices);
                """
            )
            con.execute(
                """
                INSERT INTO spot_prices (zone, time_start, time_end, nok_per_kwh, eur_per_kwh, exr, source)
                SELECT zone, time_start, time_end, nok_per_kwh, eur_per_kwh, exr, source
                FROM tmp_prices;
                """
            )
            con.unregister("tmp_prices")