from __future__ import annotations

from datetime import date
from pathlib import Path

import duckdb
import pandas as pd


class DuckDBStore:
    def __init__(self, path: Path) -> None:
        self.path = Path(path)

    def connect(self) -> duckdb.DuckDBPyConnection:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        return duckdb.connect(str(self.path))

    def ensure_tables(self) -> None:
        with self.connect() as con:
            con.execute(
                """
                CREATE TABLE IF NOT EXISTS spot_prices (
                    zone        TEXT          NOT NULL,
                    time_start  TIMESTAMPTZ   NOT NULL,
                    time_end    TIMESTAMPTZ   NOT NULL,
                    nok_per_kwh DOUBLE,
                    eur_per_kwh DOUBLE,
                    exr         DOUBLE,
                    source      TEXT,
                    ingested_at TIMESTAMPTZ   DEFAULT now(),
                    PRIMARY KEY (zone, time_start)
                );
                """
            )

    def upsert_spot_prices(self, df: pd.DataFrame) -> int:
        """Idempotent upsert by (zone, time_start). Returns number of rows written."""
        required = {"zone", "time_start", "time_end", "NOK_per_kWh", "EUR_per_kWh", "EXR"}
        missing = required - set(df.columns)
        if missing:
            raise ValueError(f"Missing columns: {sorted(missing)}")

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

        return len(out)

    def query_prices(
        self,
        zones: list[str] | None = None,
        start: date | None = None,
        end: date | None = None,
    ) -> pd.DataFrame:
        """Return spot prices filtered by zone and/or date range (inclusive)."""
        conditions: list[str] = []
        params: list[object] = []

        if zones:
            placeholders = ", ".join("?" for _ in zones)
            conditions.append(f"zone IN ({placeholders})")
            params.extend(zones)
        if start:
            conditions.append("time_start >= ?")
            params.append(pd.Timestamp(start, tz="UTC"))
        if end:
            conditions.append("time_start < ?")
            params.append(pd.Timestamp(end, tz="UTC") + pd.Timedelta(days=1))

        where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
        sql = f"""
            SELECT zone, time_start, time_end, nok_per_kwh, eur_per_kwh, exr, source
            FROM spot_prices
            {where}
            ORDER BY zone, time_start
        """

        with self.connect() as con:
            self.ensure_tables()
            return con.execute(sql, params).df()

    def summary(self) -> pd.DataFrame:
        """Row counts, date ranges, and average prices per zone."""
        sql = """
            SELECT
                zone,
                COUNT(*)                        AS rows,
                MIN(time_start)::DATE           AS first_obs,
                MAX(time_start)::DATE           AS last_obs,
                ROUND(AVG(nok_per_kwh), 4)      AS avg_nok_per_kwh,
                ROUND(AVG(eur_per_kwh), 4)      AS avg_eur_per_kwh
            FROM spot_prices
            GROUP BY zone
            ORDER BY zone
        """
        with self.connect() as con:
            self.ensure_tables()
            return con.execute(sql).df()
