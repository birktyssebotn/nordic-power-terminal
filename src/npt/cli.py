from __future__ import annotations

from datetime import date, datetime, timedelta

import pandas as pd
import typer
from rich import print

from npt.settings import Settings
from npt.data.connectors.hvakosterstrommen import HvakosterstrommenClient
from npt.data.storage.duckdb_store import DuckDBStore

app = typer.Typer(no_args_is_help=True)


@app.command()
def version() -> None:
    from npt import __version__

    print(f"npt {__version__}")


@app.command()
def init() -> None:
    s = Settings()
    s.ensure_dirs()
    print("[green]Initialized local data directories.[/green]")


def _parse_date(s: str) -> date:
    return datetime.strptime(s, "%Y-%m-%d").date()


@app.command("ingest-prices")
def ingest_prices(
    start: str = typer.Option(..., help="Start date (YYYY-MM-DD)"),
    end: str = typer.Option(..., help="End date inclusive (YYYY-MM-DD)"),
    zones: str = typer.Option("NO1,NO2,NO3,NO4,NO5", help="Comma-separated zones (NO1..NO5)"),
    save_bronze: bool = typer.Option(True, help="Save raw json to data/bronze"),
) -> None:
    s = Settings()
    s.ensure_dirs()

    d0 = _parse_date(start)
    d1 = _parse_date(end)
    if d1 < d0:
        raise typer.BadParameter("end must be >= start")

    zone_list = [z.strip().upper() for z in zones.split(",") if z.strip()]
    client = HvakosterstrommenClient()
    store = DuckDBStore(s.duckdb_path)

    cur = d0
    total_rows = 0

    while cur <= d1:
        for zone in zone_list:
            if save_bronze:
                client.save_bronze(cur, zone, s.bronze_dir)

            payload = client.fetch_day(cur, zone)
            df = pd.DataFrame(payload)
            if df.empty:
                continue

            df["zone"] = zone
            df["time_start"] = pd.to_datetime(df["time_start"], utc=True)
            df["time_end"] = pd.to_datetime(df["time_end"], utc=True)

            store.upsert_spot_prices(df)
            total_rows += len(df)

        cur += timedelta(days=1)

    print(f"[green]Done.[/green] Inserted/updated {total_rows} rows into {s.duckdb_path}.")