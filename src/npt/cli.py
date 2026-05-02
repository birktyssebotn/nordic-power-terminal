from __future__ import annotations

from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Optional

import pandas as pd
import requests
import typer
from rich import print
from rich.console import Console
from rich.progress import BarColumn, MofNCompleteColumn, Progress, SpinnerColumn, TextColumn
from rich.table import Table

from npt.settings import Settings
from npt.data.connectors.hvakosterstrommen import HvakosterstrommenClient
from npt.data.storage.duckdb_store import DuckDBStore
from npt.data.transform import clean_spot_prices

app = typer.Typer(
    no_args_is_help=True,
    help="Nordic Power Terminal — ingest, query, and analyse Nordic electricity spot prices.",
)
console = Console()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _parse_date(s: str) -> date:
    try:
        return datetime.strptime(s, "%Y-%m-%d").date()
    except ValueError:
        raise typer.BadParameter(f"Expected YYYY-MM-DD, got {s!r}")


def _date_range(start: date, end: date) -> list[date]:
    days = []
    cur = start
    while cur <= end:
        days.append(cur)
        cur += timedelta(days=1)
    return days


def _rich_df_table(df: pd.DataFrame, title: str) -> Table:
    table = Table(title=title, show_lines=False, header_style="bold cyan")
    for col in df.columns:
        table.add_column(str(col), no_wrap=True)
    for _, row in df.iterrows():
        table.add_row(*[str(v) for v in row])
    return table


# ---------------------------------------------------------------------------
# Commands
# ---------------------------------------------------------------------------


@app.command()
def version() -> None:
    """Print the installed package version."""
    from npt import __version__

    print(f"npt {__version__}")


@app.command()
def init() -> None:
    """Initialise local data directories (bronze / silver / gold)."""
    s = Settings()
    s.ensure_dirs()
    print("[green]Initialized local data directories.[/green]")
    print(f"  bronze → {s.bronze_dir}")
    print(f"  silver → {s.silver_dir}")
    print(f"  gold   → {s.gold_dir}")
    print(f"  db     → {s.duckdb_path}")


@app.command("ingest-prices")
def ingest_prices(
    start: str = typer.Option(..., help="Start date (YYYY-MM-DD)"),
    end: str = typer.Option(..., help="End date inclusive (YYYY-MM-DD)"),
    zones: str = typer.Option("NO1,NO2,NO3,NO4,NO5", help="Comma-separated zones"),
    save_bronze: bool = typer.Option(True, help="Persist raw JSON to data/bronze/"),
) -> None:
    """Fetch hourly spot prices from hvakosterstrommen.no and store in DuckDB."""
    s = Settings()
    s.ensure_dirs()

    d0 = _parse_date(start)
    d1 = _parse_date(end)
    if d1 < d0:
        raise typer.BadParameter("--end must be >= --start")

    zone_list = [z.strip().upper() for z in zones.split(",") if z.strip()]
    days = _date_range(d0, d1)
    client = HvakosterstrommenClient()
    store = DuckDBStore(s.duckdb_path)
    total_rows = 0
    errors: list[str] = []

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        MofNCompleteColumn(),
        console=console,
        transient=True,
    ) as progress:
        task = progress.add_task("Ingesting…", total=len(days) * len(zone_list))

        for day in days:
            for zone in zone_list:
                progress.update(task, description=f"{day} {zone}")
                try:
                    payload = client.fetch_day(day, zone)
                except requests.HTTPError as exc:
                    errors.append(f"{day} {zone}: HTTP {exc.response.status_code}")
                    progress.advance(task)
                    continue
                except requests.RequestException as exc:
                    errors.append(f"{day} {zone}: {exc}")
                    progress.advance(task)
                    continue

                if save_bronze:
                    client.save_bronze(day, zone, s.bronze_dir, payload)

                df = pd.DataFrame(payload)
                if df.empty:
                    progress.advance(task)
                    continue

                df["zone"] = zone
                df = clean_spot_prices(df)
                total_rows += store.upsert_spot_prices(df)
                progress.advance(task)

    if errors:
        print(f"[yellow]Completed with {len(errors)} error(s):[/yellow]")
        for e in errors:
            print(f"  [red]✗[/red] {e}")
    else:
        print("[green]✓ All requests succeeded.[/green]")

    print(f"Inserted/updated [bold]{total_rows}[/bold] rows → {s.duckdb_path}")


@app.command("query-prices")
def query_prices(
    zones: str = typer.Option("NO1", help="Comma-separated zones"),
    start: str = typer.Option(..., help="Start date (YYYY-MM-DD)"),
    end: str = typer.Option(..., help="End date inclusive (YYYY-MM-DD)"),
    limit: int = typer.Option(48, help="Max rows to display (0 = all)"),
) -> None:
    """Display spot prices from the local DuckDB database."""
    s = Settings()
    store = DuckDBStore(s.duckdb_path)

    zone_list = [z.strip().upper() for z in zones.split(",") if z.strip()]
    d0 = _parse_date(start)
    d1 = _parse_date(end)
    if d1 < d0:
        raise typer.BadParameter("--end must be >= --start")

    df = store.query_prices(zones=zone_list, start=d0, end=d1)

    if df.empty:
        print("[yellow]No data found. Have you run 'npt ingest-prices'?[/yellow]")
        raise typer.Exit(1)

    display = df.head(limit) if limit > 0 else df
    display = display.copy()
    display["time_start"] = display["time_start"].dt.strftime("%Y-%m-%d %H:%M")
    display["nok_per_kwh"] = display["nok_per_kwh"].map("{:.4f}".format)
    display["eur_per_kwh"] = display["eur_per_kwh"].map("{:.4f}".format)

    table = _rich_df_table(
        display[["zone", "time_start", "nok_per_kwh", "eur_per_kwh"]],
        title=f"Spot prices — {', '.join(zone_list)}  ({start} → {end})",
    )
    console.print(table)

    if limit > 0 and len(df) > limit:
        print(f"[dim]Showing {limit} of {len(df)} rows. Use --limit 0 to show all.[/dim]")


@app.command()
def backtest(
    zone: str = typer.Option("NO1", help="Zone to evaluate"),
    start: Optional[str] = typer.Option(None, help="Start date (YYYY-MM-DD)"),
    end: Optional[str] = typer.Option(None, help="End date inclusive (YYYY-MM-DD)"),
) -> None:
    """
    Run a seasonal-naïve walk-forward backtest on stored prices.

    The model forecasts the next 24 hours using the same 24 hours from
    7 days earlier (168-hour seasonal lag). At least 8 days of data required.
    """
    from npt.backtest.walk_forward import mae_rmse, walk_forward_daily_seasonal_naive

    s = Settings()
    store = DuckDBStore(s.duckdb_path)

    d0 = _parse_date(start) if start else None
    d1 = _parse_date(end) if end else None
    if d0 and d1 and d1 < d0:
        raise typer.BadParameter("--end must be >= --start")

    df = store.query_prices(zones=[zone.upper()], start=d0, end=d1)

    if df.empty:
        print("[yellow]No data found. Run 'npt ingest-prices' first.[/yellow]")
        raise typer.Exit(1)

    df = df.set_index("time_start").sort_index()

    result = walk_forward_daily_seasonal_naive(df)

    if result.preds.empty:
        print(
            "[yellow]Not enough data to backtest — need at least 8 days of hourly observations.[/yellow]"
        )
        raise typer.Exit(1)

    mae, rmse = mae_rmse(result.preds)
    n_forecasts = len(result.preds) // 24

    table = Table(title=f"Seasonal-Naïve Walk-Forward Backtest — {zone.upper()}", header_style="bold cyan")
    table.add_column("Metric", style="bold")
    table.add_column("Value", justify="right")
    table.add_row("Zone", zone.upper())
    table.add_row("Forecast periods (days)", str(n_forecasts))
    table.add_row("Total hours evaluated", str(len(result.preds)))
    table.add_row("MAE  (NOK/kWh)", f"{mae:.5f}")
    table.add_row("RMSE (NOK/kWh)", f"{rmse:.5f}")
    console.print(table)

    first_date = result.preds.index.min().date()
    last_date = result.preds.index.max().date()
    print(f"[dim]Evaluation window: {first_date} → {last_date}[/dim]")


@app.command()
def export(
    zones: str = typer.Option("NO1,NO2,NO3,NO4,NO5", help="Comma-separated zones"),
    start: Optional[str] = typer.Option(None, help="Start date (YYYY-MM-DD)"),
    end: Optional[str] = typer.Option(None, help="End date inclusive (YYYY-MM-DD)"),
    fmt: str = typer.Option("csv", help="Output format: csv or parquet"),
    out: Optional[str] = typer.Option(None, help="Output file path (default: auto-named)"),
) -> None:
    """Export spot prices to CSV or Parquet."""
    s = Settings()
    store = DuckDBStore(s.duckdb_path)

    zone_list = [z.strip().upper() for z in zones.split(",") if z.strip()]
    d0 = _parse_date(start) if start else None
    d1 = _parse_date(end) if end else None

    df = store.query_prices(zones=zone_list, start=d0, end=d1)

    if df.empty:
        print("[yellow]No data found. Run 'npt ingest-prices' first.[/yellow]")
        raise typer.Exit(1)

    fmt = fmt.lower()
    if fmt not in {"csv", "parquet"}:
        raise typer.BadParameter("--fmt must be 'csv' or 'parquet'")

    if out:
        dest = Path(out)
    else:
        suffix = "csv" if fmt == "csv" else "parquet"
        zone_tag = "-".join(zone_list)
        date_tag = f"{start or 'all'}_{end or 'all'}"
        dest = s.gold_dir / f"spot_prices_{zone_tag}_{date_tag}.{suffix}"

    dest.parent.mkdir(parents=True, exist_ok=True)

    if fmt == "csv":
        df.to_csv(dest, index=False)
    else:
        df.to_parquet(dest, index=False)

    print(f"[green]✓[/green] Exported [bold]{len(df)}[/bold] rows → {dest}")


@app.command("db-summary")
def db_summary() -> None:
    """Print a summary of all data stored in the local DuckDB database."""
    s = Settings()
    store = DuckDBStore(s.duckdb_path)
    df = store.summary()

    if df.empty:
        print("[yellow]Database is empty. Run 'npt ingest-prices' to populate it.[/yellow]")
        raise typer.Exit(1)

    table = _rich_df_table(df, title=f"DuckDB Summary — {s.duckdb_path}")
    console.print(table)
