# Nordic Power Terminal

A command-line tool for ingesting, storing, and analysing hourly spot prices from the Nordic electricity market (Norway, NO1–NO5 bidding zones).

Built with Python 3.11+, DuckDB, Pandas, and Rich.

---

## Why Nordic power markets?

The Nordic power market (Nord Pool) is one of the world's most liquid electricity exchanges. Norwegian prices vary dramatically across five bidding zones due to hydro constraints, transmission bottlenecks, and interconnections with continental Europe. This makes it an interesting domain for time-series analysis and quantitative modelling.

---

## Architecture

```
hvakosterstrommen.no API
        │
        ▼
 ┌─────────────┐
 │   Bronze    │  Raw JSON saved to data/bronze/ for audit and replay
 └──────┬──────┘
        │  clean_spot_prices()
        ▼
 ┌─────────────┐
 │   Silver    │  Validated, deduplicated, UTC-normalised DataFrame
 └──────┬──────┘
        │  upsert_spot_prices()
        ▼
 ┌─────────────┐
 │   DuckDB    │  data/npt.duckdb — columnar OLAP store
 └──────┬──────┘
        │
   ┌────┴──────────────────┐
   ▼                       ▼
query-prices           backtest / export
(Rich table)        (MAE/RMSE, CSV, Parquet)
```

The pipeline follows a [medallion architecture](https://www.databricks.com/glossary/medallion-architecture): bronze (raw) → silver (clean) → gold (analytical output).

---

## Installation

```bash
git clone https://github.com/<you>/nordic-power-terminal.git
cd nordic-power-terminal
python -m venv .venv && source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -e ".[dev]"
```

Verify:

```bash
npt version
```

---

## Quick start

```bash
# 1. Initialise local data directories
npt init

# 2. Ingest two weeks of prices for all five zones
npt ingest-prices --start 2025-01-01 --end 2025-01-14

# 3. Inspect what's in the database
npt db-summary

# 4. Query prices for a zone and date range
npt query-prices --zones NO1 --start 2025-01-01 --end 2025-01-03

# 5. Run a walk-forward backtest on NO1
npt backtest --zone NO1 --start 2025-01-01 --end 2025-01-14

# 6. Export to CSV
npt export --zones NO1,NO2 --start 2025-01-01 --end 2025-01-14 --fmt csv
```

---

## CLI reference

| Command | Description |
|---|---|
| `npt version` | Show the installed package version |
| `npt init` | Create local data directories |
| `npt ingest-prices` | Fetch hourly prices and store in DuckDB |
| `npt query-prices` | Display prices from DuckDB in a Rich table |
| `npt backtest` | Run a seasonal-naïve walk-forward backtest |
| `npt export` | Export data to CSV or Parquet |
| `npt db-summary` | Print row counts and date ranges per zone |

Run `npt <command> --help` for full option details.

### `npt ingest-prices`

```
--start    TEXT    Start date YYYY-MM-DD  [required]
--end      TEXT    End date inclusive YYYY-MM-DD  [required]
--zones    TEXT    Comma-separated zones (default: NO1,NO2,NO3,NO4,NO5)
--save-bronze      Persist raw JSON to data/bronze/  (default: true)
```

### `npt query-prices`

```
--zones    TEXT    Comma-separated zones (default: NO1)
--start    TEXT    Start date YYYY-MM-DD  [required]
--end      TEXT    End date inclusive YYYY-MM-DD  [required]
--limit    INT     Max rows to display; 0 = all (default: 48)
```

### `npt backtest`

```
--zone     TEXT    Zone to evaluate (default: NO1)
--start    TEXT    Start date YYYY-MM-DD (optional; defaults to all stored data)
--end      TEXT    End date YYYY-MM-DD   (optional)
```

### `npt export`

```
--zones    TEXT    Comma-separated zones (default: all)
--start    TEXT    Start date (optional)
--end      TEXT    End date (optional)
--fmt      TEXT    csv or parquet (default: csv)
--out      TEXT    Output path (default: auto-named under data/gold/)
```

---

## Data model

The `spot_prices` table in `data/npt.duckdb`:

| Column | Type | Description |
|---|---|---|
| `zone` | TEXT | Bidding zone (NO1–NO5) |
| `time_start` | TIMESTAMPTZ | Hour start in UTC |
| `time_end` | TIMESTAMPTZ | Hour end in UTC |
| `nok_per_kwh` | DOUBLE | Price in Norwegian kroner per kWh |
| `eur_per_kwh` | DOUBLE | Price in EUR per kWh |
| `exr` | DOUBLE | NOK/EUR exchange rate used |
| `source` | TEXT | Data source identifier |
| `ingested_at` | TIMESTAMPTZ | Timestamp of database write |

Primary key: `(zone, time_start)` — upserts are idempotent.

---

## Forecasting methodology

`npt backtest` evaluates a **seasonal-naïve** model in a walk-forward framework:

- **Forecast**: the price for hour *t* is predicted to equal the price at hour *t − 168* (same hour, 7 days earlier).
- **Walk-forward**: the model steps forward one day at a time, re-using all available history at each step. No future data leaks into training.
- **Minimum data**: 8 days (168 h lag + 24 h forecast horizon).
- **Metrics reported**: MAE and RMSE in NOK/kWh.

The seasonal-naïve model is a natural baseline for hourly power prices because electricity consumption exhibits strong weekly seasonality.

---

## Data source

Prices are fetched from the free [hvakosterstrommen.no](https://www.hvakosterstrommen.no) API, which republishes Nord Pool spot prices under an open licence. The API covers Norway's five bidding zones (NO1 Oslo, NO2 Kristiansand, NO3 Molde, NO4 Tromsø, NO5 Bergen).

---

## Development

```bash
# Run tests
pytest

# Lint
ruff check src tests

# Format
ruff format src tests
```

### Project layout

```
src/npt/
├── __init__.py
├── settings.py               # Path config (bronze/silver/gold/duckdb)
├── cli.py                    # Typer CLI entry-points
├── backtest/
│   └── walk_forward.py       # Seasonal-naïve walk-forward engine
└── data/
    ├── transform.py           # Silver-layer cleaning & anomaly flagging
    ├── connectors/
    │   └── hvakosterstrommen.py  # HTTP client for spot-price API
    └── storage/
        └── duckdb_store.py    # DuckDB read/write layer
```

---

## Licence

MIT © Birk Tyssebotn
