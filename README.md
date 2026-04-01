# IBKR Options Strategy Scanner

A Dockerized pipeline that connects to Interactive Brokers via IB Gateway, fetches live option chain data using `ib_async`, computes vertical spreads (credit & debit), iron condors, and short strangles using PySpark, and displays the results in a Streamlit dashboard.

## Architecture

```
┌──────────────┐      ┌──────────────┐      ┌──────────────┐
│  IB Gateway  │─TCP─▶│  Spark App   │─────▶│  Dashboard   │
│  (ib-gateway)│      │  (spark-app) │      │  (dashboard) │
│              │      │              │      │              │
│  IBC + TWS   │      │  ib_async    │      │  Streamlit   │
│  Port 4004   │      │  PySpark     │      │  Port 8501   │
└──────────────┘      └──┬───────────┘      └──┬───────────┘
                         │  Parquet files       │  reads
                         └──────────────────────┘
```

**Three services** orchestrated by Docker Compose:

| Service | Image | Purpose |
|---|---|---|
| `ib-gateway` | `ghcr.io/gnzsnz/ib-gateway:stable` | IB Gateway with IBC for automated login and 2FA handling |
| `spark-app` | Multi-stage build (Apache Spark 4.1.1) | Fetches option chains, computes strategies, writes Parquet |
| `dashboard` | Multi-stage build (Python 3.12 slim) | Streamlit app reading Parquet files for interactive analysis |

Startup is sequential: `ib-gateway` must be healthy → `spark-app` runs the pipeline → `dashboard` starts.

## What It Computes

- **Vertical Spreads** — all credit and debit combinations (Bull Put Credit, Bear Call Credit, Bull Call Debit, Bear Put Debit) with metrics: credit/debit, max profit/loss (×100 contract multiplier), spread ratio, credit yield, ROC, breakeven, % to strike/breakeven, net Greeks
- **Iron Condors** — best bull-put + best bear-call credit spread per symbol/expiration, with combined metrics
- **Short Strangles** — all OTM put + OTM call pairings with total premium, breakevens, ROC, net Greeks

## Entry Points

| File | Role |
|---|---|
| `src/main.py` | Pipeline orchestrator — connects to IBKR, fetches chains, runs strategies, writes Parquet |
| `src/dashboard/run.py` | Streamlit launcher — serves the dashboard on port 8501 |
| `src/ibkr/client.py` | IBKR market data client using `ib_async` |
| `src/strategies/spreads.py` | Vertical spread + iron condor calculations (PySpark) |
| `src/strategies/strangles.py` | Short strangle calculations (PySpark) |
| `config/constants.py` | Single source of truth for all configuration (reads from env) |

## Prerequisites

1. An [Interactive Brokers](https://www.interactivebrokers.com/) account (paper or live)
2. Docker and Docker Compose installed
3. IBKR credentials set as OS environment variables:

**macOS / Linux** — add TWS_USERID, TWS_PASSWORD to `~/.zshrc` (or `~/.bashrc`):

```bash
export variable_name="abcd"
```

Then reload: `source ~/.zshrc`

**Windows (PowerShell)** — set permanently for your user:

```powershell
[System.Environment]::SetEnvironmentVariable("TWS_USERID", "abcd", "User")
[System.Environment]::SetEnvironmentVariable("TWS_PASSWORD", "abcd", "User")
```

Then **restart your terminal** (or log out/in) for the change to take effect.

Alternatively, via **Settings → System → About → Advanced system settings → Environment Variables → User variables → New**.

## Configuration

All settings are in `.env`. Key options:

| Variable | Default | Description |
|---|---|---|
| `TRADING_MODE` | `paper` | `paper` or `live` |
| `SYMBOLS` | `AAPL` | Comma-separated stock symbols |
| `TARGET_DTE` | `45` | Target days to expiration |
| `DTE_RANGE_DAYS` | `30` | ± window around target DTE |
| `MARKET_DATA_TYPE` | `3` | 1=live, 3=delayed, 4=delayed-frozen |
| `STRIKE_RANGE_PCT` | `0.15` | Keep strikes within ±15% of spot |

## Commands

All commands run from the `docker/` directory:

```bash
cd docker
```

### Start everything (first time or full rebuild)

```bash
docker compose up --build
```

This builds the images, starts IB Gateway, waits for it to be healthy, runs the Spark pipeline, then starts the dashboard. **Approve 2FA on your phone when prompted.**

### Start everything (already built)

```bash
docker compose up
```

### Start a single service

```bash
# Re-run just the Spark pipeline (e.g. after code changes):
docker compose up spark-app --force-recreate

# Rebuild and re-run the Spark pipeline:
docker compose up spark-app --build --force-recreate

# Restart just the dashboard:
docker compose restart dashboard

# Start just IB Gateway:
docker compose up ib-gateway -d
```

### View logs

```bash
# Follow all logs:
docker compose logs -f

# Last 100 lines from spark-app:
docker compose logs spark-app --tail 100

# Follow spark-app logs in real-time:
docker compose logs spark-app -f
```

### Stop services

```bash
# Stop all services (containers stay):
docker compose stop

# Stop a single service:
docker compose stop spark-app
```

### Remove containers

```bash
# Stop and remove all containers + networks:
docker compose down

# Also remove built images:
docker compose down --rmi local

# Also remove the data volume (deletes Parquet files):
docker compose down -v
```

### Check status

```bash
docker compose ps -a
```

## Dashboard

Once running, open [http://localhost:8501](http://localhost:8501) in your browser.

**Tabs:** Vertical Spreads · Iron Condors · Strangles · Raw Chain

**Filters:** Symbol, expiration, strategy type, min spread ratio

## Development

Source code is bind-mounted into the containers (`src/` and `config/`), so code changes take effect without rebuilding:

- **Spark pipeline** — re-run with `docker compose up spark-app --force-recreate`
- **Dashboard** — changes auto-reload (Streamlit watches for file changes)
