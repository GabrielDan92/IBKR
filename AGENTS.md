# AGENTS.md

## Architecture
- `src/main.py` is the source-of-truth orchestration path: `IBKRClient.get_option_chains()` → Spark DataFrame with `OPTION_CHAIN_SCHEMA` → strategy transforms → Parquet under `OUTPUT_DIR`.
- The live pipeline currently writes 8 datasets: `option_chain`, `spreads`, `iron_condors`, `strangles`, `iron_butterflies`, `calendars`, `expected_move`, `max_pain`. Prefer code over `README.md` when they differ.
- `src/ibkr/` is the only live-market boundary. Keep `src/strategies/` and `src/dashboard/` free of IB API calls.
- `src/strategies/*.py` are Spark-first transforms: filter valid rows, self/join on symbol-expiration-right, compute metrics with `pyspark.sql.functions`, then `orderBy(...)`. Examples: `calculate_spreads()` in `src/strategies/spreads.py`, `calculate_calendars()` in `src/strategies/calendars.py`.
- `src/dashboard/app.py` is read-only over Parquet via Pandas/Streamlit. If you add a new output in `src/main.py`, add a matching reader/tab here.
- `src/spark/session.py` owns the shared `SparkSession`; avoid creating ad-hoc sessions inside strategy code.
- `config/constants.py` is the central config layer and evaluates env vars at import time. Tests reload it with `importlib.reload()` after `monkeypatch.setenv()`; follow that pattern for config tests.

## Runtime boundaries and integration gotchas
- Docker Compose is the intended runtime: `docker/docker-compose.yml` sequences `ib-gateway` → `spark-app` → `dashboard` with a healthcheck and `service_completed_successfully`.
- Inside Docker, `spark-app` overrides `IB_GATEWAY_HOST=ib-gateway` and `IB_GATEWAY_PORT=4004`. `.env.example` shows `4002` for host access; do not “simplify” this without understanding the host mapping `127.0.0.1:4002:4004`.
- Credentials come from OS env (`TWS_USERID`, `TWS_PASSWORD`) plus `.env`; use `.env.example` as the template.
- `src/main.py` prepends the repo root to `sys.path` for container / Spark entrypoint compatibility. Preserve imports in the existing `from src...` / `from config...` style.

## Developer workflow
- Verified from repo root: `python3 -m pytest -q`.
- Current test status on 2026-04-14: `python3 -m pytest -q` passes (`82 passed`).
- Docker commands are expected to run from `docker/`: `docker compose up --build`, rerun pipeline with `docker compose up spark-app --force-recreate`, inspect logs with `docker compose logs spark-app -f`.
- `src/` and `config/` are bind-mounted into containers. Dashboard changes auto-reload; Spark code changes require recreating `spark-app`.

## Testing and data-shape conventions
- Strategy tests use tiny synthetic chains plus the shared `spark` fixture from `tests/conftest.py`; prefer this over mocking Spark internals.
- Preserve unit conventions carefully: many strategy outputs are contract-level (`credit`, `total_credit`, `net_debit`, `expected_move_contract`) and multiply per-share values by 100.
- Existing tests focus on numeric invariants, not snapshots. Examples: `max_profit + max_loss == width * 100` in `tests/test_strategies/test_spreads.py`, symmetric butterfly wings in `tests/test_strategies/test_butterflies.py`, and `dte_gap >= 7` in `tests/test_strategies/test_calendars.py`.

## Where to edit
- Live data fetch / pacing / qualification: `src/ibkr/client.py`.
- New or changed output columns: update `src/spark/schemas.py`, the producing strategy module, `src/main.py`, the relevant dashboard section, and tests together.
- New environment knobs: update `config/constants.py`, `.env.example`, and any Docker/README references in the same change.
- UI-only work usually stays in `src/dashboard/app.py` unless it requires a new Parquet dataset.




