"""
Application entry point — orchestrates the full pipeline.

1. Load configuration from environment / .env
2. Connect to IB Gateway via ``IBKRClient``
3. Fetch live option chains for the configured symbols
4. Create a PySpark session
5. Convert raw data to a Spark DataFrame
6. Run spread, iron-condor, and strangle calculations
7. Display results to console and persist as Parquet
"""

from __future__ import annotations

import asyncio
import logging
import os
import sys

# ── make project root importable when run via spark-submit ───────────
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from config.constants import (
    DTE_RANGE_DAYS,
    OUTPUT_DIR,
    SYMBOLS,
    TARGET_DTE,
)
from src.ibkr.client import IBKRClient
from src.spark.schemas import OPTION_CHAIN_SCHEMA
from src.spark.session import SparkSessionFactory
from src.strategies.spreads import calculate_iron_condors, calculate_spreads
from src.strategies.strangles import calculate_strangles

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


async def run() -> None:
    """Async pipeline orchestrator."""
    logger.info(
        "Starting IBKR options pipeline — symbols=%s  target_dte=%d",
        SYMBOLS,
        TARGET_DTE,
    )

    # ── 1. Fetch option chain data from IBKR ────────────────────────
    client = IBKRClient()

    try:
        await client.connect()

        raw_data = await client.get_option_chains(
            SYMBOLS,
            target_dte=TARGET_DTE,
            dte_range_days=DTE_RANGE_DAYS,
        )

        if not raw_data:
            logger.error("No option chain data received — exiting")
            return

        logger.info("Fetched %d option rows across all symbols", len(raw_data))

        # ── 2. Build Spark DataFrame ──
        spark = SparkSessionFactory.get()

        chain_df = spark.createDataFrame(raw_data, schema=OPTION_CHAIN_SCHEMA)
        chain_df.cache()

        # ── 3. Persist raw chain ────────────────────────────────────
        os.makedirs(OUTPUT_DIR, exist_ok=True)

        chain_path = os.path.join(OUTPUT_DIR, "option_chain")
        chain_df.write.mode("overwrite").parquet(chain_path)
        logger.info("Raw option chain written to %s", chain_path)

        # ── 4. Credit / debit spreads ───────────────────────────────
        logger.info("Calculating vertical spreads …")
        spreads_df = calculate_spreads(chain_df)
        spreads_path = os.path.join(OUTPUT_DIR, "spreads")
        spreads_df.write.mode("overwrite").parquet(spreads_path)
        logger.info("Spreads written to %s", spreads_path)

        # ── 5. Iron condors ─────────────────────────────────────────
        logger.info("Calculating iron condors …")
        condors_df = calculate_iron_condors(spreads_df)
        condors_path = os.path.join(OUTPUT_DIR, "iron_condors")
        condors_df.write.mode("overwrite").parquet(condors_path)
        logger.info("Iron condors written to %s", condors_path)

        # ── 6. Strangles ────────────────────────────────────────────
        logger.info("Calculating strangles …")
        strangles_df = calculate_strangles(chain_df)

        strangles_path = os.path.join(OUTPUT_DIR, "strangles")
        strangles_df.write.mode("overwrite").parquet(strangles_path)
        logger.info("Strangles written to %s", strangles_path)

        logger.info("Pipeline complete ✓")

    except ConnectionError as e:
        logger.error(str(e))
        return
    except Exception:
        logger.exception("Pipeline failed")
        raise
    finally:
        await client.disconnect()
        SparkSessionFactory.stop()


def main() -> None:
    """Synchronous wrapper for ``spark-submit`` compatibility."""
    asyncio.run(run())


if __name__ == "__main__":
    main()
