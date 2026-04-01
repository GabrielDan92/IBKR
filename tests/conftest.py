"""
Shared PySpark test fixtures.

Every test module that needs a SparkSession should use the ``spark``
fixture defined here — it creates a single session per test run and
tears it down at the end.
"""

from __future__ import annotations

import pytest
from pyspark.sql import SparkSession

from config.constants import SPARK_APP_NAME


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    """Create a lightweight local SparkSession for tests."""
    session = (
        SparkSession.builder
        .master("local[2]")
        .appName(f"{SPARK_APP_NAME}-tests")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()
