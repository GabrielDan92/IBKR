"""
PySpark session factory.

Centralises SparkSession creation so every module in the project shares
the same configuration.  The ``create()`` class method is the only public
entry point — call it once in ``main.py`` and pass the session around.
"""

from __future__ import annotations

from pyspark.sql import SparkSession

from config.constants import (
    SPARK_APP_NAME,
    SPARK_DRIVER_MEMORY,
    SPARK_LOG_LEVEL,
    SPARK_MASTER,
    SPARK_SHUFFLE_PARTITIONS,
    SPARK_TIMEZONE,
)


class SparkSessionFactory:
    """Singleton wrapper around ``SparkSession``.

    The session is created once on first access via ``get()`` and reused
    for every subsequent call.  Call ``stop()`` to tear it down.
    """

    _instance: SparkSession | None = None

    @classmethod
    def get(cls) -> SparkSession:
        """Return the shared ``SparkSession``, creating it on first call."""
        if cls._instance is None or cls._instance._jsc is None:
            session = (
                SparkSession.builder
                .appName(SPARK_APP_NAME)
                .master(SPARK_MASTER)
                .config("spark.sql.shuffle.partitions", str(SPARK_SHUFFLE_PARTITIONS))
                .config("spark.driver.memory", SPARK_DRIVER_MEMORY)
                .config("spark.sql.session.timeZone", SPARK_TIMEZONE)
                .config("spark.ui.showConsoleProgress", "false")
                .getOrCreate()
            )
            session.sparkContext.setLogLevel(SPARK_LOG_LEVEL)
            cls._instance = session
        return cls._instance

    @classmethod
    def stop(cls) -> None:
        """Stop the shared session if it exists."""
        if cls._instance is not None:
            cls._instance.stop()
            cls._instance = None
