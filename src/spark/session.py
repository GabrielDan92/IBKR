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
    """Thin factory for building a pre-configured ``SparkSession``."""

    @classmethod
    def create(
        cls,
        app_name: str = SPARK_APP_NAME,
        master: str = SPARK_MASTER,
        shuffle_partitions: int = SPARK_SHUFFLE_PARTITIONS,
        driver_memory: str = SPARK_DRIVER_MEMORY,
    ) -> SparkSession:
        """
        Build and return a ``SparkSession``.

        Parameters
        ----------
        app_name:
            Spark application name shown in the UI / logs.
        master:
            Spark master URL.  ``local[*]`` uses all cores in the
            Docker container.
        shuffle_partitions:
            ``spark.sql.shuffle.partitions`` — tuned low for the small
            option-chain datasets we work with.
        driver_memory:
            ``spark.driver.memory`` — the single-container setup is
            driver-only, so this is the effective memory ceiling.
        """
        session = (
            SparkSession.builder
            .appName(app_name)
            .master(master)
            .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
            .config("spark.driver.memory", driver_memory)
            .config("spark.sql.session.timeZone", SPARK_TIMEZONE)
            .config("spark.ui.showConsoleProgress", "false")
            .getOrCreate()
        )
        session.sparkContext.setLogLevel(SPARK_LOG_LEVEL)
        return session
