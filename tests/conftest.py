# tests/conftest.py
"""Gedeelde testconfiguratie.

Het pakket importeert op moduleniveau een paar Databricks-specifieke modules
die lokaal niet bestaan of verbinding maken met een workspace. Die worden hier
vervangen door nep-modules, vóórdat een test `datateam_moss` importeert.
"""
import os
import sys
import types
from unittest.mock import MagicMock

import pytest

# --- Spark moet de Python van deze venv gebruiken (nodig op Windows) ---
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

# --- Nep-dbutils: gedraagt zich zoals lokaal, dus zonder secrets ---
fake_dbutils = MagicMock()
fake_dbutils.secrets.get.side_effect = RuntimeError(
    "Geen Databricks: secrets niet beschikbaar"
)

# databricks.sdk.runtime maakt bij import verbinding met een workspace
fake_runtime = types.ModuleType("databricks.sdk.runtime")
fake_runtime.dbutils = fake_dbutils
fake_runtime.display = MagicMock()
fake_runtime.__all__ = ["dbutils", "display"]
sys.modules["databricks.sdk.runtime"] = fake_runtime

# pyspark.dbutils bestaat alleen op de Databricks-runtime
fake_pyspark_dbutils = types.ModuleType("pyspark.dbutils")
fake_pyspark_dbutils.DBUtils = MagicMock(return_value=fake_dbutils)
sys.modules["pyspark.dbutils"] = fake_pyspark_dbutils


# --- Fixtures ---

@pytest.fixture(scope="session")
def spark():
    """Eén lokale Spark-sessie voor alle tests."""
    from pyspark.sql import SparkSession

    return (
        SparkSession.builder.master("local[1]")
        .appName("datateam_moss-tests")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )