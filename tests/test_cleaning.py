import unittest
import pytest
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.types import StringType

from datateam_moss import cleaning

@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder
        .master("local[1]")
        .appName("pytest-pyspark")
        .getOrCreate()
    )


def test_to_databricks_boolean_column(spark):
    data = [("ja",), ("YES",), ("y",), ("true",), ("1",)]
    df = spark.createDataFrame(data, ["value"])

    result = df.withColumn(
        "bool_value",
        cleaning.to_databricks_boolean_column("value")
    )

    output = [row.bool_value for row in result.collect()]

    assert output == [True, True, True, True, True]