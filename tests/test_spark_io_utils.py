import pytest
import unittest
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.types import StringType

from datateam_moss import spark_io_utils

spark = SparkSession.builder.getOrCreate()

class TestGetData(unittest.TestCase):

    def test_create_stringtype_dataframe_from_list_basic(self):
        data = [
            {"name": "Alice", "age": "30"},
            {"name": "Bob", "age": "25"}
        ]

        df = spark_io_utils.create_stringtype_dataframe_from_list(spark, data)

        # kolomnamen moeten overeenkomen
        assert df.columns == ["name", "age"]

        # datatypes moeten allemaal StringType zijn
        for field in df.schema.fields:
            assert isinstance(field.dataType, StringType)

        # data moet correct geladen worden
        result = df.collect()
        assert result[0]["name"] == "Alice"
        assert result[1]["age"] == "25"


    def test_create_stringtype_dataframe_from_list_string_conversion(self):
        data = [
            {"id": 1, "price": 99.5},
            {"id": 2, "price": 100.0}
        ]

        df = spark_io_utils.create_stringtype_dataframe_from_list(spark, data)

        # alles moet strings worden
        result = df.collect()
        assert result[0]["id"] == "1"
        assert result[0]["price"] == "99.5"

    def test_create_stringtype_dataframe_from_list_empty_list(self):
        data = []

        with pytest.raises(ValueError):
            spark_io_utils.create_stringtype_dataframe_from_list(spark, data)

    def test_create_stringtype_dataframe_from_list_no_list(self):
        data = {}

        with pytest.raises(TypeError):
            spark_io_utils.create_stringtype_dataframe_from_list(spark, data)
        
    def test_add_metadata_columns_to_dataframe_check_type(self):
        data = [
            {"id": 1, "price": 99.5},
            {"id": 2, "price": 100.0}
        ]

        df = spark.createDataFrame(data=data, schema=["id", "price"])

        with pytest.raises(TypeError):
            spark_io_utils.add_metadata_columns_to_dataframe(df=df, m_columns={"m_bron"}, runtime=datetime.now(), bron= "test")

    def test_add_metadata_columns_to_dataframe_runtime_and_timestamps(self):
        data = [
            {"id": 1, "price": 99.5},
            {"id": 2, "price": 100.0}
        ]

        df = spark.createDataFrame(data=data, schema=["id", "price"])

        runtime = datetime(2023, 5, 10, 8, 45)

        result = spark_io_utils.add_metadata_columns_to_dataframe(
            df,
            ["m_aangemaakt_op"],
            runtime,
            "test_bron"
        )

        # timestamp converted correct?
        first_row = result.first()
        assert str(first_row["m_aangemaakt_op"]).startswith("2023-05-10 08:45")
            

if __name__ == '__main__':
    import sys
    unittest.main(argv=['first-arg-is-ignored'], exit=False)