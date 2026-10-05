from datetime import datetime

from unittest.mock import MagicMock

import pytest
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

from datateam_moss import spark_io_utils

@pytest.fixture
def numerieke_data():
    return [
        {"id": 1, "price": 99.5},
        {"id": 2, "price": 100.0},
    ]


# --- create_stringtype_dataframe_from_list ---

def test_create_stringtype_dataframe_from_list_basic(spark):
    data = [
        {"name": "Alice", "age": "30"},
        {"name": "Bob", "age": "25"},
    ]

    df = spark_io_utils.create_stringtype_dataframe_from_list(spark, data)

    # kolomnamen moeten overeenkomen
    assert df.columns == ["name", "age"]

    # datatypes moeten allemaal StringType zijn
    assert all(isinstance(field.dataType, StringType) for field in df.schema.fields)

    # data moet correct geladen worden
    result = df.collect()
    assert result[0]["name"] == "Alice"
    assert result[1]["age"] == "25"


def test_create_stringtype_dataframe_from_list_string_conversion(spark, numerieke_data):
    df = spark_io_utils.create_stringtype_dataframe_from_list(spark, numerieke_data)

    # alles moet strings worden
    result = df.collect()
    assert result[0]["id"] == "1"
    assert result[0]["price"] == "99.5"


@pytest.mark.parametrize(
    "data, verwachte_fout",
    [
        ([], ValueError),   # lege lijst
        ({}, TypeError),    # geen lijst
    ],
    ids=["empty_list", "no_list"],
)
def test_create_stringtype_dataframe_from_list_ongeldige_input(spark, data, verwachte_fout):
    with pytest.raises(verwachte_fout):
        spark_io_utils.create_stringtype_dataframe_from_list(spark, data)


# --- add_metadata_columns_to_dataframe ---

def test_add_metadata_columns_to_dataframe_check_type(spark, numerieke_data):
    df = spark.createDataFrame(data=numerieke_data, schema=["id", "price"])

    with pytest.raises(TypeError):
        spark_io_utils.add_metadata_columns_to_dataframe(
            df=df, m_columns={"m_bron"}, runtime=datetime.now(), bron="test"
        )


def test_add_metadata_columns_to_dataframe_runtime_and_timestamps(spark, numerieke_data):
    df = spark.createDataFrame(data=numerieke_data, schema=["id", "price"])
    runtime = datetime(2023, 5, 10, 8, 45)

    result = spark_io_utils.add_metadata_columns_to_dataframe(
        df, ["m_aangemaakt_op"], runtime, "test_bron"
    )

    # timestamp correct omgezet?
    first_row = result.first()
    assert str(first_row["m_aangemaakt_op"]).startswith("2023-05-10 08:45")

def maak_spark_mock(bestaand_schema: StructType) -> MagicMock:
    spark = MagicMock()
    spark.table.return_value.schema = bestaand_schema
    return spark

def test_voegt_ontbrekende_kolom_toe_na_vorige_kolom():
    spark = maak_spark_mock(StructType([
        StructField("id", IntegerType()),
        StructField("naam", StringType()),
    ]))
    definitie = {"columns": [
        {"name": "id", "type": "IntegerType()"},
        {"name": "email", "type": "StringType()"},
        {"name": "naam", "type": "StringType()"},
    ]}

    add_new_column_to_table(spark, "db.klanten", definitie)

    spark.sql.assert_called_once()
    query = spark.sql.call_args.args[0]
    assert "ALTER TABLE db.klanten ADD COLUMNS" in query
    assert "`email` string AFTER `id`" in query


def test_eerste_kolom_krijgt_first():
    spark = maak_spark_mock(StructType([StructField("naam", StringType())]))
    definitie = {"columns": [
        {"name": "id", "type": "IntegerType()"},
        {"name": "naam", "type": "StringType()"},
    ]}

    add_new_column_to_table(spark, "db.klanten", definitie)

    assert "`id` int FIRST" in spark.sql.call_args.args[0]


def test_niets_te_doen_geen_alter_table():
    spark = maak_spark_mock(StructType([StructField("id", IntegerType())]))
    definitie = {"columns": [{"name": "ID", "type": "IntegerType()"}]}  # hoofdletterongevoelig

    add_new_column_to_table(spark, "db.klanten", definitie)

    spark.sql.assert_not_called()


# if __name__ == '__main__':
#     import sys
#     unittest.main(argv=['first-arg-is-ignored'], exit=False)