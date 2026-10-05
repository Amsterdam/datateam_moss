from datetime import datetime

from unittest.mock import MagicMock

import pytest
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

from datateam_moss import spark_io_utils

@pytest.fixture
def numeric_data():
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


def test_create_stringtype_dataframe_from_list_string_conversion(spark, numeric_data):
    df = spark_io_utils.create_stringtype_dataframe_from_list(spark, numeric_data)

    # alles moet strings worden
    result = df.collect()
    assert result[0]["id"] == "1"
    assert result[0]["price"] == "99.5"


@pytest.mark.parametrize(
    "data, expected_error",
    [
        ([], ValueError),   # lege lijst
        ({}, TypeError),    # geen lijst
    ],
    ids=["empty_list", "no_list"],
)
def test_create_stringtype_dataframe_from_list_wrong_input(spark, data, expected_error):
    with pytest.raises(expected_error):
        spark_io_utils.create_stringtype_dataframe_from_list(spark, data)


# --- add_metadata_columns_to_dataframe ---

def test_add_metadata_columns_to_dataframe_check_type(spark, numeric_data):
    df = spark.createDataFrame(data=numeric_data, schema=["id", "price"])

    with pytest.raises(TypeError):
        spark_io_utils.add_metadata_columns_to_dataframe(
            df=df, m_columns={"m_bron"}, runtime=datetime.now(), bron="test"
        )


def test_add_metadata_columns_to_dataframe_runtime_and_timestamps(spark, numeric_data):
    df = spark.createDataFrame(data=numeric_data, schema=["id", "price"])
    runtime = datetime(2023, 5, 10, 8, 45)

    result = spark_io_utils.add_metadata_columns_to_dataframe(
        df, ["m_aangemaakt_op"], runtime, "test_bron"
    )

    # timestamp correct omgezet?
    first_row = result.first()
    assert str(first_row["m_aangemaakt_op"]).startswith("2023-05-10 08:45")

def create_spark_mock_with_schema(existing_schema: StructType) -> MagicMock:
    spark = MagicMock()
    spark.table.return_value.schema = existing_schema
    return spark

def test_add_new_columns_to_table_adds_missing_col_after_previous_col():
    spark = create_spark_mock_with_schema(StructType([
        StructField("id", IntegerType()),
        StructField("naam", StringType()),
    ]))
    definitie = {"columns": [
        {"name": "id", "type": "IntegerType()"},
        {"name": "email", "type": "StringType()"},
        {"name": "naam", "type": "StringType()"},
    ]}

    spark_io_utils.add_new_columns_to_table(spark, "db.klanten", definitie)

    spark.sql.assert_called_once()
    query = spark.sql.call_args.args[0]
    assert "ALTER TABLE db.klanten ADD COLUMNS" in query
    assert "`email` string AFTER `id`" in query


def test_add_new_columns_to_table_first_col_gets_first():
    spark = create_spark_mock_with_schema(StructType([StructField("naam", StringType())]))
    definitie = {"columns": [
        {"name": "id", "type": "IntegerType()"},
        {"name": "naam", "type": "StringType()"},
    ]}

    spark_io_utils.add_new_columns_to_table(spark, "db.klanten", definitie)

    assert "`id` int FIRST" in spark.sql.call_args.args[0]


def test_add_new_columns_to_table_nothing_to_add():
    spark = create_spark_mock_with_schema(StructType([StructField("id", IntegerType())]))
    definitie = {"columns": [{"name": "id", "type": "IntegerType()"}]}  # hoofdletterongevoelig

    spark_io_utils.add_new_columns_to_table(spark, "db.klanten", definitie)

    spark.sql.assert_not_called()

@pytest.fixture
def mocked_functions(monkeypatch):
    """Vervangt de twee onderliggende functies door mocks, zodat we alleen
    de beslislogica van create_or_update_table testen."""
    create_mock = MagicMock()
    add_mock = MagicMock()
    monkeypatch.setattr(spark_io_utils, "create_table_from_ddl", create_mock)
    monkeypatch.setattr(spark_io_utils, "add_new_columns_to_table", add_mock)
    return create_mock, add_mock


@pytest.fixture
def table_definition():
    return {"columns": [{"name": "id", "type": "IntegerType()"}]}


def create_spark_mock_with_table_exists(table_exists: bool) -> MagicMock:
    spark = MagicMock()
    spark.catalog.tableExists.return_value = table_exists
    return spark


def test_create_or_update_table_create_table_if_not_exists(mocked_functions, table_definition):
    create_mock, add_mock = mocked_functions
    spark = create_spark_mock_with_table_exists(table_exists=False)

    spark_io_utils.create_or_update_table(spark, "db.klanten", table_definition)

    spark.catalog.tableExists.assert_called_once_with("db.klanten")
    create_mock.assert_called_once_with(spark, "db.klanten", table_definition)
    add_mock.assert_not_called()


def test_create_or_update_table_add_columns_if_table_exists(mocked_functions, table_definition):
    create_mock, add_mock = mocked_functions
    spark = create_spark_mock_with_table_exists(table_exists=True)

    spark_io_utils.create_or_update_table(spark, "db.klanten", table_definition)

    spark.catalog.tableExists.assert_called_once_with("db.klanten")
    add_mock.assert_called_once_with(spark, "db.klanten", table_definition)
    create_mock.assert_not_called()
