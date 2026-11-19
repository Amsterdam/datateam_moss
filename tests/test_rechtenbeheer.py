from unittest.mock import MagicMock, call

import pytest

from datateam_moss import rechtenbeheer


def create_spark_mock_with_collect_result(rows):
    spark_mock = MagicMock()

    result_mock = MagicMock()
    result_mock.collect.return_value = rows

    spark_mock.sql.return_value = result_mock

    return spark_mock


def patch_logger(monkeypatch):
    logger_mock = MagicMock()

    monkeypatch.setattr(
        rechtenbeheer,
        "logger",
        logger_mock,
        raising=False,
    )

    return logger_mock


def patch_perform_sql(monkeypatch):
    perform_sql_mock = MagicMock()

    monkeypatch.setattr(
        rechtenbeheer,
        "_perform_sql",
        perform_sql_mock,
        raising=False,
    )

    return perform_sql_mock


# Test dat _perform_sql bij debug=True en execute=False alleen naar het bestand schrijft.
def test_perform_sql_writes_file(monkeypatch):
    spark_mock = MagicMock()
    logger_mock = patch_logger(monkeypatch)

    sql_statement = "GRANT SELECT ON SCHEMA test.schema TO `group`"
    file_handle = MagicMock()

    rechtenbeheer._perform_sql(
        spark=spark_mock,
        sql_statement=sql_statement,
        file_handle=file_handle,
        debug=True,
        execute=False,
    )

    file_handle.write.assert_called_once_with(
        "GRANT SELECT ON SCHEMA test.schema TO `group`;\n"
    )

    spark_mock.sql.assert_not_called()
    logger_mock.info.assert_not_called()


# Test dat _perform_sql een fout geeft als file_handle ontbreekt in debug-only mode.
def test_perform_sql_needs_file_handle(monkeypatch):
    spark_mock = MagicMock()
    logger_mock = patch_logger(monkeypatch)

    with pytest.raises(ValueError, match="file_handle is vereist"):
        rechtenbeheer._perform_sql(
            spark=spark_mock,
            sql_statement="SELECT 1",
            file_handle=None,
            debug=True,
            execute=False,
        )

    spark_mock.sql.assert_not_called()
    logger_mock.info.assert_not_called()
    logger_mock.error.assert_not_called()


# Test dat _perform_sql schrijft én uitvoert bij debug=True en execute=True.
def test_perform_sql_writes_and_runs(monkeypatch):
    spark_mock = MagicMock()
    logger_mock = patch_logger(monkeypatch)

    sql_statement = "SELECT 1"
    file_handle = MagicMock()

    rechtenbeheer._perform_sql(
        spark=spark_mock,
        sql_statement=sql_statement,
        file_handle=file_handle,
        debug=True,
        execute=True,
    )

    file_handle.write.assert_called_once_with(
        "SELECT 1;\n"
    )
    spark_mock.sql.assert_called_once_with("SELECT 1")
    logger_mock.info.assert_called_once_with("Uitgevoerd: SELECT 1")
    logger_mock.error.assert_not_called()


# Test dat _perform_sql zonder file_handle wel uitvoert bij debug=True en execute=True.
def test_perform_sql_runs_without_file(monkeypatch):
    spark_mock = MagicMock()
    logger_mock = patch_logger(monkeypatch)

    rechtenbeheer._perform_sql(
        spark=spark_mock,
        sql_statement="SELECT 1",
        file_handle=None,
        debug=True,
        execute=True,
    )

    spark_mock.sql.assert_called_once_with("SELECT 1")
    logger_mock.info.assert_called_once_with("Uitgevoerd: SELECT 1")
    logger_mock.error.assert_not_called()


# Test dat _perform_sql bij debug=False en execute=True alleen uitvoert.
def test_perform_sql_runs_only(monkeypatch):
    spark_mock = MagicMock()
    logger_mock = patch_logger(monkeypatch)
    file_handle = MagicMock()

    rechtenbeheer._perform_sql(
        spark=spark_mock,
        sql_statement="SELECT 1",
        file_handle=file_handle,
        debug=False,
        execute=True,
    )

    file_handle.write.assert_not_called()
    spark_mock.sql.assert_called_once_with("SELECT 1")
    logger_mock.info.assert_called_once_with("Uitgevoerd: SELECT 1")
    logger_mock.error.assert_not_called()


# Test het huidige gedrag bij debug=False en execute=False.
def test_perform_sql_runs_by_default(monkeypatch):
    spark_mock = MagicMock()
    logger_mock = patch_logger(monkeypatch)
    file_handle = MagicMock()

    rechtenbeheer._perform_sql(
        spark=spark_mock,
        sql_statement="SELECT 1",
        file_handle=file_handle,
        debug=False,
        execute=False,
    )

    file_handle.write.assert_not_called()
    spark_mock.sql.assert_called_once_with("SELECT 1")
    logger_mock.info.assert_called_once_with("Uitgevoerd: SELECT 1")
    logger_mock.error.assert_not_called()


# Test dat _perform_sql fouten van spark.sql logt en opnieuw doorgeeft.
def test_perform_sql_logs_error(monkeypatch):
    spark_mock = MagicMock()
    logger_mock = patch_logger(monkeypatch)

    spark_mock.sql.side_effect = Exception("Spark error")

    with pytest.raises(Exception, match="Spark error"):
        rechtenbeheer._perform_sql(
            spark=spark_mock,
            sql_statement="SELECT 1",
            debug=False,
            execute=True,
        )

    spark_mock.sql.assert_called_once_with("SELECT 1")
    logger_mock.error.assert_called_once()


# Test revoke_grants voor normale scenario's.
@pytest.mark.parametrize(
    "grants_rows, users_to_ignore, expected_revoke_statements",
    [
        (
            [
                {"ActionType": "SELECT", "Principal": "group-a"},
                {"ActionType": "USE SCHEMA", "Principal": "group-b"},
            ],
            [],
            [
                "REVOKE SELECT ON SCHEMA catalog.schema FROM `group-a`",
                "REVOKE USE SCHEMA ON SCHEMA catalog.schema FROM `group-b`",
            ],
        ),
        (
            [
                {"ActionType": "SELECT", "Principal": "group-a"},
                {"ActionType": "SELECT", "Principal": "group-b"},
            ],
            ["group-a"],
            [
                "REVOKE SELECT ON SCHEMA catalog.schema FROM `group-b`",
            ],
        ),
        (
            [
                {"ActionType": "SELECT", "Principal": "group-a"},
                {"ActionType": "USE SCHEMA", "Principal": "group-b"},
            ],
            ["group-a", "group-b"],
            [],
        ),
        (
            [],
            [],
            [],
        ),
    ],
)
def test_revoke_grants(
    monkeypatch,
    grants_rows,
    users_to_ignore,
    expected_revoke_statements,
):
    spark_mock = create_spark_mock_with_collect_result(grants_rows)
    logger_mock = patch_logger(monkeypatch)
    perform_sql_mock = patch_perform_sql(monkeypatch)
    file_handle = MagicMock()

    rechtenbeheer.revoke_grants(
        spark=spark_mock,
        object_type="SCHEMA",
        object_name="catalog.schema",
        users_to_ignore=users_to_ignore,
        file_handle=file_handle,
        debug=True,
        execute=False,
    )

    spark_mock.sql.assert_called_once_with(
        "SHOW GRANTS ON SCHEMA catalog.schema"
    )

    logger_mock.info.assert_called_once_with(
        "Grants opgehaald voor SCHEMA catalog.schema"
    )

    if expected_revoke_statements:
        perform_sql_mock.assert_has_calls(
            [
                call(
                    spark=spark_mock,
                    sql_statement=statement,
                    file_handle=file_handle,
                    debug=True,
                    execute=False,
                )
                for statement in expected_revoke_statements
            ],
            any_order=False,
        )

        assert perform_sql_mock.call_count == len(expected_revoke_statements)
    else:
        perform_sql_mock.assert_not_called()

    logger_mock.error.assert_not_called()


# Test dat revoke_grants fouten logt en opnieuw doorgeeft.
def test_revoke_grants_logs_error(monkeypatch):
    spark_mock = MagicMock()
    logger_mock = patch_logger(monkeypatch)
    perform_sql_mock = patch_perform_sql(monkeypatch)

    spark_mock.sql.side_effect = Exception("Spark error")

    with pytest.raises(Exception, match="Spark error"):
        rechtenbeheer.revoke_grants(
            spark=spark_mock,
            object_type="SCHEMA",
            object_name="catalog.schema",
            users_to_ignore=[],
            debug=True,
            execute=False,
        )

    spark_mock.sql.assert_called_once_with(
        "SHOW GRANTS ON SCHEMA catalog.schema"
    )
    perform_sql_mock.assert_not_called()
    logger_mock.error.assert_called_once_with(
        "Fout bij SCHEMA catalog.schema: Spark error"
    )


# Test de eenvoudige grant-functies die één SQL-statement aan _perform_sql doorgeven.
@pytest.mark.parametrize(
    "function_name, kwargs, expected_sql",
    [
        (
            "grant_all_privileges_catalog_to_group",
            {"catalog": "test_catalog", "group": "test-group"},
            "GRANT ALL PRIVILEGES ON CATALOG test_catalog TO `test-group`",
        ),
        (
            "grant_manage_catalog_to_group",
            {"catalog": "test_catalog", "group": "test-group"},
            "GRANT MANAGE ON CATALOG test_catalog TO `test-group`",
        ),
        (
            "grant_use_catalog_to_group",
            {"catalog": "test_catalog", "group": "test-group"},
            "GRANT USE CATALOG ON CATALOG test_catalog TO `test-group`",
        ),
        (
            "grant_use_schema_on_catalog_to_group",
            {"catalog": "test_catalog", "group": "test-group"},
            "GRANT USE SCHEMA ON CATALOG test_catalog TO `test-group`",
        ),
        (
            "grant_select_catalog_to_group",
            {"catalog": "test_catalog", "group": "test-group"},
            "GRANT SELECT ON CATALOG test_catalog TO `test-group`",
        ),
        (
            "grant_select_schema_to_group",
            {"full_schema_name": "test_catalog.test_schema", "group": "test-group"},
            "GRANT SELECT ON SCHEMA test_catalog.test_schema TO `test-group`",
        ),
        (
            "grant_use_schema",
            {"full_schema_name": "test_catalog.test_schema", "group": "test-group"},
            "GRANT USE SCHEMA ON SCHEMA test_catalog.test_schema TO `test-group`",
        ),
        (
            "grant_read_volume_on_schema_to_group",
            {"full_schema_name": "test_catalog.test_schema", "group": "test-group"},
            "GRANT READ VOLUME ON SCHEMA test_catalog.test_schema TO `test-group`",
        ),
    ],
)
def test_grant_sql_statement(
    monkeypatch,
    function_name,
    kwargs,
    expected_sql,
):
    spark_mock = MagicMock()
    perform_sql_mock = patch_perform_sql(monkeypatch)
    file_handle = MagicMock()

    grant_function = getattr(rechtenbeheer, function_name)

    grant_function(
        spark=spark_mock,
        **kwargs,
        file_handle=file_handle,
        debug=True,
        execute=False,
    )

    perform_sql_mock.assert_called_once_with(
        spark=spark_mock,
        sql_statement=expected_sql,
        file_handle=file_handle,
        debug=True,
        execute=False,
    )


# Test dat BROWSE alleen wordt toegekend als de catalognaam "prd" bevat.
@pytest.mark.parametrize(
    "catalog, expected_sql",
    [
        (
            "test_prd_catalog",
            "GRANT BROWSE ON CATALOG test_prd_catalog TO `test-group`",
        ),
        (
            "test_dev_catalog",
            None,
        ),
    ],
)
def test_grant_browse_catalog(monkeypatch, catalog, expected_sql):
    spark_mock = MagicMock()
    perform_sql_mock = patch_perform_sql(monkeypatch)
    file_handle = MagicMock()

    rechtenbeheer.grant_browse_catalog_to_group_prd_only(
        spark=spark_mock,
        catalog=catalog,
        group="test-group",
        file_handle=file_handle,
        debug=True,
        execute=False,
    )

    if expected_sql:
        perform_sql_mock.assert_called_once_with(
            spark=spark_mock,
            sql_statement=expected_sql,
            file_handle=file_handle,
            debug=True,
            execute=False,
        )
    else:
        perform_sql_mock.assert_not_called()


# Test dat SELECT-rechten worden opgebouwd voor alle tabellen in een schema.
@pytest.mark.parametrize(
    "table_rows, expected_sql_statements",
    [
        (
            [
                {"tableName": "table_a"},
                {"tableName": "table_b"},
            ],
            [
                "GRANT SELECT ON TABLE test_catalog.test_schema.table_a TO `test-group`",
                "GRANT SELECT ON TABLE test_catalog.test_schema.table_b TO `test-group`",
            ],
        ),
        (
            [],
            [],
        ),
    ],
)
def test_grant_select_all_tables_in_schema(
    monkeypatch,
    table_rows,
    expected_sql_statements,
):
    spark_mock = create_spark_mock_with_collect_result(table_rows)
    perform_sql_mock = patch_perform_sql(monkeypatch)
    file_handle = MagicMock()

    rechtenbeheer.grant_select_on_all_tables_in_schema_to_group(
        spark=spark_mock,
        full_schema_name="test_catalog.test_schema",
        group="test-group",
        file_handle=file_handle,
        debug=True,
        execute=False,
    )

    spark_mock.sql.assert_called_once_with(
        "SHOW TABLES IN test_catalog.test_schema"
    )

    if expected_sql_statements:
        perform_sql_mock.assert_has_calls(
            [
                call(
                    spark=spark_mock,
                    sql_statement=sql_statement,
                    file_handle=file_handle,
                    debug=True,
                    execute=False,
                )
                for sql_statement in expected_sql_statements
            ],
            any_order=False,
        )

        assert perform_sql_mock.call_count == len(expected_sql_statements)
    else:
        perform_sql_mock.assert_not_called()
