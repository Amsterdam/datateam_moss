import sys
import types
import importlib
from unittest.mock import MagicMock, call
from pathlib import Path
import pytest

# Laadt de rechtenbeheer-module zonder de echte datateam_moss-importketen te gebruiken.


@pytest.fixture
def rechtenbeheer_module(monkeypatch):
    # Verwijderd eventueel eerder geladen modules
    monkeypatch.delitem(
        sys.modules, "datateam_moss.rechtenbeheer", raising=False)
    monkeypatch.delitem(sys.modules, "datateam_moss.logger", raising=False)
    monkeypatch.delitem(sys.modules, "datateam_moss.cleaning", raising=False)
    monkeypatch.delitem(sys.modules, "datateam_moss", raising=False)

    # Het pad naar src/datateam_moss
    package_path = (
        Path(__file__).resolve().parents[1]
        / "src"
        / "datateam_moss"
    )

    # Maakt een nep datateam-package.
    # Hierdoor wordt de echte datateam/__init__.py niet uitgevoerd.
    fake_datateam = types.ModuleType("datateam_moss")
    fake_datateam.__path__ = [str(package_path)]

    monkeypatch.setitem(sys.modules, "datateam_moss", fake_datateam)

    # Maakt een nep datateam.logger-module.
    # Hierdoor wordt de echte logger.py niet uitgevoerd en start Spark niet.
    logger_mock = MagicMock()

    fake_logger_module = types.ModuleType("datateam_moss.logger")
    fake_logger_module.get_logger = MagicMock(return_value=logger_mock)

    monkeypatch.setitem(
        sys.modules, "datateam_moss.logger", fake_logger_module)

    # Door fake_datateam.__path__ kan Python wel src/datateam_moss/rechtenbeheer.py vinden.
    rechtenbeheer = importlib.import_module("datateam_moss.rechtenbeheer")
    return rechtenbeheer, logger_mock

# Zet mock-objecten voor Spark en logger op de rechtenbeheer-module.


@pytest.fixture
def rechtenbeheer_with_mocks(rechtenbeheer_module, monkeypatch):
    rechtenbeheer, logger_mock = rechtenbeheer_module

    spark_mock = MagicMock()

    monkeypatch.setattr(rechtenbeheer, "spark", spark_mock, raising=False)
    monkeypatch.setattr(rechtenbeheer, "logger", logger_mock, raising=False)

    return rechtenbeheer, spark_mock, logger_mock


# Test dat _perform_sql bij debug=True en execute=False alleen naar het bestand schrijft. De SQL mag in dit scenario niet worden uitgevoerd.
def test_perform_sql_writes_file(
    rechtenbeheer_with_mocks,
):
    rechtenbeheer, spark_mock, logger_mock = rechtenbeheer_with_mocks

    sql_statement = "GRANT SELECT ON SCHEMA test.schema TO `group`"
    file_handle = MagicMock()

    rechtenbeheer._perform_sql(
        sql_statement=sql_statement,
        file_handle=file_handle,
        debug=True,
        execute=False,
    )

    file_handle.write.assert_called_once_with(
        "GRANT SELECT ON SCHEMA test.schema TO `group`;\n"
        "\n"
    )

    spark_mock.sql.assert_not_called()
    logger_mock.info.assert_not_called()


# Test dat _perform_sql een ValueError geeft als debug=True en execute=False, maar er geen file_handle is opgegeven.
def test_perform_sql_needs_file_handle(
    rechtenbeheer_with_mocks,
):
    rechtenbeheer, spark_mock, logger_mock = rechtenbeheer_with_mocks

    with pytest.raises(ValueError, match="file_handle is vereist"):
        rechtenbeheer._perform_sql(
            sql_statement="SELECT 1",
            file_handle=None,
            debug=True,
            execute=False,
        )

    spark_mock.sql.assert_not_called()
    logger_mock.info.assert_not_called()
    logger_mock.error.assert_not_called()


# Test dat _perform_sql bij debug=True en execute=True naar bestand schrijft en de SQL uitvoert via spark.sql.
def test_perform_sql_writes_and_runs(
    rechtenbeheer_with_mocks,
):
    rechtenbeheer, spark_mock, logger_mock = rechtenbeheer_with_mocks

    sql_statement = "SELECT 1"
    file_handle = MagicMock()

    rechtenbeheer._perform_sql(
        sql_statement=sql_statement,
        file_handle=file_handle,
        debug=True,
        execute=True,
    )

    file_handle.write.assert_called_once_with(
        "SELECT 1;\n"
        "\n"
    )
    spark_mock.sql.assert_called_once_with("SELECT 1")
    logger_mock.info.assert_called_once_with("Uitgevoerd: SELECT 1")
    logger_mock.error.assert_not_called()


# Test dat _perform_sql bij debug=True en execute=True zonder file_handle alleen de SQL uitvoert en niets naar een bestand schrijft.
def test_perform_sql_runs_without_file(
    rechtenbeheer_with_mocks,
):
    rechtenbeheer, spark_mock, logger_mock = rechtenbeheer_with_mocks

    sql_statement = "SELECT 1"

    rechtenbeheer._perform_sql(
        sql_statement=sql_statement,
        file_handle=None,
        debug=True,
        execute=True,
    )

    spark_mock.sql.assert_called_once_with("SELECT 1")
    logger_mock.info.assert_called_once_with("Uitgevoerd: SELECT 1")
    logger_mock.error.assert_not_called()


# Test dat _perform_sql bij debug=False en execute=True alleen de SQL uitvoert. Er wordt niets naar het bestand geschreven.
def test_perform_sql_runs_only(
    rechtenbeheer_with_mocks,
):
    rechtenbeheer, spark_mock, logger_mock = rechtenbeheer_with_mocks

    sql_statement = "SELECT 1"
    file_handle = MagicMock()

    rechtenbeheer._perform_sql(
        sql_statement=sql_statement,
        file_handle=file_handle,
        debug=False,
        execute=True,
    )

    file_handle.write.assert_not_called()
    spark_mock.sql.assert_called_once_with("SELECT 1")
    logger_mock.info.assert_called_once_with("Uitgevoerd: SELECT 1")
    logger_mock.error.assert_not_called()


# Test het huidige gedrag bij debug=False en execute=False. Door de else voert de functie de SQL nu toch uit.
def test_perform_sql_runs_by_default(
    rechtenbeheer_with_mocks,
):
    rechtenbeheer, spark_mock, logger_mock = rechtenbeheer_with_mocks

    sql_statement = "SELECT 1"
    file_handle = MagicMock()

    rechtenbeheer._perform_sql(
        sql_statement=sql_statement,
        file_handle=file_handle,
        debug=False,
        execute=False,
    )

    file_handle.write.assert_not_called()
    spark_mock.sql.assert_called_once_with("SELECT 1")
    logger_mock.info.assert_called_once_with("Uitgevoerd: SELECT 1")
    logger_mock.error.assert_not_called()

# Test revoke_grants voor meerdere normale scenario's.
# De test controleert of de juiste REVOKE-statements worden gemaakt
# en of users_to_ignore worden overgeslagen.


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
    rechtenbeheer_with_mocks,
    monkeypatch,
    grants_rows,
    users_to_ignore,
    expected_revoke_statements,
):
    rechtenbeheer, spark_mock, logger_mock = rechtenbeheer_with_mocks

    file_handle = MagicMock()
    perform_sql_mock = MagicMock()

    grants_result = MagicMock()
    grants_result.collect.return_value = grants_rows

    spark_mock.sql.return_value = grants_result

    monkeypatch.setattr(
        rechtenbeheer,
        "_perform_sql",
        perform_sql_mock,
        raising=False,
    )

    rechtenbeheer.revoke_grants(
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

# Test dat alle privileges op een catalog aan een groep worden toegekend.


def test_grant_all_catalog_privileges(
    rechtenbeheer_with_mocks,
    monkeypatch,
):
    rechtenbeheer, spark_mock, logger_mock = rechtenbeheer_with_mocks

    file_handle = MagicMock()
    perform_sql_mock = MagicMock()

    monkeypatch.setattr(
        rechtenbeheer,
        "_perform_sql",
        perform_sql_mock,
        raising=False,
    )

    rechtenbeheer.grant_all_privileges_catalog_to_group(
        catalog="test_catalog",
        group="test-group",
        file_handle=file_handle,
        debug=True,
        execute=False,
    )

    perform_sql_mock.assert_called_once_with(
        sql_statement="GRANT ALL PRIVILEGES ON CATALOG test_catalog TO `test-group`",
        file_handle=file_handle,
        debug=True,
        execute=False,
    )

    spark_mock.sql.assert_not_called()
    logger_mock.error.assert_not_called()

# Test dat het MANAGE-privilege op een catalog aan een groep wordt toegekend.


def test_grant_manage_catalog(
    rechtenbeheer_with_mocks,
    monkeypatch,
):
    rechtenbeheer, spark_mock, logger_mock = rechtenbeheer_with_mocks

    file_handle = MagicMock()
    perform_sql_mock = MagicMock()

    # Vervangt _perform_sql tijdelijk door een mock, zo wordt geen echte SQL uitgevoerd of weggeschreven.
    monkeypatch.setattr(
        rechtenbeheer,
        "_perform_sql",
        perform_sql_mock,
        raising=False,
    )

    # Roept de functie aan met testwaarden.
    rechtenbeheer.grant_manage_catalog_to_group(
        catalog="test_catalog",
        group="test-group",
        file_handle=file_handle,
        debug=True,
        execute=False,
    )

    # Controleert dat _perform_sql precies één keer is aangeroepen met het verwachte MANAGE-statement.
    perform_sql_mock.assert_called_once_with(
        sql_statement="GRANT MANAGE ON CATALOG test_catalog TO `test-group`",
        file_handle=file_handle,
        debug=True,
        execute=False,
    )

    # Deze grant-functie mag niet zelf spark.sql aanroepen.
    spark_mock.sql.assert_not_called()

    # Er hoort geen fout gelogd te worden.
    logger_mock.error.assert_not_called()

# Test dat het USE CATALOG-privilege op een catalog aan een groep wordt toegekend.


def test_grant_use_catalog(
    rechtenbeheer_with_mocks,
    monkeypatch,
):
    rechtenbeheer, spark_mock, logger_mock = rechtenbeheer_with_mocks

    file_handle = MagicMock()
    perform_sql_mock = MagicMock()

    monkeypatch.setattr(
        rechtenbeheer,
        "_perform_sql",
        perform_sql_mock,
        raising=False,
    )

    rechtenbeheer.grant_use_catalog_to_group(
        catalog="test_catalog",
        group="test-group",
        file_handle=file_handle,
        debug=True,
        execute=False,
    )

    perform_sql_mock.assert_called_once_with(
        sql_statement="GRANT USE CATALOG ON CATALOG test_catalog TO `test-group`",
        file_handle=file_handle,
        debug=True,
        execute=False,
    )

    spark_mock.sql.assert_not_called()
    logger_mock.error.assert_not_called()

# Test dat het USE SCHEMA-privilege op alle schema's binnen een catalog aan een groep wordt toegekend.


def test_grant_use_schema_on_catalog(
    rechtenbeheer_with_mocks,
    monkeypatch,
):
    rechtenbeheer, spark_mock, logger_mock = rechtenbeheer_with_mocks

    file_handle = MagicMock()
    perform_sql_mock = MagicMock()

    monkeypatch.setattr(
        rechtenbeheer,
        "_perform_sql",
        perform_sql_mock,
        raising=False,
    )

    rechtenbeheer.grant_use_schema_on_catalog_to_group(
        catalog="test_catalog",
        group="test-group",
        file_handle=file_handle,
        debug=True,
        execute=False,
    )

    perform_sql_mock.assert_called_once_with(
        sql_statement="GRANT USE SCHEMA ON CATALOG test_catalog TO `test-group`",
        file_handle=file_handle,
        debug=True,
        execute=False,
    )

    spark_mock.sql.assert_not_called()
    logger_mock.error.assert_not_called()

# Test dat het SELECT-privilege op een catalog aan een groep wordt toegekend.


def test_grant_select_catalog(
    rechtenbeheer_with_mocks,
    monkeypatch,
):
    rechtenbeheer, spark_mock, logger_mock = rechtenbeheer_with_mocks

    file_handle = MagicMock()
    perform_sql_mock = MagicMock()

    monkeypatch.setattr(
        rechtenbeheer,
        "_perform_sql",
        perform_sql_mock,
        raising=False,
    )

    rechtenbeheer.grant_select_catalog_to_group(
        catalog="test_catalog",
        group="test-group",
        file_handle=file_handle,
        debug=True,
        execute=False,
    )

    perform_sql_mock.assert_called_once_with(
        sql_statement="GRANT SELECT ON CATALOG test_catalog TO `test-group`",
        file_handle=file_handle,
        debug=True,
        execute=False,
    )

    spark_mock.sql.assert_not_called()
    logger_mock.error.assert_not_called()

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
def test_grant_browse_catalog(
    rechtenbeheer_with_mocks,
    monkeypatch,
    catalog,
    expected_sql,
):
    rechtenbeheer, spark_mock, logger_mock = rechtenbeheer_with_mocks

    file_handle = MagicMock()
    perform_sql_mock = MagicMock()

    monkeypatch.setattr(
        rechtenbeheer,
        "_perform_sql",
        perform_sql_mock,
        raising=False,
    )

    rechtenbeheer.grant_browse_catalog_to_group_prd_only(
        catalog=catalog,
        group="test-group",
        file_handle=file_handle,
        debug=True,
        execute=False,
    )

    # Als de catalog "prd" bevat, verwachten we een BROWSE-statement. Anders mag _perform_sql niet worden aangeroepen.
    if expected_sql:
        perform_sql_mock.assert_called_once_with(
            sql_statement=expected_sql,
            file_handle=file_handle,
            debug=True,
            execute=False,
        )
    else:
        perform_sql_mock.assert_not_called()

    spark_mock.sql.assert_not_called()
    logger_mock.error.assert_not_called()

# Controleert de SQL statement voor SELECT-rechten op een schema.


def test_grant_select_schema(
    rechtenbeheer_with_mocks,
    monkeypatch,
):
    rechtenbeheer, spark_mock, logger_mock = rechtenbeheer_with_mocks

    file_handle = MagicMock()
    perform_sql_mock = MagicMock()

    monkeypatch.setattr(
        rechtenbeheer,
        "_perform_sql",
        perform_sql_mock,
        raising=False,
    )

    rechtenbeheer.grant_select_schema_to_group(
        full_schema_name="test_catalog.test_schema",
        group="test-group",
        file_handle=file_handle,
        debug=True,
        execute=False,
    )

    perform_sql_mock.assert_called_once_with(
        sql_statement="GRANT SELECT ON SCHEMA test_catalog.test_schema TO `test-group`",
        file_handle=file_handle,
        debug=True,
        execute=False,
    )

    spark_mock.sql.assert_not_called()
    logger_mock.error.assert_not_called()

# Controleert dat SELECT-rechten worden opgebouwd voor alle tabellen in een schema.


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
    rechtenbeheer_with_mocks,
    monkeypatch,
    table_rows,
    expected_sql_statements,
):
    rechtenbeheer, spark_mock, logger_mock = rechtenbeheer_with_mocks

    file_handle = MagicMock()
    perform_sql_mock = MagicMock()

    tables_result = MagicMock()
    tables_result.collect.return_value = table_rows

    spark_mock.sql.return_value = tables_result

    monkeypatch.setattr(
        rechtenbeheer,
        "_perform_sql",
        perform_sql_mock,
        raising=False,
    )

    rechtenbeheer.grant_select_on_all_tables_in_schema_to_group(
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

    logger_mock.error.assert_not_called()

# Controleert de SQL-opbouw voor USE SCHEMA-rechten op een specifiek schema.


def test_grant_use_schema(
    rechtenbeheer_with_mocks,
    monkeypatch,
):
    rechtenbeheer, spark_mock, logger_mock = rechtenbeheer_with_mocks

    file_handle = MagicMock()
    perform_sql_mock = MagicMock()

    # Vang de interne SQL-afhandeling af.
    monkeypatch.setattr(
        rechtenbeheer,
        "_perform_sql",
        perform_sql_mock,
        raising=False,
    )

    rechtenbeheer.grant_use_schema(
        full_schema_name="test_catalog.test_schema",
        group="test-group",
        file_handle=file_handle,
        debug=True,
        execute=False,
    )

    perform_sql_mock.assert_called_once_with(
        sql_statement="GRANT USE SCHEMA ON SCHEMA test_catalog.test_schema TO `test-group`",
        file_handle=file_handle,
        debug=True,
        execute=False,
    )

    spark_mock.sql.assert_not_called()
    logger_mock.error.assert_not_called()

# Controleert de SQL-opbouw voor READ VOLUME-rechten op een schema.


def test_grant_read_volume_schema(
    rechtenbeheer_with_mocks,
    monkeypatch,
):
    rechtenbeheer, spark_mock, logger_mock = rechtenbeheer_with_mocks

    file_handle = MagicMock()
    perform_sql_mock = MagicMock()

    monkeypatch.setattr(
        rechtenbeheer,
        "_perform_sql",
        perform_sql_mock,
        raising=False,
    )

    rechtenbeheer.grant_read_volume_on_schema_to_group(
        full_schema_name="test_catalog.test_schema",
        group="test-group",
        file_handle=file_handle,
        debug=True,
        execute=False,
    )

    perform_sql_mock.assert_called_once_with(
        sql_statement="GRANT READ VOLUME ON SCHEMA test_catalog.test_schema TO `test-group`",
        file_handle=file_handle,
        debug=True,
        execute=False,
    )

    spark_mock.sql.assert_not_called()
    logger_mock.error.assert_not_called()
