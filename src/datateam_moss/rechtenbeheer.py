from datetime import datetime
import logging
logger = logging.getLogger(__name__)


def _perform_sql(sql_statement: str, file_handle=None, debug: bool = False, execute: bool = False) -> None:
    """
    Voert een SQL-statement uit of slaat het op in een logbestand, afhankelijk van de debug- en execute-vlaggen.

    Gedrag op basis van vlagcombinaties:
    - debug=True  + EXECUTE=False : schrijft het statement naar het logbestand, voert het NIET uit.
    - debug=True  + EXECUTE=True  : schrijft het statement naar het logbestand én voert het uit.
    - debug=False + EXECUTE=True  : voert het statement uit zonder logbestand.

    :param sql_statement: Het uit te voeren SQL-statement.
    :param file_handle: Bestandshandle voor het wegschrijven van SQL-statements (vereist bij debug=True).
    :param execute: Bepaalt of het statement wordt uitgevoerd en/of gelogd.
    :return: None
    """

    if debug and not execute:
        if file_handle is None:
            raise ValueError(
                "file_handle is vereist wanneer debug=True en execute=False")
        file_handle.write(sql_statement + ";\n\n")
        return

    elif debug and execute:
        if file_handle is not None:
            file_handle.write(sql_statement + ";\n\n")
        try:
            spark.sql(sql_statement)
            logger.info(f"Uitgevoerd: {sql_statement}")
        except Exception as e:
            logger.error(f"Fout bij uitvoeren: {sql_statement} — {e}")
            raise
        return

    else:
        # debug=False: alleen uitvoeren, geen logbestand
        try:
            spark.sql(sql_statement)
            logger.info(f"Uitgevoerd: {sql_statement}")
        except Exception as e:
            logger.error(f"Fout bij uitvoeren: {sql_statement} — {e}")
            raise


def revoke_grants(
    object_type: str,
    object_name: str,
    users_to_ignore: list,
    file_handle=None,
    debug: bool = False,
    execute: bool = False,
) -> None:
    """
    Trekt alle privileges op een object in voor alle principals, behalve de genegeerde gebruikers.

    :param object_type: Type van het object (CATALOG, SCHEMA, TABLE, etc.).
    :param object_name: Volledig gekwalificeerde objectnaam.
    :param users_to_ignore: Lijst van principals die niet gerevoked worden.
    :param file_handle: Bestandshandle voor het wegschrijven van SQL-statements.
    :param debug: Bepaalt of het statement wordt gelogd.
    :param execute: Bepaalt of het statement wordt uitgevoerd en/of gelogd.
    :return: None
    """
    try:
        grants = spark.sql(
            f"SHOW GRANTS ON {object_type} {object_name}").collect()
        logger.info(f"Grants opgehaald voor {object_type} {object_name}")

        for row in grants:
            privilege = row["ActionType"]
            principal = row["Principal"]

            if principal not in users_to_ignore:
                revoke_stmt = f"REVOKE {privilege} ON {object_type} {object_name} FROM `{principal}`"
                _perform_sql(sql_statement=revoke_stmt,
                             file_handle=file_handle, debug=debug, execute=execute)

    except Exception as e:
        logger.error(f"Fout bij {object_type} {object_name}: {e}")
        raise


def grant_all_privileges_catalog_to_group(
    catalog: str, group: str, file_handle=None, debug: bool = False, execute: bool = False
) -> None:
    """
    Kent alle privileges op een catalog toe aan een groep.

    :param catalog: Naam van de catalog.
    :param group: Naam van de groep die de rechten ontvangt.
    :param file_handle: Bestandshandle voor het wegschrijven van SQL-statements.
    :param execute: Bepaalt of het statement wordt uitgevoerd en/of gelogd.
    :return: None
    """
    sql_statement = f"GRANT ALL PRIVILEGES ON CATALOG {catalog} TO `{group}`"
    _perform_sql(sql_statement=sql_statement,
                 file_handle=file_handle, debug=debug, execute=execute)


def grant_manage_catalog_to_group(
    catalog: str, group: str, file_handle=None, debug: bool = False, execute: bool = False
) -> None:
    """
    Kent het MANAGE-privilege op een catalog toe aan een groep.

    :param catalog: Naam van de catalog.
    :param group: Naam van de groep die het recht ontvangt.
    :param file_handle: Bestandshandle voor het wegschrijven van SQL-statements.
    :param execute: Bepaalt of het statement wordt uitgevoerd en/of gelogd.
    :return: None
    """
    sql_statement = f"GRANT MANAGE ON CATALOG {catalog} TO `{group}`"
    _perform_sql(sql_statement=sql_statement,
                 file_handle=file_handle, debug=debug, execute=execute)


def grant_use_catalog_to_group(
    catalog: str, group: str, file_handle=None, debug: bool = False, execute: bool = False
) -> None:
    """
    Kent het USE CATALOG-privilege op een catalog toe aan een groep.

    :param catalog: Naam van de catalog.
    :param group: Naam van de groep die het recht ontvangt.
    :param file_handle: Bestandshandle voor het wegschrijven van SQL-statements.
    :param execute: Bepaalt of het statement wordt uitgevoerd en/of gelogd.
    :return: None
    """
    sql_statement = f"GRANT USE CATALOG ON CATALOG {catalog} TO `{group}`"
    _perform_sql(sql_statement=sql_statement,
                 file_handle=file_handle, debug=debug, execute=execute)


def grant_use_schema_on_catalog_to_group(
    catalog: str, group: str, file_handle=None, debug: bool = False, execute: bool = False
) -> None:
    """
    Kent het USE SCHEMA-privilege op alle schema's binnen een catalog toe aan een groep.

    :param catalog: Naam van de catalog.
    :param group: Naam van de groep die het recht ontvangt.
    :param file_handle: Bestandshandle voor het wegschrijven van SQL-statements.
    :param execute: Bepaalt of het statement wordt uitgevoerd en/of gelogd.
    :return: None
    """
    sql_statement = f"GRANT USE SCHEMA ON CATALOG {catalog} TO `{group}`"
    _perform_sql(sql_statement=sql_statement,
                 file_handle=file_handle, debug=debug, execute=execute)


def grant_select_catalog_to_group(
    catalog: str, group: str, file_handle=None, debug: bool = False, execute: bool = False
) -> None:
    """
    Kent het SELECT-privilege op een catalog toe aan een groep.

    :param catalog: Naam van de catalog.
    :param group: Naam van de groep die het recht ontvangt.
    :param file_handle: Bestandshandle voor het wegschrijven van SQL-statements.
    :param execute: Bepaalt of het statement wordt uitgevoerd en/of gelogd.
    :return: None
    """
    sql_statement = f"GRANT SELECT ON CATALOG {catalog} TO `{group}`"
    _perform_sql(sql_statement=sql_statement,
                 file_handle=file_handle, debug=debug, execute=execute)


def grant_browse_catalog_to_group_prd_only(
    catalog: str, group: str, file_handle=None, debug: bool = False, execute: bool = False
) -> None:
    """
    Kent het BROWSE-privilege op een catalog toe aan een groep, uitsluitend voor PRD-catalogs.

    :param catalog: Naam van de catalog.
    :param group: Naam van de groep die het recht ontvangt.
    :param file_handle: Bestandshandle voor het wegschrijven van SQL-statements.
    :param execute: Bepaalt of het statement wordt uitgevoerd en/of gelogd.
    :return: None
    """
    if "prd" in catalog:
        sql_statement = f"GRANT BROWSE ON CATALOG {catalog} TO `{group}`"
        _perform_sql(sql_statement=sql_statement,
                     file_handle=file_handle, debug=debug, execute=execute)


def grant_select_schema_to_group(
    full_schema_name: str, group: str, file_handle=None, debug: bool = False, execute: bool = False
) -> None:
    """
    Kent het SELECT-privilege op een schema toe aan een groep.

    :param full_schema_name: Volledig gekwalificeerde schemanaam (catalog.schema).
    :param group: Naam van de groep die het recht ontvangt.
    :param file_handle: Bestandshandle voor het wegschrijven van SQL-statements.
    :param execute: Bepaalt of het statement wordt uitgevoerd en/of gelogd.
    :return: None
    """
    sql_statement = f"GRANT SELECT ON SCHEMA {full_schema_name} TO `{group}`"
    _perform_sql(sql_statement=sql_statement,
                 file_handle=file_handle, debug=debug, execute=execute)


def grant_select_on_all_tables_in_schema_to_group(full_schema_name: str, group: str, file_handle=None, debug: bool = False, execute: bool = False) -> None:
    """
    Kent het SELECT-privilege op alle tabellen binnen een schema toe aan een groep.

    :param full_schema_name: Volledig gekwalificeerde schemanaam (catalog.schema).
    :param group: Naam van de groep die de rechten ontvangt.
    :param file_handle: Bestandshandle voor het wegschrijven van SQL-statements.
    :param execute: Bepaalt of het statement wordt uitgevoerd en/of gelogd.
    :return: None
    """
    tables = [
        row["tableName"]
        for row in spark.sql(f"SHOW TABLES IN {full_schema_name}").collect()
    ]

    for table in tables:
        full_table_name = f"{full_schema_name}.{table}"
        sql_statement = f"GRANT SELECT ON TABLE {full_table_name} TO `{group}`"
        _perform_sql(sql_statement=sql_statement,
                     file_handle=file_handle, debug=debug, execute=execute)


def grant_use_schema(
    full_schema_name: str, group: str, file_handle=None, debug: bool = False, execute: bool = False
) -> None:
    """
    Kent het USE SCHEMA-privilege op een specifiek schema toe aan een groep.

    :param full_schema_name: Volledig gekwalificeerde schemanaam (catalog.schema).
    :param group: Naam van de groep die de rechten ontvangt.
    :param file_handle: Bestandshandle voor het wegschrijven van SQL-statements.
    :param execute: Bepaalt of het statement wordt uitgevoerd en/of gelogd.
    :return: None
    """
    sql_statement = f"GRANT USE SCHEMA ON SCHEMA {full_schema_name} TO `{group}`"
    _perform_sql(sql_statement=sql_statement,
                 file_handle=file_handle, debug=debug, execute=execute)


def grant_read_volume_on_schema_to_group(
    full_schema_name: str, group: str, file_handle=None, debug: bool = False, execute: bool = False
) -> None:
    """
    Kent het READ VOLUME-privilege op een schema toe aan een groep.

    Met READ VOLUME op schema-niveau kan de groep bestanden lezen in alle
    Databricks Volumes die zich binnen het opgegeven schema bevinden.

    :param full_schema_name: Volledig gekwalificeerde schemanaam (catalog.schema).
    :param group: Naam van de groep die het recht ontvangt.
    :param file_handle: Bestandshandle voor het wegschrijven van SQL-statements.
    :param execute: Bepaalt of het statement wordt uitgevoerd en/of gelogd.
    :return: None
    """
    sql_statement = f"GRANT READ VOLUME ON SCHEMA {full_schema_name} TO `{group}`"
    _perform_sql(sql_statement=sql_statement,
                 file_handle=file_handle, debug=debug, execute=execute)
