from datateam_moss.logger import get_logger
from pyspark.sql import SparkSession

logger = get_logger("__name__")

def create_external_location(
    spark: SparkSession,
    external_location_name: str,
    storage_account: str,
    container: str,
    credential: str,
    comment: str | None = None
) -> None:
    """
    Maakt een Databricks Unity Catalog external location aan indien deze niet bestaat.

    Args:
        spark (Sparksession):
            De sparksessie
        external_location_name (str):
            Naam van de nieuwe external location.
        storage_account (str):
            Naam van de Azure storage account (zonder domain suffix).
        container (str):
            Naam van de container.
        credential (str):
            Naam van de Unity Catalog storage credential.
        comment (str, optional):
            Optioneel een comment die aangeeft waar de external volume voor nodig is.
    """

    logger.info(
        "Creating external location '%s' (storage_account=%s, container=%s)",
        external_location_name,
        storage_account,
        container
    )

    sql = f"""
    CREATE EXTERNAL LOCATION IF NOT EXISTS {external_location_name}
    URL 'abfss://{container}@{storage_account}.dfs.core.windows.net/'
    WITH (STORAGE CREDENTIAL {credential})
    """

    if comment:
        sql += f"\nCOMMENT '{comment}'"

    logger.info("Executing SQL:\n%s", sql)

    try:
        spark.sql(sql)
        logger.info("External location '%s' created or already exists.", external_location_name)

    except Exception as exc:
        logger.error(
            "Failed to create external location '%s'.",
            external_location_name,
            exc_info=True
        )
        raise

def create_volume(
    spark: SparkSession,
    catalog: str,
    schema: str,
    volume_name: str,
    storage_account: str,
    container: str,
    external: bool = False
) -> None:
    """
    Maakt een Databricks Unity Catalog volume aan indien deze niet bestaat. De external parameter bepaalt
    of het een managed of een external volume wordt. 

    Args:
        spark (Sparksession):
            De sparksessie
        catalog ( str):
            Naam van het catalog.
        schema (str):
            Naam van het schema.
        volume_name (str):
            Naam van de nieuwe external location.
        storage_account (str):
            Naam van de Azure storage account (zonder domain suffix).
        container (str):
            Naam van de container.
        external (bool, optional):
            Indien True wordt de volume als externe volume aangemaakt.
    """
    logger.info(
        "Creating (external) volume '%s' (storage_account=%s, container=%s)",
        volume_name,
        storage_account,
        container
    )

    if external:
        sql = f"""
        CREATE EXTERNAL VOLUME IF NOT EXISTS {catalog}.{schema}.{volume_name}
        LOCATION 'abfss://{container}@{storage_account}.dfs.core.windows.net/'
        """

    else:
        sql = f"""
        CREATE VOLUME IF NOT EXISTS {catalog}.{schema}.{volume_name}
        """

    logger.info("Executing SQL:\n%s", sql)

    try:
        spark.sql(sql)
        logger.info("(External) volume '%s' created or already exists.", volume_name)

    except Exception as exc:
        logger.error(
            "Failed to create (external) volume '%s'.",
            volume_name,
            exc_info=True
        )
        raise
